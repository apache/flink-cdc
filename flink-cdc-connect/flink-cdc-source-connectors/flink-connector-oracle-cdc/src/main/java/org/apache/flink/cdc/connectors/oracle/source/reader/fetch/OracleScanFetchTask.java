/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oracle.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.LogMinerOracleOffsetContextLoader;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

import static org.apache.flink.cdc.connectors.oracle.source.reader.fetch.OracleStreamFetchTask.RedoLogSplitReadTask;
import static org.apache.flink.cdc.connectors.oracle.source.utils.OracleUtils.buildSplitScanQuery;
import static org.apache.flink.cdc.connectors.oracle.source.utils.OracleUtils.readTableSplitDataStatement;

/** The task to work for fetching data of Oracle table snapshot split. */
public class OracleScanFetchTask extends AbstractScanFetchTask {

    public OracleScanFetchTask(SnapshotSplit split) {
        super(split);
    }

    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;
        OracleSnapshotSplitReadTask snapshotSplitReadTask =
                new OracleSnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getEventDispatcher(),
                        snapshotSplit);
        StoppableChangeEventSourceContext changeEventSourceContext =
                new StoppableChangeEventSourceContext();
        SnapshotResult<OracleOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext,
                        sourceFetchContext.getPartition(),
                        sourceFetchContext.getOffsetContext());
        if (!snapshotResult.isCompletedOrSkipped()) {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for oracle split %s fail", snapshotSplit));
        }
    }

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;

        final RedoLogSplitReadTask backfillRedoLogReadTask =
                createBackfillRedoLogReadTask(backfillStreamSplit, sourceFetchContext);

        final LogMinerOracleOffsetContextLoader loader =
                new LogMinerOracleOffsetContextLoader(
                        ((OracleSourceFetchTaskContext) context).getDbzConnectorConfig());
        final OracleOffsetContext oracleOffsetContext =
                loader.load(backfillStreamSplit.getStartingOffset().getOffset());
        backfillRedoLogReadTask.execute(
                new StoppableChangeEventSourceContext(),
                sourceFetchContext.getPartition(),
                oracleOffsetContext);
    }

    private RedoLogSplitReadTask createBackfillRedoLogReadTask(
            StreamSplit backfillRedoLogSplit, OracleSourceFetchTaskContext context) {
        // we should only capture events for the current table,
        // otherwise, we may can't find corresponding schema
        Configuration dezConf =
                context.getSourceConfig()
                        .getDbzConfiguration()
                        .edit()
                        // It will cause data loss before.
                        // Because the table name contains the database
                        // prefix when logminer queries the redo log, but in fact the
                        // logminer content does not contain the database prefix,
                        // this will cause the back fill tofail,
                        // thereby affecting data consistency.
                        .with(
                                "table.include.list",
                                String.format(
                                        "%s.%s",
                                        snapshotSplit.getTableId().schema(),
                                        snapshotSplit.getTableId().table()))
                        // Disable heartbeat event in snapshot split fetcher
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build();
        // task to read redo log and backfill for current split
        return new RedoLogSplitReadTask(
                new OracleConnectorConfig(dezConf),
                context.getConnection(),
                context.getEventDispatcher(),
                context.getWaterMarkDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                context.getSourceConfig().getOriginDbzConnectorConfig(),
                context.getStreamingChangeEventSourceMetrics(),
                backfillRedoLogSplit);
    }

    /** A wrapped task to fetch snapshot split of table. */
    public static class OracleSnapshotSplitReadTask
            extends AbstractSnapshotChangeEventSource<OraclePartition, OracleOffsetContext> {

        private static final Logger LOG =
                LoggerFactory.getLogger(OracleSnapshotSplitReadTask.class);

        /** Interval for showing a log statement with the progress while scanning a single table. */
        private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

        private final OracleConnectorConfig connectorConfig;
        private final OracleDatabaseSchema databaseSchema;
        private final OracleConnection jdbcConnection;
        private final EventDispatcher<OraclePartition, TableId> eventDispatcher;
        private final Clock clock;
        private final SnapshotSplit snapshotSplit;
        private final OracleOffsetContext offsetContext;
        private final SnapshotProgressListener<OraclePartition> snapshotProgressListener;

        public OracleSnapshotSplitReadTask(
                OracleConnectorConfig connectorConfig,
                OracleOffsetContext previousOffset,
                SnapshotProgressListener<OraclePartition> snapshotProgressListener,
                OracleDatabaseSchema databaseSchema,
                OracleConnection jdbcConnection,
                EventDispatcher<OraclePartition, TableId> eventDispatcher,
                SnapshotSplit snapshotSplit) {
            super(connectorConfig, snapshotProgressListener);
            this.offsetContext = previousOffset;
            this.connectorConfig = connectorConfig;
            this.databaseSchema = databaseSchema;
            this.jdbcConnection = jdbcConnection;
            this.eventDispatcher = eventDispatcher;
            this.clock = Clock.SYSTEM;
            this.snapshotSplit = snapshotSplit;
            this.snapshotProgressListener = snapshotProgressListener;
        }

        @Override
        public SnapshotResult<OracleOffsetContext> execute(
                ChangeEventSourceContext context,
                OraclePartition partition,
                OracleOffsetContext previousOffset)
                throws InterruptedException {
            SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
            final SnapshotContext<OraclePartition, OracleOffsetContext> ctx;
            try {
                ctx = prepare(partition);
            } catch (Exception e) {
                LOG.error("Failed to initialize snapshot context.", e);
                throw new RuntimeException(e);
            }
            try {
                return doExecute(context, previousOffset, ctx, snapshottingTask);
            } catch (InterruptedException e) {
                LOG.warn("Snapshot was interrupted before completion");
                throw e;
            } catch (Exception t) {
                throw new DebeziumException(t);
            }
        }

        @Override
        protected SnapshotResult<OracleOffsetContext> doExecute(
                ChangeEventSourceContext context,
                OracleOffsetContext previousOffset,
                SnapshotContext snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final OracleSnapshotContext ctx = (OracleSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;
            createDataEvents(ctx, snapshotSplit.getTableId());
            return SnapshotResult.completed(ctx.offset);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(
                OraclePartition partition, OracleOffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected SnapshotContext<OraclePartition, OracleOffsetContext> prepare(
                OraclePartition partition) throws Exception {
            return new OracleSnapshotContext(partition);
        }

        private static class OracleSnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                        OraclePartition, OracleOffsetContext> {

            public OracleSnapshotContext(OraclePartition partition) throws SQLException {
                super(partition, "");
            }
        }

        private void createDataEvents(OracleSnapshotContext snapshotContext, TableId tableId)
                throws Exception {
            EventDispatcher.SnapshotReceiver<OraclePartition> snapshotReceiver =
                    eventDispatcher.getSnapshotChangeEventReceiver();
            LOG.debug("Snapshotting table {}", tableId);
            createDataEventsForTable(
                    snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
            snapshotReceiver.completeSnapshot();
        }

        /** Dispatches the data change events for the records of a single table. */
        private void createDataEventsForTable(
                OracleSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver<OraclePartition> snapshotReceiver,
                Table table)
                throws InterruptedException {

            long exportStart = clock.currentTimeInMillis();
            LOG.info(
                    "Exporting data from split '{}' of table {}",
                    snapshotSplit.splitId(),
                    table.id());

            final String selectSql =
                    buildSplitScanQuery(
                            snapshotSplit.getTableId(),
                            snapshotSplit.getSplitKeyType(),
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null);
            LOG.info(
                    "For split '{}' of table {} using select statement: '{}'",
                    snapshotSplit.splitId(),
                    table.id(),
                    selectSql);

            try (PreparedStatement selectStatement =
                            readTableSplitDataStatement(
                                    jdbcConnection,
                                    selectSql,
                                    snapshotSplit.getSplitStart() == null,
                                    snapshotSplit.getSplitEnd() == null,
                                    snapshotSplit.getSplitStart(),
                                    snapshotSplit.getSplitEnd(),
                                    snapshotSplit.getSplitKeyType().getFieldCount(),
                                    connectorConfig.getQueryFetchSize());
                    ResultSet rs = selectStatement.executeQuery()) {

                ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                long rows = 0;
                Threads.Timer logTimer = getTableScanLogTimer();

                while (rs.next()) {
                    rows++;
                    final Object[] row =
                            jdbcConnection.rowToArray(table, databaseSchema, rs, columnArray);
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        LOG.info(
                                "Exported {} records for split '{}' after {}",
                                rows,
                                snapshotSplit.splitId(),
                                Strings.duration(stop - exportStart));
                        snapshotProgressListener.rowsScanned(
                                snapshotContext.partition, table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }
                    eventDispatcher.dispatchSnapshotEvent(
                            snapshotContext.partition,
                            table.id(),
                            getChangeRecordEmitter(snapshotContext, table.id(), row),
                            snapshotReceiver);
                }
                LOG.info(
                        "Finished exporting {} records for split '{}', total duration '{}'",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
            } catch (SQLException e) {
                throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
            }
        }

        protected ChangeRecordEmitter<OraclePartition> getChangeRecordEmitter(
                SnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                TableId tableId,
                Object[] row) {
            snapshotContext.offset.event(tableId, clock.currentTime());
            return new SnapshotChangeRecordEmitter<>(
                    snapshotContext.partition, snapshotContext.offset, row, clock);
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }
    }
}
