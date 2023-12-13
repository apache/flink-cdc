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

package org.apache.flink.cdc.connectors.db2.source.fetch;

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.db2.source.fetch.Db2StreamFetchTask.StreamSplitReadTask;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.connector.db2.Db2DatabaseSchema;
import io.debezium.connector.db2.Db2OffsetContext;
import io.debezium.connector.db2.Db2Partition;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
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

import static org.apache.flink.cdc.connectors.db2.source.utils.Db2Utils.buildSplitScanQuery;
import static org.apache.flink.cdc.connectors.db2.source.utils.Db2Utils.readTableSplitDataStatement;

/** The task to work for fetching data of Db2 table snapshot split. */
public class Db2ScanFetchTask extends AbstractScanFetchTask {

    public Db2ScanFetchTask(SnapshotSplit split) {
        super(split);
    }

    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        Db2SourceFetchTaskContext sourceFetchContext = (Db2SourceFetchTaskContext) context;
        Db2SnapshotSplitReadTask snapshotSplitReadTask =
                new Db2SnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getSnapshotReceiver(),
                        snapshotSplit);
        Db2SnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new Db2SnapshotSplitChangeEventSourceContext();
        SnapshotResult<Db2OffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext,
                        sourceFetchContext.getPartition(),
                        sourceFetchContext.getOffsetContext());
        // execute stream read task
        if (!snapshotResult.isCompletedOrSkipped()) {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for Db2 split %s fail", snapshotSplit));
        }
    }

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        Db2SourceFetchTaskContext sourceFetchContext = (Db2SourceFetchTaskContext) context;
        final Db2OffsetContext.Loader loader =
                new Db2OffsetContext.Loader(sourceFetchContext.getDbzConnectorConfig());
        final Db2OffsetContext streamOffsetContext =
                loader.load(backfillStreamSplit.getStartingOffset().getOffset());

        final StreamSplitReadTask backfillBinlogReadTask =
                createBackFillLsnSplitReadTask(backfillStreamSplit, sourceFetchContext);
        backfillBinlogReadTask.execute(
                new Db2SnapshotSplitChangeEventSourceContext(),
                sourceFetchContext.getPartition(),
                streamOffsetContext);
    }

    private StreamSplitReadTask createBackFillLsnSplitReadTask(
            StreamSplit backfillBinlogSplit, Db2SourceFetchTaskContext context) {
        // we should only capture events for the current table,
        // otherwise, we may can't find corresponding schema
        Configuration dezConf =
                context.getDbzConnectorConfig()
                        .getConfig()
                        .edit()
                        // table.include.list is schema.table format
                        .with(
                                "table.include.list",
                                new TableId(
                                        null,
                                        snapshotSplit.getTableId().schema(),
                                        snapshotSplit.getTableId().table()))
                        // Disable heartbeat event in snapshot split fetcher
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build();
        // task to read wal and backfill for current split
        return new StreamSplitReadTask(
                new Db2ConnectorConfig(dezConf),
                context.getConnection(),
                context.getMetaDataConnection(),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                backfillBinlogSplit);
    }

    /** A wrapped task to fetch snapshot split of table. */
    public static class Db2SnapshotSplitReadTask
            extends AbstractSnapshotChangeEventSource<Db2Partition, Db2OffsetContext> {

        private static final Logger LOG = LoggerFactory.getLogger(Db2SnapshotSplitReadTask.class);

        /** Interval for showing a log statement with the progress while scanning a single table. */
        private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

        private final Db2ConnectorConfig connectorConfig;
        private final Db2DatabaseSchema databaseSchema;
        private final Db2Connection jdbcConnection;
        private final JdbcSourceEventDispatcher<Db2Partition> dispatcher;
        private final Clock clock;
        private final SnapshotSplit snapshotSplit;
        private final Db2OffsetContext offsetContext;
        private final SnapshotProgressListener<Db2Partition> snapshotProgressListener;
        private final EventDispatcher.SnapshotReceiver<Db2Partition> snapshotReceiver;

        public Db2SnapshotSplitReadTask(
                Db2ConnectorConfig connectorConfig,
                Db2OffsetContext previousOffset,
                SnapshotProgressListener<Db2Partition> snapshotProgressListener,
                Db2DatabaseSchema databaseSchema,
                Db2Connection jdbcConnection,
                JdbcSourceEventDispatcher<Db2Partition> dispatcher,
                EventDispatcher.SnapshotReceiver<Db2Partition> snapshotReceiver,
                SnapshotSplit snapshotSplit) {
            super(connectorConfig, snapshotProgressListener);
            this.offsetContext = previousOffset;
            this.connectorConfig = connectorConfig;
            this.databaseSchema = databaseSchema;
            this.jdbcConnection = jdbcConnection;
            this.dispatcher = dispatcher;
            this.clock = Clock.SYSTEM;
            this.snapshotSplit = snapshotSplit;
            this.snapshotProgressListener = snapshotProgressListener;
            this.snapshotReceiver = snapshotReceiver;
        }

        @Override
        public SnapshotResult<Db2OffsetContext> execute(
                ChangeEventSourceContext context,
                Db2Partition partition,
                Db2OffsetContext previousOffset)
                throws InterruptedException {
            SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
            final Db2SnapshotContext ctx;
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
        protected SnapshotResult<Db2OffsetContext> doExecute(
                ChangeEventSourceContext context,
                Db2OffsetContext previousOffset,
                SnapshotContext<Db2Partition, Db2OffsetContext> snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final Db2SnapshotContext ctx = (Db2SnapshotContext) snapshotContext;
            ctx.offset = offsetContext;

            createDataEvents(ctx, snapshotSplit.getTableId());

            return SnapshotResult.completed(ctx.offset);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(
                Db2Partition partition, Db2OffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected Db2SnapshotContext prepare(Db2Partition partition) throws Exception {
            return new Db2SnapshotContext(partition);
        }

        private void createDataEvents(Db2SnapshotContext snapshotContext, TableId tableId)
                throws Exception {
            LOG.debug("Snapshotting table {}", tableId);
            createDataEventsForTable(
                    snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
            snapshotReceiver.completeSnapshot();
        }

        /** Dispatches the data change events for the records of a single table. */
        private void createDataEventsForTable(
                Db2SnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver<Db2Partition> snapshotReceiver,
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
                    dispatcher.dispatchSnapshotEvent(
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

        protected ChangeRecordEmitter<Db2Partition> getChangeRecordEmitter(
                Db2SnapshotContext snapshotContext, TableId tableId, Object[] row) {
            snapshotContext.offset.event(tableId, clock.currentTime());
            return new SnapshotChangeRecordEmitter<>(
                    snapshotContext.partition, snapshotContext.offset, row, clock);
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }

        private static class Db2SnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                        Db2Partition, Db2OffsetContext> {

            public Db2SnapshotContext(Db2Partition partition) throws SQLException {
                super(partition, "");
            }
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded stream task
     * of a snapshot split task.
     */
    public class Db2SnapshotSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        public void finished() {
            taskRunning = false;
        }

        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
