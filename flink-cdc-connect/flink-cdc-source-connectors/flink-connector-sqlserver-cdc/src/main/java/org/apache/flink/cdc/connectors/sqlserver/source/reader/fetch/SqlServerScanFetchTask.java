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

package org.apache.flink.cdc.connectors.sqlserver.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerPartition;
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

import static org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerUtils.buildSplitScanQuery;
import static org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerUtils.readTableSplitDataStatement;

/** The task to work for fetching data of SqlServer table snapshot split. */
public class SqlServerScanFetchTask extends AbstractScanFetchTask {

    public SqlServerScanFetchTask(SnapshotSplit split) {
        super(split);
    }

    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        SqlServerSourceFetchTaskContext sourceFetchContext =
                (SqlServerSourceFetchTaskContext) context;
        taskRunning = true;
        SqlServerSnapshotSplitReadTask snapshotSplitReadTask =
                new SqlServerSnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getEventDispatcher(),
                        sourceFetchContext.getSnapshotReceiver(),
                        snapshotSplit);
        SqlServerSnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new SqlServerSnapshotSplitChangeEventSourceContext();
        SnapshotResult<SqlServerOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext,
                        sourceFetchContext.getPartition(),
                        sourceFetchContext.getOffsetContext());
        // execute stream read task
        if (!snapshotResult.isCompletedOrSkipped()) {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for SqlServer split %s fail", snapshotSplit));
        }
    }

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        SqlServerSourceFetchTaskContext sourceFetchContext =
                (SqlServerSourceFetchTaskContext) context;
        final SqlServerOffsetContext.Loader loader =
                new SqlServerOffsetContext.Loader(sourceFetchContext.getDbzConnectorConfig());
        final SqlServerOffsetContext streamOffsetContext =
                loader.load(backfillStreamSplit.getStartingOffset().getOffset());

        final SqlServerStreamFetchTask.StreamSplitReadTask backfillBinlogReadTask =
                createBackFillLsnSplitReadTask(backfillStreamSplit, sourceFetchContext);
        backfillBinlogReadTask.execute(
                new SqlServerSnapshotSplitChangeEventSourceContext(),
                sourceFetchContext.getPartition(),
                streamOffsetContext);
    }

    private SqlServerStreamFetchTask.StreamSplitReadTask createBackFillLsnSplitReadTask(
            StreamSplit backfillBinlogSplit, SqlServerSourceFetchTaskContext context) {
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
        // task to read binlog and backfill for current split
        return new SqlServerStreamFetchTask.StreamSplitReadTask(
                new SqlServerConnectorConfig(dezConf),
                context.getConnection(),
                context.getMetaDataConnection(),
                context.getEventDispatcher(),
                context.getWaterMarkDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                backfillBinlogSplit);
    }

    /** A wrapped task to fetch snapshot split of table. */
    public static class SqlServerSnapshotSplitReadTask
            extends AbstractSnapshotChangeEventSource<SqlServerPartition, SqlServerOffsetContext> {

        private static final Logger LOG =
                LoggerFactory.getLogger(SqlServerSnapshotSplitReadTask.class);

        /** Interval for showing a log statement with the progress while scanning a single table. */
        private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

        private final SqlServerConnectorConfig connectorConfig;
        private final SqlServerDatabaseSchema databaseSchema;
        private final SqlServerConnection jdbcConnection;
        private final EventDispatcher<SqlServerPartition, TableId> eventDispatcher;
        private final Clock clock;
        private final SnapshotSplit snapshotSplit;
        private final SqlServerOffsetContext offsetContext;
        private final SnapshotProgressListener<SqlServerPartition> snapshotProgressListener;
        private final EventDispatcher.SnapshotReceiver<SqlServerPartition> snapshotReceiver;

        public SqlServerSnapshotSplitReadTask(
                SqlServerConnectorConfig connectorConfig,
                SqlServerOffsetContext previousOffset,
                SnapshotProgressListener<SqlServerPartition> snapshotProgressListener,
                SqlServerDatabaseSchema databaseSchema,
                SqlServerConnection jdbcConnection,
                EventDispatcher<SqlServerPartition, TableId> eventDispatcher,
                EventDispatcher.SnapshotReceiver<SqlServerPartition> snapshotReceiver,
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
            this.snapshotReceiver = snapshotReceiver;
        }

        @Override
        public SnapshotResult<SqlServerOffsetContext> execute(
                ChangeEventSourceContext context,
                SqlServerPartition partition,
                SqlServerOffsetContext previousOffset)
                throws InterruptedException {
            SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
            final SqlServerSnapshotContext ctx;
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
        protected SnapshotResult<SqlServerOffsetContext> doExecute(
                ChangeEventSourceContext context,
                SqlServerOffsetContext previousOffset,
                SnapshotContext<SqlServerPartition, SqlServerOffsetContext> snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final SqlServerSnapshotContext ctx = (SqlServerSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;

            LOG.info("Snapshot step 2 - Snapshotting data");
            createDataEvents(ctx, snapshotSplit.getTableId());

            return SnapshotResult.completed(ctx.offset);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(
                SqlServerPartition partition, SqlServerOffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected SqlServerSnapshotContext prepare(SqlServerPartition partition) throws Exception {
            return new SqlServerSnapshotContext(partition);
        }

        private void createDataEvents(SqlServerSnapshotContext snapshotContext, TableId tableId)
                throws Exception {
            LOG.debug("Snapshotting table {}", tableId);
            createDataEventsForTable(
                    snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
            snapshotReceiver.completeSnapshot();
        }

        /** Dispatches the data change events for the records of a single table. */
        private void createDataEventsForTable(
                SqlServerSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver<SqlServerPartition> snapshotReceiver,
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

        protected ChangeRecordEmitter<SqlServerPartition> getChangeRecordEmitter(
                SqlServerSnapshotContext snapshotContext, TableId tableId, Object[] row) {
            snapshotContext.offset.event(tableId, clock.currentTime());
            return new SnapshotChangeRecordEmitter<>(
                    snapshotContext.partition, snapshotContext.offset, row, clock);
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }

        private static class SqlServerSnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                        SqlServerPartition, SqlServerOffsetContext> {

            public SqlServerSnapshotContext(SqlServerPartition partition) throws SQLException {
                super(partition, "");
            }
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded stream task
     * of a snapshot split task.
     */
    public class SqlServerSnapshotSplitChangeEventSourceContext
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
