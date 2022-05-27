/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.sqlserver.experimental.fetch;

import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher.WatermarkKind;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.sqlserver.experimental.offset.TransactionLogOffset;
import io.debezium.DebeziumException;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
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

import static com.ververica.cdc.connectors.sqlserver.experimental.utils.SqlServerConnectionUtils.currentTransactionLogOffset;
import static com.ververica.cdc.connectors.sqlserver.experimental.utils.SqlServerUtils.buildSplitScanQuery;
import static com.ververica.cdc.connectors.sqlserver.experimental.utils.SqlServerUtils.readTableSplitDataStatement;

/** The task to work for fetching data of SqlServer table snapshot split . */
public class SqlServerScanFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;

    private SqlServerSnapshotSplitReadTask snapshotSplitReadTask;

    public SqlServerScanFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public SnapshotSplit getSplit() {
        return split;
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void execute(Context context) throws Exception {
        // TODO: 2022/5/27 snapshot task
    }

    /** A wrapped task to fetch snapshot split of table. */
    public static class SqlServerSnapshotSplitReadTask extends AbstractSnapshotChangeEventSource {

        private static final Logger LOG =
                LoggerFactory.getLogger(SqlServerSnapshotSplitReadTask.class);

        /** Interval for showing a log statement with the progress while scanning a single table. */
        private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

        private final SqlServerConnectorConfig connectorConfig;
        private final SqlServerDatabaseSchema databaseSchema;
        private final SqlServerConnection jdbcConnection;
        private final JdbcSourceEventDispatcher dispatcher;
        private final Clock clock;
        private final SnapshotSplit snapshotSplit;
        private final SqlServerOffsetContext offsetContext;
        private final SnapshotProgressListener snapshotProgressListener;

        public SqlServerSnapshotSplitReadTask(
                SqlServerConnectorConfig connectorConfig,
                SqlServerOffsetContext previousOffset,
                SnapshotProgressListener snapshotProgressListener,
                SqlServerDatabaseSchema databaseSchema,
                SqlServerConnection jdbcConnection,
                JdbcSourceEventDispatcher dispatcher,
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
        }

        @Override
        public SnapshotResult execute(
                ChangeEventSourceContext context, OffsetContext previousOffset)
                throws InterruptedException {
            SnapshottingTask snapshottingTask = getSnapshottingTask(previousOffset);
            final SnapshotContext ctx;
            try {
                ctx = prepare(context);
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
        protected SnapshotResult doExecute(
                ChangeEventSourceContext context,
                OffsetContext previousOffset,
                SnapshotContext snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final RelationalSnapshotChangeEventSource.RelationalSnapshotContext ctx =
                    (RelationalSnapshotChangeEventSource.RelationalSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;

            final TransactionLogOffset lowWatermark = currentTransactionLogOffset(jdbcConnection);
            LOG.info(
                    "Snapshot step 1 - Determining low watermark {} for split {}",
                    lowWatermark,
                    snapshotSplit);
            ((SnapshotSplitChangeEventSourceContext) (context)).setLowWatermark(lowWatermark);
            dispatcher.dispatchWatermarkEvent(
                    offsetContext.getPartition(), snapshotSplit, lowWatermark, WatermarkKind.LOW);

            LOG.info("Snapshot step 2 - Snapshotting data");
            createDataEvents(ctx, snapshotSplit.getTableId());

            final TransactionLogOffset highWatermark = currentTransactionLogOffset(jdbcConnection);
            LOG.info(
                    "Snapshot step 3 - Determining high watermark {} for split {}",
                    highWatermark,
                    snapshotSplit);
            ((SnapshotSplitChangeEventSourceContext) (context)).setHighWatermark(lowWatermark);
            dispatcher.dispatchWatermarkEvent(
                    offsetContext.getPartition(), snapshotSplit, highWatermark, WatermarkKind.HIGH);
            return SnapshotResult.completed(ctx.offset);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext)
                throws Exception {
            return new SqlServerSnapshotContext();
        }

        private static class SqlServerSnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext {

            public SqlServerSnapshotContext() throws SQLException {
                super("");
            }
        }

        private void createDataEvents(
                RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
                TableId tableId)
                throws Exception {
            EventDispatcher.SnapshotReceiver snapshotReceiver =
                    dispatcher.getSnapshotChangeEventReceiver();
            LOG.debug("Snapshotting table {}", tableId);
            createDataEventsForTable(
                    snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
            snapshotReceiver.completeSnapshot();
        }

        /** Dispatches the data change events for the records of a single table. */
        private void createDataEventsForTable(
                RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver snapshotReceiver,
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
                    final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                    for (int i = 0; i < columnArray.getColumns().length; i++) {
                        row[columnArray.getColumns()[i].position() - 1] = rs.getObject(i + 1);
                    }
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        LOG.info(
                                "Exported {} records for split '{}' after {}",
                                rows,
                                snapshotSplit.splitId(),
                                Strings.duration(stop - exportStart));
                        snapshotProgressListener.rowsScanned(table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }
                    dispatcher.dispatchSnapshotEvent(
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

        protected ChangeRecordEmitter getChangeRecordEmitter(
                SnapshotContext snapshotContext, TableId tableId, Object[] row) {
            snapshotContext.offset.event(tableId, clock.currentTime());
            return new SnapshotChangeRecordEmitter(snapshotContext.offset, row, clock);
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }
    }

    /**
     * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high
     * watermark for each {@link SnapshotSplit}.
     */
    public class SnapshotSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        private TransactionLogOffset lowWatermark;
        private TransactionLogOffset highWatermark;

        public TransactionLogOffset getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(TransactionLogOffset lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public TransactionLogOffset getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(TransactionLogOffset highWatermark) {
            this.highWatermark = highWatermark;
        }

        @Override
        public boolean isRunning() {
            return lowWatermark != null && highWatermark != null;
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded binlog task
     * of a snapshot split task.
     */
    public class SnapshotBinlogSplitChangeEventSourceContext
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
