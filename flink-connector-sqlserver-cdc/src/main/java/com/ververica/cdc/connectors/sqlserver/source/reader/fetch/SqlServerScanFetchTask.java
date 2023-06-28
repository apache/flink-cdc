/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.sqlserver.source.reader.fetch;

import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.base.source.reader.external.SnapshotSplitReadTask;
import com.ververica.cdc.connectors.sqlserver.source.offset.LsnOffset;
import com.ververica.cdc.connectors.sqlserver.source.reader.fetch.SqlServerStreamFetchTask.LsnSplitReadTask;
import com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerUtils;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

/** The task to work for fetching data of SqlServer table snapshot split. */
public class SqlServerScanFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;

    private SqlServerSnapshotSplitReadTask snapshotSplitReadTask;

    public SqlServerScanFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        SqlServerSourceFetchTaskContext sourceFetchContext =
                (SqlServerSourceFetchTaskContext) context;
        taskRunning = true;
        snapshotSplitReadTask =
                new SqlServerSnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getSnapshotReceiver(),
                        split);
        SnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new SnapshotSplitChangeEventSourceContext();
        SnapshotResult<SqlServerOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext,
                        sourceFetchContext.getPartition(),
                        sourceFetchContext.getOffsetContext());

        final StreamSplit backfillBinlogSplit = createBackFillLsnSplit(changeEventSourceContext);
        // optimization that skip the binlog read when the low watermark equals high
        // watermark
        final boolean binlogBackfillRequired =
                backfillBinlogSplit
                        .getEndingOffset()
                        .isAfter(backfillBinlogSplit.getStartingOffset());
        if (!binlogBackfillRequired) {
            dispatchLsnEndEvent(
                    backfillBinlogSplit,
                    sourceFetchContext.getPartition().getSourcePartition(),
                    sourceFetchContext.getDispatcher());
            taskRunning = false;
            return;
        }
        // execute stream read task
        if (snapshotResult.isCompletedOrSkipped()) {
            final SqlServerOffsetContext.Loader loader =
                    new SqlServerOffsetContext.Loader(sourceFetchContext.getDbzConnectorConfig());
            final SqlServerOffsetContext streamOffsetContext =
                    loader.load(backfillBinlogSplit.getStartingOffset().getOffset());

            final LsnSplitReadTask backfillBinlogReadTask =
                    createBackFillLsnSplitReadTask(backfillBinlogSplit, sourceFetchContext);
            backfillBinlogReadTask.execute(
                    new SnapshotBinlogSplitChangeEventSourceContext(),
                    sourceFetchContext.getPartition(),
                    streamOffsetContext);
        } else {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for SqlServer split %s fail", split));
        }
    }

    private StreamSplit createBackFillLsnSplit(
            SnapshotSplitChangeEventSourceContext sourceContext) {
        return new StreamSplit(
                split.splitId(),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>(),
                split.getTableSchemas(),
                0);
    }

    private void dispatchLsnEndEvent(
            StreamSplit backFillBinlogSplit,
            Map<String, ?> sourcePartition,
            JdbcSourceEventDispatcher<SqlServerPartition> eventDispatcher)
            throws InterruptedException {
        eventDispatcher.dispatchWatermarkEvent(
                sourcePartition,
                backFillBinlogSplit,
                backFillBinlogSplit.getEndingOffset(),
                WatermarkKind.END);
    }

    private LsnSplitReadTask createBackFillLsnSplitReadTask(
            StreamSplit backfillBinlogSplit, SqlServerSourceFetchTaskContext context) {
        // we should only capture events for the current table,
        // otherwise, we may can't find corresponding schema
        Configuration dezConf =
                context.getSourceConfig()
                        .getDbzConfiguration()
                        .edit()
                        .with("table.include.list", split.getTableId().toString())
                        // Disable heartbeat event in snapshot split fetcher
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build();
        // task to read binlog and backfill for current split
        return new LsnSplitReadTask(
                context.getDbzConnectorConfig(),
                context.getConnection(),
                context.getMetaDataConnection(),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                backfillBinlogSplit);
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    @Override
    public void close() {
        taskRunning = false;
    }

    /** A wrapped task to fetch snapshot split of table. */
    public static class SqlServerSnapshotSplitReadTask
            extends SnapshotSplitReadTask<SqlServerPartition, SqlServerOffsetContext> {

        private static final Logger LOG =
                LoggerFactory.getLogger(SqlServerSnapshotSplitReadTask.class);

        private final SqlServerConnectorConfig connectorConfig;
        private final SqlServerConnection jdbcConnection;
        private final SnapshotSplit snapshotSplit;

        public SqlServerSnapshotSplitReadTask(
                SqlServerConnectorConfig connectorConfig,
                SqlServerOffsetContext previousOffset,
                SnapshotProgressListener<SqlServerPartition> snapshotProgressListener,
                SqlServerDatabaseSchema databaseSchema,
                SqlServerConnection jdbcConnection,
                JdbcSourceEventDispatcher<SqlServerPartition> dispatcher,
                EventDispatcher.SnapshotReceiver<SqlServerPartition> snapshotReceiver,
                SnapshotSplit snapshotSplit) {
            super(
                    connectorConfig,
                    previousOffset,
                    snapshotProgressListener,
                    databaseSchema,
                    jdbcConnection,
                    dispatcher,
                    snapshotSplit);
            this.connectorConfig = connectorConfig;
            this.jdbcConnection = jdbcConnection;
            this.snapshotSplit = snapshotSplit;
        }

        @Override
        protected Offset currentOffset(JdbcConnection jdbcConnection) {
            return SqlServerUtils.currentLsn((SqlServerConnection) jdbcConnection);
        }

        @Override
        protected String buildSplitScanQuery(
                TableId tableId, RowType pkRowType, boolean isFirstSplit, boolean isLastSplit) {
            return SqlServerUtils.buildSplitScanQuery(
                    snapshotSplit.getTableId(),
                    snapshotSplit.getSplitKeyType(),
                    snapshotSplit.getSplitStart() == null,
                    snapshotSplit.getSplitEnd() == null);
        }

        @Override
        protected PreparedStatement readTableSplitDataStatement(
                JdbcConnection jdbc,
                String sql,
                boolean isFirstSplit,
                boolean isLastSplit,
                Object[] splitStart,
                Object[] splitEnd,
                int primaryKeyNum,
                int fetchSize) {
            return SqlServerUtils.readTableSplitDataStatement(
                    jdbcConnection,
                    sql,
                    snapshotSplit.getSplitStart() == null,
                    snapshotSplit.getSplitEnd() == null,
                    snapshotSplit.getSplitStart(),
                    snapshotSplit.getSplitEnd(),
                    snapshotSplit.getSplitKeyType().getFieldCount(),
                    connectorConfig.getQueryFetchSize());
        }

        @Override
        protected SqlSeverSnapshotContext prepare(SqlServerPartition partition) throws Exception {
            return new SqlSeverSnapshotContext(partition);
        }

        private static class SqlSeverSnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                        SqlServerPartition, SqlServerOffsetContext> {

            public SqlSeverSnapshotContext(SqlServerPartition partition) throws SQLException {
                super(partition, "");
            }
        }
    }

    /**
     * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high
     * watermark for each {@link SnapshotSplit}.
     */
    public static class SnapshotSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        private LsnOffset lowWatermark;
        private LsnOffset highWatermark;

        public LsnOffset getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(LsnOffset lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public LsnOffset getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(LsnOffset highWatermark) {
            this.highWatermark = highWatermark;
        }

        @Override
        public boolean isRunning() {
            return lowWatermark != null && highWatermark != null;
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded stream task
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
