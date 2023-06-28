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

package com.ververica.cdc.connectors.oracle.source.reader.fetch;

import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.base.source.reader.external.SnapshotSplitReadTask;
import com.ververica.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import com.ververica.cdc.connectors.oracle.source.utils.OracleConnectionUtils;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.LogMinerOracleOffsetContextLoader;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import static com.ververica.cdc.connectors.oracle.source.reader.fetch.OracleStreamFetchTask.RedoLogSplitReadTask;
import static com.ververica.cdc.connectors.oracle.source.utils.OracleConnectionUtils.createOracleConnection;

/** The task to work for fetching data of Oracle table snapshot split. */
public class OracleScanFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;

    private OracleSnapshotSplitReadTask snapshotSplitReadTask;

    public OracleScanFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public SnapshotSplit getSplit() {
        return split;
    }

    @Override
    public void close() {
        taskRunning = false;
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void execute(Context context) throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;
        taskRunning = true;
        snapshotSplitReadTask =
                new OracleSnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        split);
        SnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new SnapshotSplitChangeEventSourceContext();
        SnapshotResult<OracleOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext,
                        sourceFetchContext.getPartition(),
                        sourceFetchContext.getOffsetContext());

        final StreamSplit backfillBinlogSplit =
                createBackfillRedoLogSplit(changeEventSourceContext);
        // optimization that skip the binlog read when the low watermark equals high
        // watermark
        final boolean binlogBackfillRequired =
                backfillBinlogSplit
                        .getEndingOffset()
                        .isAfter(backfillBinlogSplit.getStartingOffset());
        if (!binlogBackfillRequired) {
            dispatchBinlogEndEvent(
                    backfillBinlogSplit,
                    sourceFetchContext.getPartition().getSourcePartition(),
                    ((OracleSourceFetchTaskContext) context).getDispatcher());
            taskRunning = false;
            return;
        }
        // execute redoLog read task
        if (snapshotResult.isCompletedOrSkipped()) {
            final RedoLogSplitReadTask backfillBinlogReadTask =
                    createBackfillRedoLogReadTask(backfillBinlogSplit, sourceFetchContext);

            final LogMinerOracleOffsetContextLoader loader =
                    new LogMinerOracleOffsetContextLoader(
                            ((OracleSourceFetchTaskContext) context).getDbzConnectorConfig());
            final OracleOffsetContext oracleOffsetContext =
                    loader.load(backfillBinlogSplit.getStartingOffset().getOffset());
            backfillBinlogReadTask.execute(
                    new SnapshotBinlogSplitChangeEventSourceContext(),
                    sourceFetchContext.getPartition(),
                    oracleOffsetContext);
            taskRunning = false;
        } else {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for oracle split %s fail", split));
        }
    }

    private StreamSplit createBackfillRedoLogSplit(
            SnapshotSplitChangeEventSourceContext sourceContext) {
        return new StreamSplit(
                split.splitId(),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>(),
                split.getTableSchemas(),
                0);
    }

    private RedoLogSplitReadTask createBackfillRedoLogReadTask(
            StreamSplit backfillBinlogSplit, OracleSourceFetchTaskContext context) {
        OracleConnectorConfig oracleConnectorConfig =
                context.getSourceConfig().getDbzConnectorConfig();
        final OffsetContext.Loader<OracleOffsetContext> loader =
                new LogMinerOracleOffsetContextLoader(oracleConnectorConfig);
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
        return new RedoLogSplitReadTask(
                new OracleConnectorConfig(dezConf),
                createOracleConnection(context.getSourceConfig().getDbzConfiguration()),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                context.getSourceConfig().getOriginDbzConnectorConfig(),
                context.getStreamingChangeEventSourceMetrics(),
                backfillBinlogSplit);
    }

    private void dispatchBinlogEndEvent(
            StreamSplit backFillBinlogSplit,
            Map<String, ?> sourcePartition,
            JdbcSourceEventDispatcher<OraclePartition> eventDispatcher)
            throws InterruptedException {
        eventDispatcher.dispatchWatermarkEvent(
                sourcePartition,
                backFillBinlogSplit,
                backFillBinlogSplit.getEndingOffset(),
                WatermarkKind.END);
    }

    /** A wrapped task to fetch snapshot split of table. */
    public static class OracleSnapshotSplitReadTask
            extends SnapshotSplitReadTask<OraclePartition, OracleOffsetContext> {

        private static final Logger LOG =
                LoggerFactory.getLogger(OracleSnapshotSplitReadTask.class);

        public OracleSnapshotSplitReadTask(
                OracleConnectorConfig connectorConfig,
                OracleOffsetContext previousOffset,
                SnapshotProgressListener<OraclePartition> snapshotProgressListener,
                OracleDatabaseSchema databaseSchema,
                OracleConnection jdbcConnection,
                JdbcSourceEventDispatcher<OraclePartition> dispatcher,
                SnapshotSplit snapshotSplit) {
            super(
                    connectorConfig,
                    previousOffset,
                    snapshotProgressListener,
                    databaseSchema,
                    jdbcConnection,
                    dispatcher,
                    snapshotSplit);
        }

        @Override
        protected Offset currentOffset(JdbcConnection jdbcConnection) {
            return OracleConnectionUtils.currentRedoLogOffset(jdbcConnection);
        }

        @Override
        protected String buildSplitScanQuery(
                TableId tableId, RowType pkRowType, boolean isFirstSplit, boolean isLastSplit) {
            return null;
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
            return null;
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
    }

    /**
     * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high
     * watermark for each {@link SnapshotSplit}.
     */
    public static class SnapshotSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        private RedoLogOffset lowWatermark;
        private RedoLogOffset highWatermark;

        public RedoLogOffset getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(RedoLogOffset lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public RedoLogOffset getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(RedoLogOffset highWatermark) {
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
