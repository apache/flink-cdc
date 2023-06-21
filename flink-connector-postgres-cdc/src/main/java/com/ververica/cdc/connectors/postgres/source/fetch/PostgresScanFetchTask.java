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

package com.ververica.cdc.connectors.postgres.source.fetch;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import com.ververica.cdc.connectors.postgres.source.offset.PostgresOffset;
import com.ververica.cdc.connectors.postgres.source.offset.PostgresOffsetUtils;
import com.ververica.cdc.connectors.postgres.source.utils.PostgresQueryUtils;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.DROP_SLOT_ON_STOP;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static io.debezium.connector.postgresql.PostgresObjectUtils.waitForReplicationSlotReady;
import static io.debezium.connector.postgresql.Utils.currentOffset;
import static io.debezium.connector.postgresql.Utils.refreshSchema;

/** A {@link FetchTask} implementation for Postgres to read snapshot split. */
public class PostgresScanFetchTask implements FetchTask<SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresScanFetchTask.class);

    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;

    public PostgresScanFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public SourceSplitBase getSplit() {
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
        LOG.info("Execute ScanFetchTask for split: {}", split);
        PostgresSourceFetchTaskContext ctx = (PostgresSourceFetchTaskContext) context;
        taskRunning = true;

        PostgresSnapshotSplitReadTask snapshotSplitReadTask =
                new PostgresSnapshotSplitReadTask(
                        ctx.getConnection(),
                        (PostgresReplicationConnection) ctx.getReplicationConnection(),
                        ctx.getDbzConnectorConfig(),
                        ctx.getDatabaseSchema(),
                        ctx.getOffsetContext(),
                        ctx.getDispatcher(),
                        ctx.getSnapshotChangeEventSourceMetrics(),
                        split,
                        ctx.getSlotName(),
                        ctx.getPluginName());

        SnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new SnapshotSplitChangeEventSourceContext();
        SnapshotResult<PostgresOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext, ctx.getPartition(), ctx.getOffsetContext());

        if (!snapshotResult.isCompletedOrSkipped()) {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for postgres split %s fail", split));
        }

        executeBackfillTask(ctx, changeEventSourceContext);
    }

    private void executeBackfillTask(
            PostgresSourceFetchTaskContext ctx,
            SnapshotSplitChangeEventSourceContext changeEventSourceContext)
            throws InterruptedException {
        final StreamSplit backfillSplit =
                new StreamSplit(
                        split.splitId(),
                        changeEventSourceContext.getLowWatermark(),
                        changeEventSourceContext.getHighWatermark(),
                        new ArrayList<>(),
                        split.getTableSchemas(),
                        0);

        // optimization that skip the WAL read when the low watermark >=  high watermark
        if (!isBackFillRequired(
                backfillSplit.getStartingOffset(), backfillSplit.getEndingOffset())) {
            LOG.info(
                    "Skip the backfill {} for split {}: low watermark >= high watermark",
                    backfillSplit,
                    split);
            ctx.getDispatcher()
                    .dispatchWatermarkEvent(
                            ctx.getPartition().getSourcePartition(),
                            backfillSplit,
                            backfillSplit.getEndingOffset(),
                            WatermarkKind.END);

            taskRunning = false;
            return;
        }

        final PostgresOffsetContext.Loader loader =
                new PostgresOffsetContext.Loader(ctx.getDbzConnectorConfig());
        final PostgresOffsetContext postgresOffsetContext =
                PostgresOffsetUtils.getPostgresOffsetContext(
                        loader, backfillSplit.getStartingOffset());

        // we should only capture events for the current table,
        // otherwise, we may not find corresponding schema
        PostgresSourceConfig config = (PostgresSourceConfig) ctx.getSourceConfig();
        Configuration dbzConf =
                ctx.getDbzConnectorConfig()
                        .getConfig()
                        .edit()
                        .with("table.include.list", split.getTableId().toString())
                        .with(SLOT_NAME.name(), config.getSlotNameForBackfillTask())
                        // drop slot for backfill stream split
                        .with(DROP_SLOT_ON_STOP.name(), true)
                        // Disable heartbeat event in snapshot split fetcher
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build();

        final PostgresStreamFetchTask.StreamSplitReadTask backfillReadTask =
                new PostgresStreamFetchTask.StreamSplitReadTask(
                        new PostgresConnectorConfig(dbzConf),
                        ctx.getSnapShotter(),
                        ctx.getConnection(),
                        ctx.getDispatcher(),
                        ctx.getPostgresDispatcher(),
                        ctx.getErrorHandler(),
                        ctx.getTaskContext().getClock(),
                        ctx.getDatabaseSchema(),
                        ctx.getTaskContext(),
                        ctx.getReplicationConnection(),
                        backfillSplit);
        LOG.info(
                "Execute backfillReadTask for split {} with slot name {}",
                split,
                dbzConf.getString(SLOT_NAME.name()));
        backfillReadTask.execute(
                new PostgresChangeEventSourceContext(), ctx.getPartition(), postgresOffsetContext);
    }

    private static boolean isBackFillRequired(Offset lowWatermark, Offset highWatermark) {
        return highWatermark.isAfter(lowWatermark);
    }

    static class SnapshotSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        private PostgresOffset lowWatermark;
        private PostgresOffset highWatermark;

        public PostgresOffset getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(PostgresOffset lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public PostgresOffset getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(PostgresOffset highWatermark) {
            this.highWatermark = highWatermark;
        }

        @Override
        public boolean isRunning() {
            return lowWatermark != null && highWatermark != null;
        }
    }

    class PostgresChangeEventSourceContext implements ChangeEventSource.ChangeEventSourceContext {

        public void finished() {
            taskRunning = false;
        }

        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }

    /** A SnapshotChangeEventSource implementation for Postgres to read snapshot split. */
    public static class PostgresSnapshotSplitReadTask
            extends AbstractSnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> {
        private static final Logger LOG =
                LoggerFactory.getLogger(PostgresSnapshotSplitReadTask.class);

        private final PostgresConnection jdbcConnection;
        private final PostgresReplicationConnection replicationConnection;
        private final PostgresConnectorConfig connectorConfig;
        private final JdbcSourceEventDispatcher<PostgresPartition> dispatcher;
        private final SnapshotSplit snapshotSplit;
        private final PostgresOffsetContext offsetContext;
        private final PostgresSchema databaseSchema;
        private final SnapshotProgressListener<PostgresPartition> snapshotProgressListener;
        private final Clock clock;
        private final String slotName;
        private final String pluginName;

        public PostgresSnapshotSplitReadTask(
                PostgresConnection jdbcConnection,
                PostgresReplicationConnection replicationConnection,
                PostgresConnectorConfig connectorConfig,
                PostgresSchema databaseSchema,
                PostgresOffsetContext previousOffset,
                JdbcSourceEventDispatcher dispatcher,
                SnapshotProgressListener snapshotProgressListener,
                SnapshotSplit snapshotSplit,
                String slotName,
                String pluginName) {
            super(connectorConfig, snapshotProgressListener);
            this.jdbcConnection = jdbcConnection;
            this.replicationConnection = replicationConnection;
            this.connectorConfig = connectorConfig;
            this.snapshotProgressListener = snapshotProgressListener;
            this.databaseSchema = databaseSchema;
            this.dispatcher = dispatcher;
            this.snapshotSplit = snapshotSplit;
            this.offsetContext = previousOffset;
            this.clock = Clock.SYSTEM;
            this.slotName = slotName;
            this.pluginName = pluginName;
        }

        @Override
        protected SnapshotResult<PostgresOffsetContext> doExecute(
                ChangeEventSourceContext context,
                PostgresOffsetContext previousOffset,
                SnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final PostgresSnapshotContext ctx = (PostgresSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;
            createSlotForBackFillReadTask();
            refreshSchema(databaseSchema, jdbcConnection, true);
            final PostgresOffset lowWatermark = currentOffset(jdbcConnection);
            LOG.info(
                    "Snapshot step 1 - Determining low watermark {} for split {}",
                    lowWatermark,
                    snapshotSplit);
            ((SnapshotSplitChangeEventSourceContext) (context)).setLowWatermark(lowWatermark);
            dispatcher.dispatchWatermarkEvent(
                    snapshotContext.partition.getSourcePartition(),
                    snapshotSplit,
                    lowWatermark,
                    WatermarkKind.LOW);

            LOG.info("Snapshot step 2 - Snapshotting data");
            createDataEvents(ctx, snapshotSplit.getTableId());

            final PostgresOffset highWatermark = currentOffset(jdbcConnection);
            LOG.info(
                    "Snapshot step 3 - Determining high watermark {} for split {}",
                    highWatermark,
                    snapshotSplit);
            ((SnapshotSplitChangeEventSourceContext) (context)).setHighWatermark(highWatermark);
            dispatcher.dispatchWatermarkEvent(
                    snapshotContext.partition.getSourcePartition(),
                    snapshotSplit,
                    highWatermark,
                    WatermarkKind.HIGH);
            // release slot timely
            if (!isBackFillRequired(lowWatermark, highWatermark)) {
                dropSlotForBackFillReadTask();
            }
            return SnapshotResult.completed(ctx.offset);
        }

        private void createDataEvents(PostgresSnapshotContext snapshotContext, TableId tableId)
                throws InterruptedException {
            EventDispatcher.SnapshotReceiver<PostgresPartition> snapshotReceiver =
                    dispatcher.getSnapshotChangeEventReceiver();
            LOG.info("Snapshotting table {}", tableId);
            createDataEventsForTable(
                    snapshotContext,
                    snapshotReceiver,
                    Objects.requireNonNull(databaseSchema.tableFor(tableId)));
            snapshotReceiver.completeSnapshot();
        }

        /** Dispatches the data change events for the records of a single table. */
        private void createDataEventsForTable(
                PostgresSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver<PostgresPartition> snapshotReceiver,
                Table table)
                throws InterruptedException {

            long exportStart = clock.currentTimeInMillis();
            LOG.info(
                    "Exporting data from split '{}' of table {}",
                    snapshotSplit.splitId(),
                    table.id());

            final String selectSql =
                    PostgresQueryUtils.buildSplitScanQuery(
                            snapshotSplit.getTableId(),
                            snapshotSplit.getSplitKeyType(),
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null);
            LOG.debug(
                    "For split '{}' of table {} using select statement: '{}'",
                    snapshotSplit.splitId(),
                    table.id(),
                    selectSql);

            try (PreparedStatement selectStatement =
                            PostgresQueryUtils.readTableSplitDataStatement(
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
                        snapshotProgressListener.rowsScanned(
                                snapshotContext.partition, table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }
                    snapshotContext.offset.event(table.id(), clock.currentTime());
                    SnapshotChangeRecordEmitter<PostgresPartition> emitter =
                            new SnapshotChangeRecordEmitter<>(
                                    snapshotContext.partition, snapshotContext.offset, row, clock);
                    dispatcher.dispatchSnapshotEvent(
                            snapshotContext.partition, table.id(), emitter, snapshotReceiver);
                }
                LOG.info(
                        "Finished exporting {} records for split '{}', total duration '{}'",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
            } catch (SQLException e) {
                throw new FlinkRuntimeException(
                        "Snapshotting of table " + table.id() + " failed", e);
            }
        }

        /**
         * Create a slot before snapshot reading so that the slot can track the WAL log during the
         * snapshot reading phase.
         */
        private void createSlotForBackFillReadTask() {
            try {
                SlotState slotInfo = null;
                try {
                    slotInfo = jdbcConnection.getReplicationSlotState(slotName, pluginName);
                } catch (SQLException e) {
                    LOG.info(
                            "Unable to load info of replication slot, will try to create the slot");
                }
                if (slotInfo == null) {
                    try {
                        replicationConnection.createReplicationSlot().orElse(null);
                    } catch (SQLException ex) {
                        String message = "Creation of replication slot failed";
                        if (ex.getMessage().contains("already exists")) {
                            message +=
                                    "; when setting up multiple connectors for the same database host, please make sure to use a distinct replication slot name for each.";
                        }
                        throw new FlinkRuntimeException(message, ex);
                    }
                }
                waitForReplicationSlotReady(30, jdbcConnection, slotName, pluginName);
            } catch (Throwable t) {
                throw new FlinkRuntimeException(t);
            }
        }

        /** Drop slot for backfill task and close replication connection. */
        private void dropSlotForBackFillReadTask() {
            try {
                replicationConnection.close(true);
            } catch (Throwable t) {
                throw new FlinkRuntimeException(t);
            }
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(
                PostgresPartition partition, PostgresOffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected PostgresSnapshotContext prepare(PostgresPartition partition) throws Exception {
            return new PostgresSnapshotContext(partition);
        }

        private static class PostgresSnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                        PostgresPartition, PostgresOffsetContext> {

            public PostgresSnapshotContext(PostgresPartition partition) throws SQLException {
                super(partition, "");
            }
        }
    }
}
