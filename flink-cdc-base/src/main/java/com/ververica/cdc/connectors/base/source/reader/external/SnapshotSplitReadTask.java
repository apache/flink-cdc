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

package com.ververica.cdc.connectors.base.source.reader.external;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
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
import java.time.Duration;
import java.util.Objects;

/** A wrapped task to fetch snapshot split of table. */
public abstract class SnapshotSplitReadTask<P extends Partition, O extends OffsetContext>
        extends AbstractSnapshotChangeEventSource<P, O> {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotSplitReadTask.class);

    /** Interval for showing a log statement with the progress while scanning a single table. */
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final RelationalDatabaseConnectorConfig connectorConfig;
    private final RelationalDatabaseSchema databaseSchema;
    private final JdbcConnection jdbcConnection;
    private final JdbcSourceEventDispatcher<P> dispatcher;
    protected final Clock clock;
    private final SnapshotSplit snapshotSplit;
    private final OffsetContext offsetContext;
    private final SnapshotProgressListener<P> snapshotProgressListener;

    public SnapshotSplitReadTask(
            RelationalDatabaseConnectorConfig connectorConfig,
            OffsetContext previousOffset,
            SnapshotProgressListener<P> snapshotProgressListener,
            RelationalDatabaseSchema databaseSchema,
            JdbcConnection jdbcConnection,
            JdbcSourceEventDispatcher<P> dispatcher,
            SnapshotSplit snapshotSplit) {
        super(connectorConfig, snapshotProgressListener);
        this.offsetContext = previousOffset;
        this.connectorConfig = connectorConfig;
        this.databaseSchema = databaseSchema;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.snapshotSplit = snapshotSplit;
        this.snapshotProgressListener = snapshotProgressListener;
        this.clock = Clock.SYSTEM;
    }

    protected abstract Offset currentOffset(JdbcConnection jdbcConnection);

    @Override
    public SnapshotResult<O> execute(
            ChangeEventSourceContext context, P partition, O previousOffset)
            throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
        final SnapshotContext<P, O> ctx;
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
    protected SnapshotResult<O> doExecute(
            ChangeEventSourceContext context,
            O o,
            SnapshotContext<P, O> snapshotContext,
            SnapshottingTask snapshottingTask)
            throws Exception {
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<P, O> ctx =
                (RelationalSnapshotContext<P, O>) snapshotContext;
        ctx.offset = (O) offsetContext;
        final Offset lowWatermark = currentOffset(jdbcConnection);
        LOG.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                snapshotSplit);
        ((JdbcSnapshotSplitChangeEventSourceContext) (context)).setLowWatermark(lowWatermark);
        dispatcher.dispatchWatermarkEvent(
                snapshotContext.partition.getSourcePartition(),
                snapshotSplit,
                lowWatermark,
                WatermarkKind.LOW);

        LOG.info("Snapshot step 2 - Snapshotting data");
        createDataEvents(ctx, snapshotSplit.getTableId());

        final Offset highWatermark = currentOffset(jdbcConnection);
        LOG.info(
                "Snapshot step 3 - Determining high watermark {} for split {}",
                highWatermark,
                snapshotSplit);
        ((JdbcSnapshotSplitChangeEventSourceContext) (context)).setHighWatermark(highWatermark);
        dispatcher.dispatchWatermarkEvent(
                snapshotContext.partition.getSourcePartition(),
                snapshotSplit,
                highWatermark,
                WatermarkKind.HIGH);
        return SnapshotResult.completed(ctx.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(P p, O o) {
        return new SnapshottingTask(false, true);
    }

    protected abstract String buildSplitScanQuery(
            TableId tableId, RowType pkRowType, boolean isFirstSplit, boolean isLastSplit);

    protected abstract PreparedStatement readTableSplitDataStatement(
            JdbcConnection jdbc,
            String sql,
            boolean isFirstSplit,
            boolean isLastSplit,
            Object[] splitStart,
            Object[] splitEnd,
            int primaryKeyNum,
            int fetchSize);

    protected void createDataEvents(
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext<P, O> snapshotContext,
            TableId tableId)
            throws InterruptedException {
        EventDispatcher.SnapshotReceiver<P> snapshotReceiver =
                dispatcher.getSnapshotChangeEventReceiver();
        LOG.info("Snapshotting table {}", tableId);
        createDataEventsForTable(
                snapshotContext,
                snapshotReceiver,
                Objects.requireNonNull(databaseSchema.tableFor(tableId)));
        snapshotReceiver.completeSnapshot();
    }

    /** Dispatches the data change events for the records of a single table. */
    protected void createDataEventsForTable(
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext<P, O> snapshotContext,
            EventDispatcher.SnapshotReceiver<P> snapshotReceiver,
            Table table)
            throws InterruptedException {
        long exportStart = clock.currentTimeInMillis();
        LOG.info("Exporting data from split '{}' of table {}", snapshotSplit.splitId(), table.id());

        final String selectSql =
                buildSplitScanQuery(
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
            throw new FlinkRuntimeException("Snapshotting of table " + table.id() + " failed", e);
        }
    }

    private ChangeRecordEmitter<P> getChangeRecordEmitter(
            SnapshotContext<P, O> snapshotContext, TableId tableId, Object[] row) {
        snapshotContext.offset.event(tableId, clock.currentTime());
        return new SnapshotChangeRecordEmitter<>(
                snapshotContext.partition, snapshotContext.offset, row, clock);
    }

    private Threads.Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }
}
