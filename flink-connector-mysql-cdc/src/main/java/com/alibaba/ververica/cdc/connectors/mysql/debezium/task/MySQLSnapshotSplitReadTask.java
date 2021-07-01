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

package com.alibaba.ververica.cdc.connectors.mysql.debezium.task;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils;
import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
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
import java.util.concurrent.atomic.AtomicReference;

/** Task to read snapshot split of table. */
public class MySQLSnapshotSplitReadTask extends AbstractSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSnapshotSplitReadTask.class);

    /** Interval for showing a log statement with the progress while scanning a single table. */
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final MySqlConnectorConfig connectorConfig;
    private final MySqlDatabaseSchema databaseSchema;
    private final MySqlConnection jdbcConnection;
    private final EventDispatcherImpl<TableId> dispatcher;
    private final Clock clock;
    private final MySQLSplit mySQLSplit;
    private final MySqlOffsetContext offsetContext;
    private final TopicSelector<TableId> topicSelector;
    private final SnapshotProgressListener snapshotProgressListener;

    public MySQLSnapshotSplitReadTask(
            MySqlConnectorConfig connectorConfig,
            MySqlOffsetContext previousOffset,
            SnapshotProgressListener snapshotProgressListener,
            MySqlDatabaseSchema databaseSchema,
            MySqlConnection jdbcConnection,
            EventDispatcherImpl<TableId> dispatcher,
            TopicSelector<TableId> topicSelector,
            Clock clock,
            MySQLSplit mySQLSplit) {
        super(connectorConfig, previousOffset, snapshotProgressListener);
        this.offsetContext = previousOffset;
        this.connectorConfig = connectorConfig;
        this.databaseSchema = databaseSchema;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.mySQLSplit = mySQLSplit;
        this.topicSelector = topicSelector;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    public SnapshotResult execute(ChangeEventSourceContext context) throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(previousOffset);
        final SnapshotContext ctx;
        try {
            ctx = prepare(context);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }
        try {
            return doExecute(context, ctx, snapshottingTask);
        } catch (InterruptedException e) {
            LOGGER.warn("Snapshot was interrupted before completion");
            throw e;
        } catch (Exception t) {
            throw new DebeziumException(t);
        }
    }

    @Override
    protected SnapshotResult doExecute(
            ChangeEventSourceContext context,
            SnapshotContext snapshotContext,
            SnapshottingTask snapshottingTask)
            throws Exception {
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext ctx =
                (RelationalSnapshotChangeEventSource.RelationalSnapshotContext) snapshotContext;

        final BinlogPosition lowWatermark = getCurrentBinlogPosition();
        LOGGER.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                mySQLSplit);
        offsetContext.setBinlogStartPoint(lowWatermark.getFilename(), lowWatermark.getPosition());
        ctx.offset = offsetContext;
        ((SnapshotSplitReader.SnapshotSplitChangeEventSourceContextImpl) (context))
                .setLowWatermark(lowWatermark);

        final SignalEventDispatcher signalEventDispatcher =
                new SignalEventDispatcher(
                        offsetContext,
                        topicSelector.topicNameFor(mySQLSplit.getTableId()),
                        dispatcher.getQueue());
        signalEventDispatcher.dispatchWatermarkEvent(
                mySQLSplit, lowWatermark, SignalEventDispatcher.WatermarkKind.LOW);

        LOGGER.info("Snapshot step 2 - Snapshotting data");
        createDataEvents(ctx, mySQLSplit.getTableId());
        final BinlogPosition highWatermark = getCurrentBinlogPosition();
        LOGGER.info(
                "Snapshot step 3 - Determining high watermark {} for split {}",
                highWatermark,
                mySQLSplit);
        signalEventDispatcher.dispatchWatermarkEvent(
                mySQLSplit, highWatermark, SignalEventDispatcher.WatermarkKind.HIGH);
        ((SnapshotSplitReader.SnapshotSplitChangeEventSourceContextImpl) (context))
                .setHighWatermark(highWatermark);

        return SnapshotResult.completed(ctx.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
        return new SnapshottingTask(false, true);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext)
            throws Exception {
        return new MySqlSnapshotContext();
    }

    private static class MySqlSnapshotContext
            extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext {

        public MySqlSnapshotContext() throws SQLException {
            super("");
        }
    }

    private void createDataEvents(
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
            TableId tableId)
            throws Exception {
        EventDispatcher.SnapshotReceiver snapshotReceiver =
                dispatcher.getSnapshotChangeEventReceiver();
        LOGGER.debug("Snapshotting table {}", tableId);
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
        LOGGER.info(
                "Exporting data from split '{}' of table {}", mySQLSplit.getSplitId(), table.id());

        final String selectSql =
                StatementUtils.buildSplitScanQuery(
                        mySQLSplit.getTableId(),
                        mySQLSplit.getSplitBoundaryType(),
                        mySQLSplit.getSplitBoundaryStart() == null,
                        mySQLSplit.getSplitBoundaryEnd() == null);
        LOGGER.info(
                "For split '{}' of table {} using select statement: '{}'",
                mySQLSplit.getSplitId(),
                table.id(),
                selectSql);

        try (PreparedStatement selectStatement =
                        StatementUtils.readTableSplitDataStatement(
                                jdbcConnection,
                                selectSql,
                                mySQLSplit.getSplitBoundaryStart() == null,
                                mySQLSplit.getSplitBoundaryEnd() == null,
                                mySQLSplit.getSplitBoundaryStart(),
                                mySQLSplit.getSplitBoundaryEnd(),
                                mySQLSplit.getSplitBoundaryType().getFieldCount(),
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
                    LOGGER.info(
                            "Exported {} records for split '{}' after {}",
                            rows,
                            mySQLSplit.getSplitId(),
                            Strings.duration(stop - exportStart));
                    snapshotProgressListener.rowsScanned(table.id(), rows);
                    logTimer = getTableScanLogTimer();
                }
                dispatcher.dispatchSnapshotEvent(
                        table.id(),
                        getChangeRecordEmitter(snapshotContext, table.id(), row),
                        snapshotReceiver);
            }
            LOGGER.info(
                    "Finished exporting {} records for split '{}' of table '{}'; total duration '{}'",
                    rows,
                    mySQLSplit.getSplitId(),
                    table.id(),
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

    private BinlogPosition getCurrentBinlogPosition() {
        AtomicReference<BinlogPosition> currentBinlogPosition =
                new AtomicReference<>(BinlogPosition.INITIAL_OFFSET);
        try {
            jdbcConnection.setAutoCommit(false);
            String showMasterStmt = "SHOW MASTER STATUS";
            jdbcConnection.query(
                    showMasterStmt,
                    rs -> {
                        if (rs.next()) {
                            String binlogFilename = rs.getString(1);
                            long binlogPosition = rs.getLong(2);
                            currentBinlogPosition.set(
                                    new BinlogPosition(binlogFilename, binlogPosition));
                            LOGGER.info(
                                    "Read binlog '{}' at position '{}'",
                                    binlogFilename,
                                    binlogPosition);
                        } else {
                            throw new IllegalStateException(
                                    "Cannot read the binlog filename and position via '"
                                            + showMasterStmt
                                            + "'. Make sure your server is correctly configured");
                        }
                    });
            jdbcConnection.commit();
        } catch (Exception e) {
            LOGGER.error("Read current binlog position error.", e);
        }
        return currentBinlogPosition.get();
    }
}
