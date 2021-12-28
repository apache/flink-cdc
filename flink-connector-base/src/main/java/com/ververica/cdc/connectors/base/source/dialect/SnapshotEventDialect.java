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

package com.ververica.cdc.connectors.base.source.dialect;

import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.source.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.internal.converter.JdbcSourceRecordConverter;
import com.ververica.cdc.connectors.base.source.offset.Offset;
import com.ververica.cdc.connectors.base.source.reader.split.SnapshotReader;
import com.ververica.cdc.connectors.base.source.reader.split.dispatcher.SignalEventDispatcher;
import com.ververica.cdc.connectors.base.source.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.split.SourceSplitBase;
import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Closeable;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

/** A dialect to handle database event during snapshotting phase. */
public abstract class SnapshotEventDialect implements Dialect, Closeable, Serializable {

    /**
     * Build the scan query sql of the {@link SnapshotSplit} based on the given {@link
     * SnapshotContext} instance.
     *
     * @param tableId table identity.
     * @param splitKeyType primary key type.
     * @param isFirstSplit whether the first split.
     * @param isLastSplit whether the last split.
     * @return query sql.
     */
    public abstract String buildSplitScanQuery(
            TableId tableId, RowType splitKeyType, boolean isFirstSplit, boolean isLastSplit);

    /**
     * Query the maximum and minimum value of the column in the table. e.g. query string <code>
     * SELECT MIN(%s) FROM %s WHERE %s > ?</code>
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @param columnName column name.
     * @return maximum and minimum value.
     */
    public abstract Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException;

    /**
     * Query the minimum value of the column in the table, and the minimum value must greater than
     * the excludedLowerBound value. e.g. prepare query string <code>
     * SELECT MIN(%s) FROM %s WHERE %s > ?</code>
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @param columnName column name.
     * @param excludedLowerBound the minimum value should be greater than this value.
     * @return minimum value.
     */
    public abstract Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException;

    /**
     * Query the maximum value of the next chunk, and the next chunk must be greater than or equal
     * to <code>includedLowerBound</code> value [min_1, max_1), [min_2, max_2),... [min_n, null).
     * Each time this method is called it will return max1, max2...
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @param columnName column name.
     * @param chunkSize chunk size.
     * @param includedLowerBound the previous chunk end value.
     * @return next chunk end value.
     */
    public abstract Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException;

    /**
     * Approximate total number of entries in the lookup table.
     *
     * @param jdbc JDBC connection.
     * @param tableId table identity.
     * @return approximate row count.
     */
    public abstract Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException;

    public abstract PreparedStatement readTableSplitDataStatement(
            String selectSql, SnapshotContext context);

    /**
     * Create a task to read snapshot split of table.
     *
     * @param statefulTaskContext the context of snapshot split and table info.
     * @return a snapshot split reading task.
     */
    public abstract Task createTask(SnapshotContext statefulTaskContext);

    /**
     * Normalize the records of snapshot split which represents the split records state on high
     * watermark. data input: [low watermark event] [snapshot events ] [high watermark event]
     * [binlog events] [binlog-end event] data output: [low watermark event] [normalized events]
     * [high watermark event]
     */
    public abstract List<SourceRecord> normalizedSplitRecords(
            SnapshotSplit currentSnapshotSplit, List<SourceRecord> sourceRecords);

    /**
     * Create a {@link SnapshotContext} instance contain snapshot read information. Pass the
     * snapshotContext to {@link SnapshotReader} for table snapshot scan query.
     *
     * @param sourceConfig a basic source config
     * @return
     */
    public abstract SnapshotContext createSnapshotContext(SourceConfig sourceConfig);

    @Override
    public abstract void close();

    /** Context of the table snapshot reading. Contains table and database information. */
    public interface SnapshotContext extends Context {

        /**
         * The {@link SnapshotContext#configure(SourceSplitBase)} method needs to be called after
         * the {@link SnapshotContext} instance is created.
         */
        void configure(SourceSplitBase sourceSplitBase);

        /** return {@link SourceConfig} instance in the {@link SnapshotContext}. */
        SourceConfig getSourceConfig();

        /** return {@link SnapshotSplit} instance in the {@link SnapshotContext}. */
        SnapshotSplit getSnapshotSplit();

        SnapshotChangeEventSourceMetrics getSnapshotChangeEventSourceMetrics();
    }

    /** Task to read snapshot split of table. */
    public interface Task {

        SnapshotResult execute(ChangeEventSourceContext sourceContext) throws InterruptedException;
    }

    /** Task to read snapshot split of table. */
    public class SnapshotSplitReadTask implements Task {
        private final Clock clock = Clock.SYSTEM;
        /** Interval for showing a log statement with the progress while scanning a single table. */
        private final Duration logInterval = Duration.ofMillis(10_000);

        private final SnapshotContext snapshotContext;
        private final SnapshotSplit snapshotSplit;
        private final JdbcSourceRecordConverter jdbcSourceRecordConverter;
        private final SnapshotProgressListener snapshotProgressListener;

        public SnapshotSplitReadTask(SnapshotContext snapshotContext) {
            this.snapshotContext = snapshotContext;
            this.snapshotSplit = snapshotContext.getSnapshotSplit();
            this.jdbcSourceRecordConverter =
                    getSourceRecordConverter(snapshotSplit.getSplitKeyType());
            this.snapshotProgressListener = snapshotContext.getSnapshotChangeEventSourceMetrics();
        }

        @Override
        public SnapshotResult execute(ChangeEventSourceContext sourceContext)
                throws InterruptedException {
            try {
                return doExecute(sourceContext);
            } catch (InterruptedException e) {
                LOG.warn("Snapshot was interrupted before completion");
                throw e;
            } catch (Exception t) {
                throw new DebeziumException(t);
            }
        }

        public SnapshotResult doExecute(ChangeEventSourceContext context) throws Exception {
            final RelationalSnapshotChangeEventSource.RelationalSnapshotContext ctx =
                    new BaseSnapshotContext();
            final SignalEventDispatcher signalEventDispatcher =
                    new SignalEventDispatcher(snapshotContext);

            final Offset lowWatermark = displayCurrentOffset(snapshotContext.getSourceConfig());
            LOG.info(
                    "Snapshot step 1 - Determining low watermark {} for split {}",
                    lowWatermark,
                    snapshotSplit);
            ((SnapshotReader.SnapshotSplitChangeEventSourceContextImpl) (context))
                    .setLowWatermark(lowWatermark);
            signalEventDispatcher.dispatchWatermarkEvent(
                    snapshotSplit, lowWatermark, SignalEventDispatcher.WatermarkKind.LOW);

            LOG.info("Snapshot step 2 - Snapshotting data");
            createDataEvents(ctx, snapshotSplit.getTableId());

            final Offset highWatermark = displayCurrentOffset(snapshotContext.getSourceConfig());
            LOG.info(
                    "Snapshot step 3 - Determining high watermark {} for split {}",
                    highWatermark,
                    snapshotSplit);
            signalEventDispatcher.dispatchWatermarkEvent(
                    snapshotSplit, highWatermark, SignalEventDispatcher.WatermarkKind.HIGH);
            ((SnapshotReader.SnapshotSplitChangeEventSourceContextImpl) (context))
                    .setHighWatermark(highWatermark);

            return SnapshotResult.completed(ctx.offset);
        }

        private void createDataEvents(
                RelationalSnapshotChangeEventSource.RelationalSnapshotContext ctx, TableId tableId)
                throws Exception {
            //            EventDispatcher.SnapshotReceiver snapshotReceiver =
            //                    dispatcher.getSnapshotChangeEventReceiver();
            LOG.debug("Snapshotting table {}", tableId);
            createDataEventsForTable(ctx, tableId);
            //            snapshotReceiver.completeSnapshot();
        }

        /** Dispatches the data change events for the records of a single table. */
        private void createDataEventsForTable(
                RelationalSnapshotChangeEventSource.RelationalSnapshotContext ctx,
                //                EventDispatcher.SnapshotReceiver snapshotReceiver,
                TableId tableId)
                throws InterruptedException {

            long exportStart = clock.currentTimeInMillis();
            LOG.info(
                    "Exporting data from split '{}' of table {}", snapshotSplit.splitId(), tableId);

            final String selectSql =
                    buildSplitScanQuery(
                            snapshotSplit.getTableId(),
                            snapshotSplit.getSplitKeyType(),
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null);
            LOG.info(
                    "For split '{}' of table {} using select statement: '{}'",
                    snapshotSplit.splitId(),
                    tableId,
                    selectSql);

            try (PreparedStatement selectStatement =
                            readTableSplitDataStatement(selectSql, this.snapshotContext);
                    ResultSet rs = selectStatement.executeQuery()) {

                //                ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs,
                // table);
                long rows = 0;
                Threads.Timer logTimer = getTableScanLogTimer();

                while (rs.next()) {
                    rows++;
                    //                    final Object[] row = new
                    // Object[columnArray.getGreatestColumnPosition()];
                    //                    for (int i = 0; i < columnArray.getColumns().length; i++)
                    // {
                    //                        Column actualColumn = table.columns().get(i);
                    //                        row[columnArray.getColumns()[i].position() - 1] =
                    //                                readField(rs, i + 1, actualColumn, tableId);
                    //                    }
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        LOG.info(
                                "Exported {} records for split '{}' after {}",
                                rows,
                                snapshotSplit.splitId(),
                                Strings.duration(stop - exportStart));
                        snapshotProgressListener.rowsScanned(tableId, rows);
                        logTimer = getTableScanLogTimer();
                    }
                    SourceRecord sourceRecord = jdbcSourceRecordConverter.toInternal(rs);
                    ctx.offset.event(tableId, clock.currentTime());
                    //                    dispatcher.dispatchSnapshotEvent(
                    //                            tableId,
                    //                            getChangeRecordEmitter(snapshotContext, tableId,
                    // row),
                    //                            snapshotReceiver);

                }
                LOG.info(
                        "Finished exporting {} records for split '{}', total duration '{}'",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
            } catch (SQLException e) {
                throw new ConnectException("Snapshotting of table " + tableId + " failed", e);
            }
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, logInterval);
        }
    }

    private static class BaseSnapshotContext
            extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext {

        public BaseSnapshotContext() throws SQLException {
            super("");
        }
    }
}
