/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.ververica.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetKind;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.connectors.mysql.source.utils.ChunkUtils;
import com.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getBinlogPosition;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getSplitKey;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getTableId;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isDataChangeRecord;

/**
 * A Debezium binlog reader implementation that also support reads binlog and filter overlapping
 * snapshot data that {@link SnapshotSplitReader} read.
 */
public class BinlogSplitReader implements DebeziumReader<SourceRecords, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogSplitReader.class);
    private final StatefulTaskContext statefulTaskContext;
    private final ExecutorService executorService;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;
    private volatile Throwable readException;

    private MySqlBinlogSplitReadTask binlogSplitReadTask;
    private MySqlBinlogSplit currentBinlogSplit;
    private Map<TableId, List<FinishedSnapshotSplitInfo>> finishedSplitsInfo;
    // tableId -> the max splitHighWatermark
    private Map<TableId, BinlogOffset> maxSplitHighWatermarkMap;
    private final Set<TableId> pureBinlogPhaseTables;
    private Tables.TableFilter capturedTableFilter;

    private static final long READER_CLOSE_TIMEOUT = 30L;

    public BinlogSplitReader(StatefulTaskContext statefulTaskContext, int subTaskId) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("binlog-reader-" + subTaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = true;
        this.pureBinlogPhaseTables = new HashSet<>();
    }

    public void submitSplit(MySqlSplit mySqlSplit) {
        this.currentBinlogSplit = mySqlSplit.asBinlogSplit();
        configureFilter();
        statefulTaskContext.configure(currentBinlogSplit);
        this.capturedTableFilter =
                statefulTaskContext.getConnectorConfig().getTableFilters().dataCollectionFilter();
        this.queue = statefulTaskContext.getQueue();
        this.binlogSplitReadTask =
                new MySqlBinlogSplitReadTask(
                        statefulTaskContext.getConnectorConfig(),
                        statefulTaskContext.getConnection(),
                        statefulTaskContext.getDispatcher(),
                        statefulTaskContext.getSignalEventDispatcher(),
                        statefulTaskContext.getErrorHandler(),
                        StatefulTaskContext.getClock(),
                        statefulTaskContext.getTaskContext(),
                        (MySqlStreamingChangeEventSourceMetrics)
                                statefulTaskContext.getStreamingChangeEventSourceMetrics(),
                        currentBinlogSplit,
                        createEventFilter(currentBinlogSplit.getStartingOffset()));

        executorService.submit(
                () -> {
                    try {
                        binlogSplitReadTask.execute(
                                new BinlogSplitChangeEventSourceContextImpl(),
                                statefulTaskContext.getOffsetContext());
                    } catch (Exception e) {
                        currentTaskRunning = false;
                        LOG.error(
                                String.format(
                                        "Execute binlog read task for mysql split %s fail",
                                        currentBinlogSplit),
                                e);
                        readException = e;
                    }
                });
    }

    private class BinlogSplitChangeEventSourceContextImpl
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return currentTaskRunning;
        }
    }

    @Override
    public boolean isFinished() {
        return currentBinlogSplit == null || !currentTaskRunning;
    }

    @Nullable
    @Override
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        checkReadException();
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        if (currentTaskRunning) {
            List<DataChangeEvent> batch = queue.poll();
            for (DataChangeEvent event : batch) {
                if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                }
            }
            List<SourceRecords> sourceRecordsSet = new ArrayList<>();
            sourceRecordsSet.add(new SourceRecords(sourceRecords));
            return sourceRecordsSet.iterator();
        } else {
            return null;
        }
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentBinlogSplit, readException.getMessage()),
                    readException);
        }
    }

    @Override
    public void close() {
        try {
            if (statefulTaskContext.getConnection() != null) {
                statefulTaskContext.getConnection().close();
            }
            if (statefulTaskContext.getBinaryLogClient() != null) {
                statefulTaskContext.getBinaryLogClient().disconnect();
            }
            // set currentTaskRunning to false to terminate the
            // while loop in MySqlStreamingChangeEventSource's execute method
            currentTaskRunning = false;
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(READER_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the binlog split reader in {} seconds.",
                            READER_CLOSE_TIMEOUT);
                }
            }
        } catch (Exception e) {
            LOG.error("Close binlog reader error", e);
        }
    }

    /**
     * Returns the record should emit or not.
     *
     * <p>The watermark signal algorithm is the binlog split reader only sends the binlog event that
     * belongs to its finished snapshot splits. For each snapshot split, the binlog event is valid
     * since the offset is after its high watermark.
     *
     * <pre> E.g: the data input is :
     *    snapshot-split-0 info : [0,    1024) highWatermark0
     *    snapshot-split-1 info : [1024, 2048) highWatermark1
     *  the data output is:
     *  only the binlog event belong to [0,    1024) and offset is after highWatermark0 should send,
     *  only the binlog event belong to [1024, 2048) and offset is after highWatermark1 should send.
     * </pre>
     */
    private boolean shouldEmit(SourceRecord sourceRecord) {
        if (isDataChangeRecord(sourceRecord)) {
            TableId tableId = getTableId(sourceRecord);
            BinlogOffset position = getBinlogPosition(sourceRecord);
            if (hasEnterPureBinlogPhase(tableId, position)) {
                return true;
            }
            // only the table who captured snapshot splits need to filter
            if (finishedSplitsInfo.containsKey(tableId)) {
                RowType splitKeyType =
                        ChunkUtils.getChunkKeyColumnType(
                                statefulTaskContext.getDatabaseSchema().tableFor(tableId),
                                statefulTaskContext.getSourceConfig().getChunkKeyColumn());
                Object[] key =
                        getSplitKey(
                                splitKeyType,
                                sourceRecord,
                                statefulTaskContext.getSchemaNameAdjuster());
                for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo.get(tableId)) {
                    if (RecordUtils.splitKeyRangeContains(
                                    key, splitInfo.getSplitStart(), splitInfo.getSplitEnd())
                            && position.isAfter(splitInfo.getHighWatermark())) {
                        return true;
                    }
                }
            }
            // not in the monitored splits scope, do not emit
            return false;
        }
        // always send the schema change event and signal event
        // we need record them to state of Flink
        return true;
    }

    private boolean hasEnterPureBinlogPhase(TableId tableId, BinlogOffset position) {
        if (pureBinlogPhaseTables.contains(tableId)) {
            return true;
        }
        // the existed tables those have finished snapshot reading
        if (maxSplitHighWatermarkMap.containsKey(tableId)
                && position.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))) {
            pureBinlogPhaseTables.add(tableId);
            return true;
        }
        // capture dynamically new added tables
        // TODO: there is still very little chance that we can't capture new added table.
        //  That the tables dynamically added after discovering captured tables in enumerator
        //  and before the lowest binlog offset of all table splits. This interval should be
        //  very short, so we don't support it for now.
        return !maxSplitHighWatermarkMap.containsKey(tableId)
                && capturedTableFilter.isIncluded(tableId);
    }

    private void configureFilter() {
        List<FinishedSnapshotSplitInfo> finishedSplitInfos =
                currentBinlogSplit.getFinishedSnapshotSplitInfos();
        Map<TableId, List<FinishedSnapshotSplitInfo>> splitsInfoMap = new HashMap<>();
        Map<TableId, BinlogOffset> tableIdBinlogPositionMap = new HashMap<>();
        // specific offset mode
        if (finishedSplitInfos.isEmpty()) {
            for (TableId tableId : currentBinlogSplit.getTableSchemas().keySet()) {
                tableIdBinlogPositionMap.put(tableId, currentBinlogSplit.getStartingOffset());
            }
        }
        // initial mode
        else {
            for (FinishedSnapshotSplitInfo finishedSplitInfo : finishedSplitInfos) {
                TableId tableId = finishedSplitInfo.getTableId();
                List<FinishedSnapshotSplitInfo> list =
                        splitsInfoMap.getOrDefault(tableId, new ArrayList<>());
                list.add(finishedSplitInfo);
                splitsInfoMap.put(tableId, list);

                BinlogOffset highWatermark = finishedSplitInfo.getHighWatermark();
                BinlogOffset maxHighWatermark = tableIdBinlogPositionMap.get(tableId);
                if (maxHighWatermark == null || highWatermark.isAfter(maxHighWatermark)) {
                    tableIdBinlogPositionMap.put(tableId, highWatermark);
                }
            }
        }
        this.finishedSplitsInfo = splitsInfoMap;
        this.maxSplitHighWatermarkMap = tableIdBinlogPositionMap;
        this.pureBinlogPhaseTables.clear();
    }

    private Predicate<Event> createEventFilter(BinlogOffset startingOffset) {
        // If the startup mode is set as TIMESTAMP, we need to apply a filter on event to drop
        // events earlier than the specified timestamp.
        if (BinlogOffsetKind.TIMESTAMP.equals(startingOffset.getOffsetKind())) {
            long startTimestampSec = startingOffset.getTimestampSec();
            // Notes:
            // 1. Heartbeat event doesn't contain timestamp, so we just keep it
            // 2. Timestamp of event is in epoch millisecond
            return event ->
                    EventType.HEARTBEAT.equals(event.getHeader().getEventType())
                            || event.getHeader().getTimestamp() >= startTimestampSec * 1000;
        }
        return event -> true;
    }

    public void stopBinlogReadTask() {
        this.currentTaskRunning = false;
    }

    @VisibleForTesting
    public ExecutorService getExecutorService() {
        return executorService;
    }
}
