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

package org.apache.flink.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupMode;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Struct;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils.createBinaryClient;
import static org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils.createMySqlConnection;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isEndWatermarkEvent;

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
    private Predicate capturedTableFilter;
    private final StoppableChangeEventSourceContext changeEventSourceContext =
            new StoppableChangeEventSourceContext();
    private final boolean isParsingOnLineSchemaChanges;
    private final boolean isBackfillSkipped;

    private static final long READER_CLOSE_TIMEOUT = 30L;

    public BinlogSplitReader(MySqlSourceConfig sourceConfig, int subtaskId) {
        this(
                new StatefulTaskContext(
                        sourceConfig,
                        createBinaryClient(sourceConfig.getDbzConfiguration()),
                        createMySqlConnection(sourceConfig)),
                subtaskId);
    }

    public BinlogSplitReader(StatefulTaskContext statefulTaskContext, int subtaskId) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("binlog-reader-" + subtaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = true;
        this.pureBinlogPhaseTables = new HashSet<>();
        this.isParsingOnLineSchemaChanges =
                statefulTaskContext.getSourceConfig().isParseOnLineSchemaChanges();
        this.isBackfillSkipped = statefulTaskContext.getSourceConfig().isSkipSnapshotBackfill();
    }

    public void submitSplit(MySqlSplit mySqlSplit) {
        this.currentBinlogSplit = mySqlSplit.asBinlogSplit();
        configureFilter();
        statefulTaskContext.configure(currentBinlogSplit);
        this.capturedTableFilter = statefulTaskContext.getSourceConfig().getTableFilter();
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
                        createEventFilter());

        executorService.submit(
                () -> {
                    try {
                        binlogSplitReadTask.execute(
                                changeEventSourceContext,
                                statefulTaskContext.getMySqlPartition(),
                                statefulTaskContext.getOffsetContext());
                    } catch (Throwable t) {
                        LOG.error(
                                String.format(
                                        "Execute binlog read task for mysql split %s fail",
                                        currentBinlogSplit),
                                t);
                        readException = t;
                    }
                });
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
                if (isEndWatermarkEvent(event.getRecord())) {
                    LOG.info("Read split {} end watermark event", currentBinlogSplit);
                    try {
                        stopBinlogReadTask();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    break;
                }

                if (isParsingOnLineSchemaChanges) {
                    Optional<SourceRecord> oscRecord =
                            parseOnLineSchemaChangeEvent(event.getRecord());
                    if (oscRecord.isPresent()) {
                        sourceRecords.add(oscRecord.get());
                        continue;
                    }
                }
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
            stopBinlogReadTask();
            if (statefulTaskContext != null) {
                statefulTaskContext.close();
            }
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

    private Optional<SourceRecord> parseOnLineSchemaChangeEvent(SourceRecord sourceRecord) {
        if (RecordUtils.isOnLineSchemaChangeEvent(sourceRecord)) {
            // This is a gh-ost initialized schema change event and should be emitted if the
            // peeled tableId matches the predicate.
            TableId originalTableId = RecordUtils.getTableId(sourceRecord);
            TableId peeledTableId = RecordUtils.peelTableId(originalTableId);
            if (capturedTableFilter.test(peeledTableId)) {
                return Optional.of(
                        RecordUtils.setTableId(sourceRecord, originalTableId, peeledTableId));
            }
        }
        return Optional.empty();
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
        if (RecordUtils.isDataChangeRecord(sourceRecord)) {
            TableId tableId = RecordUtils.getTableId(sourceRecord);
            if (pureBinlogPhaseTables.contains(tableId)) {
                return true;
            }
            BinlogOffset position = RecordUtils.getBinlogPosition(sourceRecord);
            if (hasEnterPureBinlogPhase(tableId, position)) {
                return true;
            }

            // only the table who captured snapshot splits need to filter
            if (finishedSplitsInfo.containsKey(tableId)) {
                // if backfill skipped, don't need to filter
                if (isBackfillSkipped) {
                    return true;
                }
                RowType splitKeyType =
                        ChunkUtils.getChunkKeyColumnType(
                                statefulTaskContext.getDatabaseSchema().tableFor(tableId),
                                statefulTaskContext.getSourceConfig().getChunkKeyColumns(),
                                statefulTaskContext.getSourceConfig().isTreatTinyInt1AsBoolean());

                Struct target = RecordUtils.getStructContainsChunkKey(sourceRecord);
                Object[] chunkKey =
                        RecordUtils.getSplitKey(
                                splitKeyType, statefulTaskContext.getSchemaNameAdjuster(), target);
                for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo.get(tableId)) {
                    if (RecordUtils.splitKeyRangeContains(
                                    chunkKey, splitInfo.getSplitStart(), splitInfo.getSplitEnd())
                            && position.isAfter(splitInfo.getHighWatermark())) {
                        return true;
                    }
                }
            }
            // not in the monitored splits scope, do not emit
            return false;
        } else if (RecordUtils.isSchemaChangeEvent(sourceRecord)) {
            if (RecordUtils.isTableChangeRecord(sourceRecord)) {
                TableId tableId = RecordUtils.getTableId(sourceRecord);
                return capturedTableFilter.test(tableId);
            } else {
                // Not related to changes in table structure, like `CREATE/DROP DATABASE`, skip it
                return false;
            }
        }
        return true;
    }

    private boolean hasEnterPureBinlogPhase(TableId tableId, BinlogOffset position) {
        // the existed tables those have finished snapshot reading
        if (maxSplitHighWatermarkMap.containsKey(tableId)
                && position.isAfter(maxSplitHighWatermarkMap.get(tableId))) {
            pureBinlogPhaseTables.add(tableId);
            return true;
        }

        // Use still need to capture new sharding table if user disable scan new added table,
        // The history records for all new added tables(including sharding table and normal table)
        // will be capture after restore from a savepoint if user enable scan new added table
        if (!statefulTaskContext.getSourceConfig().isScanNewlyAddedTableEnabled()) {
            // the new added sharding table without history records
            return !maxSplitHighWatermarkMap.containsKey(tableId)
                    && capturedTableFilter.test(tableId);
        }
        return false;
    }

    private void configureFilter() {
        List<FinishedSnapshotSplitInfo> finishedSplitInfos =
                currentBinlogSplit.getFinishedSnapshotSplitInfos();
        Map<TableId, List<FinishedSnapshotSplitInfo>> splitsInfoMap = new HashMap<>();
        Map<TableId, BinlogOffset> tableIdBinlogPositionMap = new HashMap<>();
        // startup mode which is stream only
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

    private Predicate<Event> createEventFilter() {
        // If the startup mode is set as TIMESTAMP, we need to apply a filter on event to drop
        // events earlier than the specified timestamp.

        // NOTE: Here we take user's configuration (statefulTaskContext.getSourceConfig())
        // as the ground truth. This might be fragile if user changes the config and recover
        // the job from savepoint / checkpoint, as there might be conflict between user's config
        // and the state in savepoint / checkpoint. But as we don't promise compatibility of
        // checkpoint after changing the config, this is acceptable for now.
        StartupOptions startupOptions = statefulTaskContext.getSourceConfig().getStartupOptions();
        if (startupOptions.startupMode.equals(StartupMode.TIMESTAMP)) {
            if (startupOptions.binlogOffset == null) {
                throw new NullPointerException(
                        "The startup option was set to TIMESTAMP "
                                + "but unable to find starting binlog offset. Please check if the timestamp is specified in "
                                + "configuration. ");
            }
            long startTimestampSec = startupOptions.binlogOffset.getTimestampSec();
            // We only skip data change event, as other kinds of events are necessary for updating
            // some internal state inside MySqlStreamingChangeEventSource
            LOG.info(
                    "Creating event filter that dropping row mutation events before timestamp in second {}",
                    startTimestampSec);
            return event -> {
                if (!EventType.isRowMutation(getEventType(event))) {
                    return true;
                }
                return event.getHeader().getTimestamp() >= startTimestampSec * 1000;
            };
        }
        return event -> true;
    }

    public void stopBinlogReadTask() {
        currentTaskRunning = false;
        // Terminate the while loop in MySqlStreamingChangeEventSource's execute method
        changeEventSourceContext.stopChangeEventSource();
    }

    private EventType getEventType(Event event) {
        return event.getHeader().getEventType();
    }

    @VisibleForTesting
    public ExecutorService getExecutorService() {
        return executorService;
    }

    @VisibleForTesting
    MySqlBinlogSplitReadTask getBinlogSplitReadTask() {
        return binlogSplitReadTask;
    }

    @VisibleForTesting
    public StoppableChangeEventSourceContext getChangeEventSourceContext() {
        return changeEventSourceContext;
    }
}
