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

package com.ververica.cdc.connectors.base.source.reader.external;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.utils.SourceRecordUtils;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.getTableId;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.splitKeyRangeContains;

/** Fetcher to fetch data from table split, the split is the stream split {@link StreamSplit}. */
public class JdbcSourceStreamFetcher implements Fetcher<SourceRecord, SourceSplitBase> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceStreamFetcher.class);

    private final JdbcSourceFetchTaskContext taskContext;
    private final ExecutorService executor;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile Throwable readException;

    private FetchTask<SourceSplitBase> streamFetchTask;
    private StreamSplit currentStreamSplit;
    private Map<TableId, List<FinishedSnapshotSplitInfo>> finishedSplitsInfo;
    // tableId -> the max splitHighWatermark
    private Map<TableId, Offset> maxSplitHighWatermarkMap;
    private Tables.TableFilter capturedTableFilter;

    public JdbcSourceStreamFetcher(JdbcSourceFetchTaskContext taskContext, int subTaskId) {
        this.taskContext = taskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("debezium-reader-" + subTaskId).build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
    }

    @Override
    public void submitTask(FetchTask<SourceSplitBase> fetchTask) {
        this.streamFetchTask = fetchTask;
        this.currentStreamSplit = fetchTask.getSplit().asStreamSplit();
        configureFilter();
        taskContext.configure(currentStreamSplit);
        this.queue = taskContext.getQueue();
        executor.submit(
                () -> {
                    try {
                        streamFetchTask.execute(taskContext);
                    } catch (Exception e) {
                        LOG.error(
                                String.format(
                                        "Execute binlog read task for stream split %s fail",
                                        currentStreamSplit),
                                e);
                        readException = e;
                    }
                });
    }

    @Override
    public boolean isFinished() {
        return currentStreamSplit == null || !streamFetchTask.isRunning();
    }

    @Nullable
    @Override
    public Iterator<SourceRecord> pollSplitRecords() throws InterruptedException {
        checkReadException();
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        if (streamFetchTask.isRunning()) {
            List<DataChangeEvent> batch = queue.poll();
            for (DataChangeEvent event : batch) {
                if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                }
            }
        }
        return sourceRecords.iterator();
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentStreamSplit, readException.getMessage()),
                    readException);
        }
    }

    @Override
    public void close() {}

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
            Offset position = taskContext.getStreamOffset(sourceRecord);
            if (hasEnterPureBinlogPhase(tableId, position)) {
                return true;
            }
            // only the table who captured snapshot splits need to filter
            if (finishedSplitsInfo.containsKey(tableId)) {
                RowType splitKeyType =
                        taskContext.getSplitType(taskContext.getDatabaseSchema().tableFor(tableId));
                Object[] key =
                        SourceRecordUtils.getSplitKey(
                                splitKeyType, sourceRecord, taskContext.getSchemaNameAdjuster());
                for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo.get(tableId)) {
                    if (splitKeyRangeContains(
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

    private boolean hasEnterPureBinlogPhase(TableId tableId, Offset position) {
        // the existed tables those have finished snapshot reading
        if (maxSplitHighWatermarkMap.containsKey(tableId)
                && position.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))) {
            return true;
        }

        return !maxSplitHighWatermarkMap.containsKey(tableId)
                && capturedTableFilter.isIncluded(tableId);
    }

    private void configureFilter() {
        List<FinishedSnapshotSplitInfo> finishedSplitInfos =
                currentStreamSplit.getFinishedSnapshotSplitInfos();
        Map<TableId, List<FinishedSnapshotSplitInfo>> splitsInfoMap = new HashMap<>();
        Map<TableId, Offset> tableIdBinlogPositionMap = new HashMap<>();
        // latest-offset mode
        if (finishedSplitInfos.isEmpty()) {
            for (TableId tableId : currentStreamSplit.getTableSchemas().keySet()) {
                tableIdBinlogPositionMap.put(tableId, currentStreamSplit.getStartingOffset());
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

                Offset highWatermark = finishedSplitInfo.getHighWatermark();
                Offset maxHighWatermark = tableIdBinlogPositionMap.get(tableId);
                if (maxHighWatermark == null || highWatermark.isAfter(maxHighWatermark)) {
                    tableIdBinlogPositionMap.put(tableId, highWatermark);
                }
            }
        }
        this.finishedSplitsInfo = splitsInfoMap;
        this.maxSplitHighWatermarkMap = tableIdBinlogPositionMap;
    }
}
