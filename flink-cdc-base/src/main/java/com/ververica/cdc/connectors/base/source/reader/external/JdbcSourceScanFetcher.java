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

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Struct;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.formatMessageTimestamp;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.getSplitKey;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.isEndWatermarkEvent;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.isHighWatermarkEvent;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.isLowWatermarkEvent;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.splitKeyRangeContains;
import static com.ververica.cdc.connectors.base.utils.SourceRecordUtils.upsertBinlog;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Fetcher to fetch data from table split, the split is the snapshot split {@link SnapshotSplit}.
 */
public class JdbcSourceScanFetcher implements Fetcher<SourceRecords, SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceScanFetcher.class);

    public AtomicBoolean hasNextElement;
    public AtomicBoolean reachEnd;

    private final JdbcSourceFetchTaskContext taskContext;
    private final ExecutorService executor;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile Throwable readException;

    // task to read snapshot for current split
    private FetchTask<SourceSplitBase> snapshotSplitReadTask;
    private SnapshotSplit currentSnapshotSplit;
    private SchemaNameAdjuster nameAdjuster;

    public JdbcSourceScanFetcher(JdbcSourceFetchTaskContext taskContext, int subtaskId) {
        this.taskContext = taskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("debezium-snapshot-reader-" + subtaskId)
                        .build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.hasNextElement = new AtomicBoolean(false);
        this.reachEnd = new AtomicBoolean(false);
    }

    @Override
    public void submitTask(FetchTask<SourceSplitBase> fetchTask) {
        this.snapshotSplitReadTask = fetchTask;
        this.currentSnapshotSplit = fetchTask.getSplit().asSnapshotSplit();
        taskContext.configure(currentSnapshotSplit);
        this.nameAdjuster = taskContext.getSchemaNameAdjuster();
        this.queue = taskContext.getQueue();
        this.hasNextElement.set(true);
        this.reachEnd.set(false);
        executor.submit(
                () -> {
                    try {
                        snapshotSplitReadTask.execute(taskContext);
                    } catch (Exception e) {
                        LOG.error(
                                String.format(
                                        "Execute snapshot read task for snapshot split %s fail",
                                        currentSnapshotSplit),
                                e);
                        readException = e;
                    }
                });
    }

    @Override
    public boolean isFinished() {
        return currentSnapshotSplit == null
                || (!snapshotSplitReadTask.isRunning() && !hasNextElement.get() && reachEnd.get());
    }

    @Nullable
    @Override
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        checkReadException();

        if (hasNextElement.get()) {
            // data input: [low watermark event][snapshot events][high watermark event][binlog
            // events][binlog-end event]
            // data output: [low watermark event][normalized events][high watermark event]
            boolean reachBinlogStart = false;
            boolean reachBinlogEnd = false;
            SourceRecord lowWatermark = null;
            SourceRecord highWatermark = null;
            Map<Struct, SourceRecord> snapshotRecords = new HashMap<>();
            while (!reachBinlogEnd) {
                checkReadException();
                List<DataChangeEvent> batch = queue.poll();
                for (DataChangeEvent event : batch) {
                    SourceRecord record = event.getRecord();
                    if (lowWatermark == null) {
                        lowWatermark = record;
                        assertLowWatermark(lowWatermark);
                        continue;
                    }

                    if (highWatermark == null && isHighWatermarkEvent(record)) {
                        highWatermark = record;
                        // snapshot events capture end and begin to capture binlog events
                        reachBinlogStart = true;
                        continue;
                    }

                    if (reachBinlogStart && isEndWatermarkEvent(record)) {
                        // capture to end watermark events, stop the loop
                        reachBinlogEnd = true;
                        break;
                    }

                    if (!reachBinlogStart) {
                        snapshotRecords.put((Struct) record.key(), record);
                    } else {
                        if (isRequiredBinlogRecord(record)) {
                            // upsert binlog events through the record key
                            upsertBinlog(snapshotRecords, record);
                        }
                    }
                }
            }
            // snapshot split return its data once
            hasNextElement.set(false);

            final List<SourceRecord> normalizedRecords = new ArrayList<>();
            normalizedRecords.add(lowWatermark);
            normalizedRecords.addAll(formatMessageTimestamp(snapshotRecords.values()));
            normalizedRecords.add(highWatermark);

            final List<SourceRecords> sourceRecordsSet = new ArrayList<>();
            sourceRecordsSet.add(new SourceRecords(normalizedRecords));
            return sourceRecordsSet.iterator();
        }
        // the data has been polled, no more data
        reachEnd.compareAndSet(false, true);
        return null;
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentSnapshotSplit, readException.getMessage()),
                    readException);
        }
    }

    @Override
    public void close() {}

    private void assertLowWatermark(SourceRecord lowWatermark) {
        checkState(
                isLowWatermarkEvent(lowWatermark),
                String.format(
                        "The first record should be low watermark signal event, but actual is %s",
                        lowWatermark));
    }

    private boolean isRequiredBinlogRecord(SourceRecord record) {
        if (isDataChangeRecord(record)) {
            Object[] key =
                    getSplitKey(currentSnapshotSplit.getSplitKeyType(), record, nameAdjuster);
            return splitKeyRangeContains(
                    key, currentSnapshotSplit.getSplitStart(), currentSnapshotSplit.getSplitEnd());
        }
        return false;
    }
}
