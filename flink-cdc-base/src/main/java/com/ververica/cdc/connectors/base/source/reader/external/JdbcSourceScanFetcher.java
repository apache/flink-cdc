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

package com.ververica.cdc.connectors.base.source.reader.external;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.utils.SourceRecordUtils;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fetcher to fetch data from table split, the split is the snapshot split {@link SnapshotSplit}.
 */
public class JdbcSourceScanFetcher implements Fetcher<SourceRecord, SourceSplitBase> {

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
                                        "Execute snapshot read task for mysql split %s fail",
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
    public Iterator<SourceRecord> pollSplitRecords() throws InterruptedException {
        checkReadException();

        if (hasNextElement.get()) {
            // data input: [low watermark event][snapshot events][high watermark event][binlog
            // events][binlog-end event]
            // data output: [low watermark event][normalized events][high watermark event]
            boolean reachBinlogEnd = false;
            final List<SourceRecord> sourceRecords = new ArrayList<>();
            while (!reachBinlogEnd) {
                List<DataChangeEvent> batch = queue.poll();
                for (DataChangeEvent event : batch) {
                    sourceRecords.add(event.getRecord());
                    if (SourceRecordUtils.isEndWatermarkEvent(event.getRecord())) {
                        reachBinlogEnd = true;
                        break;
                    }
                }
            }
            // snapshot split return its data once
            hasNextElement.set(false);
            return SourceRecordUtils.normalizedSplitRecords(
                            currentSnapshotSplit,
                            sourceRecords,
                            new SchemaNameAdjuster() {
                                @Override
                                public String adjust(String proposedName) {
                                    return null;
                                }
                            })
                    .iterator();
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
}
