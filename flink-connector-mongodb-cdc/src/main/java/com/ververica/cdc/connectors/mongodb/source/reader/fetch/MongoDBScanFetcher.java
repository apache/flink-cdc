/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.reader.fetch;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.base.source.reader.external.Fetcher;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fetcher to fetch data from snapshot split, the split is the snapshot split {@link SnapshotSplit}.
 */
public class MongoDBScanFetcher
        implements Fetcher<SourceRecord, SourceSplitBase<CollectionId, CollectionSchema>> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBScanFetcher.class);

    public AtomicBoolean hasNextElement;
    public AtomicBoolean reachEnd;

    private final MongoDBFetchTaskContext taskContext;
    private final ExecutorService executor;
    private final Time time = new SystemTime();

    private volatile Queue<SourceRecord> copyExistingQueue;
    private volatile Throwable readException;

    // task to read snapshot for current split
    private MongoDBScanFetchTask snapshotSplitReadTask;
    private SnapshotSplit<CollectionId, CollectionSchema> currentSnapshotSplit;

    public MongoDBScanFetcher(MongoDBFetchTaskContext taskContext, int subtaskId) {
        this.taskContext = taskContext;
        BlockingQueue<SourceRecord> copyExistingQueue =
                new ArrayBlockingQueue<>(taskContext.getSourceConfig().getCopyExistingQueueSize());
        taskContext.setCopyExistingQueue(copyExistingQueue);
        this.copyExistingQueue = copyExistingQueue;

        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("mongodb-snapshot-reader-" + subtaskId)
                        .build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.hasNextElement = new AtomicBoolean(false);
        this.reachEnd = new AtomicBoolean(false);
    }

    @Nullable
    @Override
    public Iterator<SourceRecord> pollSplitRecords() throws InterruptedException {
        checkReadException();

        MongoDBSourceConfig sourceConfig = taskContext.getSourceConfig();
        int pollMaxBatchSize = sourceConfig.getPollMaxBatchSize();
        int pollAwaitTimeMillis = sourceConfig.getPollAwaitTimeMillis();

        if (hasNextElement.get()) {
            final long startPoll = time.milliseconds();
            long nextUpdate = startPoll + pollAwaitTimeMillis;

            final List<SourceRecord> sourceRecords = new ArrayList<>();
            while (true) {
                long untilNext = nextUpdate - time.milliseconds();
                SourceRecord record = copyExistingQueue.poll();
                if (record == null) {
                    if (snapshotSplitReadTask.isFinished() && copyExistingQueue.isEmpty()) {
                        hasNextElement.set(false);
                        break;
                    }

                    if (untilNext > 0) {
                        LOG.debug("Waiting {} ms to poll snapshot records", untilNext);
                        time.sleep(untilNext);
                        continue;
                    }
                    break;
                }

                sourceRecords.add(record);
                if (sourceRecords.size() >= pollMaxBatchSize) {
                    LOG.debug(
                            "Reached max batch size: {}, returning snapshot records",
                            pollMaxBatchSize);
                    break;
                }
            }
            return sourceRecords.iterator();
        }
        // the data has been polled, no more data
        reachEnd.compareAndSet(false, true);
        return null;
    }

    @Override
    public void submitTask(FetchTask<SourceSplitBase<CollectionId, CollectionSchema>> fetchTask) {
        this.snapshotSplitReadTask = (MongoDBScanFetchTask) fetchTask;
        this.currentSnapshotSplit = fetchTask.getSplit().asSnapshotSplit();

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

    @Override
    public void close() {}

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentSnapshotSplit, readException.getMessage()),
                    readException);
        }
    }
}
