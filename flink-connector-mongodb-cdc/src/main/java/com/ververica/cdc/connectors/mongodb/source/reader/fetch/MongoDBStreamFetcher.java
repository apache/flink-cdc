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

import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.base.source.reader.external.Fetcher;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
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

/** Fetcher to fetch data from stream split, the split is the stream split {@link StreamSplit}. */
public class MongoDBStreamFetcher
        implements Fetcher<SourceRecord, SourceSplitBase<CollectionId, CollectionSchema>> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBStreamFetcher.class);

    private final MongoDBFetchTaskContext taskContext;
    private final ExecutorService executor;

    private volatile Throwable readException;

    private MongoDBStreamFetchTask streamFetchTask;
    private StreamSplit<CollectionId, CollectionSchema> currentStreamSplit;

    public MongoDBStreamFetcher(MongoDBFetchTaskContext taskContext, int subTaskId) {
        this.taskContext = taskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("mongodb-stream-reader-" + subTaskId)
                        .build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
    }

    @Override
    public void submitTask(FetchTask<SourceSplitBase<CollectionId, CollectionSchema>> fetchTask) {
        this.streamFetchTask = (MongoDBStreamFetchTask) fetchTask;
        this.currentStreamSplit = fetchTask.getSplit().asStreamSplit();

        executor.submit(
                () -> {
                    try {
                        streamFetchTask.execute(taskContext);
                    } catch (Exception e) {
                        LOG.error(
                                String.format(
                                        "Execute stream read task for split %s fail",
                                        currentStreamSplit),
                                e);
                        readException = e;
                    }
                });
    }

    @Nullable
    @Override
    public Iterator<SourceRecord> pollSplitRecords() {
        checkReadException();
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        if (streamFetchTask.isRunning()) {
            try {
                sourceRecords.addAll(streamFetchTask.poll());
            } catch (Exception e) {
                readException = e;
            }
        }
        return sourceRecords.iterator();
    }

    @Override
    public boolean isFinished() {
        return currentStreamSplit == null || !streamFetchTask.isRunning();
    }

    @Override
    public void close() {
        if (streamFetchTask != null && streamFetchTask.isRunning()) {
            streamFetchTask.close();
        }
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
}
