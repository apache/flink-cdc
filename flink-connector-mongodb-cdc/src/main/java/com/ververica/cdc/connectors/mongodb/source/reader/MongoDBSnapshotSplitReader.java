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

package com.ververica.cdc.connectors.mongodb.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.OperationType;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBRecords;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSnapshotSplit;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.COPY_KEY_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.DOCUMENT_KEY_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.FULL_DOCUMENT_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_COLLECTION_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_DATABASE_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.OPERATION_TYPE_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.SNAPSHOT_KEY_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.SOURCE_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.TIMESTAMP_KEY_FIELD;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.clientFor;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.collectionFor;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.createPartitionMap;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.createSourceOffsetMap;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.createSourceRecord;

/** MongoDB snapshot split reader. */
public class MongoDBSnapshotSplitReader implements MongoDBSplitReader<MongoDBSnapshotSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSnapshotSplitReader.class);

    private final MongoDBSourceConfig sourceConfig;
    private final int subtaskId;

    private final Time time;
    private final ExecutorService executor;
    private volatile BlockingQueue<SourceRecord> queue;

    private volatile boolean closed;
    private volatile boolean isRunning;
    private volatile Throwable readException;

    private MongoDBSnapshotSplit currentSplit;
    private AtomicBoolean reachEnd;

    public MongoDBSnapshotSplitReader(MongoDBSourceConfig sourceConfig, int subtaskId) {
        this.sourceConfig = sourceConfig;
        this.subtaskId = subtaskId;
        this.time = new SystemTime();
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("mongodb-snapshot-reader-" + subtaskId)
                        .build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.queue = new ArrayBlockingQueue<>(sourceConfig.getCopyExistingQueueSize());
        this.closed = false;
        this.isRunning = false;
        this.reachEnd = new AtomicBoolean(false);
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        checkReadException();

        final long startPoll = time.milliseconds();
        LOG.debug("Fetching snapshot records start: {} of subtask {}", startPoll, subtaskId);

        int maxBatchSize = sourceConfig.getPollMaxBatchSize();
        long nextUpdate = startPoll + sourceConfig.getPollAwaitTimeMillis();

        List<SourceRecord> sourceRecords = new ArrayList<>();
        while (isRunning) {
            SourceRecord record = queue.poll();
            long untilNext = nextUpdate - time.milliseconds();

            if (record == null) {
                if (!sourceRecords.isEmpty()) {
                    return MongoDBRecords.forRecords(
                            currentSplit.splitId(), sourceRecords.iterator());
                }

                if (reachEnd.get()) {
                    LOG.info(
                            "Snapshot split {} is finished of subtask {}", currentSplit, subtaskId);
                    return MongoDBRecords.forFinishedSplit(currentSplit.splitId());
                }

                if (untilNext > 0) {
                    LOG.debug("Waiting {} ms to poll", untilNext);
                    time.sleep(untilNext);
                }
            } else {
                sourceRecords.add(record);
                if (sourceRecords.size() == maxBatchSize) {
                    LOG.debug("Reached 'poll.max.batch.size': {}, returning records", maxBatchSize);
                    return MongoDBRecords.forRecords(
                            currentSplit.splitId(), sourceRecords.iterator());
                }
            }
        }

        return MongoDBRecords.forRecords(currentSplit.splitId(), sourceRecords.iterator());
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MongoDBSnapshotSplit> splitsChanges) {
        MongoDBSnapshotSplit newSplit = splitsChanges.splits().get(0).asSnapshotSplit();
        LOG.info("Handle snapshot split changes of {}, subtask {}", newSplit, subtaskId);
        this.currentSplit = newSplit;
        this.reachEnd.set(false);
        this.executor.submit(this::doSnapshot);
    }

    private void doSnapshot() {
        if (currentSplit == null) {
            return;
        }
        isRunning = true;
        CollectionId collectionId = currentSplit.getCollectionId();

        MongoCursor<RawBsonDocument> cursor = null;
        try {
            MongoClient mongoClient = clientFor(sourceConfig);
            MongoCollection<RawBsonDocument> collection =
                    collectionFor(mongoClient, collectionId, RawBsonDocument.class);

            // Using min and max operation to perform a specific index scan
            // See: https://www.mongodb.com/docs/manual/reference/method/cursor.min/
            cursor =
                    collection
                            .find()
                            .min(currentSplit.getMin())
                            .max(currentSplit.getMax())
                            .hint(currentSplit.getHint())
                            .batchSize(sourceConfig.getBatchSize())
                            .noCursorTimeout(true)
                            .cursor();

            BsonDocument keyDocument, valueDocument;
            while (cursor.hasNext()) {
                if (!isRunning) {
                    throw new InterruptedException(
                            "Interrupted while snapshotting collection "
                                    + collectionId.identifier());
                }

                valueDocument = normalizeSnapshotDocument(collectionId, cursor.next());
                keyDocument = new BsonDocument(ID_FIELD, valueDocument.get(ID_FIELD));

                queue.put(
                        (createSourceRecord(
                                createPartitionMap(
                                        sourceConfig.getHosts(),
                                        collectionId.getDatabaseName(),
                                        collectionId.getCollectionName()),
                                createSourceOffsetMap(keyDocument, true),
                                collectionId.identifier(),
                                keyDocument,
                                valueDocument)));
            }

            reachEnd.compareAndSet(false, true);
        } catch (Exception e) {
            isRunning = false;
            LOG.error(
                    String.format(
                            "Execute snapshot read subtask {} for mongo split %s fail",
                            subtaskId, currentSplit),
                    e);
            readException = e;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public boolean canAssignNextSplit() {
        return currentSplit == null || (isRunning && reachEnd.get());
    }

    @Override
    public void suspend() {
        this.isRunning = false;
    }

    @Override
    public void wakeUp() {
        this.isRunning = true;
    }

    @Override
    public void close() {
        if (!closed) {
            LOG.debug("Shutting down SnapshotSplitReader of subtask {}", subtaskId);
            closed = true;
            isRunning = false;
            currentSplit = null;
            executor.shutdownNow();
        }
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error of subtask {} due to %s.",
                            currentSplit, subtaskId, readException.getMessage()),
                    readException);
        }
    }

    private BsonDocument normalizeSnapshotDocument(
            final CollectionId collectionId, final BsonDocument originalDocument) {
        final BsonDocument valueDocument = new BsonDocument();

        // id
        BsonDocument id = new BsonDocument();
        id.put(ID_FIELD, originalDocument.get(ID_FIELD));
        id.put(COPY_KEY_FIELD, new BsonString("true"));
        valueDocument.put(ID_FIELD, id);

        // operationType
        valueDocument.put(OPERATION_TYPE_FIELD, new BsonString(OperationType.INSERT.getValue()));

        // ns
        BsonDocument ns = new BsonDocument();
        ns.put(NAMESPACE_DATABASE_FIELD, new BsonString(collectionId.getDatabaseName()));
        ns.put(NAMESPACE_COLLECTION_FIELD, new BsonString(collectionId.getCollectionName()));
        valueDocument.put(NAMESPACE_FIELD, ns);

        // documentKey
        valueDocument.put(
                DOCUMENT_KEY_FIELD, new BsonDocument(ID_FIELD, originalDocument.get(ID_FIELD)));

        // fullDocument
        valueDocument.put(FULL_DOCUMENT_FIELD, originalDocument);

        // ts_ms: It indicates the time at which the reader processed the event.
        valueDocument.put(TIMESTAMP_KEY_FIELD, new BsonInt64(System.currentTimeMillis()));

        // source
        BsonDocument source = new BsonDocument();
        source.put(SNAPSHOT_KEY_FIELD, new BsonString("true"));
        // source.ts_ms
        // It indicates the time that the change was made in the database. If the record is read
        // from snapshot of the table instead of the change stream, the value is always 0.
        source.put(TIMESTAMP_KEY_FIELD, new BsonInt64(0L));
        valueDocument.put(SOURCE_FIELD, source);

        return valueDocument;
    }
}
