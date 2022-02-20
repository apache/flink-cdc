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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.kafka.connect.source.heartbeat.HeartbeatManager;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBChangeStreamConfig;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.offset.MongoDBChangeStreamOffset;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBRecords;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBStreamSplit;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.CLUSTER_TIME_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.HEARTBEAT_TOPIC_NAME;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.HEARTBEAT_VALUE_SCHEMA;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_COLLECTION_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_DATABASE_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.SNAPSHOT_KEY_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.SOURCE_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.TIMESTAMP_KEY_FIELD;
import static com.ververica.cdc.connectors.mongodb.source.split.MongoDBStreamSplit.toSuspendedStreamSplit;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.FAILED_TO_PARSE_ERROR;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.ILLEGAL_OPERATION_ERROR;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.UNAUTHORIZED_ERROR;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.UNKNOWN_FIELD_ERROR;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.clientFor;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getChangeStreamIterable;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.bsonTimestampFromEpochMillis;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.createHeartbeatPartitionMap;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.createPartitionMap;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.createSourceOffsetMap;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.createSourceRecord;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.currentBsonTimestamp;
import static org.apache.flink.util.IOUtils.closeQuietly;

/** MongoDB change stream split reader. */
public class MongoDBStreamSplitReader implements MongoDBSplitReader<MongoDBStreamSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBStreamSplitReader.class);

    private final MongoDBSourceConfig sourceConfig;
    private final int subtaskId;

    private final Time time = new SystemTime();
    private final AtomicBoolean isRunning;

    private MongoDBStreamSplit currentSplit;
    private HeartbeatManager heartbeatManager;
    private MongoChangeStreamCursor<? extends BsonDocument> cursor;
    private boolean supportsStartAtOperationTime = true;
    private boolean supportsStartAfter = true;

    public MongoDBStreamSplitReader(MongoDBSourceConfig sourceConfig, int subtaskId) {
        this.sourceConfig = sourceConfig;
        this.subtaskId = subtaskId;
        this.isRunning = new AtomicBoolean(false);
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        final long startPoll = time.milliseconds();
        long nextUpdate = startPoll + sourceConfig.getPollAwaitTimeMillis();
        long maxBatchSize = sourceConfig.getPollMaxBatchSize();

        List<SourceRecord> sourceRecords = new ArrayList<>();
        while (isRunning.get() && cursor != null) {
            Optional<BsonDocument> next = Optional.ofNullable(cursor.tryNext());
            long untilNext = nextUpdate - time.milliseconds();

            if (!next.isPresent()) {
                if (untilNext > 0) {
                    LOG.debug("Waiting {} ms to poll", untilNext);
                    time.sleep(untilNext);
                    continue;
                }

                if (sourceRecords.isEmpty() && heartbeatManager != null) {
                    heartbeatManager
                            .heartbeat()
                            .map(this::normalizeHeartbeatRecord)
                            .ifPresent(sourceRecords::add);
                }

                return MongoDBRecords.forRecords(currentSplit.splitId(), sourceRecords.iterator());
            } else {
                BsonDocument changeStreamDocument = next.get();
                MongoNamespace namespace = getMongoNamespace(changeStreamDocument);

                BsonDocument valueDocument = normalizeChangeStreamDocument(changeStreamDocument);
                BsonDocument keyDocument = new BsonDocument(ID_FIELD, valueDocument.get(ID_FIELD));

                LOG.trace("Adding {} to {}", valueDocument, namespace.getFullName());

                sourceRecords.add(
                        createSourceRecord(
                                createPartitionMap(
                                        sourceConfig.getHosts(),
                                        namespace.getDatabaseName(),
                                        namespace.getCollectionName()),
                                createSourceOffsetMap(valueDocument.getDocument(ID_FIELD), false),
                                namespace.getFullName(),
                                keyDocument,
                                valueDocument));

                if (sourceRecords.size() == maxBatchSize) {
                    LOG.debug("Reached 'poll.max.batch.size': {}, returning records", maxBatchSize);
                    return MongoDBRecords.forRecords(
                            currentSplit.splitId(), sourceRecords.iterator());
                }
            }
        }
        return MongoDBRecords.forFinishedSplit(currentSplit.splitId());
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MongoDBStreamSplit> splitsChanges) {
        MongoDBStreamSplit newSplit = splitsChanges.splits().get(0).asStreamSplit();
        LOG.info("Handle stream split changes of {}", newSplit);
        currentSplit = newSplit;
        openCursor();
        openHeartbeatManager();
        isRunning.set(true);
    }

    @Override
    public boolean canAssignNextSplit() {
        return currentSplit == null && !isRunning.get();
    }

    @Override
    public void suspend() {
        isRunning.set(false);
    }

    @Override
    public void wakeUp() {
        isRunning.compareAndSet(false, true);
    }

    @Override
    public void close() {
        LOG.info("Stopping MongoDB stream reader");
        isRunning.set(false);

        closeCursor();
        closeHeartbeatManger();
        currentSplit = toSuspendedStreamSplit(currentSplit);
    }

    private void openCursor() {
        MongoClient mongoClient = clientFor(sourceConfig);
        MongoDBChangeStreamConfig changeStreamConfig = currentSplit.getChangeStreamConfig();

        ChangeStreamIterable<Document> changeStreamIterable =
                getChangeStreamIterable(mongoClient, changeStreamConfig);

        MongoDBChangeStreamOffset offset = currentSplit.getChangeStreamOffset();
        BsonDocument resumeToken = offset.getResumeToken();
        BsonTimestamp timestamp = offset.getTimestamp();

        if (resumeToken != null) {
            if (supportsStartAfter) {
                LOG.info("Open the change stream after the previous offset: {}", resumeToken);
                changeStreamIterable.startAfter(resumeToken);
            } else {
                LOG.info(
                        "Open the change stream after the previous offset using resumeAfter: {}",
                        resumeToken);
                changeStreamIterable.resumeAfter(resumeToken);
            }
        } else {
            if (supportsStartAtOperationTime) {
                LOG.info("Open the change stream at the timestamp: {}", timestamp);
                changeStreamIterable.startAtOperationTime(timestamp);
            } else {
                LOG.warn("Open the change stream of the latest offset");
            }
        }

        try {
            this.cursor =
                    (MongoChangeStreamCursor<BsonDocument>)
                            changeStreamIterable.withDocumentClass(BsonDocument.class).cursor();
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == FAILED_TO_PARSE_ERROR
                    || e.getErrorCode() == UNKNOWN_FIELD_ERROR) {
                if (e.getErrorMessage().contains("startAtOperationTime")) {
                    supportsStartAtOperationTime = false;
                    openCursor();
                } else if (e.getErrorMessage().contains("startAfter")) {
                    supportsStartAfter = false;
                    openCursor();
                } else {
                    LOG.error("Open change stream failed ", e);
                    throw new FlinkRuntimeException("Open change stream failed", e);
                }
            } else if (e.getErrorCode() == ILLEGAL_OPERATION_ERROR) {
                LOG.error(
                        "Illegal $changeStream operation: {} {}",
                        e.getErrorMessage(),
                        e.getErrorCode());
                throw new FlinkRuntimeException("Illegal $changeStream operation", e);
            } else if (e.getErrorCode() == UNAUTHORIZED_ERROR) {
                LOG.error(
                        "Unauthorized $changeStream operation: {} {}",
                        e.getErrorMessage(),
                        e.getErrorCode());
                throw new FlinkRuntimeException("Unauthorized $changeStream operation", e);
            } else {
                LOG.error("Open change stream failed ", e);
                throw new FlinkRuntimeException("Open change stream failed", e);
            }
        }
    }

    private void openHeartbeatManager() {
        this.heartbeatManager =
                new HeartbeatManager(
                        time,
                        cursor,
                        sourceConfig.getHeartbeatIntervalMillis(),
                        HEARTBEAT_TOPIC_NAME,
                        createHeartbeatPartitionMap(sourceConfig.getHosts()));
    }

    private void closeCursor() {
        closeQuietly(cursor);
        cursor = null;
    }

    private void closeHeartbeatManger() {
        heartbeatManager = null;
    }

    private BsonDocument normalizeChangeStreamDocument(BsonDocument changeStreamDocument) {
        // ts_ms: It indicates the time at which the reader processed the event.
        changeStreamDocument.put(TIMESTAMP_KEY_FIELD, new BsonInt64(System.currentTimeMillis()));

        // source
        BsonDocument source = new BsonDocument();
        source.put(SNAPSHOT_KEY_FIELD, new BsonString("false"));

        // cluster time after MongoDB version 4.0.
        if (!changeStreamDocument.containsKey(CLUSTER_TIME_FIELD)) {
            // Legacy ts_ms of MongoDB version [3.6, 4.0)
            if (changeStreamDocument.containsKey(TIMESTAMP_KEY_FIELD)) {
                long timestampValue = changeStreamDocument.getInt64(TIMESTAMP_KEY_FIELD).getValue();
                BsonTimestamp legacyTimestamp = bsonTimestampFromEpochMillis(timestampValue);
                changeStreamDocument.put(CLUSTER_TIME_FIELD, legacyTimestamp);
            } else { // Fallback to current timestamp.
                LOG.warn(
                        "Cannot extract clusterTime from change stream event, fallback to current timestamp.");
                changeStreamDocument.put(CLUSTER_TIME_FIELD, currentBsonTimestamp());
            }
        }

        // source.ts_ms
        // It indicates the time that the change was made in the database. If the record is read
        // from snapshot of the table instead of the change stream, the value is always 0.
        BsonTimestamp clusterTime = changeStreamDocument.getTimestamp(CLUSTER_TIME_FIELD);
        Instant clusterInstant = Instant.ofEpochSecond(clusterTime.getTime());
        source.put(TIMESTAMP_KEY_FIELD, new BsonInt64(clusterInstant.toEpochMilli()));
        changeStreamDocument.put(SOURCE_FIELD, source);

        return changeStreamDocument;
    }

    private SourceRecord normalizeHeartbeatRecord(SourceRecord heartbeatRecord) {
        final Struct heartbeatValue = new Struct(HEARTBEAT_VALUE_SCHEMA);
        heartbeatValue.put(TIMESTAMP_KEY_FIELD, Instant.now().toEpochMilli());

        return new SourceRecord(
                heartbeatRecord.sourcePartition(),
                heartbeatRecord.sourceOffset(),
                heartbeatRecord.topic(),
                heartbeatRecord.keySchema(),
                heartbeatRecord.key(),
                HEARTBEAT_VALUE_SCHEMA,
                heartbeatValue);
    }

    private MongoNamespace getMongoNamespace(BsonDocument changeStreamDocument) {
        BsonDocument ns = changeStreamDocument.getDocument(NAMESPACE_FIELD);

        return new MongoNamespace(
                ns.getString(NAMESPACE_DATABASE_FIELD).getValue(),
                ns.getString(NAMESPACE_COLLECTION_FIELD).getValue());
    }
}
