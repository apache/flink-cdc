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

package org.apache.flink.cdc.connectors.mongodb.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.offset.ChangeStreamDescriptor;
import org.apache.flink.cdc.connectors.mongodb.source.offset.ChangeStreamOffset;
import org.apache.flink.cdc.connectors.mongodb.source.utils.MongoRecordUtils;
import org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils;
import org.apache.flink.util.FlinkRuntimeException;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.kafka.connect.source.heartbeat.HeartbeatManager;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
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

import java.time.Instant;
import java.util.Optional;

import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.CLUSTER_TIME_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.DOCUMENT_KEY_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.HEARTBEAT_TOPIC_NAME;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.HEARTBEAT_VALUE_SCHEMA;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_COLLECTION_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_DATABASE_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.OPERATION_TYPE_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.SNAPSHOT_KEY_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.SOURCE_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.TIMESTAMP_KEY_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.WATERMARK_TOPIC_NAME;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.FAILED_TO_PARSE_ERROR;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.ILLEGAL_OPERATION_ERROR;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.UNAUTHORIZED_ERROR;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.UNKNOWN_FIELD_ERROR;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.clientFor;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.getChangeStreamIterable;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.getCurrentClusterTime;

/** The task to work for fetching data of MongoDB stream split . */
public class MongoDBStreamFetchTask implements FetchTask<SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBStreamFetchTask.class);

    private final StreamSplit streamSplit;
    private volatile boolean taskRunning = false;

    private MongoDBSourceConfig sourceConfig;
    private final Time time = new SystemTime();
    private boolean supportsStartAtOperationTime = true;
    private boolean supportsStartAfter = true;

    public MongoDBStreamFetchTask(StreamSplit streamSplit) {
        this.streamSplit = streamSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        MongoDBFetchTaskContext taskContext = (MongoDBFetchTaskContext) context;
        this.sourceConfig = taskContext.getSourceConfig();

        ChangeStreamDescriptor descriptor = taskContext.getChangeStreamDescriptor();
        ChangeEventQueue<DataChangeEvent> queue = taskContext.getQueue();

        MongoClient mongoClient = clientFor(sourceConfig);
        MongoChangeStreamCursor<BsonDocument> changeStreamCursor =
                openChangeStreamCursor(descriptor);
        HeartbeatManager heartbeatManager = openHeartbeatManagerIfNeeded(changeStreamCursor);

        final long startPoll = time.milliseconds();
        long nextUpdate = startPoll + sourceConfig.getPollAwaitTimeMillis();
        this.taskRunning = true;
        try {
            while (taskRunning) {
                Optional<BsonDocument> next;
                try {
                    next = Optional.ofNullable(changeStreamCursor.tryNext());
                } catch (MongoCommandException e) {
                    if (MongoUtils.checkIfChangeStreamCursorExpires(e)) {
                        LOG.warn("Change stream cursor has expired, trying to recreate cursor");
                        boolean resumeTokenExpires = MongoUtils.checkIfResumeTokenExpires(e);
                        if (resumeTokenExpires) {
                            LOG.warn(
                                    "Resume token has expired, fallback to timestamp restart mode");
                        }
                        changeStreamCursor = openChangeStreamCursor(descriptor, resumeTokenExpires);
                        heartbeatManager = openHeartbeatManagerIfNeeded(changeStreamCursor);
                        next = Optional.ofNullable(changeStreamCursor.tryNext());
                    } else {
                        throw e;
                    }
                }
                SourceRecord changeRecord = null;
                if (!next.isPresent()) {
                    long untilNext = nextUpdate - time.milliseconds();
                    if (untilNext > 0) {
                        LOG.debug("Waiting {} ms to poll change records", untilNext);
                        time.sleep(untilNext);
                        continue;
                    }

                    if (heartbeatManager != null) {
                        changeRecord =
                                heartbeatManager
                                        .heartbeat()
                                        .map(this::normalizeHeartbeatRecord)
                                        .orElse(null);
                    }
                    // update nextUpdateTime
                    nextUpdate = time.milliseconds() + sourceConfig.getPollAwaitTimeMillis();
                } else {
                    BsonDocument changeStreamDocument = next.get();
                    OperationType operationType = getOperationType(changeStreamDocument);

                    switch (operationType) {
                        case INSERT:
                        case UPDATE:
                        case REPLACE:
                        case DELETE:
                            MongoNamespace namespace = getMongoNamespace(changeStreamDocument);

                            BsonDocument resumeToken = changeStreamDocument.getDocument(ID_FIELD);
                            BsonDocument valueDocument =
                                    normalizeChangeStreamDocument(changeStreamDocument);

                            LOG.trace("Adding {} to {}", valueDocument, namespace.getFullName());

                            changeRecord =
                                    MongoRecordUtils.createSourceRecord(
                                            MongoRecordUtils.createPartitionMap(
                                                    sourceConfig.getScheme(),
                                                    sourceConfig.getHosts(),
                                                    namespace.getDatabaseName(),
                                                    namespace.getCollectionName()),
                                            MongoRecordUtils.createSourceOffsetMap(
                                                    resumeToken, false),
                                            namespace.getFullName(),
                                            changeStreamDocument.getDocument(ID_FIELD),
                                            valueDocument);
                            break;
                        default:
                            // Ignore drop、drop_database、rename and other record to prevent
                            // documentKey from being empty.
                            LOG.info("Ignored {} record: {}", operationType, changeStreamDocument);
                    }
                }
                if (changeRecord != null && !isBoundedRead()) {
                    queue.enqueue(new DataChangeEvent(changeRecord));
                }

                if (isBoundedRead()) {
                    ChangeStreamOffset currentOffset;
                    if (changeRecord != null) {
                        currentOffset =
                                new ChangeStreamOffset(
                                        MongoRecordUtils.getResumeToken(changeRecord));
                        // The log after the high watermark won't emit.
                        if (currentOffset.isAtOrBefore(streamSplit.getEndingOffset())) {
                            queue.enqueue(new DataChangeEvent(changeRecord));
                        }

                    } else {
                        // Heartbeat is not turned on or there is no update event
                        currentOffset = new ChangeStreamOffset(getCurrentClusterTime(mongoClient));
                    }

                    // Reach the high watermark, the binlog fetcher should be finished
                    if (currentOffset.isAtOrAfter(streamSplit.getEndingOffset())) {
                        // send watermark end event
                        SourceRecord watermark =
                                WatermarkEvent.create(
                                        MongoRecordUtils.createWatermarkPartitionMap(
                                                descriptor.toString()),
                                        WATERMARK_TOPIC_NAME,
                                        streamSplit.splitId(),
                                        WatermarkKind.END,
                                        currentOffset);

                        queue.enqueue(new DataChangeEvent(watermark));
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Poll change stream records failed ", e);
            throw e;
        } finally {
            taskRunning = false;
            if (changeStreamCursor != null) {
                changeStreamCursor.close();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public StreamSplit getSplit() {
        return streamSplit;
    }

    @Override
    public void close() {
        taskRunning = false;
    }

    private MongoChangeStreamCursor<BsonDocument> openChangeStreamCursor(
            ChangeStreamDescriptor changeStreamDescriptor) {
        return openChangeStreamCursor(changeStreamDescriptor, false);
    }

    private MongoChangeStreamCursor<BsonDocument> openChangeStreamCursor(
            ChangeStreamDescriptor changeStreamDescriptor, boolean forceTimestampStartup) {
        ChangeStreamOffset offset =
                new ChangeStreamOffset(streamSplit.getStartingOffset().getOffset());
        ChangeStreamIterable<Document> changeStreamIterable =
                getChangeStreamIterable(sourceConfig, changeStreamDescriptor);

        BsonDocument resumeToken = offset.getResumeToken();
        BsonTimestamp timestamp = offset.getTimestamp();

        if (resumeToken != null && !forceTimestampStartup) {
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
            } else if (forceTimestampStartup) {
                LOG.error("Open change stream failed. Unable to resume from timestamp");
                throw new FlinkRuntimeException(
                        "Open change stream failed. Unable to resume from timestamp");
            } else {
                LOG.warn("Open the change stream of the latest offset");
            }
        }

        try {
            return (MongoChangeStreamCursor<BsonDocument>)
                    changeStreamIterable.withDocumentClass(BsonDocument.class).cursor();
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == FAILED_TO_PARSE_ERROR
                    || e.getErrorCode() == UNKNOWN_FIELD_ERROR) {
                if (e.getErrorMessage().contains("startAtOperationTime")) {
                    supportsStartAtOperationTime = false;
                    return openChangeStreamCursor(changeStreamDescriptor);
                } else if (e.getErrorMessage().contains("startAfter")) {
                    supportsStartAfter = false;
                    return openChangeStreamCursor(changeStreamDescriptor);
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
            } else if (!forceTimestampStartup && MongoUtils.checkIfResumeTokenExpires(e)) {
                LOG.info("Failed to open cursor with resume token, fallback to timestamp startup");
                return openChangeStreamCursor(changeStreamDescriptor, true);
            } else {
                LOG.error("Open change stream failed ", e);
                throw new FlinkRuntimeException("Open change stream failed", e);
            }
        }
    }

    private HeartbeatManager openHeartbeatManagerIfNeeded(
            MongoChangeStreamCursor<BsonDocument> changeStreamCursor) {
        if (sourceConfig.getHeartbeatIntervalMillis() > 0) {
            return new HeartbeatManager(
                    time,
                    changeStreamCursor,
                    sourceConfig.getHeartbeatIntervalMillis(),
                    HEARTBEAT_TOPIC_NAME,
                    MongoRecordUtils.createHeartbeatPartitionMap(
                            sourceConfig.getScheme(), sourceConfig.getHosts()));
        }
        return null;
    }

    private BsonDocument normalizeChangeStreamDocument(BsonDocument changeStreamDocument) {
        // _id: primary key of change document.
        changeStreamDocument.put(ID_FIELD, normalizeKeyDocument(changeStreamDocument));

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
                BsonTimestamp legacyTimestamp =
                        MongoRecordUtils.bsonTimestampFromEpochMillis(timestampValue);
                changeStreamDocument.put(CLUSTER_TIME_FIELD, legacyTimestamp);
            } else { // Fallback to current timestamp.
                LOG.warn(
                        "Cannot extract clusterTime from change stream event, fallback to current timestamp.");
                changeStreamDocument.put(
                        CLUSTER_TIME_FIELD, MongoRecordUtils.currentBsonTimestamp());
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

    private BsonDocument normalizeKeyDocument(BsonDocument changeStreamDocument) {
        BsonDocument documentKey = changeStreamDocument.getDocument(DOCUMENT_KEY_FIELD);
        BsonDocument primaryKey = new BsonDocument(ID_FIELD, documentKey.get(ID_FIELD));
        return new BsonDocument(ID_FIELD, primaryKey);
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

    private OperationType getOperationType(BsonDocument changeStreamDocument) {
        return OperationType.fromString(
                changeStreamDocument.getString(OPERATION_TYPE_FIELD).getValue());
    }

    private boolean isBoundedRead() {
        return !ChangeStreamOffset.NO_STOPPING_OFFSET.equals(streamSplit.getEndingOffset());
    }
}
