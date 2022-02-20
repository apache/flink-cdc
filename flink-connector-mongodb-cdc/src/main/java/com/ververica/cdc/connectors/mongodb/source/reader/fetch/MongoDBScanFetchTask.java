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

package com.ververica.cdc.connectors.mongodb.source.reader.fetch;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.OperationType;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffset;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

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
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.WATERMARK_TOPIC_NAME;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.createPartitionMap;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.createSourceOffsetMap;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.createSourceRecord;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.createWatermarkPartitionMap;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.clientFor;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.collectionFor;

/** The task to work for fetching data of MongoDB collection snapshot split . */
public class MongoDBScanFetchTask implements FetchTask<SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBScanFetchTask.class);

    private final SnapshotSplit snapshotSplit;
    private volatile boolean taskRunning = false;

    public MongoDBScanFetchTask(SnapshotSplit snapshotSplit) {
        this.snapshotSplit = snapshotSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        MongoDBFetchTaskContext taskContext = (MongoDBFetchTaskContext) context;
        MongoDBSourceConfig sourceConfig = taskContext.getSourceConfig();
        MongoDBDialect dialect = taskContext.getDialect();
        ChangeEventQueue<DataChangeEvent> changeEventQueue = taskContext.getQueue();

        taskRunning = true;
        TableId collectionId = snapshotSplit.getTableId();

        final ChangeStreamOffset lowWatermark = dialect.displayCurrentOffset(sourceConfig);
        LOG.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                snapshotSplit);

        changeEventQueue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                createWatermarkPartitionMap(collectionId.identifier()),
                                WATERMARK_TOPIC_NAME,
                                snapshotSplit.splitId(),
                                WatermarkKind.LOW,
                                lowWatermark)));

        LOG.info("Snapshot step 2 - Snapshotting data");
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
                            .min((BsonDocument) snapshotSplit.getSplitStart()[1])
                            .max((BsonDocument) snapshotSplit.getSplitEnd()[1])
                            .hint((BsonDocument) snapshotSplit.getSplitStart()[0])
                            .batchSize(sourceConfig.getBatchSize())
                            .noCursorTimeout(true)
                            .cursor();

            BsonDocument keyDocument, valueDocument;
            while (cursor.hasNext()) {
                if (!taskRunning) {
                    throw new InterruptedException(
                            "Interrupted while snapshotting collection "
                                    + collectionId.identifier());
                }

                valueDocument = normalizeSnapshotDocument(collectionId, cursor.next());
                keyDocument = new BsonDocument(ID_FIELD, valueDocument.get(ID_FIELD));

                SourceRecord snapshotRecord =
                        createSourceRecord(
                                createPartitionMap(
                                        sourceConfig.getHosts(),
                                        collectionId.catalog(),
                                        collectionId.table()),
                                createSourceOffsetMap(keyDocument.getDocument(ID_FIELD), true),
                                collectionId.identifier(),
                                keyDocument,
                                valueDocument);

                changeEventQueue.enqueue(new DataChangeEvent(snapshotRecord));
            }

            ChangeStreamOffset highWatermark = dialect.displayCurrentOffset(sourceConfig);

            LOG.info(
                    "Snapshot step 3 - Determining high watermark {} for split {}",
                    highWatermark,
                    snapshotSplit);
            changeEventQueue.enqueue(
                    new DataChangeEvent(
                            WatermarkEvent.create(
                                    createWatermarkPartitionMap(collectionId.identifier()),
                                    WATERMARK_TOPIC_NAME,
                                    snapshotSplit.splitId(),
                                    WatermarkKind.HIGH,
                                    highWatermark)));

            LOG.info(
                    "Snapshot step 4 - Back fill stream split for snapshot split {}",
                    snapshotSplit);
            final StreamSplit backfillStreamSplit =
                    createBackfillStreamSplit(lowWatermark, highWatermark);

            // optimization that skip the stream read when the low watermark equals high watermark
            final boolean streamBackfillRequired =
                    backfillStreamSplit
                            .getEndingOffset()
                            .isAfter(backfillStreamSplit.getStartingOffset());

            if (!streamBackfillRequired) {
                changeEventQueue.enqueue(
                        new DataChangeEvent(
                                WatermarkEvent.create(
                                        createWatermarkPartitionMap(collectionId.identifier()),
                                        WATERMARK_TOPIC_NAME,
                                        backfillStreamSplit.splitId(),
                                        WatermarkKind.END,
                                        backfillStreamSplit.getEndingOffset())));
            } else {
                MongoDBStreamFetchTask backfillStreamTask =
                        new MongoDBStreamFetchTask(backfillStreamSplit);
                backfillStreamTask.execute(taskContext);
            }

            taskRunning = false;
        } catch (Exception e) {
            taskRunning = false;
            LOG.error(
                    String.format(
                            "Execute snapshot read subtask for mongo split %s fail", snapshotSplit),
                    e);
            throw e;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SnapshotSplit getSplit() {
        return snapshotSplit;
    }

    private StreamSplit createBackfillStreamSplit(
            ChangeStreamOffset lowWatermark, ChangeStreamOffset highWatermark) {
        return new StreamSplit(
                snapshotSplit.splitId(),
                lowWatermark,
                highWatermark,
                new ArrayList<>(),
                snapshotSplit.getTableSchemas(),
                0);
    }

    private BsonDocument normalizeSnapshotDocument(
            final TableId collectionId, final BsonDocument originalDocument) {
        final BsonDocument valueDocument = new BsonDocument();

        // id
        BsonDocument id = new BsonDocument();
        id.put(ID_FIELD, originalDocument.get(ID_FIELD));
        valueDocument.put(ID_FIELD, id);

        // operationType
        valueDocument.put(OPERATION_TYPE_FIELD, new BsonString(OperationType.INSERT.getValue()));

        // ns
        BsonDocument ns = new BsonDocument();
        ns.put(NAMESPACE_DATABASE_FIELD, new BsonString(collectionId.catalog()));
        ns.put(NAMESPACE_COLLECTION_FIELD, new BsonString(collectionId.table()));
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
