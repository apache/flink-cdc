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

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.utils.MongoRecordUtils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.OperationType;
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

import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.DOCUMENT_KEY_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.FULL_DOCUMENT_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_COLLECTION_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_DATABASE_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.OPERATION_TYPE_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.SNAPSHOT_KEY_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.SOURCE_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.TIMESTAMP_KEY_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.WATERMARK_TOPIC_NAME;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.clientFor;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.collectionFor;

/** The task to work for fetching data of MongoDB collection snapshot split . */
public class MongoDBScanFetchTask extends AbstractScanFetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBScanFetchTask.class);

    public MongoDBScanFetchTask(SnapshotSplit snapshotSplit) {
        super(snapshotSplit);
    }

    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        ChangeEventQueue<DataChangeEvent> changeEventQueue = context.getQueue();
        MongoDBFetchTaskContext taskContext = (MongoDBFetchTaskContext) context;
        MongoDBSourceConfig sourceConfig = taskContext.getSourceConfig();
        TableId collectionId = snapshotSplit.getTableId();

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
                            .noCursorTimeout(sourceConfig.disableCursorTimeout())
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
                        MongoRecordUtils.createSourceRecord(
                                MongoRecordUtils.createPartitionMap(
                                        sourceConfig.getScheme(),
                                        sourceConfig.getHosts(),
                                        collectionId.catalog(),
                                        collectionId.table()),
                                MongoRecordUtils.createSourceOffsetMap(
                                        keyDocument.getDocument(ID_FIELD), true),
                                collectionId.identifier(),
                                keyDocument,
                                valueDocument);

                changeEventQueue.enqueue(new DataChangeEvent(snapshotRecord));
            }

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
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        MongoDBStreamFetchTask backfillStreamTask = new MongoDBStreamFetchTask(backfillStreamSplit);
        backfillStreamTask.execute(context);
    }

    @Override
    protected void dispatchLowWaterMarkEvent(
            Context context, SourceSplitBase split, Offset lowWatermark)
            throws InterruptedException {
        ChangeEventQueue<DataChangeEvent> changeEventQueue = context.getQueue();
        changeEventQueue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                MongoRecordUtils.createWatermarkPartitionMap(
                                        snapshotSplit.getTableId().identifier()),
                                WATERMARK_TOPIC_NAME,
                                snapshotSplit.splitId(),
                                WatermarkKind.LOW,
                                lowWatermark)));
    }

    @Override
    protected void dispatchHighWaterMarkEvent(
            Context context, SourceSplitBase split, Offset highWatermark)
            throws InterruptedException {
        ChangeEventQueue<DataChangeEvent> changeEventQueue = context.getQueue();
        changeEventQueue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                MongoRecordUtils.createWatermarkPartitionMap(
                                        snapshotSplit.getTableId().identifier()),
                                WATERMARK_TOPIC_NAME,
                                split.splitId(),
                                WatermarkKind.HIGH,
                                highWatermark)));
    }

    @Override
    protected void dispatchEndWaterMarkEvent(
            Context context, SourceSplitBase split, Offset endWatermark)
            throws InterruptedException {
        ChangeEventQueue<DataChangeEvent> changeEventQueue = context.getQueue();
        changeEventQueue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                MongoRecordUtils.createWatermarkPartitionMap(
                                        snapshotSplit.getTableId().identifier()),
                                WATERMARK_TOPIC_NAME,
                                split.splitId(),
                                WatermarkKind.END,
                                endWatermark)));
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
