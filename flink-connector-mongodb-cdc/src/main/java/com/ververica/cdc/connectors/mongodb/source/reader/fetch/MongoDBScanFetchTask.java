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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.OperationType;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

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
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.collectionFor;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.createPartitionMap;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.createSourceOffsetMap;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.createSourceRecord;

/** The task to work for fetching data of MongoDB collection snapshot split . */
public class MongoDBScanFetchTask
        implements FetchTask<SourceSplitBase<CollectionId, CollectionSchema>> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBScanFetchTask.class);

    private final SnapshotSplit<CollectionId, CollectionSchema> snapshotSplit;
    private volatile boolean taskRunning = false;
    private volatile boolean finished = false;

    public MongoDBScanFetchTask(SnapshotSplit<CollectionId, CollectionSchema> snapshotSplit) {
        this.snapshotSplit = snapshotSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        MongoDBFetchTaskContext taskContext = (MongoDBFetchTaskContext) context;
        MongoDBSourceConfig sourceConfig = taskContext.getSourceConfig();
        BlockingQueue<SourceRecord> copyExistingQueue = taskContext.getCopyExistingQueue();

        taskRunning = true;
        CollectionId collectionId = snapshotSplit.getTableId();

        MongoCursor<RawBsonDocument> cursor = null;
        try {
            MongoClient mongoClient = taskContext.getMongoClient();
            MongoCollection<RawBsonDocument> collection =
                    collectionFor(mongoClient, collectionId, RawBsonDocument.class);

            CollectionSchema collectionSchema = snapshotSplit.getTableSchemas().get(collectionId);

            // Using min and max operation to perform a specific index scan
            // See: https://www.mongodb.com/docs/manual/reference/method/cursor.min/
            cursor =
                    collection
                            .find()
                            .min((BsonDocument) snapshotSplit.getSplitStart()[0])
                            .max((BsonDocument) snapshotSplit.getSplitEnd()[0])
                            .hint(collectionSchema.getShardKeys())
                            .batchSize(sourceConfig.getBatchSize())
                            .noCursorTimeout(true)
                            .cursor();

            BsonDocument keyDocument, valueDocument;
            int totalCount = 0;
            while (cursor.hasNext()) {
                if (!taskRunning) {
                    throw new InterruptedException(
                            "Interrupted while snapshotting collection "
                                    + collectionId.identifier());
                }

                valueDocument = normalizeSnapshotDocument(collectionId, cursor.next());
                keyDocument = new BsonDocument(ID_FIELD, valueDocument.get(ID_FIELD));

                copyExistingQueue.put(
                        createSourceRecord(
                                createPartitionMap(
                                        sourceConfig.getHosts(),
                                        collectionId.getDatabaseName(),
                                        collectionId.getCollectionName()),
                                createSourceOffsetMap(keyDocument, true),
                                collectionId.identifier(),
                                keyDocument,
                                valueDocument));
            }
            finished = true;
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

    public boolean isFinished() {
        return finished;
    }

    @Override
    public SnapshotSplit<CollectionId, CollectionSchema> getSplit() {
        return snapshotSplit;
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
