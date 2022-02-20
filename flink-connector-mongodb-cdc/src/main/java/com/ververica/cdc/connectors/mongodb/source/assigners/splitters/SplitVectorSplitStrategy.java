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

package com.ververica.cdc.connectors.mongodb.source.assigners.splitters;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.RowType;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
import org.apache.commons.collections.CollectionUtils;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.BSON_MIN_KEY;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_INDEX;
import static com.ververica.cdc.connectors.mongodb.source.utils.ChunkUtils.boundOf;
import static com.ververica.cdc.connectors.mongodb.source.utils.ChunkUtils.maxUpperBound;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.UNAUTHORIZED_ERROR;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.splitVector;

/**
 * The SplitVector Splitter.
 *
 * <p>Uses the `SplitVector` command to generate chunks for a collection. eg. <code>
 * db.runCommand({splitVector:"inventory.products", keyPattern:{_id:1}, maxChunkSize:64})</code>
 * Requires `splitVector` privilege.
 */
@Internal
public class SplitVectorSplitStrategy implements SplitStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(SplitVectorSplitStrategy.class);

    public static final SplitVectorSplitStrategy INSTANCE = new SplitVectorSplitStrategy();

    private boolean hasPermission = true;

    private SplitVectorSplitStrategy() {}

    @Override
    public Collection<SnapshotSplit<CollectionId, CollectionSchema>> split(
            SplitContext splitContext) {
        if (!hasPermission) {
            LOG.warn("Unauthorized to execute splitVector command, fallback to SampleSplitter");
            return SampleBucketSplitStrategy.INSTANCE.split(splitContext);
        }

        MongoClient mongoClient = splitContext.getMongoClient();
        CollectionId collectionId = splitContext.getCollectionId();
        int chunkSizeMB = splitContext.getChunkSizeMB();

        BsonDocument keyPattern = new BsonDocument(ID_FIELD, new BsonInt32(1));

        BsonDocument splitResult;
        try {
            splitResult = splitVector(mongoClient, collectionId, keyPattern, chunkSizeMB);
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == UNAUTHORIZED_ERROR) {
                LOG.warn(
                        "Unauthorized to execute splitVector command: {}, fallback to SampleSplitter",
                        e.getErrorMessage());
                hasPermission = false;
            } else {
                LOG.warn(
                        "Execute splitVector command failed: {}, fallback to SampleSplitter",
                        e.getErrorMessage());
            }
            return SampleBucketSplitStrategy.INSTANCE.split(splitContext);
        }

        boolean isOk = new BsonDouble(1.0d).equals(splitResult.getDouble("ok"));
        if (!isOk) {
            LOG.warn(
                    "Could not calculate standalone splits: {}, fallback to SampleSplitter",
                    splitResult.getString("errmsg"));
            return SampleBucketSplitStrategy.INSTANCE.split(splitContext);
        }

        BsonArray splitKeys = splitResult.getArray("splitKeys");
        if (CollectionUtils.isEmpty(splitKeys)) {
            // documents size is less than chunk size, treat the entire collection as single chunk.
            return SingleSplitStrategy.INSTANCE.split(splitContext);
        }

        CollectionSchema collectionSchema = new CollectionSchema(ID_INDEX);
        RowType rowType = collectionSchema.shardKeysToRowType();
        Map<CollectionId, CollectionSchema> collectionSchemas =
                collectionSchemas(collectionId, collectionSchema);
        List<SnapshotSplit<CollectionId, CollectionSchema>> snapshotSplits =
                new ArrayList<>(splitKeys.size() + 1);

        BsonValue lowerValue = BSON_MIN_KEY;
        for (int i = 0; i < splitKeys.size(); i++) {
            BsonValue splitKeyValue = splitKeys.get(i).asDocument().get(ID_FIELD);
            snapshotSplits.add(
                    new SnapshotSplit<>(
                            collectionId,
                            splitId(collectionId, i),
                            rowType,
                            boundOf(ID_FIELD, lowerValue),
                            boundOf(ID_FIELD, splitKeyValue),
                            null,
                            collectionSchemas));
            lowerValue = splitKeyValue;
        }

        SnapshotSplit<CollectionId, CollectionSchema> lastSplit =
                new SnapshotSplit<>(
                        collectionId,
                        splitId(collectionId, splitKeys.size()),
                        rowType,
                        boundOf(ID_FIELD, lowerValue),
                        maxUpperBound(ID_FIELD),
                        null,
                        collectionSchemas);
        snapshotSplits.add(lastSplit);

        return snapshotSplits;
    }
}
