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

import com.mongodb.client.MongoCollection;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Aggregates.bucketAuto;
import static com.mongodb.client.model.Aggregates.sample;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.BSON_MAX_KEY;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.BSON_MIN_KEY;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_INDEX;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.MAX_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.MIN_FIELD;
import static com.ververica.cdc.connectors.mongodb.source.utils.ChunkUtils.boundOf;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.collectionFor;

/**
 * The Sample Splitter.
 *
 * <p>Uses the document size and 5% sampling rate of the collection to generate chunks for the
 * collection.
 *
 * <pre>
 * If all of the following conditions are true, $sample uses a pseudo-random cursor to select the N documents:
 *
 *      $sample is the first stage of the pipeline.
 *      N is less than 5% of the total documents in the collection.
 *      The collection contains more than 100 documents.
 *      If any of the previous conditions are false, $sample:
 *
 * Reads all documents that are output from a preceding aggregation stage or a collection scan.
 *      Performs a random sort to select N documents.
 * </pre>
 */
@Internal
public class SampleBucketSplitStrategy implements SplitStrategy {

    public static final SampleBucketSplitStrategy INSTANCE = new SampleBucketSplitStrategy();
    private static final int DEFAULT_SAMPLING_THRESHOLD = 102400;
    private static final double DEFAULT_SAMPLING_RATE = 0.05;

    private SampleBucketSplitStrategy() {}

    @Override
    public Collection<SnapshotSplit<CollectionId, CollectionSchema>> split(
            SplitContext splitContext) {
        long chunkSizeInBytes = splitContext.getChunkSizeMB() * 1024 * 1024;

        long sizeInBytes = splitContext.getSizeInBytes();
        long count = splitContext.getDocumentCount();

        // If collection's total uncompressed size less than chunk size,
        // treat the entire collection as single chunk.
        if (sizeInBytes < chunkSizeInBytes) {
            return SingleSplitStrategy.INSTANCE.split(splitContext);
        }

        int numChunks = (int) (sizeInBytes / chunkSizeInBytes) + 1;
        int numberOfSamples;
        if (count < DEFAULT_SAMPLING_THRESHOLD) {
            // full sampling if document count less than sampling size threshold.
            numberOfSamples = (int) count;
        } else {
            // sampled using sample rate.
            numberOfSamples = (int) Math.floor(count * DEFAULT_SAMPLING_RATE);
        }

        CollectionId collectionId = splitContext.getCollectionId();

        MongoCollection<BsonDocument> collection =
                collectionFor(splitContext.getMongoClient(), collectionId, BsonDocument.class);

        List<Bson> pipeline = new ArrayList<>();
        if (numberOfSamples != count) {
            pipeline.add(sample(numberOfSamples));
        }
        pipeline.add(bucketAuto("$" + ID_FIELD, numChunks));

        List<BsonDocument> chunks =
                collection.aggregate(pipeline).allowDiskUse(true).into(new ArrayList<>());

        CollectionSchema collectionSchema = new CollectionSchema(ID_INDEX);
        Map<CollectionId, CollectionSchema> collectionSchemas =
                collectionSchemas(collectionId, collectionSchema);
        RowType rowType = collectionSchema.shardKeysToRowType();

        List<SnapshotSplit<CollectionId, CollectionSchema>> snapshotSplits =
                new ArrayList<>(chunks.size() + 2);

        SnapshotSplit<CollectionId, CollectionSchema> firstSplit =
                new SnapshotSplit<>(
                        collectionId,
                        splitId(collectionId, 0),
                        rowType,
                        boundOf(ID_FIELD, BSON_MIN_KEY),
                        boundOf(ID_FIELD, lowerBoundOfBucket(chunks.get(0))),
                        null,
                        collectionSchemas);
        snapshotSplits.add(firstSplit);

        for (int i = 0; i < chunks.size(); i++) {
            BsonDocument bucket = chunks.get(i);
            snapshotSplits.add(
                    new SnapshotSplit<>(
                            collectionId,
                            splitId(collectionId, i + 1),
                            rowType,
                            boundOf(ID_FIELD, lowerBoundOfBucket(bucket)),
                            boundOf(ID_FIELD, upperBoundOfBucket(bucket)),
                            null,
                            collectionSchemas));
        }

        SnapshotSplit<CollectionId, CollectionSchema> lastSplit =
                new SnapshotSplit<>(
                        collectionId,
                        splitId(collectionId, chunks.size() + 1),
                        rowType,
                        boundOf(ID_FIELD, upperBoundOfBucket(chunks.get(chunks.size() - 1))),
                        boundOf(ID_FIELD, BSON_MAX_KEY),
                        null,
                        collectionSchemas);
        snapshotSplits.add(lastSplit);

        return snapshotSplits;
    }

    private BsonDocument bucketBounds(BsonDocument bucket) {
        return bucket.getDocument(ID_FIELD);
    }

    private BsonValue lowerBoundOfBucket(BsonDocument bucket) {
        return bucketBounds(bucket).get(MIN_FIELD);
    }

    private BsonValue upperBoundOfBucket(BsonDocument bucket) {
        return bucketBounds(bucket).get(MAX_FIELD);
    }
}
