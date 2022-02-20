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

import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;

import java.util.Collection;
import java.util.Map;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_INDEX;
import static com.ververica.cdc.connectors.mongodb.source.utils.ChunkUtils.maxUpperBound;
import static com.ververica.cdc.connectors.mongodb.source.utils.ChunkUtils.minLowerBound;
import static java.util.Collections.singletonList;

/**
 * The Single Partitioner
 *
 * <p>Split collection as a single chunk.
 */
@Internal
public class SingleSplitStrategy implements SplitStrategy {

    public static final SingleSplitStrategy INSTANCE = new SingleSplitStrategy();

    private SingleSplitStrategy() {}

    @Override
    public Collection<SnapshotSplit<CollectionId, CollectionSchema>> split(
            SplitContext splitContext) {
        CollectionId collectionId = splitContext.getCollectionId();
        CollectionSchema collectionSchema = new CollectionSchema(ID_INDEX);
        Map<CollectionId, CollectionSchema> collectionSchemas =
                collectionSchemas(collectionId, collectionSchema);

        SnapshotSplit<CollectionId, CollectionSchema> snapshotSplit =
                new SnapshotSplit<>(
                        collectionId,
                        splitId(collectionId, 0),
                        collectionSchema.shardKeysToRowType(),
                        minLowerBound(ID_FIELD),
                        maxUpperBound(ID_FIELD),
                        null,
                        collectionSchemas);

        return singletonList(snapshotSplit);
    }
}
