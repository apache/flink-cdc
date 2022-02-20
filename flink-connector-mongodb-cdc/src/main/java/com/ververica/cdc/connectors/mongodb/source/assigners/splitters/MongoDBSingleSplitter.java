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

import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSnapshotSplit;
import org.bson.BsonDocument;

import java.util.Collection;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.source.utils.ChunkUtils.maxUpperBound;
import static com.ververica.cdc.connectors.mongodb.source.utils.ChunkUtils.minLowerBound;
import static java.util.Collections.singletonList;

/**
 * The Single Partitioner
 *
 * <p>Split collection as a single chunk.
 */
public class MongoDBSingleSplitter implements MongoDBSplitter {

    public static final MongoDBSingleSplitter INSTANCE = new MongoDBSingleSplitter();

    private MongoDBSingleSplitter() {}

    @Override
    public Collection<MongoDBSnapshotSplit> split(MongoDBSplitContext splitContext) {
        BsonDocument min = minLowerBound(ID_FIELD);
        BsonDocument max = maxUpperBound(ID_FIELD);
        return singletonList(MongoDBSnapshotSplit.of(splitContext.getCollectionId(), 0, min, max));
    }
}
