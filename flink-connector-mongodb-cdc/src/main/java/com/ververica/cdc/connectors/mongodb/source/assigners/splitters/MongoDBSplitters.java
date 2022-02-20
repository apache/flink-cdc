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

import com.ververica.cdc.connectors.mongodb.source.assigners.MongoDBSplitAssigner;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSnapshotSplit;

import java.util.Collection;

/**
 * The splitter used by {@link MongoDBSplitAssigner} to split collection into a set of chunks for
 * MongoDB data source.
 */
public class MongoDBSplitters {

    private final MongoDBSourceConfig sourceConfig;
    private final int currentParallelism;

    public MongoDBSplitters(MongoDBSourceConfig sourceConfig, int currentParallelism) {
        this.sourceConfig = sourceConfig;
        this.currentParallelism = currentParallelism;
    }

    public Collection<MongoDBSnapshotSplit> split(CollectionId collectionId) {
        MongoDBSplitContext splitContext =
                MongoDBSplitContext.of(sourceConfig, currentParallelism, collectionId);
        if (splitContext.isShardedCollection()) {
            return MongoDBShardedSplitter.INSTANCE.split(splitContext);
        }
        return MongoDBSplitVectorSplitter.INSTANCE.split(splitContext);
    }
}
