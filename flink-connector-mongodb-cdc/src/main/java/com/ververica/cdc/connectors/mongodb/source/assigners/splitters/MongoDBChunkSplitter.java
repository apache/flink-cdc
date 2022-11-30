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

package com.ververica.cdc.connectors.mongodb.source.assigners.splitters;

import org.apache.flink.annotation.Experimental;

import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import io.debezium.relational.TableId;

import java.util.Collection;

/** The splitter used to split collection into a set of chunks for MongoDB data source. */
@Experimental
public class MongoDBChunkSplitter implements ChunkSplitter {

    private final MongoDBSourceConfig sourceConfig;

    public MongoDBChunkSplitter(MongoDBSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public Collection<SnapshotSplit> generateSplits(TableId collectionId) {
        SplitContext splitContext = SplitContext.of(sourceConfig, collectionId);
        if (splitContext.isShardedCollection()) {
            return ShardedSplitStrategy.INSTANCE.split(splitContext);
        }
        return SplitVectorSplitStrategy.INSTANCE.split(splitContext);
    }
}
