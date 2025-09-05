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

package org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;

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

    @Override
    public void open() {}

    @Override
    public boolean hasNextChunk() {
        // mongo cdc doesn't support chunk split asynchronously now.
        return false;
    }

    @Override
    public ChunkSplitterState snapshotState(long checkpointId) {
        return ChunkSplitterState.NO_SPLITTING_TABLE_STATE;
    }

    @Override
    public TableId getCurrentSplittingTableId() {
        return null;
    }

    @Override
    public void close() throws Exception {}
}
