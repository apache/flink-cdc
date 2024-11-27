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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import org.apache.flink.cdc.connectors.mongodb.source.utils.ChunkUtils;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;

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
    public Collection<SnapshotSplit> split(SplitContext splitContext) {
        TableId collectionId = splitContext.getCollectionId();
        Map<TableId, TableChanges.TableChange> schema = new HashMap<>();
        schema.put(collectionId, MongoDBDialect.collectionSchema(collectionId));

        SnapshotSplit snapshotSplit =
                new SnapshotSplit(
                        collectionId,
                        0,
                        shardKeysToRowType(singleton(ID_FIELD)),
                        ChunkUtils.minLowerBoundOfId(),
                        ChunkUtils.maxUpperBoundOfId(),
                        null,
                        schema);

        return singletonList(snapshotSplit);
    }
}
