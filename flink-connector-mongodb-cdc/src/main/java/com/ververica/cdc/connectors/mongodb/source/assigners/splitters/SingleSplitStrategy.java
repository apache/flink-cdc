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

import org.apache.flink.annotation.Internal;

import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.source.dialect.MongoDBDialect.collectionSchema;
import static com.ververica.cdc.connectors.mongodb.source.utils.ChunkUtils.maxUpperBoundOfId;
import static com.ververica.cdc.connectors.mongodb.source.utils.ChunkUtils.minLowerBoundOfId;
import static java.util.Collections.singleton;
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
    public Collection<SnapshotSplit> split(SplitContext splitContext) {
        TableId collectionId = splitContext.getCollectionId();
        Map<TableId, TableChanges.TableChange> schema = new HashMap<>();
        schema.put(collectionId, collectionSchema(collectionId));

        SnapshotSplit snapshotSplit =
                new SnapshotSplit(
                        collectionId,
                        splitId(collectionId, 0),
                        shardKeysToRowType(singleton(ID_FIELD)),
                        minLowerBoundOfId(),
                        maxUpperBoundOfId(),
                        null,
                        schema);

        return singletonList(snapshotSplit);
    }
}
