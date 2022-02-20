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

package com.ververica.cdc.connectors.mongodb.source.assigners;

import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.ververica.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;

/** Assigner for MongoDB Hybrid split which contains snapshot splits and stream splits. */
public class MongoDBHybridSplitAssigner
        extends HybridSplitAssigner<CollectionId, CollectionSchema, MongoDBSourceConfig> {

    public MongoDBHybridSplitAssigner(
            MongoDBSourceConfig sourceConfig,
            int currentParallelism,
            List<CollectionId> remainingCollections,
            DataSourceDialect<CollectionId, CollectionSchema, MongoDBSourceConfig> dialect,
            OffsetFactory offsetFactory) {
        super(sourceConfig, currentParallelism, remainingCollections, true, dialect, offsetFactory);
    }

    public MongoDBHybridSplitAssigner(
            MongoDBSourceConfig sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState<CollectionId, CollectionSchema> checkpoint,
            DataSourceDialect<CollectionId, CollectionSchema, MongoDBSourceConfig> dialect,
            OffsetFactory offsetFactory) {
        super(sourceConfig, currentParallelism, checkpoint, dialect, offsetFactory);
    }

    @Override
    protected StreamSplit<CollectionId, CollectionSchema> createStreamSplit() {
        return new StreamSplit<>(
                STREAM_SPLIT_ID,
                getStartupOffset(),
                offsetFactory.createNoStoppingOffset(),
                new ArrayList<>(),
                new HashMap<>(),
                snapshotSplitAssigner.getAssignedSplits().size());
    }
}
