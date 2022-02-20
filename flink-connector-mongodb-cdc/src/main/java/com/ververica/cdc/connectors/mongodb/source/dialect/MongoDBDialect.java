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

package com.ververica.cdc.connectors.mongodb.source.dialect;

import org.apache.flink.annotation.Experimental;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.mongodb.source.assigners.splitters.MongoDBChunkSplitter;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamDescriptor;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffset;
import com.ververica.cdc.connectors.mongodb.source.reader.fetch.MongoDBFetchTaskContext;
import com.ververica.cdc.connectors.mongodb.source.reader.fetch.MongoDBScanFetchTask;
import com.ververica.cdc.connectors.mongodb.source.reader.fetch.MongoDBStreamFetchTask;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionNames;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionsFilter;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseFilter;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseNames;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.clientFor;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getChangeStreamDescriptor;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getChangeStreamIterable;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getResumeToken;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.currentBsonTimestamp;

/** The {@link DataSourceDialect} implementation for MongoDB datasource. */
@Experimental
public class MongoDBDialect
        implements DataSourceDialect<CollectionId, CollectionSchema, MongoDBSourceConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBDialect.class);

    private final MongoDBSourceConfigFactory configFactory;
    private final MongoDBSourceConfig sourceConfig;

    private volatile Cache cache = null;

    public MongoDBDialect(MongoDBSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public String getName() {
        return "MongoDB";
    }

    @Override
    public List<CollectionId> discoverDataCollections(MongoDBSourceConfig sourceConfig) {
        discoverAndCacheDataCollections(sourceConfig);
        return CollectionId.parse(cache.discoveredCollections);
    }

    @Override
    public ChangeStreamOffset displayCurrentOffset(MongoDBSourceConfig sourceConfig) {
        discoverAndCacheDataCollections(sourceConfig);

        ChangeStreamDescriptor changeStreamDescriptor =
                getChangeStreamDescriptor(
                        sourceConfig, cache.discoveredDatabases, cache.discoveredCollections);

        ChangeStreamIterable<Document> changeStreamIterable =
                getChangeStreamIterable(sourceConfig, changeStreamDescriptor);

        BsonDocument startupResumeToken = getResumeToken(changeStreamIterable);

        ChangeStreamOffset changeStreamOffset =
                new ChangeStreamOffset(
                        changeStreamDescriptor, startupResumeToken, currentBsonTimestamp());

        LOG.info("Cached startup change stream offset : {}", changeStreamOffset);

        return changeStreamOffset;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(MongoDBSourceConfig sourceConfig) {
        // MongoDB's database names and collection names are case-sensitive.
        return true;
    }

    @Override
    public ChunkSplitter<CollectionId, CollectionSchema> createChunkSplitter(
            MongoDBSourceConfig sourceConfig) {
        return new MongoDBChunkSplitter(sourceConfig);
    }

    @Override
    public FetchTask<SourceSplitBase<CollectionId, CollectionSchema>> createFetchTask(
            SourceSplitBase<CollectionId, CollectionSchema> sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new MongoDBScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new MongoDBStreamFetchTask(sourceSplitBase.asStreamSplit());
        }
    }

    @Override
    public MongoDBFetchTaskContext createFetchTaskContext(
            SourceSplitBase<CollectionId, CollectionSchema> sourceSplitBase) {
        final MongoClient mongoClient = clientFor(sourceConfig);
        return new MongoDBFetchTaskContext(mongoClient, sourceConfig);
    }

    private void discoverAndCacheDataCollections(MongoDBSourceConfig sourceConfig) {
        if (cache == null) {
            synchronized (this) {
                if (cache == null) {
                    MongoClient mongoClient = clientFor(sourceConfig);
                    List<String> discoveredDatabases =
                            databaseNames(
                                    mongoClient, databaseFilter(sourceConfig.getDatabaseList()));
                    List<String> discoveredCollections =
                            collectionNames(
                                    mongoClient,
                                    discoveredDatabases,
                                    collectionsFilter(sourceConfig.getCollectionList()));
                    cache = new Cache(discoveredDatabases, discoveredCollections);
                }
            }
        }
    }

    private static class Cache implements Serializable {
        private final List<String> discoveredDatabases;
        private final List<String> discoveredCollections;

        private Cache(List<String> discoveredDatabases, List<String> discoveredCollections) {
            this.discoveredDatabases = discoveredDatabases;
            this.discoveredCollections = discoveredCollections;
        }
    }
}
