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

package com.ververica.cdc.connectors.mongodb.source.dialect;

import org.apache.flink.annotation.Experimental;

import com.mongodb.client.MongoClient;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.mongodb.source.assigners.splitters.MongoDBChunkSplitter;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamDescriptor;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffset;
import com.ververica.cdc.connectors.mongodb.source.reader.fetch.MongoDBFetchTaskContext;
import com.ververica.cdc.connectors.mongodb.source.reader.fetch.MongoDBScanFetchTask;
import com.ververica.cdc.connectors.mongodb.source.reader.fetch.MongoDBStreamFetchTask;
import com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.CollectionDiscoveryInfo;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionNames;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionsFilter;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseFilter;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseNames;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.clientFor;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getChangeStreamDescriptor;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getCurrentClusterTime;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getLatestResumeToken;

/** The {@link DataSourceDialect} implementation for MongoDB datasource. */
@Experimental
public class MongoDBDialect implements DataSourceDialect<MongoDBSourceConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBDialect.class);

    private final Map<MongoDBSourceConfig, CollectionDiscoveryInfo> cache =
            new ConcurrentHashMap<>();

    @Override
    public String getName() {
        return "MongoDB";
    }

    @Override
    public List<TableId> discoverDataCollections(MongoDBSourceConfig sourceConfig) {
        CollectionDiscoveryInfo discoveryInfo = discoverAndCacheDataCollections(sourceConfig);
        return discoveryInfo.getDiscoveredCollections().stream()
                .map(TableId::parse)
                .collect(Collectors.toList());
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(
            MongoDBSourceConfig sourceConfig) {
        List<TableId> discoveredCollections = discoverDataCollections(sourceConfig);
        Map<TableId, TableChange> schemas = new HashMap<>(discoveredCollections.size());
        for (TableId collectionId : discoveredCollections) {
            schemas.put(collectionId, collectionSchema(collectionId));
        }
        return schemas;
    }

    private CollectionDiscoveryInfo discoverAndCacheDataCollections(
            MongoDBSourceConfig sourceConfig) {
        return cache.computeIfAbsent(
                sourceConfig,
                config -> {
                    MongoClient mongoClient = clientFor(sourceConfig);
                    List<String> discoveredDatabases =
                            databaseNames(
                                    mongoClient, databaseFilter(sourceConfig.getDatabaseList()));
                    List<String> discoveredCollections =
                            collectionNames(
                                    mongoClient,
                                    discoveredDatabases,
                                    collectionsFilter(sourceConfig.getCollectionList()));
                    return new CollectionDiscoveryInfo(discoveredDatabases, discoveredCollections);
                });
    }

    public static TableChange collectionSchema(TableId tableId) {
        Table table =
                Table.editor()
                        .tableId(tableId)
                        .addColumn(Column.editor().name(ID_FIELD).optional(false).create())
                        .setPrimaryKeyNames(ID_FIELD)
                        .create();
        return new TableChange(TableChanges.TableChangeType.CREATE, table);
    }

    @Override
    public ChangeStreamOffset displayCurrentOffset(MongoDBSourceConfig sourceConfig) {
        MongoClient mongoClient = clientFor(sourceConfig);
        CollectionDiscoveryInfo discoveryInfo = discoverAndCacheDataCollections(sourceConfig);
        ChangeStreamDescriptor changeStreamDescriptor =
                getChangeStreamDescriptor(
                        sourceConfig,
                        discoveryInfo.getDiscoveredDatabases(),
                        discoveryInfo.getDiscoveredCollections());
        BsonDocument startupResumeToken = getLatestResumeToken(mongoClient, changeStreamDescriptor);

        ChangeStreamOffset changeStreamOffset;
        if (startupResumeToken != null) {
            changeStreamOffset = new ChangeStreamOffset(startupResumeToken);
        } else {
            // The resume token may be null before MongoDB 4.0.7
            // when the ChangeStream opened and no change record received.
            // In this case, fallback to the current clusterTime as Offset.
            changeStreamOffset = new ChangeStreamOffset(getCurrentClusterTime(mongoClient));
        }

        LOG.info("Current change stream offset : {}", changeStreamOffset);
        return changeStreamOffset;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(MongoDBSourceConfig sourceConfig) {
        // MongoDB's database names and collection names are case-sensitive.
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(MongoDBSourceConfig sourceConfig) {
        return new MongoDBChunkSplitter(sourceConfig);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new MongoDBScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new MongoDBStreamFetchTask(sourceSplitBase.asStreamSplit());
        }
    }

    @Override
    public MongoDBFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, MongoDBSourceConfig sourceConfig) {
        CollectionDiscoveryInfo discoveryInfo = discoverAndCacheDataCollections(sourceConfig);
        ChangeStreamDescriptor changeStreamDescriptor =
                getChangeStreamDescriptor(
                        sourceConfig,
                        discoveryInfo.getDiscoveredDatabases(),
                        discoveryInfo.getDiscoveredCollections());
        return new MongoDBFetchTaskContext(this, sourceConfig, changeStreamDescriptor);
    }
}
