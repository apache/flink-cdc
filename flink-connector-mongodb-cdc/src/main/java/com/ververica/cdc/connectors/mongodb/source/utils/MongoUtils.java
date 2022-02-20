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

package com.ververica.cdc.connectors.mongodb.source.utils;

import com.mongodb.ConnectionString;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.connection.MongoClientPool;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamDescriptor;
import io.debezium.relational.TableId;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.DROPPED_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.KEY_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.MONGODB_SCHEME;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.UUID_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.encodeValue;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.ADD_NS_FIELD;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.ADD_NS_FIELD_NAME;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.includeListAsFlatPattern;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.isIncludeListExplicitlySpecified;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utilities of MongoDB operations. */
public class MongoUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MongoUtils.class);

    public static final BsonDouble COMMAND_SUCCEED_FLAG = new BsonDouble(1.0d);

    public static final int FAILED_TO_PARSE_ERROR = 9;
    public static final int UNAUTHORIZED_ERROR = 13;
    public static final int ILLEGAL_OPERATION_ERROR = 20;
    public static final int UNKNOWN_FIELD_ERROR = 40415;

    private MongoUtils() {}

    public static ChangeStreamDescriptor getChangeStreamDescriptor(
            MongoDBSourceConfig sourceConfig,
            List<String> discoveredDatabases,
            List<String> discoveredCollections) {
        List<String> databaseList = sourceConfig.getDatabaseList();
        List<String> collectionList = sourceConfig.getCollectionList();

        ChangeStreamDescriptor changeStreamFilter;
        if (collectionList != null) {
            // Watching collections changes
            if (isIncludeListExplicitlySpecified(collectionList, discoveredCollections)) {
                changeStreamFilter =
                        ChangeStreamDescriptor.collection(
                                TableId.parse(discoveredCollections.get(0)));
            } else {
                Pattern namespaceRegex = includeListAsFlatPattern(collectionList);
                if (databaseList != null) {
                    if (isIncludeListExplicitlySpecified(databaseList, discoveredDatabases)) {
                        changeStreamFilter =
                                ChangeStreamDescriptor.database(
                                        discoveredDatabases.get(0), namespaceRegex);
                    } else {
                        Pattern databaseRegex = includeListAsFlatPattern(databaseList);
                        changeStreamFilter =
                                ChangeStreamDescriptor.deployment(databaseRegex, namespaceRegex);
                    }
                } else {
                    changeStreamFilter = ChangeStreamDescriptor.deployment(null, namespaceRegex);
                }
            }
        } else if (databaseList != null) {
            if (isIncludeListExplicitlySpecified(databaseList, discoveredDatabases)) {
                changeStreamFilter = ChangeStreamDescriptor.database(discoveredDatabases.get(0));
            } else {
                Pattern databaseRegex = includeListAsFlatPattern(databaseList);
                changeStreamFilter = ChangeStreamDescriptor.deployment(databaseRegex);
            }
        } else {
            // Watching all changes on the cluster
            changeStreamFilter = ChangeStreamDescriptor.deployment();
        }
        return changeStreamFilter;
    }

    public static ChangeStreamIterable<Document> getChangeStreamIterable(
            MongoDBSourceConfig sourceConfig, ChangeStreamDescriptor descriptor) {
        return getChangeStreamIterable(
                clientFor(sourceConfig),
                descriptor.getDatabase(),
                descriptor.getCollection(),
                descriptor.getDatabaseRegex(),
                descriptor.getNamespaceRegex(),
                sourceConfig.getBatchSize(),
                sourceConfig.isUpdateLookup());
    }

    public static ChangeStreamIterable<Document> getChangeStreamIterable(
            MongoClient mongoClient,
            ChangeStreamDescriptor descriptor,
            int batchSize,
            boolean updateLookup) {
        return getChangeStreamIterable(
                mongoClient,
                descriptor.getDatabase(),
                descriptor.getCollection(),
                descriptor.getDatabaseRegex(),
                descriptor.getNamespaceRegex(),
                batchSize,
                updateLookup);
    }

    public static ChangeStreamIterable<Document> getChangeStreamIterable(
            MongoClient mongoClient,
            @Nullable String database,
            @Nullable String collection,
            @Nullable Pattern databaseRegex,
            @Nullable Pattern namespaceRegex,
            int batchSize,
            boolean updateLookup) {
        ChangeStreamIterable<Document> changeStream;
        if (StringUtils.isNotEmpty(database) && StringUtils.isNotEmpty(collection)) {
            MongoCollection<Document> coll =
                    mongoClient.getDatabase(database).getCollection(collection);
            LOG.info("Preparing change stream for collection {}.{}", database, collection);
            changeStream = coll.watch();
        } else if (StringUtils.isNotEmpty(database) && namespaceRegex != null) {
            MongoDatabase db = mongoClient.getDatabase(database);
            List<Bson> pipeline = new ArrayList<>();
            pipeline.add(ADD_NS_FIELD);
            Bson nsFilter = regex(ADD_NS_FIELD_NAME, namespaceRegex);
            pipeline.add(match(nsFilter));
            LOG.info(
                    "Preparing change stream for database {} with namespace regex filter {}",
                    database,
                    namespaceRegex);
            changeStream = db.watch(pipeline);
        } else if (StringUtils.isNotEmpty(database)) {
            MongoDatabase db = mongoClient.getDatabase(database);
            LOG.info("Preparing change stream for database {}", database);
            changeStream = db.watch();
        } else if (namespaceRegex != null) {
            List<Bson> pipeline = new ArrayList<>();
            pipeline.add(ADD_NS_FIELD);

            Bson nsFilter = regex(ADD_NS_FIELD_NAME, namespaceRegex);
            if (databaseRegex != null) {
                Bson dbFilter = regex("ns.db", databaseRegex);
                nsFilter = and(dbFilter, nsFilter);
                LOG.info(
                        "Preparing change stream for deployment with"
                                + " database regex filter {} and namespace regex filter {}",
                        databaseRegex,
                        namespaceRegex);
            } else {
                LOG.info(
                        "Preparing change stream for deployment with namespace regex filter {}",
                        namespaceRegex);
            }

            pipeline.add(match(nsFilter));
            changeStream = mongoClient.watch(pipeline);
        } else if (databaseRegex != null) {
            List<Bson> pipeline = new ArrayList<>();
            pipeline.add(match(regex("ns.db", databaseRegex)));

            LOG.info(
                    "Preparing change stream for deployment  with database regex filter {}",
                    databaseRegex);
            changeStream = mongoClient.watch(pipeline);
        } else {
            LOG.info("Preparing change stream for deployment");
            changeStream = mongoClient.watch();
        }

        if (batchSize > 0) {
            changeStream.batchSize(batchSize);
        }

        if (updateLookup) {
            changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);
        }
        return changeStream;
    }

    @Nullable
    public static BsonDocument getLatestResumeToken(
            MongoClient mongoClient, ChangeStreamDescriptor descriptor) {
        ChangeStreamIterable<Document> changeStreamIterable =
                getChangeStreamIterable(mongoClient, descriptor, 1, false);

        // Nullable when no change record or postResumeToken (new in MongoDB 4.0.7).
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> changeStreamCursor =
                changeStreamIterable.cursor()) {
            ChangeStreamDocument<Document> firstResult = changeStreamCursor.tryNext();

            return firstResult != null
                    ? firstResult.getResumeToken()
                    : changeStreamCursor.getResumeToken();
        }
    }

    public static boolean isCommandSucceed(BsonDocument commandResult) {
        return commandResult != null && COMMAND_SUCCEED_FLAG.equals(commandResult.getDouble("ok"));
    }

    public static String commandErrorMessage(BsonDocument commandResult) {
        return Optional.ofNullable(commandResult)
                .map(doc -> doc.getString("errmsg"))
                .map(BsonString::getValue)
                .orElse(null);
    }

    public static BsonDocument collStats(MongoClient mongoClient, TableId collectionId) {
        BsonDocument collStatsCommand =
                new BsonDocument("collStats", new BsonString(collectionId.table()));
        return mongoClient
                .getDatabase(collectionId.catalog())
                .runCommand(collStatsCommand, BsonDocument.class);
    }

    public static BsonDocument splitVector(
            MongoClient mongoClient,
            TableId collectionId,
            BsonDocument keyPattern,
            int maxChunkSizeMB) {
        return splitVector(mongoClient, collectionId, keyPattern, maxChunkSizeMB, null, null);
    }

    public static BsonDocument splitVector(
            MongoClient mongoClient,
            TableId collectionId,
            BsonDocument keyPattern,
            int maxChunkSizeMB,
            @Nullable BsonDocument min,
            @Nullable BsonDocument max) {
        BsonDocument splitVectorCommand =
                new BsonDocument("splitVector", new BsonString(collectionId.identifier()))
                        .append("keyPattern", keyPattern)
                        .append("maxChunkSize", new BsonInt32(maxChunkSizeMB));
        Optional.ofNullable(min).ifPresent(v -> splitVectorCommand.append("min", v));
        Optional.ofNullable(max).ifPresent(v -> splitVectorCommand.append("max", v));
        return mongoClient
                .getDatabase(collectionId.catalog())
                .runCommand(splitVectorCommand, BsonDocument.class);
    }

    public static BsonTimestamp getCurrentClusterTime(MongoClient mongoClient) {
        BsonDocument isMasterResult = isMaster(mongoClient);
        if (!isCommandSucceed(isMasterResult)) {
            throw new IllegalStateException(
                    "Failed to execute isMaster command: " + commandErrorMessage(isMasterResult));
        }
        return isMasterResult.getDocument("$clusterTime").getTimestamp("clusterTime");
    }

    public static BsonDocument isMaster(MongoClient mongoClient) {
        BsonDocument isMasterCommand = new BsonDocument("isMaster", new BsonInt32(1));
        return mongoClient.getDatabase("admin").runCommand(isMasterCommand, BsonDocument.class);
    }

    public static List<BsonDocument> readChunks(
            MongoClient mongoClient, BsonDocument collectionMetadata) {
        MongoCollection<BsonDocument> chunks =
                collectionFor(mongoClient, TableId.parse("config.chunks"), BsonDocument.class);
        List<BsonDocument> collectionChunks = new ArrayList<>();

        Bson filter =
                or(
                        new BsonDocument(NAMESPACE_FIELD, collectionMetadata.get(ID_FIELD)),
                        // MongoDB 4.9.0 removed ns field of config.chunks collection, using
                        // collection's uuid instead.
                        // See: https://jira.mongodb.org/browse/SERVER-53105
                        new BsonDocument(UUID_FIELD, collectionMetadata.get(UUID_FIELD)));

        chunks.find(filter)
                .projection(include("min", "max", "shard"))
                .sort(ascending("min"))
                .into(collectionChunks);
        return collectionChunks;
    }

    @Nullable
    public static BsonDocument readCollectionMetadata(
            MongoClient mongoClient, TableId collectionId) {
        MongoCollection<BsonDocument> collection =
                collectionFor(mongoClient, TableId.parse("config.collections"), BsonDocument.class);

        return collection
                .find(eq(ID_FIELD, collectionId.identifier()))
                .projection(include(ID_FIELD, UUID_FIELD, DROPPED_FIELD, KEY_FIELD))
                .first();
    }

    public static <T> MongoCollection<T> collectionFor(
            MongoClient mongoClient, TableId collectionId, Class<T> documentClass) {
        return mongoClient
                .getDatabase(collectionId.catalog())
                .getCollection(collectionId.table())
                .withDocumentClass(documentClass);
    }

    public static MongoClient clientFor(MongoDBSourceConfig sourceConfig) {
        return MongoClientPool.getInstance().getOrCreateMongoClient(sourceConfig);
    }

    public static ConnectionString buildConnectionString(
            @Nullable String username,
            @Nullable String password,
            String hosts,
            @Nullable String connectionOptions) {
        StringBuilder sb = new StringBuilder(MONGODB_SCHEME).append("://");

        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            sb.append(encodeValue(username)).append(":").append(encodeValue(password)).append("@");
        }

        sb.append(checkNotNull(hosts));

        if (StringUtils.isNotEmpty(connectionOptions)) {
            sb.append("/?").append(connectionOptions);
        }

        return new ConnectionString(sb.toString());
    }
}
