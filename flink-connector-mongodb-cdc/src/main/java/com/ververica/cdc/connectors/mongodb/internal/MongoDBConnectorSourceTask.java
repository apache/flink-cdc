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

package com.ververica.cdc.connectors.mongodb.internal;

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceTask;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Envelope;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.bson.conversions.Bson;
import org.bson.json.JsonReader;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.regex;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.HEARTBEAT_VALUE_SCHEMA;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.ADD_NS_FIELD;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.ADD_NS_FIELD_NAME;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.bsonListToJson;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionNames;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionsFilter;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.completionPattern;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseFilter;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseNames;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.includeListAsPatterns;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.isIncludeListExplicitlySpecified;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.isHeartbeatEvent;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.isSnapshotRecord;

/**
 * Source Task that proxies mongodb kafka connector's {@link MongoSourceTask} to adapt to {@link
 * com.ververica.cdc.debezium.internal.DebeziumChangeFetcher}.
 */
public class MongoDBConnectorSourceTask extends SourceTask {

    public static final String DATABASE_INCLUDE_LIST = "database.include.list";

    public static final String COLLECTION_INCLUDE_LIST = "collection.include.list";

    private final MongoSourceTask target;

    private final Field isCopyingField;

    private SourceRecord currentLastSnapshotRecord;

    private boolean isInSnapshotPhase = false;

    public MongoDBConnectorSourceTask() throws NoSuchFieldException {
        this.target = new MongoSourceTask();
        this.isCopyingField = MongoSourceTask.class.getDeclaredField("isCopying");
    }

    @Override
    public String version() {
        return target.version();
    }

    @Override
    public void initialize(SourceTaskContext context) {
        this.target.initialize(context);
        this.context = context;
    }

    @Override
    public void start(Map<String, String> props) {
        initCapturedCollections(props);
        target.start(props);
        isInSnapshotPhase = isCopying();
    }

    @Override
    public void commit() throws InterruptedException {
        target.commit();
    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        target.commitRecord(record);
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata)
            throws InterruptedException {
        target.commitRecord(record, metadata);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> sourceRecords = target.poll();
        List<SourceRecord> outSourceRecords = new LinkedList<>();
        if (isInSnapshotPhase) {
            // Step1. Snapshot Phase
            if (sourceRecords != null && !sourceRecords.isEmpty()) {
                for (SourceRecord sourceRecord : sourceRecords) {
                    SourceRecord current = markRecordTimestamp(sourceRecord);

                    if (isSnapshotRecord(current)) {
                        markSnapshotRecord(current);
                        if (currentLastSnapshotRecord != null) {
                            outSourceRecords.add(currentLastSnapshotRecord);
                        }
                        // Keep the current last snapshot record.
                        // When exit snapshot phase, mark it as the last of all snapshot records.
                        currentLastSnapshotRecord = current;
                    } else {
                        // Snapshot Phase Ended, Condition 1:
                        // Received non-snapshot record, exit snapshot phase immediately.
                        if (currentLastSnapshotRecord != null) {
                            outSourceRecords.add(
                                    markLastSnapshotRecordOfAll(currentLastSnapshotRecord));
                            currentLastSnapshotRecord = null;
                            isInSnapshotPhase = false;
                        }
                        outSourceRecords.add(current);
                    }
                }
            } else {
                // Snapshot Phase Ended, Condition 2:
                // No changing stream event comes and source task is finished copying,
                // then exit the snapshot phase.
                if (!isCopying()) {
                    if (currentLastSnapshotRecord != null) {
                        outSourceRecords.add(
                                markLastSnapshotRecordOfAll(currentLastSnapshotRecord));
                        currentLastSnapshotRecord = null;
                    }
                    isInSnapshotPhase = false;
                }
            }
        } else {
            // Step2. Change Streaming Phase
            if (sourceRecords != null && !sourceRecords.isEmpty()) {
                for (SourceRecord current : sourceRecords) {
                    outSourceRecords.add(markRecordTimestamp(current));
                }
            }
        }
        return outSourceRecords;
    }

    @Override
    public void stop() {
        target.stop();
    }

    private SourceRecord markRecordTimestamp(SourceRecord record) {
        if (isHeartbeatEvent(record)) {
            return markTimestampForHeartbeatRecord(record);
        }
        return markTimestampForDataRecord(record);
    }

    private SourceRecord markTimestampForDataRecord(SourceRecord record) {
        final Struct value = (Struct) record.value();
        // It indicates the time at which the reader processed the event.
        value.put(MongoDBEnvelope.TIMESTAMP_KEY_FIELD, System.currentTimeMillis());

        final Struct source = new Struct(value.schema().field(Envelope.FieldName.SOURCE).schema());
        // It indicates the time that the change was made in the database. If the record is read
        // from snapshot of the table instead of the change stream, the value is always 0.
        long timestamp = 0L;
        if (value.schema().field(MongoDBEnvelope.CLUSTER_TIME_FIELD) != null) {
            String clusterTime = value.getString(MongoDBEnvelope.CLUSTER_TIME_FIELD);
            if (clusterTime != null) {
                timestamp = new JsonReader(clusterTime).readTimestamp().getTime() * 1000L;
            }
        }
        source.put(MongoDBEnvelope.TIMESTAMP_KEY_FIELD, timestamp);
        value.put(Envelope.FieldName.SOURCE, source);

        return record;
    }

    private SourceRecord markTimestampForHeartbeatRecord(SourceRecord record) {
        final Struct heartbeatValue = new Struct(HEARTBEAT_VALUE_SCHEMA);
        heartbeatValue.put(MongoDBEnvelope.TIMESTAMP_KEY_FIELD, Instant.now().toEpochMilli());

        return new SourceRecord(
                record.sourcePartition(),
                record.sourceOffset(),
                record.topic(),
                record.keySchema(),
                record.key(),
                HEARTBEAT_VALUE_SCHEMA,
                heartbeatValue);
    }

    private void markSnapshotRecord(SourceRecord record) {
        final Struct value = (Struct) record.value();
        final Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        SnapshotRecord.TRUE.toSource(source);
    }

    private SourceRecord markLastSnapshotRecordOfAll(SourceRecord record) {
        final Struct value = (Struct) record.value();
        final Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        final SnapshotRecord snapshot = SnapshotRecord.fromSource(source);
        if (snapshot == SnapshotRecord.TRUE) {
            SnapshotRecord.LAST.toSource(source);
        }
        return record;
    }

    private boolean isCopying() {
        isCopyingField.setAccessible(true);
        try {
            return ((AtomicBoolean) isCopyingField.get(target)).get();
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot access isCopying field of SourceTask", e);
        }
    }

    private void initCapturedCollections(Map<String, String> props) {
        ConnectionString connectionString =
                new ConnectionString(props.get(MongoSourceConfig.CONNECTION_URI_CONFIG));

        String databaseIncludeList = props.get(DATABASE_INCLUDE_LIST);
        String collectionIncludeList = props.get(COLLECTION_INCLUDE_LIST);

        List<String> databaseList =
                Optional.ofNullable(databaseIncludeList)
                        .map(input -> Arrays.asList(input.split(",")))
                        .orElse(null);

        List<String> collectionList =
                Optional.ofNullable(collectionIncludeList)
                        .map(input -> Arrays.asList(input.split(",")))
                        .orElse(null);

        if (collectionList != null) {
            // Watching collections changes
            List<String> discoveredDatabases;
            List<String> discoveredCollections;
            try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                discoveredDatabases = databaseNames(mongoClient, databaseFilter(databaseList));
                discoveredCollections =
                        collectionNames(
                                mongoClient,
                                discoveredDatabases,
                                collectionsFilter(collectionList));
            }

            // case: database = db0, collection = coll1
            if (isIncludeListExplicitlySpecified(collectionList, discoveredCollections)) {
                MongoNamespace namespace = new MongoNamespace(discoveredCollections.get(0));
                props.put(MongoSourceConfig.DATABASE_CONFIG, namespace.getDatabaseName());
                props.put(MongoSourceConfig.COLLECTION_CONFIG, namespace.getCollectionName());
            } else { // case: database = db0|db2, collection = (db0.coll[0-9])|(db1.coll[1-2])
                String namespacesRegex =
                        includeListAsPatterns(collectionList).stream()
                                .map(Pattern::pattern)
                                .collect(Collectors.joining("|"));

                List<Bson> pipeline = new ArrayList<>();
                pipeline.add(ADD_NS_FIELD);

                Bson nsFilter = regex(ADD_NS_FIELD_NAME, namespacesRegex);
                if (databaseList != null) {
                    if (isIncludeListExplicitlySpecified(databaseList, discoveredDatabases)) {
                        props.put(MongoSourceConfig.DATABASE_CONFIG, discoveredDatabases.get(0));
                    } else {
                        String databaseRegex =
                                includeListAsPatterns(databaseList).stream()
                                        .map(Pattern::pattern)
                                        .collect(Collectors.joining("|"));
                        Bson dbFilter = regex("ns.db", databaseRegex);
                        nsFilter = and(dbFilter, nsFilter);
                    }
                }
                pipeline.add(match(nsFilter));

                props.put(MongoSourceConfig.PIPELINE_CONFIG, bsonListToJson(pipeline));

                String copyExistingNamespaceRegex =
                        discoveredCollections.stream()
                                .map(ns -> completionPattern(ns).pattern())
                                .collect(Collectors.joining("|"));

                props.put(
                        MongoSourceConfig.COPY_EXISTING_NAMESPACE_REGEX_CONFIG,
                        copyExistingNamespaceRegex);
            }
        } else if (databaseList != null) {
            // Watching databases changes
            List<String> discoveredDatabases;
            try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                discoveredDatabases = databaseNames(mongoClient, databaseFilter(databaseList));
            }

            if (isIncludeListExplicitlySpecified(databaseList, discoveredDatabases)) {
                props.put(MongoSourceConfig.DATABASE_CONFIG, discoveredDatabases.get(0));
            } else {
                String databaseRegex =
                        includeListAsPatterns(databaseList).stream()
                                .map(Pattern::pattern)
                                .collect(Collectors.joining("|"));

                List<Bson> pipeline = new ArrayList<>();
                pipeline.add(match(regex("ns.db", databaseRegex)));
                props.put(MongoSourceConfig.PIPELINE_CONFIG, bsonListToJson(pipeline));

                String copyExistingNamespaceRegex =
                        discoveredDatabases.stream()
                                .map(db -> completionPattern(db + "\\..*").pattern())
                                .collect(Collectors.joining("|"));

                props.put(
                        MongoSourceConfig.COPY_EXISTING_NAMESPACE_REGEX_CONFIG,
                        copyExistingNamespaceRegex);
            }
        } else {
            // Watching all changes on the cluster by default, we do nothing here
        }
    }
}
