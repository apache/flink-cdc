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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSourceBuilder;
import org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils;
import org.apache.flink.cdc.connectors.mongodb.utils.TestTable;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.Network;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Sorts.descending;
import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.FLINK_USER_PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.MONGO_SUPER_PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.MONGO_SUPER_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBTestUtils.fetchRowData;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.catalog.Column.physical;

/** Legacy sharded splitter IT cases for MongoDB version boundaries. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class LegacyShardedChunkSplitterITCase {

    private static final String PROFILE_NAMESPACE = "config.chunks";
    private static final String PROFILE_COMMAND_NAMESPACE = "config.$cmd";
    private static final String TARGET_COLLECTION = "shopping_cart";

    static Stream<Arguments> mongoVersions() {
        return Stream.of(Arguments.of("4.4.29", "ns"), Arguments.of("5.0.2", "uuid"));
    }

    @ParameterizedTest(name = "legacy-mongo-{0} filter-{1}")
    @MethodSource("mongoVersions")
    void testLegacyShardedChunkSplitterWithSource(String mongoVersion, String expectedFilterField)
            throws Exception {
        Network network = Network.newNetwork();
        LegacyMongoDBContainer config =
                new LegacyMongoDBContainer(
                        network,
                        LegacyMongoDBContainer.ShardingClusterRole.CONFIG,
                        "mongo:" + mongoVersion);
        LegacyMongoDBContainer shard =
                new LegacyMongoDBContainer(
                        network,
                        LegacyMongoDBContainer.ShardingClusterRole.SHARD,
                        "mongo:" + mongoVersion);
        LegacyMongoDBContainer router =
                new LegacyMongoDBContainer(
                        network,
                        LegacyMongoDBContainer.ShardingClusterRole.ROUTER,
                        "mongo:" + mongoVersion);
        shard.dependsOn(config);
        router.dependsOn(shard);

        MongoClient configClient = null;
        try {
            config.start();
            shard.start();
            router.start();

            String database = router.executeCommandFileInSeparateDatabase("chunk_test");

            setConfigProfiler(config, 2);
            Instant queryStart = Instant.now();

            List<String> records = readSnapshotWithSource(router, database);
            Assertions.assertThat(records).hasSize(20480);
            Assertions.assertThat(records).contains("+I[1, KIND_1, user_1, my shopping cart 1]");
            Assertions.assertThat(records)
                    .contains("+I[20480, KIND_20480, user_20480, my shopping cart 20480]");

            configClient = createMongoClient(config);
            // In legacy sharding topologies, mongos-level commands are not always visible in
            // config.profile. Trigger the same chunks read directly against config server.
            triggerChunksReadForProfiling(configClient, database);

            List<Document> rawProfileEntries =
                    rawProfileEntries(configClient.getDatabase("config"), queryStart);
            List<Document> profileEntries =
                    waitProfiledFindEntries(configClient.getDatabase("config"), queryStart);
            Assertions.assertThat(profileEntries)
                    .withFailMessage(
                            "No matching profile entries. Raw profile entries:\n%s",
                            profileDebugLines(rawProfileEntries))
                    .isNotEmpty();
            Assertions.assertThat(profiledPlanSummaries(profileEntries))
                    .allMatch(
                            plan ->
                                    plan != null
                                            && plan.contains("IXSCAN")
                                            && !plan.contains("COLLSCAN"));

            for (Document entry : profileEntries) {
                Document filter = findCommandFilter(entry);
                Assertions.assertThat(filter).isNotNull();
                Assertions.assertThat(filter).doesNotContainKey("$or");
                Assertions.assertThat(filter).containsKey(expectedFilterField);
            }
        } finally {
            setConfigProfiler(config, 0);
            if (configClient != null) {
                configClient.close();
            }
            router.close();
            shard.close();
            config.close();
            network.close();
        }
    }

    private void triggerChunksReadForProfiling(MongoClient configClient, String database) {
        TableId tableId = new TableId(database, null, TARGET_COLLECTION);
        org.bson.BsonDocument collectionMetadata =
                MongoUtils.readCollectionMetadata(configClient, tableId);
        Assertions.assertThat(collectionMetadata).isNotNull();
        MongoUtils.readChunks(configClient, collectionMetadata);
    }

    private MongoClient createMongoClient(LegacyMongoDBContainer container) {
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(
                                        container.getConnectionString(
                                                MONGO_SUPER_USER, MONGO_SUPER_PASSWORD)))
                        .build();
        return MongoClients.create(settings);
    }

    private void setConfigProfiler(LegacyMongoDBContainer config, int profileLevel) {
        config.executeCommand(
                String.format(
                        "db = db.getSiblingDB('config');"
                                + "var result = db.runCommand({ profile: %d, slowms: 0 });"
                                + "if (!result.ok) { throw new Error(tojson(result)); }",
                        profileLevel));
    }

    private List<String> readSnapshotWithSource(LegacyMongoDBContainer router, String database)
            throws Exception {
        ResolvedSchema schema =
                new ResolvedSchema(
                        java.util.Arrays.asList(
                                physical("product_no", BIGINT().notNull()),
                                physical("product_kind", STRING()),
                                physical("user_id", STRING()),
                                physical("description", STRING())),
                        new ArrayList<>(),
                        UniqueConstraint.primaryKey(
                                "pk", java.util.Collections.singletonList("product_no")));
        TestTable table = new TestTable(database, TARGET_COLLECTION, schema);

        MongoDBSource<RowData> source =
                new MongoDBSourceBuilder<RowData>()
                        .hosts(router.getHostAndPort())
                        .databaseList(database)
                        .collectionList(database + "." + TARGET_COLLECTION)
                        .username(FLINK_USER)
                        .password(FLINK_USER_PASSWORD)
                        .startupOptions(StartupOptions.snapshot())
                        .scanFullChangelog(false)
                        .deserializer(table.getDeserializer(false))
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        try (CloseableIterator<RowData> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Legacy Mongo source")
                        .executeAndCollect()) {
            return fetchRowData(iterator, 20480, table::stringify);
        }
    }

    private List<Document> waitProfiledFindEntries(MongoDatabase database, Instant since)
            throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            List<Document> entries = profiledFindEntries(database, since);
            if (!entries.isEmpty()) {
                return entries;
            }
            Thread.sleep(200);
        }
        return new ArrayList<>();
    }

    private List<Document> profiledFindEntries(MongoDatabase database, Instant since) {
        List<Document> entries = rawProfileEntries(database, since);
        return entries.stream()
                .filter(LegacyShardedChunkSplitterITCase::isFindOperation)
                .collect(Collectors.toList());
    }

    private List<Document> rawProfileEntries(MongoDatabase database, Instant since) {
        MongoCollection<Document> profile = database.getCollection("system.profile");
        return profile.find(
                        and(
                                in("ns", PROFILE_NAMESPACE, PROFILE_COMMAND_NAMESPACE),
                                in("op", "query", "command")))
                .sort(descending("ts"))
                .limit(200)
                .into(new ArrayList<>());
    }

    private static boolean isFindOperation(Document profileEntry) {
        Document command = profileEntry.get("command", Document.class);
        return command != null && "chunks".equals(command.getString("find"));
    }

    private List<String> profiledPlanSummaries(List<Document> entries) {
        return entries.stream().map(e -> e.getString("planSummary")).collect(Collectors.toList());
    }

    private String profileDebugLines(List<Document> entries) {
        return entries.stream()
                .limit(20)
                .map(
                        entry -> {
                            Document command = entry.get("command", Document.class);
                            Object find = command == null ? null : command.get("find");
                            Object filter = command == null ? null : command.get("filter");
                            return String.format(
                                    "ns=%s, op=%s, find=%s, filter=%s, plan=%s",
                                    entry.get("ns"),
                                    entry.get("op"),
                                    find,
                                    filter,
                                    entry.get("planSummary"));
                        })
                .collect(Collectors.joining("\n"));
    }

    private static Document findCommandFilter(Document profileEntry) {
        Document command = profileEntry.get("command", Document.class);
        return command == null ? null : command.get("filter", Document.class);
    }
}
