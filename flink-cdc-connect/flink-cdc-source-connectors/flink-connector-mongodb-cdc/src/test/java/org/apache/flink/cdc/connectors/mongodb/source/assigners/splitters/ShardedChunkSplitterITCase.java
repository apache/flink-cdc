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
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSourceBuilder;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Sorts.descending;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBTestUtils.fetchRowData;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.catalog.Column.physical;

/** IT tests for shard chunk splitter query plans. */
@Timeout(value = 240, unit = TimeUnit.SECONDS)
class ShardedChunkSplitterITCase {

    private static final String TARGET_COLLECTION = "shopping_cart";
    private static final String PROFILE_NAMESPACE = "config.chunks";
    private static final int CONFIG_SERVER_PORT = 27019;

    static Stream<Arguments> mongoVersions() {
        return Stream.of(Arguments.of("6.0.13", "uuid"), Arguments.of("8.0.14", "uuid"));
    }

    @ParameterizedTest(name = "mongo-{0} filter-{1}")
    @MethodSource("mongoVersions")
    void testShardedChunkSplitterReadsChunksWithIndexedPlan(
            String mongoVersion, String expectedFilterField) throws Exception {
        MongoDBContainer container = new MongoDBContainer("mongo:" + mongoVersion).withSharding();
        container.start();
        MongoClient mongoClient = createMongoClient(container);
        try {
            String database = container.executeCommandFileInSeparateDatabase("chunk_test");
            MongoDBSourceConfig sourceConfig = createSourceConfig(container, database);
            SplitContext splitContext =
                    SplitContext.of(sourceConfig, new TableId(database, null, TARGET_COLLECTION));

            setConfigServerProfilerLevel(container, 2);
            Instant queryStart = Instant.now();

            Collection<SnapshotSplit> splits = ShardedSplitStrategy.INSTANCE.split(splitContext);
            Assertions.assertThat(splits).isNotEmpty();

            List<String> records = readSnapshotWithSource(container, database);
            Assertions.assertThat(records).hasSize(20480);
            Assertions.assertThat(records).contains("+I[1, KIND_1, user_1, my shopping cart 1]");
            Assertions.assertThat(records)
                    .contains("+I[20480, KIND_20480, user_20480, my shopping cart 20480]");

            List<Document> profiledEntries =
                    waitProfiledFindEntries(mongoClient.getDatabase("config"), queryStart);
            Assertions.assertThat(profiledEntries).isNotEmpty();
            Assertions.assertThat(profiledPlanSummaries(profiledEntries))
                    .allMatch(plan -> plan != null && plan.contains("IXSCAN"));

            for (Document entry : profiledEntries) {
                Document filter = findCommandFilter(entry);
                Assertions.assertThat(filter).isNotNull();
                Assertions.assertThat(filter).doesNotContainKey("$or");
                Assertions.assertThat(filter).containsKey(expectedFilterField);
            }
        } finally {
            setConfigServerProfilerLevel(container, 0);
            mongoClient.close();
            container.stop();
        }
    }

    private MongoClient createMongoClient(MongoDBContainer container) {
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(container.getConnectionString()))
                        .build();
        return MongoClients.create(settings);
    }

    private MongoDBSourceConfig createSourceConfig(MongoDBContainer container, String database) {
        return new MongoDBSourceConfigFactory()
                .hosts(container.getHostAndPort())
                .databaseList(database)
                .collectionList(database + "." + TARGET_COLLECTION)
                .username(FLINK_USER)
                .password(FLINK_USER_PASSWORD)
                .splitSizeMB(1)
                .samplesPerChunk(10)
                .pollAwaitTimeMillis(500)
                .create(0);
    }

    private void setConfigServerProfilerLevel(MongoDBContainer container, int profileLevel) {
        String command =
                String.format(
                        "const cfg = new Mongo('mongodb://127.0.0.1:%d/?directConnection=true');"
                                + "const database = cfg.getDB('config');"
                                + "database.runCommand({ profile: %d, slowms: 0 });",
                        CONFIG_SERVER_PORT, profileLevel);
        container.executeCommand(command);
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

    private List<String> readSnapshotWithSource(MongoDBContainer container, String database)
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
                        .hosts(container.getHostAndPort())
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
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Mongo source")
                        .executeAndCollect()) {
            return fetchRowData(iterator, 20480, table::stringify);
        }
    }

    private List<Document> profiledFindEntries(MongoDatabase database, Instant since) {
        MongoCollection<Document> profile = database.getCollection("system.profile");
        List<Document> entries =
                profile.find(
                                and(
                                        eq("ns", PROFILE_NAMESPACE),
                                        in("op", "query", "command"),
                                        gte("ts", Date.from(since))))
                        .sort(descending("ts"))
                        .into(new ArrayList<>());
        return entries.stream()
                .filter(ShardedChunkSplitterITCase::isFindOperation)
                .collect(Collectors.toList());
    }

    private static boolean isFindOperation(Document profileEntry) {
        Document command = profileEntry.get("command", Document.class);
        return command != null && "chunks".equals(command.getString("find"));
    }

    private List<String> profiledPlanSummaries(List<Document> entries) {
        return entries.stream().map(e -> e.getString("planSummary")).collect(Collectors.toList());
    }

    private static Document findCommandFilter(Document profileEntry) {
        Document command = profileEntry.get("command", Document.class);
        return command == null ? null : command.get("filter", Document.class);
    }
}
