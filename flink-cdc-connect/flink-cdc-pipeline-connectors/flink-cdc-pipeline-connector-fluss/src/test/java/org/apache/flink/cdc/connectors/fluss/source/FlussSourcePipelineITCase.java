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

package org.apache.flink.cdc.connectors.fluss.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.source.discover.TableDiscoverer;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.fluss.source.deserializer.FlussRecordDeserializer;
import org.apache.flink.cdc.connectors.fluss.source.discover.FlussDefaultDiscoverer;
import org.apache.flink.cdc.connectors.fluss.source.discover.FlussSubscriberTableDiscoverer;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link FlussSource} as a CDC pipeline source. */
public class FlussSourcePipelineITCase {

    private static final Logger LOG = LoggerFactory.getLogger(FlussSourcePipelineITCase.class);
    private static final int MAX_PARALLELISM = 4;
    private static final String DATABASE_NAME = "test_source_db";
    private static final Duration COLLECT_TIMEOUT = Duration.ofSeconds(60);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(MAX_PARALLELISM)
                            .build());

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    protected TableEnvironment tBatchEnv;

    @BeforeEach
    void before() throws Exception {
        waitForFlussClusterReady();
        String bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        tBatchEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tBatchEnv.executeSql(
                String.format(
                        "CREATE CATALOG test_catalog WITH ('type' = 'fluss', '%s' = '%s')",
                        BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tBatchEnv.executeSql("USE CATALOG test_catalog");
        tBatchEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tBatchEnv.executeSql("CREATE DATABASE " + DATABASE_NAME);
        tBatchEnv.useDatabase(DATABASE_NAME);
    }

    @AfterEach
    void after() {
        tBatchEnv.useDatabase(BUILTIN_DATABASE);
        tBatchEnv.executeSql(String.format("DROP DATABASE %s CASCADE", DATABASE_NAME));
    }

    @Test
    void testNonPartitionedPkTable() throws Exception {
        String tableName = "pk_table";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
                                tableName))
                .await();

        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "earliest");
        List<Event> allEvents = collectAllEvents(source, 4, COLLECT_TIMEOUT);
        List<String> actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE pk_table (`id` INT NOT NULL, `name` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[1, Alice]",
                        "+I[2, Bob]",
                        "+I[3, Charlie]");
    }

    @Test
    void testNonPartitionedLogTable() throws Exception {
        String tableName = "log_table";
        tBatchEnv
                .executeSql(String.format("CREATE TABLE %s (id INT, name STRING)", tableName))
                .await();

        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve')",
                                tableName))
                .await();

        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "earliest");
        List<Event> allEvents = collectAllEvents(source, 6, COLLECT_TIMEOUT);
        List<String> actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE log_table (`id` INT, `name` STRING)",
                        "+I[1, Alice]",
                        "+I[2, Bob]",
                        "+I[3, Charlie]",
                        "+I[4, David]",
                        "+I[5, Eve]");
    }

    @Test
    void testPartitionedPkTable() throws Exception {
        String tableName = "part_pk_table";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s ("
                                        + "id INT, ds STRING, name STRING, "
                                        + "PRIMARY KEY (id, ds) NOT ENFORCED"
                                        + ") PARTITIONED BY (ds)",
                                tableName))
                .await();

        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES "
                                        + "(1, '20240101', 'Alice'), (2, '20240101', 'Bob'), "
                                        + "(3, '20240102', 'Charlie'), (4, '20240102', 'David')",
                                tableName))
                .await();

        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "earliest");
        List<Event> allEvents = collectAllEvents(source, 5, COLLECT_TIMEOUT, 1);
        List<Event> schemaEvents =
                allEvents.stream()
                        .filter(CreateTableEvent.class::isInstance)
                        .collect(Collectors.toList());
        RecordData.FieldGetter partitionFieldGetter =
                RecordData.createFieldGetter(DataTypes.STRING(), 1);
        Map<String, List<Event>> eventsByPartition =
                allEvents.stream()
                        .filter(DataChangeEvent.class::isInstance)
                        .collect(
                                Collectors.groupingBy(
                                        event ->
                                                partitionFieldGetter
                                                        .getFieldOrNull(
                                                                ((DataChangeEvent) event).after())
                                                        .toString()));

        assertThat(
                        convertToStringList(
                                schemaEvents,
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.STRING()))
                .containsExactly(
                        "CREATE TABLE part_pk_table (`id` INT NOT NULL, `ds` STRING NOT NULL, `name` STRING, PRIMARY KEY (id, ds) NOT ENFORCED) PARTITIONED BY (ds)");
        assertThat(
                        convertToStringList(
                                eventsByPartition.get("20240101"),
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.STRING()))
                .containsExactly("+I[1, 20240101, Alice]", "+I[2, 20240101, Bob]");
        assertThat(
                        convertToStringList(
                                eventsByPartition.get("20240102"),
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.STRING()))
                .containsExactly("+I[3, 20240102, Charlie]", "+I[4, 20240102, David]");
    }

    @Test
    void testMixedTables() throws Exception {
        String pkTable = "mixed_pk";
        String logTable = "mixed_log";
        String partTable = "mixed_part";

        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                pkTable))
                .await();
        tBatchEnv
                .executeSql(String.format("CREATE TABLE %s (id INT, val STRING)", logTable))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s ("
                                        + "id INT, ds STRING, val STRING, "
                                        + "PRIMARY KEY (id, ds) NOT ENFORCED"
                                        + ") PARTITIONED BY (ds)",
                                partTable))
                .await();

        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'pk1'), (2, 'pk2')", pkTable))
                .await();
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (1, 'log1'), (2, 'log2')", logTable))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, '20240101', 'part1'), (2, '20240102', 'part2')",
                                partTable))
                .await();

        // Use wildcard pattern to read all tables
        FlussSource<Event> source = createFlussSource(DATABASE_NAME, "mixed_*", "earliest");
        List<Event> allEvents = collectAllEvents(source, 9, COLLECT_TIMEOUT, 1);
        Map<String, List<Event>> eventsByTable =
                allEvents.stream()
                        .collect(
                                Collectors.groupingBy(
                                        event -> ((ChangeEvent) event).tableId().getTableName()));

        assertThat(
                        convertToStringList(
                                eventsByTable.get(pkTable), DataTypes.INT(), DataTypes.STRING()))
                .containsExactly(
                        "CREATE TABLE mixed_pk (`id` INT NOT NULL, `val` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[1, pk1]",
                        "+I[2, pk2]");
        assertThat(
                        convertToStringList(
                                eventsByTable.get(logTable), DataTypes.INT(), DataTypes.STRING()))
                .containsExactly(
                        "CREATE TABLE mixed_log (`id` INT, `val` STRING)",
                        "+I[1, log1]",
                        "+I[2, log2]");
        assertThat(
                        convertToStringList(
                                eventsByTable.get(partTable),
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.STRING()))
                .containsExactly(
                        "CREATE TABLE mixed_part (`id` INT NOT NULL, `ds` STRING NOT NULL, `val` STRING, PRIMARY KEY (id, ds) NOT ENFORCED) PARTITIONED BY (ds)",
                        "+I[1, 20240101, part1]",
                        "+I[2, 20240102, part2]");
    }

    @Test
    void testEarliestStartupMode() throws Exception {
        String tableName = "earliest_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C')", tableName))
                .await();

        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "earliest");
        List<Event> allEvents = collectAllEvents(source, 4, COLLECT_TIMEOUT);
        List<String> actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE earliest_test (`id` INT NOT NULL, `name` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[1, A]",
                        "+I[2, B]",
                        "+I[3, C]");
    }

    @Test
    void testLatestStartupMode() throws Exception {

        String tableName = "latest_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        // Write initial data BEFORE starting the source
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (1, 'Old1'), (2, 'Old2')", tableName))
                .await();

        // Start source in "latest" mode in background
        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "latest");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("LatestModeTest");

        // Wait for source to be initialized
        Thread.sleep(5000);

        // Write new data AFTER source started
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (3, 'New1'), (4, 'New2')", tableName))
                .await();

        // Should only receive the NEW data (written after source started)
        List<Event> allEvents = collectAllEvents(iter, 3, COLLECT_TIMEOUT, true);
        List<String> actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE latest_test (`id` INT NOT NULL, `name` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[3, New1]",
                        "+I[4, New2]");
    }

    @Test
    void testFullStartupMode() throws Exception {
        String tableName = "full_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        // Write initial data (will be captured by KV snapshot)
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'Snap1'), (2, 'Snap2'), (3, 'Snap3')",
                                tableName))
                .await();

        // Wait for KV snapshot to be taken (configured interval is 1s)
        Thread.sleep(3000);

        // Write more data (will be in log after snapshot)
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (4, 'Log1'), (5, 'Log2')", tableName))
                .await();

        // Start source in "full" mode - should read snapshot + log
        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "full");
        List<Event> allEvents = collectAllEvents(source, 6, COLLECT_TIMEOUT);
        List<String> actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE full_test (`id` INT NOT NULL, `name` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[1, Snap1]",
                        "+I[2, Snap2]",
                        "+I[3, Snap3]",
                        "+I[4, Log1]",
                        "+I[5, Log2]");
    }

    @Test
    void testTimestampStartupMode() throws Exception {
        String tableName = "timestamp_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        // Write phase 1 data
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'Before1'), (2, 'Before2')", tableName))
                .await();

        // Record timestamp marker
        Thread.sleep(1000);
        long timestampMarker = System.currentTimeMillis();
        Thread.sleep(1000);

        // Write phase 2 data
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (3, 'After1'), (4, 'After2'), (5, 'After3')",
                                tableName))
                .await();

        // Start source from timestamp - should only read data after the marker
        FlussSource<Event> source =
                createFlussSourceWithTimestamp(DATABASE_NAME, tableName, timestampMarker);
        List<Event> allEvents = collectAllEvents(source, 4, COLLECT_TIMEOUT);
        List<String> actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE timestamp_test (`id` INT NOT NULL, `name` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[3, After1]",
                        "+I[4, After2]",
                        "+I[5, After3]");
    }

    @Test
    void testSavepointAndRestore(@TempDir Path tmpDir) throws Exception {
        String tableName = "savepoint_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        // Write initial data
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C')", tableName))
                .await();

        // Phase 1: Start source job, consume data, take savepoint
        FlussSource<Event> source1 = createFlussSource(DATABASE_NAME, tableName, "earliest");
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.setParallelism(2);
        env1.enableCheckpointing(200);

        env1.fromSource(
                        source1,
                        WatermarkStrategy.noWatermarks(),
                        "FlussSource",
                        new EventTypeInfo())
                .uid("fluss-source")
                .sinkTo(new DiscardingSink<>())
                .uid("discard-sink");

        JobClient jobClient = env1.executeAsync("SavepointPhase1");
        Thread.sleep(10000); // Wait for data to be consumed and checkpoints to complete

        // Take savepoint and stop
        String savepointPath =
                jobClient
                        .stopWithSavepoint(
                                false,
                                tmpDir.toAbsolutePath().toString(),
                                SavepointFormatType.CANONICAL)
                        .get();
        LOG.info("Savepoint taken at: {}", savepointPath);

        // Write more data
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (4, 'D'), (5, 'E')", tableName))
                .await();

        // Phase 2: Restore from savepoint
        org.apache.flink.configuration.Configuration restoreConf =
                new org.apache.flink.configuration.Configuration();
        restoreConf.setString("execution.savepoint.path", savepointPath);
        // restoreConf.setString("execution.savepoint.ignore-unclaimed-state", "true");
        StreamExecutionEnvironment env2 =
                StreamExecutionEnvironment.getExecutionEnvironment(restoreConf);
        env2.setParallelism(2);
        env2.enableCheckpointing(200);

        FlussSource<Event> source2 = createFlussSource(DATABASE_NAME, tableName, "earliest");

        CloseableIterator<Event> iter =
                env2.fromSource(
                                source2,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .uid("fluss-source")
                        .executeAndCollect("SavepointPhase2");

        // After restore, should receive only the new events (D, E), not the old ones
        List<Event> allEvents = collectAllEvents(iter, 3, COLLECT_TIMEOUT, true);
        List<String> actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE savepoint_test (`id` INT NOT NULL, `name` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[4, D]",
                        "+I[5, E]");
    }

    @Test
    void testNewTableDiscovery() throws Exception {
        String tableA = "discover_a";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableA))
                .await();
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'a1'), (2, 'a2')", tableA))
                .await();

        // Start source with wildcard pattern and short discovery interval (2s)
        FlussSource<Event> source =
                createFlussSourceWithDiscoveryInterval(
                        DATABASE_NAME, "discover_*", "earliest", Duration.ofSeconds(10));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("DiscoveryTest");

        List<Event> allEvents = collectAllEvents(iter, 3, Duration.ofMinutes(5), false);
        List<String> actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE discover_a (`id` INT NOT NULL, `val` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[1, a1]",
                        "+I[2, a2]");

        // Create a new table and write data
        String tableB = "discover_b";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableB))
                .await();
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'b1'), (2, 'b2')", tableB))
                .await();

        allEvents = collectAllEvents(iter, 3, Duration.ofMinutes(5), true);
        actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE discover_b (`id` INT NOT NULL, `val` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[1, b1]",
                        "+I[2, b2]");
    }

    @Test
    void testNewTableDiscoveryViaSubscriptionTable() throws Exception {
        // 1. Create the subscription table: single STRING pk column that holds the FQN
        //    (database.tableName) of each subscribed table.
        String subscriptionTable = "subscription_list";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (table_name STRING, PRIMARY KEY (table_name) NOT ENFORCED)",
                                subscriptionTable))
                .await();

        // 2. Create target table A and seed initial subscription pointing to A.
        String tableA = "sub_discover_a";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableA))
                .await();
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'a1'), (2, 'a2')", tableA))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES ('%s.%s')",
                                subscriptionTable, DATABASE_NAME, tableA))
                .await();

        // 3. Start the source using FlussTableSubscriber.
        FlussSource<Event> source =
                createFlussSourceWithTableSubscriber(
                        DATABASE_NAME + "." + subscriptionTable,
                        "earliest",
                        Duration.ofSeconds(10));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("FlussTableSubscriberDiscoveryTest");

        // Phase 1: should receive CreateTable(tableA) + its 2 data rows.
        List<Event> allEvents = collectAllEvents(iter, 3, Duration.ofMinutes(5), false);
        List<String> actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE sub_discover_a (`id` INT NOT NULL, `val` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[1, a1]",
                        "+I[2, a2]");

        // 4. Dynamically extend the subscription: append a row pointing to a freshly
        //    created table B. The running enumerator should discover B on its next cycle.
        String tableB = "sub_discover_b";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableB))
                .await();
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'b1'), (2, 'b2')", tableB))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES ('%s.%s')",
                                subscriptionTable, DATABASE_NAME, tableB))
                .await();

        // Phase 2: should receive CreateTable(tableB) + its 2 data rows.
        allEvents = collectAllEvents(iter, 3, Duration.ofMinutes(5), true);
        actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE sub_discover_b (`id` INT NOT NULL, `val` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[1, b1]",
                        "+I[2, b2]");
    }

    @Test
    void testNewPartitionDiscovery() throws Exception {
        String tableName = "part_discover_table";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s ("
                                        + "id INT, ds STRING, val STRING, "
                                        + "PRIMARY KEY (id, ds) NOT ENFORCED"
                                        + ") PARTITIONED BY (ds)",
                                tableName))
                .await();

        // Write data to partition p1
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, '20240101', 'p1_v1'), (2, '20240101', 'p1_v2')",
                                tableName))
                .await();

        // Start source with short discovery interval
        FlussSource<Event> source =
                createFlussSourceWithDiscoveryInterval(
                        DATABASE_NAME, tableName, "earliest", Duration.ofSeconds(10));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("PartitionDiscoveryTest");

        List<Event> allEvents = collectAllEvents(iter, 3, Duration.ofMinutes(5), false);
        List<String> actual =
                convertToStringList(
                        allEvents, DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE part_discover_table (`id` INT NOT NULL, `ds` STRING NOT NULL, `val` STRING, PRIMARY KEY (id, ds) NOT ENFORCED) PARTITIONED BY (ds)",
                        "+I[1, 20240101, p1_v1]",
                        "+I[2, 20240101, p1_v2]");

        // Add a new partition (p2) by inserting data with a new partition value
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (3, '20240102', 'p2_v1'), (4, '20240102', 'p2_v2')",
                                tableName))
                .await();

        // Should discover and read from both partitions
        allEvents = collectAllEvents(iter, 3, Duration.ofMinutes(5), true);
        actual =
                convertToStringList(
                        allEvents, DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE part_discover_table (`id` INT NOT NULL, `ds` STRING NOT NULL, `val` STRING, PRIMARY KEY (id, ds) NOT ENFORCED) PARTITIONED BY (ds)",
                        "+I[3, 20240102, p2_v1]",
                        "+I[4, 20240102, p2_v2]");
    }

    @Test
    void testAddColumnSchemaEvolution() throws Exception {
        String tableName = "add_column_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        // Write initial data with original schema
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName))
                .await();

        // Start source in earliest mode with discovery interval
        FlussSource<Event> source =
                createFlussSourceWithDiscoveryInterval(
                        DATABASE_NAME, tableName, "full", Duration.ofSeconds(10));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("AddColumnTest");

        // Phase 1: Collect CreateTableEvent + initial 2 DataChangeEvents
        List<Event> allEvents = collectAllEvents(iter, 3, Duration.ofMinutes(2), false);
        List<String> actual = convertToStringList(allEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactly(
                        "CREATE TABLE add_column_test (`id` INT NOT NULL, `name` STRING, PRIMARY KEY (id) NOT ENFORCED)",
                        "+I[1, Alice]",
                        "+I[2, Bob]");

        // Phase 2: ALTER TABLE to add a new nullable column
        tBatchEnv.executeSql(String.format("ALTER TABLE %s ADD `age` INT", tableName)).await();

        // Write new data with the new column populated
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (3, 'Charlie', 30), (4, 'David', 40)",
                                tableName))
                .await();

        // Phase 3: Collect remaining events — expect 1 AddColumnEvent + 2 DataChangeEvents
        allEvents = collectAllEvents(iter, 3, Duration.ofMinutes(2), true);
        List<String> newStrings =
                convertToStringList(
                        allEvents, DataTypes.INT(), DataTypes.STRING(), DataTypes.INT());
        assertThat(newStrings)
                .containsExactly(
                        "ALTER TABLE add_column_test ADD `age` INT LAST",
                        "+I[3, Charlie, 30]",
                        "+I[4, David, 40]");
    }

    // ======================== Helper methods ========================

    private FlussSource<Event> createFlussSource(
            String database, String tablePattern, String startupMode) {
        return createFlussSourceWithDiscoveryInterval(
                database, tablePattern, startupMode, Duration.ofMinutes(1));
    }

    private FlussSource<Event> createFlussSourceWithTimestamp(
            String database, String tablePattern, long timestampMs) {
        org.apache.fluss.config.Configuration flussConfig =
                FLUSS_CLUSTER_EXTENSION.getClientConfig();
        FlussDefaultDiscoverer discoverer = new FlussDefaultDiscoverer();
        Configuration sourceConfig =
                buildSourceConfig(flussConfig, toFqnRegex(database, tablePattern));
        OffsetsInitializer offsetsInitializer = OffsetsInitializer.timestamp(timestampMs);
        return new FlussSource<>(
                discoverer,
                flussConfig,
                sourceConfig,
                offsetsInitializer,
                Duration.ofMinutes(1).toMillis(),
                new FlussRecordDeserializer());
    }

    private FlussSource<Event> createFlussSourceWithTableSubscriber(
            String subscriptionTableFqn, String startupMode, Duration discoveryInterval) {
        return createFlussSourceWithDiscoveryInterval(
                null,
                null,
                startupMode,
                discoveryInterval,
                new FlussSubscriberTableDiscoverer(subscriptionTableFqn, 100));
    }

    private FlussSource<Event> createFlussSourceWithDiscoveryInterval(
            String database, String tablePattern, String startupMode, Duration discoveryInterval) {
        return createFlussSourceWithDiscoveryInterval(
                database,
                tablePattern,
                startupMode,
                discoveryInterval,
                new FlussDefaultDiscoverer());
    }

    private FlussSource<Event> createFlussSourceWithDiscoveryInterval(
            @Nullable String database,
            @Nullable String tablePattern,
            String startupMode,
            Duration discoveryInterval,
            TableDiscoverer tableDiscoverer) {
        org.apache.fluss.config.Configuration flussConfig =
                FLUSS_CLUSTER_EXTENSION.getClientConfig();
        String pattern =
                (database == null || tablePattern == null)
                        ? null
                        : toFqnRegex(database, tablePattern);
        Configuration sourceConfig = buildSourceConfig(flussConfig, pattern);
        OffsetsInitializer offsetsInitializer;
        switch (startupMode) {
            case "earliest":
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case "latest":
                offsetsInitializer = OffsetsInitializer.latest();
                break;
            case "full":
                offsetsInitializer = OffsetsInitializer.full();
                break;
            default:
                throw new IllegalArgumentException("Unknown startup mode: " + startupMode);
        }
        return new FlussSource<>(
                tableDiscoverer,
                flussConfig,
                sourceConfig,
                offsetsInitializer,
                discoveryInterval.toMillis(),
                new FlussRecordDeserializer());
    }

    /**
     * Translates the legacy (database, tablePattern) arguments into a single Java regex matching
     * fully-qualified {@code database.tableName} names. The {@code '*'} wildcard in the old
     * tablePattern is translated to regex {@code .*}.
     */
    private static String toFqnRegex(String database, String tablePattern) {
        return java.util.regex.Pattern.quote(database) + "\\." + tablePattern.replace("*", ".*");
    }

    /**
     * Builds a source {@link Configuration} for use with {@link
     * TableDiscoverer#open(TableDiscoverer.Context)}. Includes bootstrap.servers and optionally the
     * table.discoverer.pattern.
     */
    private static Configuration buildSourceConfig(
            org.apache.fluss.config.Configuration flussConfig, String pattern) {
        Map<String, String> map = new HashMap<>();
        String bootstrapServers =
                flussConfig
                        .toMap()
                        .get(org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS.key());
        if (bootstrapServers != null) {
            map.put("bootstrap.servers", bootstrapServers);
        }
        if (pattern != null) {
            map.put("table.discoverer.pattern", pattern);
        }
        // Copy client.* properties
        flussConfig
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith("client.")) {
                                map.put("properties." + key, value);
                            }
                        });
        return Configuration.fromMap(map);
    }

    /**
     * Collects ALL event types from a FlussSource using executeAndCollect. Returns when
     * expectedCount events have been collected or timeout is reached.
     */
    private List<Event> collectAllEvents(
            FlussSource<Event> source, int expectedCount, Duration timeout) throws Exception {
        return collectAllEvents(source, expectedCount, timeout, 2);
    }

    private List<Event> collectAllEvents(
            FlussSource<Event> source, int expectedCount, Duration timeout, int parallelism)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("FlussSourceCollect");
        return collectAllEvents(iter, expectedCount, timeout, true);
    }

    /**
     * Collects ALL event types (DataChangeEvent, SchemaChangeEvent, etc.) from the iterator.
     * Returns when expectedCount events have been collected or timeout is reached.
     */
    private List<Event> collectAllEvents(
            CloseableIterator<Event> iter,
            int expectedCount,
            Duration timeout,
            boolean closeIterator)
            throws Exception {

        List<Event> events = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(expectedCount);

        Thread collector =
                new Thread(
                        () -> {
                            try {
                                while (events.size() < expectedCount && iter.hasNext()) {
                                    Event event = iter.next();
                                    events.add(event);
                                    latch.countDown();
                                }
                            } catch (Exception ignored) {
                                // Iterator closed or interrupted
                            }
                        },
                        "all-event-collector");
        collector.setDaemon(true);
        collector.start();

        try {
            boolean completed = latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!completed) {
                LOG.warn(
                        "Timeout collecting all events. Expected {}, got {}",
                        expectedCount,
                        events.size());
            }
        } finally {
            if (closeIterator) {
                iter.close();
            }
            collector.join(5000);
        }

        return events;
    }

    private String createTableEventToString(CreateTableEvent event) {
        String columns =
                event.getSchema().getColumns().stream()
                        .map(Column::asSummaryString)
                        .collect(Collectors.joining(", "));
        List<String> tableConstraints = new ArrayList<>();
        tableConstraints.add(columns);
        if (!event.getSchema().primaryKeys().isEmpty()) {
            tableConstraints.add(
                    String.format(
                            "PRIMARY KEY (%s) NOT ENFORCED",
                            String.join(", ", event.getSchema().primaryKeys())));
        }
        String ddl =
                String.format(
                        "CREATE TABLE %s (%s)",
                        event.tableId().getTableName(), String.join(", ", tableConstraints));
        if (!event.getSchema().partitionKeys().isEmpty()) {
            ddl +=
                    String.format(
                            " PARTITIONED BY (%s)",
                            String.join(", ", event.getSchema().partitionKeys()));
        }
        return ddl;
    }

    /** Converts a list of events to human-readable strings while preserving event order. */
    private List<String> convertToStringList(List<? extends Event> events, DataType... fieldTypes) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>();
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldGetters.add(RecordData.createFieldGetter(fieldTypes[i], i));
        }
        List<String> result = new ArrayList<>();
        for (Event event : events) {
            if (event instanceof CreateTableEvent) {
                result.add(createTableEventToString((CreateTableEvent) event));
            } else if (event instanceof AddColumnEvent) {
                result.add(addColumnEventToString((AddColumnEvent) event));
            } else if (event instanceof DataChangeEvent) {
                result.add(eventToString((DataChangeEvent) event, fieldGetters));
            } else {
                throw new IllegalStateException(String.format("%s is not expected", event));
            }
        }
        return result;
    }

    private String addColumnEventToString(AddColumnEvent event) {
        return event.getAddedColumns().stream()
                .map(
                        columnWithPosition -> {
                            String addColumn = columnWithPosition.getAddColumn().asSummaryString();
                            AddColumnEvent.ColumnPosition position =
                                    columnWithPosition.getPosition();
                            if (position == AddColumnEvent.ColumnPosition.BEFORE
                                    || position == AddColumnEvent.ColumnPosition.AFTER) {
                                return String.format(
                                        "ALTER TABLE %s ADD %s %s %s",
                                        event.tableId().getTableName(),
                                        addColumn,
                                        position,
                                        columnWithPosition.getExistedColumnName());
                            }
                            return String.format(
                                    "ALTER TABLE %s ADD %s %s",
                                    event.tableId().getTableName(), addColumn, position);
                        })
                .collect(Collectors.joining("; "));
    }

    private String eventToString(DataChangeEvent event, List<RecordData.FieldGetter> fieldGetters) {
        String prefix;
        RecordData record;
        switch (event.op()) {
            case INSERT:
                prefix = "+I";
                record = event.after();
                break;
            case DELETE:
                prefix = "-D";
                record = event.before();
                break;
            case REPLACE:
                prefix = "+R";
                record = event.after();
                break;
            case UPDATE:
                prefix = "+U";
                record = event.after();
                break;
            default:
                throw new IllegalArgumentException("Unknown op: " + event.op());
        }
        List<Object> fields = new ArrayList<>();
        int fieldCount = Math.min(record.getArity(), fieldGetters.size());
        for (int i = 0; i < fieldCount; i++) {
            fields.add(fieldGetters.get(i).getFieldOrNull(record));
        }
        return prefix + fields;
    }

    private void waitForFlussClusterReady() throws Exception {
        int maxRetries = 30;
        int retryIntervalMs = 1000;
        Exception lastException = null;
        for (int i = 0; i < maxRetries; i++) {
            try (Connection connection =
                    ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
                return;
            } catch (Exception e) {
                lastException = e;
                Thread.sleep(retryIntervalMs);
            }
        }
        throw new IllegalStateException(
                "Failed to connect to Fluss cluster after " + maxRetries + " attempts",
                lastException);
    }

    private static org.apache.fluss.config.Configuration initConfig() {
        org.apache.fluss.config.Configuration conf = new org.apache.fluss.config.Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        conf.setDouble(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO, 1.0);
        return conf;
    }
}
