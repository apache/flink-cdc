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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.gaussdb.GaussDBTestBase;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link GaussDBSourceBuilder.GaussDBIncrementalSource}. */
@Timeout(value = 600, unit = TimeUnit.SECONDS)
class GaussDBSourceITCase extends GaussDBTestBase {

    private static final String BASE_CUSTOMER_SCHEMA = "customer";
    private static final String BASE_TYPES_SCHEMA = "test_types";
    private static final String BASE_REPLICATION_SCHEMA = "replication_test";

    private static final int DEFAULT_PARALLELISM = 4;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .build()));

    private final List<String> createdSchemas = new ArrayList<>();
    private final Set<String> createdSlots = new HashSet<>();

    private String slotName;
    private String customerSchema;
    private String typesSchema;
    private String replicationSchema;

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @BeforeEach
    public void before() throws Exception {
        TestValuesTableFactory.clearAllData();

        this.slotName = getSlotName();
        this.createdSlots.clear();
        this.createdSlots.add(slotName);

        String suffix = Long.toString(System.nanoTime());
        this.customerSchema = BASE_CUSTOMER_SCHEMA + "_" + suffix;
        this.typesSchema = BASE_TYPES_SCHEMA + "_" + suffix;
        this.replicationSchema = BASE_REPLICATION_SCHEMA + "_" + suffix;

        this.createdSchemas.clear();
        this.createdSchemas.add(customerSchema);
        this.createdSchemas.add(typesSchema);
        this.createdSchemas.add(replicationSchema);

        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setRestartStrategy(RestartStrategies.noRestart());
        this.env.enableCheckpointing(200L);
        this.env.setParallelism(DEFAULT_PARALLELISM);
        this.tEnv = StreamTableEnvironment.create(env);

        executeIsolatedSqlScript("ddl/customer.sql", BASE_CUSTOMER_SCHEMA, customerSchema);
        executeIsolatedSqlScript("ddl/datatypes.sql", BASE_TYPES_SCHEMA, typesSchema);
        executeIsolatedSqlScript(
                "ddl/replica_identity.sql", BASE_REPLICATION_SCHEMA, replicationSchema);
    }

    @AfterEach
    public void after() throws Exception {
        // Sleep a bit to wait until source connections are closed.
        Thread.sleep(1000L);

        for (String slot : createdSlots) {
            try {
                dropReplicationSlot(slot);
            } catch (Exception e) {
                LOG.warn("Failed to drop replication slot '{}'", slot, e);
            }
        }

        for (String schema : createdSchemas) {
            dropSchemaQuietly(schema);
        }

        if (env != null) {
            env.close();
        }
    }

    @Test
    public void testReadSingleTableAllRecords() throws Exception {
        env.setParallelism(1);
        tEnv.executeSql(createCustomersTableDDL("customers_source", "initial", slotName));

        TableResult tableResult = tEnv.executeSql("SELECT id, name FROM customers_source");
        try (CloseableIterator<Row> iter = tableResult.collect()) {
            List<Row> snapshotRows = drainRows(iter, 4);
            assertThat(snapshotRows)
                    .extracting(r -> ((Number) r.getField(0)).intValue())
                    .containsExactlyInAnyOrder(101, 102, 103, 109);

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "INSERT INTO %s.customers (id, name, address, phone_number, email) "
                                        + "VALUES (110, 'user_110', 'Beijing', '100000000000', 'user110@example.com')",
                                customerSchema));
            }

            Row inserted =
                    waitForRowMatching(iter, row -> ((Number) row.getField(0)).intValue() == 110);
            assertThat(((Number) inserted.getField(0)).intValue()).isEqualTo(110);
        } finally {
            cancelJob(tableResult);
        }
    }

    @Test
    public void testReadMultipleTablesWithFilter() throws Exception {
        env.setParallelism(DEFAULT_PARALLELISM);

        String[] tableList =
                new String[] {customerSchema + ".customers", customerSchema + ".orders"};
        DataStream<String> stream =
                env.fromSource(
                        buildGaussDBSource(
                                tableList,
                                slotName,
                                new JsonDebeziumDeserializationSchema(),
                                StartupOptions.initial(),
                                1000),
                        WatermarkStrategy.noWatermarks(),
                        "gaussdb-cdc");

        try (CloseableIterator<String> iter = stream.executeAndCollect()) {
            List<ChangeEvent> snapshot = drainChangeEvents(iter, 7);
            assertThat(snapshot).extracting(e -> e.table).containsOnly("customers", "orders");
            assertThat(snapshot.stream().filter(e -> "customers".equals(e.table)).count())
                    .isEqualTo(4);
            assertThat(snapshot.stream().filter(e -> "orders".equals(e.table)).count())
                    .isEqualTo(3);

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "UPDATE %s.customers SET address = 'Hangzhou' WHERE id = 101",
                                customerSchema));
                statement.execute(
                        format("DELETE FROM %s.orders WHERE order_id = 10002", customerSchema));
            }

            List<ChangeEvent> changes = drainChangeEvents(iter, 2);
            assertThat(changes).extracting(e -> e.op).containsExactlyInAnyOrder("u", "d");
            assertThat(changes).anySatisfy(e -> assertThat(e.table).isEqualTo("customers"));
            assertThat(changes).anySatisfy(e -> assertThat(e.table).isEqualTo("orders"));
        }
    }

    @Test
    public void testSnapshotAndStreamingMode() throws Exception {
        env.setParallelism(1);

        String[] tableList = new String[] {customerSchema + ".customers"};
        DataStream<String> stream =
                env.fromSource(
                        buildGaussDBSource(
                                tableList,
                                slotName,
                                new JsonDebeziumDeserializationSchema(),
                                StartupOptions.initial(),
                                3),
                        WatermarkStrategy.noWatermarks(),
                        "gaussdb-cdc");

        try (CloseableIterator<String> iter = stream.executeAndCollect()) {
            List<ChangeEvent> snapshot = drainChangeEvents(iter, 4);
            assertThat(snapshot).extracting(e -> e.op).containsOnly("r");

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "INSERT INTO %s.customers (id, name, address, phone_number, email) "
                                        + "VALUES (120, 'user_120', 'Shenzhen', '188000000000', 'user120@example.com')",
                                customerSchema));
                statement.execute(
                        format(
                                "UPDATE %s.customers SET phone_number = '199000000000' WHERE id = 103",
                                customerSchema));
                statement.execute(
                        format("DELETE FROM %s.customers WHERE id = 102", customerSchema));
            }

            List<ChangeEvent> changes = drainChangeEvents(iter, 3);
            assertThat(changes).extracting(e -> e.op).contains("c", "u", "d");
        }
    }

    @Test
    public void testCheckpointAndRestore() throws Exception {
        TestValuesTableFactory.clearAllData();
        String sinkName = "sink_" + System.currentTimeMillis();

        StreamExecutionEnvironment env1 = getStreamExecutionEnvironment(null, 1);
        StreamTableEnvironment tEnv1 = StreamTableEnvironment.create(env1);

        tEnv1.executeSql(createCustomersTableDDL("customers_source", "initial", slotName));
        tEnv1.executeSql(createValuesSinkDDL(sinkName));

        TableResult result1 =
                tEnv1.executeSql(
                        format(
                                "INSERT INTO %s SELECT row_kind, id, name FROM customers_source",
                                sinkName));
        JobClient jobClient1 = result1.getJobClient().get();

        waitForSinkSize(sinkName, 4);
        String savepointDir = Files.createTempDirectory("gaussdb-savepoints").toString();
        String savepointPath = triggerSavepointWithRetry(jobClient1, savepointDir);
        jobClient1.cancel().get();

        // Perform DML after savepoint but before restore.
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    format(
                            "UPDATE %s.customers SET address = 'Nanjing' WHERE id = 101",
                            customerSchema));
            statement.execute(
                    format(
                            "INSERT INTO %s.customers (id, name, address, phone_number, email) "
                                    + "VALUES (130, 'user_130', 'Suzhou', '166000000000', 'user130@example.com')",
                            customerSchema));
            statement.execute(format("DELETE FROM %s.customers WHERE id = 109", customerSchema));
        }

        TestValuesTableFactory.clearAllData();

        StreamExecutionEnvironment env2 = getStreamExecutionEnvironment(savepointPath, 1);
        StreamTableEnvironment tEnv2 = StreamTableEnvironment.create(env2);

        tEnv2.executeSql(createCustomersTableDDL("customers_source", "initial", slotName));
        tEnv2.executeSql(createValuesSinkDDL(sinkName));

        TableResult result2 =
                tEnv2.executeSql(
                        format(
                                "INSERT INTO %s SELECT row_kind, id, name FROM customers_source",
                                sinkName));
        JobClient jobClient2 = result2.getJobClient().get();

        // Update -> (-U, +U), Insert -> (+I), Delete -> (-D)
        waitForSinkSize(sinkName, 4);
        List<String> events = TestValuesTableFactory.getRawResultsAsStrings(sinkName);
        assertThat(events).hasSize(4);
        assertThat(events)
                .anySatisfy(e -> assertThat(e).contains("-U").contains("101"))
                .anySatisfy(e -> assertThat(e).contains("+U").contains("101"))
                .anySatisfy(e -> assertThat(e).contains("+I").contains("130"))
                .anySatisfy(e -> assertThat(e).contains("-D").contains("109"));

        jobClient2.cancel().get();
        env1.close();
        env2.close();
    }

    @Test
    public void testBoundedMode() throws Exception {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(1);

        String sinkName = "bounded_sink_" + System.currentTimeMillis();
        tEnv.executeSql(createCustomersTableDDL("customers_bounded", "snapshot", slotName));
        tEnv.executeSql(createValuesSinkDDL(sinkName));

        TableResult result =
                tEnv.executeSql(
                        format(
                                "INSERT INTO %s SELECT row_kind, id, name FROM customers_bounded",
                                sinkName));

        Optional<JobClient> jobClient = result.getJobClient();
        assertThat(jobClient).isPresent();
        waitUntilJobFinished(jobClient.get(), 60_000L);

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings(sinkName);
        assertThat(actual).hasSize(4);
    }

    @Test
    public void testLatestOffsetMode() throws Exception {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(1);

        String sinkName = "latest_sink_" + System.currentTimeMillis();
        tEnv.executeSql(createCustomersTableDDL("customers_latest", "latest-offset", slotName));
        tEnv.executeSql(createValuesSinkDDL(sinkName));

        TableResult result =
                tEnv.executeSql(
                        format(
                                "INSERT INTO %s SELECT row_kind, id, name FROM customers_latest",
                                sinkName));

        waitForReplicationSlotCreatedOrFallback(slotName);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    format(
                            "INSERT INTO %s.customers (id, name, address, phone_number, email) "
                                    + "VALUES (140, 'user_140', 'Xiamen', '155000000000', 'user140@example.com')",
                            customerSchema));
        }

        waitForSinkSize(sinkName, 1);
        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings(sinkName);
        assertThat(actual)
                .singleElement()
                .satisfies(s -> assertThat(s).contains("140").doesNotContain("101"));

        cancelJob(result);
    }

    @Test
    public void testDatatypeConversion() throws Exception {
        env.setParallelism(1);

        tEnv.executeSql(createNumericTypesDDL("numeric_types_source", slotName));
        try (CloseableIterator<Row> iter =
                tEnv.executeSql("SELECT * FROM numeric_types_source").collect()) {
            Row row = iter.next();
            assertThat(((Number) row.getField("id")).intValue()).isEqualTo(1);
            assertThat(row.getField("smallint_col")).isEqualTo((short) 100);
            assertThat(String.valueOf(row.getField("decimal_col"))).isEqualTo("123.45");
            assertThat(String.valueOf(row.getField("numeric_col"))).isEqualTo("12345.67890");
        }

        tEnv.executeSql(createStringTypesDDL("string_types_source", slotName));
        try (CloseableIterator<Row> iter =
                tEnv.executeSql("SELECT * FROM string_types_source").collect()) {
            Row row = iter.next();
            assertThat(((Number) row.getField("id")).intValue()).isEqualTo(1);
            assertThat(String.valueOf(row.getField("char_col"))).contains("test");
            assertThat(row.getField("varchar_col")).isEqualTo("variable length");
            assertThat(String.valueOf(row.getField("text_col"))).contains("long text");
        }

        tEnv.executeSql(createTemporalTypesDDL("temporal_types_source", slotName));
        try (CloseableIterator<Row> iter =
                tEnv.executeSql("SELECT * FROM temporal_types_source").collect()) {
            Row row = iter.next();
            assertThat(((Number) row.getField("id")).intValue()).isEqualTo(1);
            assertThat(String.valueOf(row.getField("date_col"))).isEqualTo("2024-01-15");
            assertThat(String.valueOf(row.getField("time_col"))).contains("14:30");
            assertThat(row.getField("timestamp_col")).isNotNull();
            assertThat(row.getField("timestamptz_col")).isNotNull();
        }

        tEnv.executeSql(createSpecialTypesDDL("special_types_source", slotName));
        try (CloseableIterator<Row> iter =
                tEnv.executeSql("SELECT * FROM special_types_source").collect()) {
            Row row = iter.next();
            assertThat(((Number) row.getField("id")).intValue()).isEqualTo(1);
            assertThat(row.getField("boolean_col")).isEqualTo(true);
            assertThat(String.valueOf(row.getField("uuid_col")))
                    .contains("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            assertThat(String.valueOf(row.getField("json_col"))).contains("\"key\"");
            assertThat(String.valueOf(row.getField("array_col"))).contains("1").contains("5");
        }
    }

    @Test
    public void testChunkSplitting() throws Exception {
        String schema = "chunk_" + System.currentTimeMillis();
        String table = "big_table";
        int total = 12_000;
        createdSchemas.add(schema);
        createLargeTable(schema, table, total);

        String chunkSlot = getSlotName();
        createdSlots.add(chunkSlot);

        StreamExecutionEnvironment localEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        localEnv.setRestartStrategy(RestartStrategies.noRestart());
        localEnv.enableCheckpointing(200L);
        localEnv.setParallelism(DEFAULT_PARALLELISM);

        DataStream<String> stream =
                localEnv.fromSource(
                                buildGaussDBSource(
                                        new String[] {schema + "." + table},
                                        chunkSlot,
                                        new JsonDebeziumDeserializationSchema(),
                                        StartupOptions.snapshot(),
                                        1000),
                                WatermarkStrategy.noWatermarks(),
                                "gaussdb-chunk-snapshot")
                        .map(
                                new org.apache.flink.api.common.functions.RichMapFunction<
                                        String, String>() {
                                    @Override
                                    public String map(String value) {
                                        return getRuntimeContext().getIndexOfThisSubtask()
                                                + "|"
                                                + value;
                                    }
                                });

        Set<Integer> subtasks = new HashSet<>();
        Set<Integer> ids = new HashSet<>();
        int count = 0;
        try (CloseableIterator<String> iter = stream.executeAndCollect()) {
            while (iter.hasNext()) {
                String s = iter.next();
                int sep = s.indexOf('|');
                if (sep > 0) {
                    subtasks.add(Integer.parseInt(s.substring(0, sep)));
                    ChangeEvent evt = parseChangeEvent(s.substring(sep + 1));
                    if (evt != null
                            && "r".equals(evt.op)
                            && table.equals(evt.table)
                            && evt.afterId != null) {
                        count++;
                        ids.add(evt.afterId);
                    }
                }
            }
        }

        assertThat(count).isEqualTo(total);
        assertThat(ids).hasSize(total);
        assertThat(subtasks)
                .withFailMessage("Expected snapshot to be processed by multiple subtasks")
                .hasSizeGreaterThan(1);
    }

    // ----------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------

    private void cancelJob(TableResult tableResult) throws Exception {
        Optional<JobClient> optionalJobClient = tableResult.getJobClient();
        if (optionalJobClient.isPresent()) {
            try {
                optionalJobClient.get().cancel().get();
            } catch (java.util.concurrent.ExecutionException e) {
                // Job may have already failed or completed; log and continue cleanup
                LOG.warn(
                        "Failed to cancel job, it may have already terminated: {}", e.getMessage());
            }
            Thread.sleep(1000L);
        }
    }

    private String createCustomersTableDDL(String tableName, String startupMode, String slotName) {
        return format(
                "CREATE TABLE %s ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " address STRING,"
                        + " phone_number STRING,"
                        + " email STRING,"
                        + " created_at TIMESTAMP(0),"
                        + " updated_at TIMESTAMP(0),"
                        + " row_kind STRING METADATA FROM 'row_kind' VIRTUAL,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'gaussdb-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%d',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.startup.mode' = '%s',"
                        + " 'scan.incremental.snapshot.chunk.size' = '3',"
                        + " 'decoding.plugin.name' = 'mppdb_decoding',"
                        + " 'slot.name' = '%s'"
                        + ")",
                tableName,
                HOSTNAME,
                PORT,
                USERNAME,
                PASSWORD,
                DATABASE_NAME,
                customerSchema,
                "customers",
                startupMode,
                slotName);
    }

    private String createNumericTypesDDL(String tableName, String slotName) {
        return format(
                "CREATE TABLE %s ("
                        + " id INT NOT NULL,"
                        + " smallint_col SMALLINT,"
                        + " integer_col INT,"
                        + " bigint_col BIGINT,"
                        + " decimal_col DECIMAL(10,2),"
                        + " numeric_col DECIMAL(15,5),"
                        + " real_col FLOAT,"
                        + " double_col DOUBLE,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'gaussdb-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%d',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.startup.mode' = 'snapshot',"
                        + " 'decoding.plugin.name' = 'mppdb_decoding',"
                        + " 'slot.name' = '%s'"
                        + ")",
                tableName,
                HOSTNAME,
                PORT,
                USERNAME,
                PASSWORD,
                DATABASE_NAME,
                typesSchema,
                "numeric_types",
                slotName);
    }

    private String createStringTypesDDL(String tableName, String slotName) {
        return format(
                "CREATE TABLE %s ("
                        + " id INT NOT NULL,"
                        + " char_col STRING,"
                        + " varchar_col STRING,"
                        + " text_col STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'gaussdb-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%d',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.startup.mode' = 'snapshot',"
                        + " 'decoding.plugin.name' = 'mppdb_decoding',"
                        + " 'slot.name' = '%s'"
                        + ")",
                tableName,
                HOSTNAME,
                PORT,
                USERNAME,
                PASSWORD,
                DATABASE_NAME,
                typesSchema,
                "string_types",
                slotName);
    }

    private String createTemporalTypesDDL(String tableName, String slotName) {
        return format(
                "CREATE TABLE %s ("
                        + " id INT NOT NULL,"
                        + " date_col DATE,"
                        + " time_col TIME(0),"
                        + " timestamp_col TIMESTAMP(0),"
                        + " timestamptz_col TIMESTAMP_LTZ(3),"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'gaussdb-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%d',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.startup.mode' = 'snapshot',"
                        + " 'decoding.plugin.name' = 'mppdb_decoding',"
                        + " 'slot.name' = '%s'"
                        + ")",
                tableName,
                HOSTNAME,
                PORT,
                USERNAME,
                PASSWORD,
                DATABASE_NAME,
                typesSchema,
                "temporal_types",
                slotName);
    }

    private String createSpecialTypesDDL(String tableName, String slotName) {
        return format(
                "CREATE TABLE %s ("
                        + " id INT NOT NULL,"
                        + " boolean_col BOOLEAN,"
                        + " uuid_col STRING,"
                        + " json_col STRING,"
                        + " array_col ARRAY<INT>,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'gaussdb-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%d',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.startup.mode' = 'snapshot',"
                        + " 'decoding.plugin.name' = 'mppdb_decoding',"
                        + " 'slot.name' = '%s'"
                        + ")",
                tableName,
                HOSTNAME,
                PORT,
                USERNAME,
                PASSWORD,
                DATABASE_NAME,
                typesSchema,
                "special_types",
                slotName);
    }

    private String createValuesSinkDDL(String sinkName) {
        return format(
                "CREATE TABLE %s ("
                        + " row_kind STRING,"
                        + " id INT,"
                        + " name STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")",
                sinkName);
    }

    private void executeIsolatedSqlScript(String resourcePath, String baseSchema, String schemaName)
            throws Exception {
        String original = readResourceAsString(resourcePath);
        String rewritten =
                original.replace(
                                "DROP SCHEMA IF EXISTS " + baseSchema,
                                "DROP SCHEMA IF EXISTS " + schemaName)
                        .replace("CREATE SCHEMA " + baseSchema, "CREATE SCHEMA " + schemaName)
                        .replace(baseSchema + ".", schemaName + ".");
        Path tmp = Files.createTempFile("gaussdb-ddl-", ".sql");
        Files.write(tmp, rewritten.getBytes(StandardCharsets.UTF_8));
        executeSqlScript(tmp.toAbsolutePath().toString());
    }

    private static String readResourceAsString(String resourcePath) throws Exception {
        java.net.URL url = GaussDBSourceITCase.class.getClassLoader().getResource(resourcePath);
        assertThat(url).withFailMessage("Cannot locate resource %s", resourcePath).isNotNull();
        return new String(
                Files.readAllBytes(new File(url.toURI()).toPath()), StandardCharsets.UTF_8);
    }

    private static List<Row> drainRows(CloseableIterator<Row> iter, int expectedCount) {
        return runWithTimeout(
                () -> {
                    List<Row> rows = new ArrayList<>(expectedCount);
                    while (rows.size() < expectedCount) {
                        if (!iter.hasNext()) {
                            break;
                        }
                        rows.add(iter.next());
                    }
                    assertThat(rows)
                            .withFailMessage(
                                    "Expected %s rows but got %s", expectedCount, rows.size())
                            .hasSize(expectedCount);
                    return rows;
                },
                60_000L);
    }

    private static Row waitForRowMatching(CloseableIterator<Row> iter, Predicate<Row> predicate) {
        return runWithTimeout(
                () -> {
                    while (iter.hasNext()) {
                        Row row = iter.next();
                        if (predicate.test(row)) {
                            return row;
                        }
                    }
                    throw new AssertionError("Iterator closed before a matching row is found");
                },
                30_000L);
    }

    private static class ChangeEvent {
        private final String op;
        private final String schema;
        private final String table;
        private final Integer afterId;

        private ChangeEvent(String op, String schema, String table, Integer afterId) {
            this.op = op;
            this.schema = schema;
            this.table = table;
            this.afterId = afterId;
        }
    }

    private static ChangeEvent parseChangeEvent(String json) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(json);
            JsonNode payload = root.get("payload");
            if (payload == null || payload.isNull()) {
                return null;
            }
            JsonNode source = payload.get("source");
            if (source == null || source.isNull()) {
                return null;
            }
            String op = payload.hasNonNull("op") ? payload.get("op").asText() : null;
            String schema = source.hasNonNull("schema") ? source.get("schema").asText() : null;
            String table = source.hasNonNull("table") ? source.get("table").asText() : null;
            Integer afterId = null;
            JsonNode after = payload.get("after");
            if (after != null && after.hasNonNull("id")) {
                afterId = after.get("id").asInt();
            }
            if (op == null || schema == null || table == null) {
                return null;
            }
            return new ChangeEvent(op, schema, table, afterId);
        } catch (Exception e) {
            return null;
        }
    }

    private static List<ChangeEvent> drainChangeEvents(CloseableIterator<String> iter, int expected)
            throws Exception {
        return runWithTimeout(
                () -> {
                    List<ChangeEvent> events = new ArrayList<>(expected);
                    while (events.size() < expected) {
                        if (!iter.hasNext()) {
                            break;
                        }
                        ChangeEvent evt = parseChangeEvent(iter.next());
                        if (evt != null) {
                            events.add(evt);
                        }
                    }
                    assertThat(events)
                            .withFailMessage(
                                    "Expected %s change events but got %s", expected, events.size())
                            .hasSize(expected);
                    return events;
                },
                60_000L);
    }

    private void dropSchemaQuietly(String schemaName) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(format("DROP SCHEMA IF EXISTS %s CASCADE", schemaName));
        } catch (Exception e) {
            LOG.warn("Failed to drop schema {}", schemaName, e);
        }
    }

    private void createLargeTable(String schema, String table, int rows) throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(format("CREATE SCHEMA %s", schema));
            statement.execute(format("DROP TABLE IF EXISTS %s.%s", schema, table));
            statement.execute(
                    format(
                            "CREATE TABLE %s.%s (id INT PRIMARY KEY, name VARCHAR(100))",
                            schema, table));
        }
        try (Connection connection = getJdbcConnection();
                PreparedStatement ps =
                        connection.prepareStatement(
                                format(
                                        "INSERT INTO %s.%s (id, name) VALUES (?, ?)",
                                        schema, table))) {
            for (int i = 1; i <= rows; i++) {
                ps.setInt(1, i);
                ps.setString(2, "user_" + i);
                ps.addBatch();
                if (i % 1000 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
    }

    private void waitForReplicationSlotCreatedOrFallback(String slotName) throws Exception {
        final long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement();
                    ResultSet rs =
                            statement.executeQuery(
                                    format(
                                            "SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s'",
                                            slotName))) {
                if (rs.next()) {
                    return;
                }
            } catch (Exception e) {
                LOG.debug("Failed to check pg_replication_slots, falling back to fixed sleep", e);
                break;
            }
            Thread.sleep(500L);
        }
        Thread.sleep(2000L);
    }

    private static StreamExecutionEnvironment getStreamExecutionEnvironment(
            String finishedSavepointPath, int parallelism) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (finishedSavepointPath != null) {
            java.lang.reflect.Field field =
                    StreamExecutionEnvironment.class.getDeclaredField("configuration");
            field.setAccessible(true);
            org.apache.flink.configuration.Configuration configuration =
                    (org.apache.flink.configuration.Configuration) field.get(env);
            configuration.setString(
                    org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH,
                    finishedSavepointPath);
        }
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }

    private static String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws Exception {
        int retryTimes = 0;
        while (retryTimes < 600) {
            try {
                return jobClient.triggerSavepoint(savepointDirectory).get();
            } catch (Exception e) {
                Thread.sleep(100);
                retryTimes++;
            }
        }
        throw new AssertionError("Failed to trigger savepoint within timeout");
    }

    private static void waitUntilJobFinished(JobClient jobClient, long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            JobStatus status = jobClient.getJobStatus().get();
            if (status == JobStatus.FINISHED) {
                return;
            }
            if (status == JobStatus.FAILED || status == JobStatus.CANCELED) {
                throw new AssertionError("Job ended with status " + status);
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Timed out waiting for job to finish");
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResultsAsStrings(sinkName).size();
            } catch (IllegalArgumentException e) {
                return 0;
            }
        }
    }

    private static void waitForSinkSize(String sinkName, int expectedSize) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            if (sinkSize(sinkName) >= expectedSize) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError(
                format(
                        "Timed out waiting for sink '%s' size %d, current=%d",
                        sinkName, expectedSize, sinkSize(sinkName)));
    }

    private static <T> T runWithTimeout(Callable<T> callable, long timeoutMillis) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> future = executor.submit(callable);
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new AssertionError("Timed out after " + timeoutMillis + " ms", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new AssertionError("Failed", cause);
        } finally {
            executor.shutdownNow();
        }
    }
}
