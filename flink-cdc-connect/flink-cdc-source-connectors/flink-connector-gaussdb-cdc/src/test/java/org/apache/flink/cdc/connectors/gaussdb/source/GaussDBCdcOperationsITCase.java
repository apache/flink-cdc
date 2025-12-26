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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.gaussdb.GaussDBTestBase;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for GaussDB CDC operations (INSERT, UPDATE, DELETE) using DataStream API. This
 * test class focuses on validating CDC capabilities directly without Table API overhead.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS)
class GaussDBCdcOperationsITCase extends GaussDBTestBase {

    private static final String BASE_CUSTOMER_SCHEMA = "cdc_ops_test";
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

    private String slotName;
    private String customerSchema;
    private StreamExecutionEnvironment env;

    @BeforeEach
    public void before() throws Exception {
        this.slotName = getSlotName();
        String suffix = Long.toString(System.nanoTime());
        this.customerSchema = BASE_CUSTOMER_SCHEMA + "_" + suffix;

        // Create schema and test table
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(format("DROP SCHEMA IF EXISTS %s CASCADE", customerSchema));
            statement.execute(format("CREATE SCHEMA %s", customerSchema));
            statement.execute(
                    format(
                            "CREATE TABLE %s.customers ("
                                    + "id INT PRIMARY KEY, "
                                    + "name VARCHAR(100), "
                                    + "address VARCHAR(200), "
                                    + "phone_number VARCHAR(20), "
                                    + "email VARCHAR(100)"
                                    + ")",
                            customerSchema));

            // Insert initial data
            statement.execute(
                    format(
                            "INSERT INTO %s.customers VALUES "
                                    + "(101, 'user_1', 'Shanghai', '123567891234', 'user1@example.com'), "
                                    + "(102, 'user_2', 'Beijing', '123567891235', 'user2@example.com'), "
                                    + "(103, 'user_3', 'Hangzhou', '123567891236', 'user3@example.com'), "
                                    + "(104, 'user_4', 'Shenzhen', '123567891237', 'user4@example.com')",
                            customerSchema));
        }

        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setRestartStrategy(RestartStrategies.noRestart());
        this.env.enableCheckpointing(200L);
        this.env.setParallelism(1); // Use single parallelism for predictable event ordering
    }

    @AfterEach
    public void after() throws Exception {
        Thread.sleep(1000L);

        try {
            dropReplicationSlot(slotName);
        } catch (Exception e) {
            LOG.warn("Failed to drop replication slot '{}'", slotName, e);
        }

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(format("DROP SCHEMA IF EXISTS %s CASCADE", customerSchema));
        } catch (Exception e) {
            LOG.warn("Failed to drop schema ''", customerSchema, e);
        }

        if (env != null) {
            env.close();
        }
    }

    /**
     * Test comprehensive CDC operations: snapshot + INSERT + UPDATE + DELETE. This test validates
     * that all CDC operations are correctly captured and processed.
     */
    @Test
    public void testComprehensiveCdcOperations() throws Exception {
        String[] tableList = new String[] {customerSchema + ".customers"};
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
            // Step 1: Validate snapshot data (4 initial records)
            List<ChangeEvent> snapshotEvents = collectChangeEvents(iter, 4, 60000L);
            assertThat(snapshotEvents).hasSize(4);
            assertThat(snapshotEvents)
                    .allMatch(e -> "r".equals(e.op), "All snapshot events should have op='r'");
            assertThat(snapshotEvents)
                    .extracting(e -> e.afterId)
                    .containsExactlyInAnyOrder(101, 102, 103, 104);

            LOG.info("✓ Snapshot phase completed: {} records", snapshotEvents.size());

            // Step 2: Perform INSERT operation
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "INSERT INTO %s.customers VALUES "
                                        + "(105, 'user_5', 'Guangzhou', '123567891238', 'user5@example.com')",
                                customerSchema));
            }

            List<ChangeEvent> insertEvents = collectChangeEvents(iter, 1, 30000L);
            assertThat(insertEvents).hasSize(1);
            assertThat(insertEvents.get(0).op).isEqualTo("c");
            assertThat(insertEvents.get(0).afterId).isEqualTo(105);
            assertThat(insertEvents.get(0).afterName).isEqualTo("user_5");

            LOG.info("✓ INSERT operation captured: id={}", insertEvents.get(0).afterId);

            // Step 3: Perform UPDATE operation
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "UPDATE %s.customers SET address = 'Nanjing', name = 'user_1_updated' WHERE id = 101",
                                customerSchema));
            }

            List<ChangeEvent> updateEvents = collectChangeEvents(iter, 1, 30000L);
            assertThat(updateEvents).hasSize(1);
            assertThat(updateEvents.get(0).op).isEqualTo("u");
            assertThat(updateEvents.get(0).afterId).isEqualTo(101);
            assertThat(updateEvents.get(0).afterName).isEqualTo("user_1_updated");
            assertThat(updateEvents.get(0).afterAddress).isEqualTo("Nanjing");

            LOG.info(
                    "✓ UPDATE operation captured: id={}, new_name={}",
                    updateEvents.get(0).afterId,
                    updateEvents.get(0).afterName);

            // Step 4: Perform DELETE operation
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format("DELETE FROM %s.customers WHERE id = 102", customerSchema));
            }

            List<ChangeEvent> deleteEvents = collectChangeEvents(iter, 1, 30000L);
            assertThat(deleteEvents).hasSize(1);
            assertThat(deleteEvents.get(0).op).isEqualTo("d");
            assertThat(deleteEvents.get(0).beforeId).isEqualTo(102);

            LOG.info("✓ DELETE operation captured: id={}", deleteEvents.get(0).beforeId);

            // Step 5: Perform multiple operations in sequence
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "INSERT INTO %s.customers VALUES "
                                        + "(106, 'user_6', 'Chengdu', '123567891239', 'user6@example.com')",
                                customerSchema));
                statement.execute(
                        format(
                                "UPDATE %s.customers SET phone_number = '999999999999' WHERE id = 103",
                                customerSchema));
                statement.execute(
                        format("DELETE FROM %s.customers WHERE id = 104", customerSchema));
            }

            List<ChangeEvent> batchEvents = collectChangeEvents(iter, 3, 30000L);
            assertThat(batchEvents).hasSize(3);
            assertThat(batchEvents).extracting(e -> e.op).containsExactlyInAnyOrder("c", "u", "d");

            LOG.info("✓ Batch operations captured: {} events", batchEvents.size());

            LOG.info("========================================");
            LOG.info("✓ ALL CDC OPERATIONS TEST PASSED");
            LOG.info("  - Snapshot: 4 records");
            LOG.info("  - INSERT: 2 operations");
            LOG.info("  - UPDATE: 2 operations");
            LOG.info("  - DELETE: 2 operations");
            LOG.info("========================================");
        }
    }

    /** Test INSERT operations with various data patterns. */
    @Test
    public void testInsertOperations() throws Exception {
        String[] tableList = new String[] {customerSchema + ".customers"};
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
            // Consume snapshot data
            collectChangeEvents(iter, 4, 60000L);

            // Test single INSERT
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "INSERT INTO %s.customers VALUES "
                                        + "(201, 'insert_test_1', 'City1', '111111111111', 'test1@example.com')",
                                customerSchema));
            }

            List<ChangeEvent> events1 = collectChangeEvents(iter, 1, 30000L);
            assertThat(events1).hasSize(1);
            assertThat(events1.get(0).op).isEqualTo("c");
            assertThat(events1.get(0).afterId).isEqualTo(201);

            // Test batch INSERT
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "INSERT INTO %s.customers VALUES "
                                        + "(202, 'insert_test_2', 'City2', '222222222222', 'test2@example.com'), "
                                        + "(203, 'insert_test_3', 'City3', '333333333333', 'test3@example.com'), "
                                        + "(204, 'insert_test_4', 'City4', '444444444444', 'test4@example.com')",
                                customerSchema));
            }

            List<ChangeEvent> events2 = collectChangeEvents(iter, 3, 30000L);
            assertThat(events2).hasSize(3);
            assertThat(events2).allMatch(e -> "c".equals(e.op));
            assertThat(events2).extracting(e -> e.afterId).containsExactlyInAnyOrder(202, 203, 204);

            LOG.info("✓ INSERT operations test passed");
        }
    }

    /** Test UPDATE operations with various update patterns. */
    @Test
    public void testUpdateOperations() throws Exception {
        String[] tableList = new String[] {customerSchema + ".customers"};
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
            // Consume snapshot data
            collectChangeEvents(iter, 4, 60000L);

            // Test single column UPDATE
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "UPDATE %s.customers SET name = 'updated_name' WHERE id = 101",
                                customerSchema));
            }

            List<ChangeEvent> events1 = collectChangeEvents(iter, 1, 30000L);
            assertThat(events1).hasSize(1);
            assertThat(events1.get(0).op).isEqualTo("u");
            assertThat(events1.get(0).afterId).isEqualTo(101);
            assertThat(events1.get(0).afterName).isEqualTo("updated_name");

            // Test multiple columns UPDATE
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "UPDATE %s.customers SET name = 'multi_update', address = 'NewCity', phone_number = '000000000000' WHERE id = 102",
                                customerSchema));
            }

            List<ChangeEvent> events2 = collectChangeEvents(iter, 1, 30000L);
            assertThat(events2).hasSize(1);
            assertThat(events2.get(0).op).isEqualTo("u");
            assertThat(events2.get(0).afterId).isEqualTo(102);
            assertThat(events2.get(0).afterName).isEqualTo("multi_update");
            assertThat(events2.get(0).afterAddress).isEqualTo("NewCity");

            // Test batch UPDATE
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "UPDATE %s.customers SET address = 'BatchCity' WHERE id IN (103, 104)",
                                customerSchema));
            }

            List<ChangeEvent> events3 = collectChangeEvents(iter, 2, 30000L);
            assertThat(events3).hasSize(2);
            assertThat(events3).allMatch(e -> "u".equals(e.op));
            assertThat(events3).allMatch(e -> "BatchCity".equals(e.afterAddress));

            LOG.info("✓ UPDATE operations test passed");
        }
    }

    /** Test DELETE operations with various patterns. */
    @Test
    public void testDeleteOperations() throws Exception {
        String[] tableList = new String[] {customerSchema + ".customers"};
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
            // Consume snapshot data
            collectChangeEvents(iter, 4, 60000L);

            // Test single DELETE
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format("DELETE FROM %s.customers WHERE id = 101", customerSchema));
            }

            List<ChangeEvent> events1 = collectChangeEvents(iter, 1, 30000L);
            assertThat(events1).hasSize(1);
            assertThat(events1.get(0).op).isEqualTo("d");
            assertThat(events1.get(0).beforeId).isEqualTo(101);

            // Test batch DELETE
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format("DELETE FROM %s.customers WHERE id IN (102, 103)", customerSchema));
            }

            List<ChangeEvent> events2 = collectChangeEvents(iter, 2, 30000L);
            assertThat(events2).hasSize(2);
            assertThat(events2).allMatch(e -> "d".equals(e.op));
            assertThat(events2).extracting(e -> e.beforeId).containsExactlyInAnyOrder(102, 103);

            LOG.info("✓ DELETE operations test passed");
        }
    }

    /** Test mixed CDC operations in a realistic scenario. */
    @Test
    public void testMixedCdcOperations() throws Exception {
        String[] tableList = new String[] {customerSchema + ".customers"};
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
            // Consume snapshot data
            List<ChangeEvent> snapshot = collectChangeEvents(iter, 4, 60000L);
            assertThat(snapshot).hasSize(4);

            // Simulate realistic business operations
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                // New customer registration
                statement.execute(
                        format(
                                "INSERT INTO %s.customers VALUES "
                                        + "(301, 'new_customer', 'RegisterCity', '555555555555', 'new@example.com')",
                                customerSchema));

                // Customer updates profile
                statement.execute(
                        format(
                                "UPDATE %s.customers SET email = 'updated@example.com', phone_number = '666666666666' WHERE id = 301",
                                customerSchema));

                // Another customer updates address
                statement.execute(
                        format(
                                "UPDATE %s.customers SET address = 'MovedCity' WHERE id = 101",
                                customerSchema));

                // Customer account deletion
                statement.execute(
                        format("DELETE FROM %s.customers WHERE id = 102", customerSchema));

                // New customer registration
                statement.execute(
                        format(
                                "INSERT INTO %s.customers VALUES "
                                        + "(302, 'another_customer', 'AnotherCity', '777777777777', 'another@example.com')",
                                customerSchema));
            }

            // Collect all events
            List<ChangeEvent> events = collectChangeEvents(iter, 5, 30000L);
            assertThat(events).hasSize(5);

            // Verify operation types
            long insertCount = events.stream().filter(e -> "c".equals(e.op)).count();
            long updateCount = events.stream().filter(e -> "u".equals(e.op)).count();
            long deleteCount = events.stream().filter(e -> "d".equals(e.op)).count();

            assertThat(insertCount).isEqualTo(2);
            assertThat(updateCount).isEqualTo(2);
            assertThat(deleteCount).isEqualTo(1);

            LOG.info("✓ Mixed CDC operations test passed");
            LOG.info(
                    "  - INSERT: {}, UPDATE: {}, DELETE: {}",
                    insertCount,
                    updateCount,
                    deleteCount);
        }
    }

    /**
     * Minimal snapshot test - single table, single row, snapshot only. This test is designed to
     * isolate the root cause of snapshot reading issues.
     */
    @Test
    public void testMinimalSnapshot() throws Exception {
        LOG.info("========================================");
        LOG.info("=== MINIMAL SNAPSHOT TEST STARTED ===");
        LOG.info("========================================");

        String minimalSchema = customerSchema + "_minimal";

        // Create the simplest possible table
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            LOG.info("Creating minimal test schema and table...");
            statement.execute(format("DROP SCHEMA IF EXISTS %s CASCADE", minimalSchema));
            statement.execute(format("CREATE SCHEMA %s", minimalSchema));
            statement.execute(
                    format(
                            "CREATE TABLE %s.test_minimal ("
                                    + "id INT PRIMARY KEY, "
                                    + "name VARCHAR(50)"
                                    + ")",
                            minimalSchema));
            LOG.info("✓ Table created: {}.test_minimal", minimalSchema);

            // Insert just 1 row
            statement.execute(
                    format("INSERT INTO %s.test_minimal VALUES (1, 'test_row')", minimalSchema));
            LOG.info("✓ Inserted 1 test row");

            // Verify the row exists
            var rs = statement.executeQuery(format("SELECT * FROM %s.test_minimal", minimalSchema));
            if (rs.next()) {
                LOG.info(
                        "✓ Verified row exists: id={}, name={}",
                        rs.getInt("id"),
                        rs.getString("name"));
            } else {
                LOG.error("✗ Row verification failed - no data found!");
            }
        }

        // Build CDC source with snapshot-only mode
        String[] tableList = new String[] {minimalSchema + ".test_minimal"};
        LOG.info("Building GaussDB source for table: {}", tableList[0]);

        DataStream<String> stream =
                env.fromSource(
                        buildGaussDBSource(
                                tableList,
                                slotName + "_minimal",
                                new JsonDebeziumDeserializationSchema(),
                                StartupOptions.snapshot(), // Snapshot only, no streaming
                                1000),
                        WatermarkStrategy.noWatermarks(),
                        "gaussdb-minimal-test");

        LOG.info("Starting Flink job to collect snapshot data...");

        try (CloseableIterator<String> iter = stream.executeAndCollect()) {
            LOG.info("Iterator created, waiting for data...");

            // Try to collect just 1 event with detailed logging
            List<ChangeEvent> events = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            int iterations = 0;
            boolean hasData = false;

            while (events.size() < 1 && iterations < 100) {
                iterations++;
                LOG.info("Iteration {}: Checking if iterator has next...", iterations);

                if (iter.hasNext()) {
                    LOG.info("✓ Iterator has data!");
                    hasData = true;
                    String json = iter.next();
                    LOG.info(
                            "Received JSON (length={}): {}",
                            json.length(),
                            json.length() > 500 ? json.substring(0, 500) + "..." : json);

                    ChangeEvent event = parseChangeEvent(json);
                    if (event != null) {
                        events.add(event);
                        LOG.info(
                                "✓ Successfully parsed event: op={}, id={}, name={}",
                                event.op,
                                event.afterId,
                                event.afterName);
                        break;
                    } else {
                        LOG.warn("✗ Failed to parse event from JSON");
                    }
                } else {
                    LOG.warn(
                            "Iteration {}: Iterator has no data yet, waiting 1 second...",
                            iterations);
                    Thread.sleep(1000);
                }

                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed > 60000) {
                    LOG.error("✗ Timeout after {} ms, {} iterations", elapsed, iterations);
                    break;
                }
            }

            long totalTime = System.currentTimeMillis() - startTime;

            LOG.info("========================================");
            LOG.info("=== MINIMAL SNAPSHOT TEST RESULTS ===");
            LOG.info("Total time: {} ms", totalTime);
            LOG.info("Total iterations: {}", iterations);
            LOG.info("Iterator had data: {}", hasData);
            LOG.info("Events collected: {}", events.size());

            if (events.size() > 0) {
                LOG.info("✓✓✓ SUCCESS! Snapshot data was received!");
                LOG.info(
                        "Event details: op={}, id={}, name={}",
                        events.get(0).op,
                        events.get(0).afterId,
                        events.get(0).afterName);
                assertThat(events).hasSize(1);
                assertThat(events.get(0).afterId).isEqualTo(1);
                assertThat(events.get(0).afterName).isEqualTo("test_row");
            } else {
                LOG.error("✗✗✗ FAILURE! No snapshot data received!");
                LOG.error("This indicates the snapshot reading mechanism is not working.");
                LOG.error("Check the logs above for errors in:");
                LOG.error("  - GaussDBDialect.discoverDataCollections");
                LOG.error("  - GaussDBDialect.createFetchTask");
                LOG.error("  - GaussDBScanFetchTask.executeDataSnapshot");
                throw new AssertionError("No snapshot data received after " + totalTime + " ms");
            }
            LOG.info("========================================");
        } finally {
            // Cleanup
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(format("DROP SCHEMA IF EXISTS %s CASCADE", minimalSchema));
                LOG.info("✓ Cleaned up minimal test schema");
            } catch (Exception e) {
                LOG.warn("Failed to cleanup minimal schema", e);
            }
        }
    }

    /** Test latest-offset startup mode (skip snapshot, only capture streaming changes). */
    @Test
    public void testLatestOffsetMode() throws Exception {
        String[] tableList = new String[] {customerSchema + ".customers"};
        DataStream<String> stream =
                env.fromSource(
                        buildGaussDBSource(
                                tableList,
                                slotName,
                                new JsonDebeziumDeserializationSchema(),
                                StartupOptions.latest(),
                                1000),
                        WatermarkStrategy.noWatermarks(),
                        "gaussdb-cdc");

        try (CloseableIterator<String> iter = stream.executeAndCollect()) {
            // Wait for replication slot to be created
            Thread.sleep(3000L);

            // Perform operations after source starts
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        format(
                                "INSERT INTO %s.customers VALUES "
                                        + "(401, 'latest_mode_test', 'TestCity', '888888888888', 'latest@example.com')",
                                customerSchema));
            }

            // Should only capture the INSERT, not the initial 4 snapshot records
            List<ChangeEvent> events = collectChangeEvents(iter, 1, 30000L);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).op).isEqualTo("c");
            assertThat(events.get(0).afterId).isEqualTo(401);

            LOG.info("✓ Latest-offset mode test passed (no snapshot, only streaming)");
        }
    }

    // ----------------------------------------------------------------------------------------
    // Helper Methods
    // ----------------------------------------------------------------------------------------

    private static class ChangeEvent {
        String op;
        String schema;
        String table;
        Integer beforeId;
        String beforeName;
        String beforeAddress;
        Integer afterId;
        String afterName;
        String afterAddress;

        ChangeEvent(
                String op,
                String schema,
                String table,
                Integer beforeId,
                String beforeName,
                String beforeAddress,
                Integer afterId,
                String afterName,
                String afterAddress) {
            this.op = op;
            this.schema = schema;
            this.table = table;
            this.beforeId = beforeId;
            this.beforeName = beforeName;
            this.beforeAddress = beforeAddress;
            this.afterId = afterId;
            this.afterName = afterName;
            this.afterAddress = afterAddress;
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

            Integer beforeId = null;
            String beforeName = null;
            String beforeAddress = null;
            JsonNode before = payload.get("before");
            if (before != null && !before.isNull()) {
                if (before.hasNonNull("id")) {
                    beforeId = before.get("id").asInt();
                }
                if (before.hasNonNull("name")) {
                    beforeName = before.get("name").asText();
                }
                if (before.hasNonNull("address")) {
                    beforeAddress = before.get("address").asText();
                }
            }

            Integer afterId = null;
            String afterName = null;
            String afterAddress = null;
            JsonNode after = payload.get("after");
            if (after != null && !after.isNull()) {
                if (after.hasNonNull("id")) {
                    afterId = after.get("id").asInt();
                }
                if (after.hasNonNull("name")) {
                    afterName = after.get("name").asText();
                }
                if (after.hasNonNull("address")) {
                    afterAddress = after.get("address").asText();
                }
            }

            if (op == null || schema == null || table == null) {
                return null;
            }

            return new ChangeEvent(
                    op,
                    schema,
                    table,
                    beforeId,
                    beforeName,
                    beforeAddress,
                    afterId,
                    afterName,
                    afterAddress);
        } catch (Exception e) {
            LOG.warn("Failed to parse change event: {}", json, e);
            return null;
        }
    }

    private List<ChangeEvent> collectChangeEvents(
            CloseableIterator<String> iter, int expectedCount, long timeoutMillis)
            throws Exception {
        return runWithTimeout(
                () -> {
                    List<ChangeEvent> events = new ArrayList<>(expectedCount);
                    LOG.info(
                            "Starting to collect {} events with timeout {}ms",
                            expectedCount,
                            timeoutMillis);
                    int nullEventCount = 0;
                    int totalIterations = 0;
                    while (events.size() < expectedCount) {
                        totalIterations++;
                        if (totalIterations % 100 == 0) {
                            LOG.info("Iteration {}: hasNext check...", totalIterations);
                        }
                        if (!iter.hasNext()) {
                            LOG.warn(
                                    "Iterator has no more elements after {} iterations. Collected {} events so far.",
                                    totalIterations,
                                    events.size());
                            break;
                        }
                        String json = iter.next();
                        if (totalIterations <= 10 || totalIterations % 100 == 0) {
                            LOG.info("Iteration {}: Received JSON event", totalIterations);
                        }
                        ChangeEvent event = parseChangeEvent(json);
                        if (event != null) {
                            events.add(event);
                            LOG.info(
                                    "Collected event {}/{}: op={}, id={}, schema={}, table={}",
                                    events.size(),
                                    expectedCount,
                                    event.op,
                                    event.afterId != null ? event.afterId : event.beforeId,
                                    event.schema,
                                    event.table);
                        } else {
                            nullEventCount++;
                            if (nullEventCount <= 5) {
                                LOG.warn(
                                        "Received null event after parsing (count: {}). JSON: {}",
                                        nullEventCount,
                                        json != null && json.length() > 200
                                                ? json.substring(0, 200) + "..."
                                                : json);
                            }
                        }
                    }
                    LOG.info(
                            "Finished collecting. Total iterations: {}, Collected events: {}, Null events: {}",
                            totalIterations,
                            events.size(),
                            nullEventCount);
                    if (events.size() < expectedCount) {
                        throw new AssertionError(
                                format(
                                        "Expected %d events but only collected %d after %d iterations",
                                        expectedCount, events.size(), totalIterations));
                    }
                    return events;
                },
                timeoutMillis);
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
