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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT tests for FLINK-38965: Fix PostgreSQL CDC connector issue when table names contain underscore
 * or percent characters that match other tables due to LIKE wildcard behavior.
 *
 * <p>PostgreSQL LIKE wildcards:
 *
 * <ul>
 *   <li>'_' (underscore) matches any single character. E.g., 'user_sink' matches 'userbsink'
 *   <li>'%' (percent) matches any sequence of characters. E.g., 'user%data' matches
 *       'user_test_data'
 * </ul>
 *
 * <p>When table names contain these special characters, JDBC metadata queries using LIKE may return
 * columns from unintended tables.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class SimilarTableNamesITCase extends PostgresTestBase {

    private static final int DEFAULT_PARALLELISM = 2;
    private static final String DB_NAME_PREFIX = "postgres";
    private static final String SCHEMA_NAME = "similar_names";

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

    private final UniqueDatabase similarNamesDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    DB_NAME_PREFIX,
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private String slotName;

    @BeforeEach
    public void before() {
        similarNamesDatabase.createAndInitialize();
        this.slotName = getSlotName();
    }

    @AfterEach
    public void after() throws Exception {
        Thread.sleep(1000L);
        similarNamesDatabase.removeSlot(slotName);
    }

    // ==================== Test Cases ====================

    /**
     * Test that when capturing CDC events for table 'ndi_pg_user_sink_1', we don't accidentally
     * capture events from 'ndi_pg_userbsink_1' which would match due to PostgreSQL's LIKE wildcard
     * behavior (underscore '_' matches any single character).
     *
     * <p>Before the fix (FLINK-38965), this test would fail with: "java.lang.IllegalStateException:
     * Duplicate key Optional.empty"
     */
    @Test
    void testReadTableWithSimilarNameUnderscore() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        // Only capture events from 'ndi_pg_user_sink_1' table
        tEnv.executeSql(createSourceDDL("target_table", "ndi_pg_user_sink_1"));

        TableResult result = tEnv.executeSql("SELECT * FROM target_table");
        CloseableIterator<Row> iterator = result.collect();

        try {
            // Verify snapshot data (3 rows from ndi_pg_user_sink_1)
            List<String> expectedSnapshotData =
                    Arrays.asList(
                            "+I[1, user_1, Shanghai]",
                            "+I[2, user_2, Beijing]",
                            "+I[3, user_3, Hangzhou]");
            assertRowsEquals(collectRows(iterator, 3), expectedSnapshotData);

            // Perform DML operations on both tables
            executeDmlOperations();

            // Verify streaming events - should only contain events from ndi_pg_user_sink_1
            List<String> expectedStreamData =
                    Arrays.asList(
                            "+I[4, user_4, Wuhan]",
                            "-U[1, user_1, Shanghai]",
                            "+U[1, user_1, Suzhou]");
            assertRowsEquals(collectRows(iterator, 3), expectedStreamData);
        } finally {
            closeResourcesAndWaitForJobTermination(iterator, result);
        }
    }

    /**
     * Test that when capturing CDC events for table 'user%data' (which contains a literal '%'
     * character), we don't accidentally capture events from 'user_test_data' which would match due
     * to PostgreSQL's LIKE wildcard behavior (percent '%' matches any sequence of characters).
     *
     * <p>When querying for table 'user%data', the LIKE pattern may also match 'user_test_data'
     * because '%' matches the '_test_' sequence.
     */
    @Test
    void testReadTableWithSimilarNamePercent() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        // Only capture events from 'user%data' table (note: % is a literal character in table name)
        tEnv.executeSql(createSourceDDL("target_table", "user%data"));

        TableResult result = tEnv.executeSql("SELECT * FROM target_table");
        CloseableIterator<Row> iterator = result.collect();

        try {
            // Verify snapshot data (3 rows from user%data only)
            List<String> expectedSnapshotData =
                    Arrays.asList(
                            "+I[201, percent_1, Tianjin]",
                            "+I[202, percent_2, Dalian]",
                            "+I[203, percent_3, Qingdao]");
            assertRowsEquals(collectRows(iterator, 3), expectedSnapshotData);

            // Perform DML on user_test_data table - these should NOT be captured
            executeDmlOperationsOnTestDataTable();

            // Perform DML on target table - these SHOULD be captured
            try (Connection conn =
                            getJdbcConnection(
                                    POSTGRES_CONTAINER, similarNamesDatabase.getDatabaseName());
                    Statement stmt = conn.createStatement()) {
                stmt.execute(
                        "INSERT INTO similar_names.\"user%data\" VALUES (204, 'percent_4', 'Xiamen')");
            }

            // Should only see the insert from target table, not user_test_data table
            List<String> expectedStreamData = Arrays.asList("+I[204, percent_4, Xiamen]");
            assertRowsEquals(collectRows(iterator, 1), expectedStreamData);
        } finally {
            closeResourcesAndWaitForJobTermination(iterator, result);
        }
    }

    /**
     * Test reading from all similar-named tables to verify they can be captured independently
     * without interference.
     */
    @Test
    void testReadAllSimilarNamedTables() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        // Capture events from underscore-related tables using regex pattern
        tEnv.executeSql(createSourceDDL("underscore_tables", "ndi_pg_user.*"));

        TableResult result = tEnv.executeSql("SELECT * FROM underscore_tables");
        CloseableIterator<Row> iterator = result.collect();

        try {
            // Verify snapshot data (3 rows from each table = 6 total)
            List<String> expectedSnapshotData =
                    Arrays.asList(
                            // From ndi_pg_user_sink_1
                            "+I[1, user_1, Shanghai]",
                            "+I[2, user_2, Beijing]",
                            "+I[3, user_3, Hangzhou]",
                            // From ndi_pg_userbsink_1
                            "+I[101, userb_1, Guangzhou]",
                            "+I[102, userb_2, Shenzhen]",
                            "+I[103, userb_3, Chengdu]");
            assertRowsEquals(collectRows(iterator, 6), expectedSnapshotData);
        } finally {
            closeResourcesAndWaitForJobTermination(iterator, result);
        }
    }

    // ==================== Helper Methods ====================

    /** Creates and configures the StreamTableEnvironment. */
    private StreamTableEnvironment createTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200L);
        return StreamTableEnvironment.create(env);
    }

    /** Creates the source DDL for the given table name pattern. */
    private String createSourceDDL(String flinkTableName, String pgTablePattern) {
        return format(
                "CREATE TABLE %s ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " address STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'postgres-cdc',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.startup.mode' = 'initial',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'scan.incremental.snapshot.chunk.size' = '100',"
                        + " 'decoding.plugin.name' = 'pgoutput',"
                        + " 'slot.name' = '%s',"
                        + " 'scan.lsn-commit.checkpoints-num-delay' = '1'"
                        + ")",
                flinkTableName,
                similarNamesDatabase.getHost(),
                similarNamesDatabase.getDatabasePort(),
                similarNamesDatabase.getUsername(),
                similarNamesDatabase.getPassword(),
                similarNamesDatabase.getDatabaseName(),
                SCHEMA_NAME,
                pgTablePattern,
                slotName);
    }

    /** Collects specified number of rows from the iterator. */
    private List<String> collectRows(CloseableIterator<Row> iterator, int expectedCount) {
        List<String> rows = new ArrayList<>();
        int count = 0;
        while (count < expectedCount && iterator.hasNext()) {
            rows.add(iterator.next().toString());
            count++;
        }
        return rows;
    }

    /** Asserts that actual rows match expected rows (order-insensitive). */
    private void assertRowsEquals(List<String> actual, List<String> expected) {
        assertThat(actual.stream().sorted().collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(
                        expected.stream().sorted().collect(Collectors.toList()));
    }

    /** Executes DML operations on target and similar-named tables for streaming phase testing. */
    private void executeDmlOperations() throws Exception {
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, similarNamesDatabase.getDatabaseName());
                Statement stmt = conn.createStatement()) {
            // Insert into target table
            stmt.execute(
                    "INSERT INTO similar_names.ndi_pg_user_sink_1 VALUES (4, 'user_4', 'Wuhan')");
            // Insert into similar-named table (should NOT be captured when only listening to
            // target)
            stmt.execute(
                    "INSERT INTO similar_names.ndi_pg_userbsink_1 VALUES (104, 'userb_4', 'Nanjing')");
            // Update target table
            stmt.execute(
                    "UPDATE similar_names.ndi_pg_user_sink_1 SET address = 'Suzhou' WHERE id = 1");
        }
    }

    /** Executes DML operations on user_test_data table for '%' wildcard testing. */
    private void executeDmlOperationsOnTestDataTable() throws Exception {
        try (Connection conn =
                        getJdbcConnection(
                                POSTGRES_CONTAINER, similarNamesDatabase.getDatabaseName());
                Statement stmt = conn.createStatement()) {
            // Insert into user_test_data table (should NOT be captured when only listening to
            // user%data)
            stmt.execute(
                    "INSERT INTO similar_names.user_test_data VALUES (304, 'test_4', 'Harbin')");
            // Update user_test_data table
            stmt.execute(
                    "UPDATE similar_names.user_test_data SET address = 'Changchun' WHERE id = 301");
        }
    }

    /** Properly closes resources and waits for job termination. */
    private void closeResourcesAndWaitForJobTermination(
            CloseableIterator<Row> iterator, TableResult result) throws Exception {
        iterator.close();
        result.getJobClient()
                .ifPresent(
                        client -> {
                            client.cancel();
                            try {
                                client.getJobExecutionResult().get(30, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                // Job cancelled, expected
                            }
                        });
    }
}
