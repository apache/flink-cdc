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
 * that matches other tables due to LIKE wildcard behavior.
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
     * Test reading from both similar-named tables to verify they can be captured independently
     * without interference.
     */
    @Test
    void testReadBothSimilarNamedTables() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv();

        // Capture events from both tables using regex pattern
        tEnv.executeSql(createSourceDDL("all_tables", "ndi_pg_user.*"));

        TableResult result = tEnv.executeSql("SELECT * FROM all_tables");
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

    /** Executes DML operations on both tables for streaming phase testing. */
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
