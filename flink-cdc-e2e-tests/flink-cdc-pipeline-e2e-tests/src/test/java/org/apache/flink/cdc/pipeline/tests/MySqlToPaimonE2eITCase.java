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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** End-to-end tests for mysql cdc to Paimon pipeline job. */
public class MySqlToPaimonE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlToPaimonE2eITCase.class);

    public static final Duration TESTCASE_TIMEOUT = Duration.ofMinutes(3);

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";

    @ClassRule
    public static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer(
                                    MySqlVersion.V8_0) // v8 support both ARM and AMD architectures
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases("mysql")
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL, "paimon_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @BeforeClass
    public static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        LOG.info("Containers are started.");
    }

    @Before
    public void before() throws Exception {
        super.before();
        inventoryDatabase.createAndInitialize();
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("paimon-sql-connector.jar")),
                sharedVolume.toString() + "/paimon-sql-connector.jar");
    }

    @After
    public void after() {
        super.after();
        inventoryDatabase.dropDatabase();
    }

    @Test
    public void testSyncWholeDatabase() throws Exception {
        String warehouse = sharedVolume.toString() + "/" + "paimon_" + UUID.randomUUID();
        String database = inventoryDatabase.getDatabaseName();
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: mysql\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: paimon\n"
                                + "  catalog.properties.warehouse: %s\n"
                                + "  catalog.properties.metastore: filesystem\n"
                                + "  catalog.properties.cache-enabled: false\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: 4",
                        MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, database, warehouse);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, paimonCdcConnector, mysqlDriverJar, hadoopJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        validateSinkResult(
                warehouse,
                database,
                "products",
                Arrays.asList(
                        "101, One, Alice, 3.202, red, {\"key1\":\"value1\"}, null",
                        "102, Two, Bob, 1.703, white, {\"key2\":\"value2\"}, null",
                        "103, Three, Cecily, 4.105, red, {\"key3\":\"value3\"}, null",
                        "104, Four, Derrida, 1.857, white, {\"key4\":\"value4\"}, null",
                        "105, Five, Evelyn, 5.211, red, {\"K\":\"V\",\"k\":\"v\"}, null",
                        "106, Six, Ferris, 9.813, null, null, null",
                        "107, Seven, Grace, 2.117, null, null, null",
                        "108, Eight, Hesse, 6.819, null, null, null",
                        "109, Nine, IINA, 5.223, null, null, null"));

        validateSinkResult(
                warehouse,
                database,
                "customers",
                Arrays.asList(
                        "101, user_1, Shanghai, 123567891234",
                        "102, user_2, Shanghai, 123567891234",
                        "103, user_3, Shanghai, 123567891234",
                        "104, user_4, Shanghai, 123567891234"));

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), database);
        List<String> recordsInIncrementalPhase;
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            stat.execute(
                    "INSERT INTO products VALUES (default,'Ten','Jukebox',0.2, null, null, null);"); // 110
            stat.execute("UPDATE products SET description='Fay' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.125' WHERE id=107;");

            // modify table schema
            stat.execute("ALTER TABLE products DROP COLUMN point_c;");
            stat.execute("DELETE FROM products WHERE id=101;");

            stat.execute(
                    "INSERT INTO products VALUES (default,'Eleven','Kryo',5.18, null, null);"); // 111
            stat.execute(
                    "INSERT INTO products VALUES (default,'Twelve', 'Lily', 2.14, null, null);"); // 112
            recordsInIncrementalPhase = createChangesAndValidate(stat);
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
        List<String> recordsInSnapshotPhase =
                new ArrayList<>(
                        Arrays.asList(
                                "102, Two, Bob, 1.703, white, {\"key2\":\"value2\"}, null, null, null, null, null, null, null, null, null, null",
                                "103, Three, Cecily, 4.105, red, {\"key3\":\"value3\"}, null, null, null, null, null, null, null, null, null, null",
                                "104, Four, Derrida, 1.857, white, {\"key4\":\"value4\"}, null, null, null, null, null, null, null, null, null, null",
                                "105, Five, Evelyn, 5.211, red, {\"K\":\"V\",\"k\":\"v\"}, null, null, null, null, null, null, null, null, null, null",
                                "106, Six, Fay, 9.813, null, null, null, null, null, null, null, null, null, null, null, null",
                                "107, Seven, Grace, 5.125, null, null, null, null, null, null, null, null, null, null, null, null",
                                "108, Eight, Hesse, 6.819, null, null, null, null, null, null, null, null, null, null, null, null",
                                "109, Nine, IINA, 5.223, null, null, null, null, null, null, null, null, null, null, null, null",
                                "110, Ten, Jukebox, 0.2, null, null, null, null, null, null, null, null, null, null, null, null",
                                "111, Eleven, Kryo, 5.18, null, null, null, null, null, null, null, null, null, null, null, null",
                                "112, Twelve, Lily, 2.14, null, null, null, null, null, null, null, null, null, null, null, null"));
        recordsInSnapshotPhase.addAll(recordsInIncrementalPhase);
        validateSinkResult(warehouse, database, "products", recordsInSnapshotPhase);
    }

    /**
     * Basic Schema: id INTEGER NOT NULL, name VARCHAR(255) NOT NULL, description VARCHAR(512),
     * weight FLOAT, enum_c enum('red', 'white'), json_c JSON.
     */
    private List<String> createChangesAndValidate(Statement stat) throws SQLException {
        List<String> result = new ArrayList<>();
        StringBuilder sqlFields = new StringBuilder();

        // Add Column.
        for (int addColumnRepeat = 0; addColumnRepeat < 10; addColumnRepeat++) {
            stat.execute(
                    String.format(
                            "ALTER TABLE products ADD COLUMN point_c_%s VARCHAR(10);",
                            addColumnRepeat));
            sqlFields.append(", '1'");
            StringBuilder resultFields = new StringBuilder();
            for (int j = 0; j < 10; j++) {
                if (j <= addColumnRepeat) {
                    resultFields.append(", 1");
                } else {
                    resultFields.append(", null");
                }
            }
            for (int j = 0; j < 1000; j++) {
                stat.addBatch(
                        String.format(
                                "INSERT INTO products VALUES (default,'finally', null, 2.14, null, null %s);",
                                sqlFields));
                int id = addColumnRepeat * 1000 + j + 113;
                result.add(
                        String.format("%s, finally, null, 2.14, null, null%s", id, resultFields));
            }
            stat.executeBatch();
        }

        // Modify Column type.
        for (int modifyColumnRepeat = 0; modifyColumnRepeat < 10; modifyColumnRepeat++) {
            for (int j = 0; j < 1000; j++) {
                stat.addBatch(
                        String.format(
                                "INSERT INTO products VALUES (default,'finally', null, 2.14, null, null %s);",
                                sqlFields));
                int id = modifyColumnRepeat * 1000 + j + 10113;
                result.add(
                        String.format(
                                "%s, finally, null, 2.14, null, null%s",
                                id, ", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1"));
            }
            stat.executeBatch();
            stat.execute(
                    String.format(
                            "ALTER TABLE products MODIFY point_c_0 VARCHAR(%s);",
                            10 + modifyColumnRepeat));
        }

        return result;
    }

    private List<String> fetchPaimonTableRows(String warehouse, String database, String table)
            throws Exception {
        String template =
                readLines("docker/peek-paimon.sql").stream()
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));
        String sql = String.format(template, warehouse, database, table);
        String containerSqlPath = sharedVolume.toString() + "/peek.sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);

        Container.ExecResult result =
                jobManager.execInContainer(
                        "/flink/bin/sql-client.sh",
                        "--jar",
                        sharedVolume.toString() + "/paimon-sql-connector.jar",
                        "-f",
                        containerSqlPath);
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    "Failed to execute peek script. Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }

        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(MySqlToPaimonE2eITCase::extractRow)
                .map(row -> String.format("%s", String.join(", ", row)))
                .collect(Collectors.toList());
    }

    private static String[] extractRow(String row) {
        return Arrays.stream(row.split("\\|"))
                .map(String::trim)
                .filter(col -> !col.isEmpty())
                .map(col -> col.equals("<NULL>") ? "null" : col)
                .toArray(String[]::new);
    }

    private void validateSinkResult(
            String warehouse, String database, String table, List<String> expected)
            throws InterruptedException {
        LOG.info("Verifying Paimon {}::{}::{} results...", warehouse, database, table);
        long deadline = System.currentTimeMillis() + TESTCASE_TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchPaimonTableRows(warehouse, database, table);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info(
                        "Successfully verified {} records in {} seconds.",
                        expected.size(),
                        (System.currentTimeMillis() - deadline + TESTCASE_TIMEOUT.toMillis())
                                / 1000);
                return;
            } catch (Exception e) {
                LOG.warn("Validate failed, waiting for the next loop...", e);
            } catch (AssertionError ignored) {
                // AssertionError contains way too much records and might flood the log output.
                LOG.warn(
                        "Results mismatch, expected {} records, but got {} actually. Waiting for the next loop...",
                        expected.size(),
                        results.size());
            }
            Thread.sleep(1000L);
        }
        Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }
}
