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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** End-to-end tests for mysql cdc to Paimon pipeline job. */
@RunWith(Parameterized.class)
public class MySqlToPaimonE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlToPaimonE2eITCase.class);

    public static final int TESTCASE_TIMEOUT_SECONDS = 60;

    private TableEnvironment tEnv;

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

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    public MySqlToPaimonE2eITCase() throws IOException {}

    @BeforeClass
    public static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        LOG.info("Containers are started.");
    }

    @Before
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    @After
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
    }

    @Test
    public void testSyncWholeDatabase() throws Exception {
        String warehouse = temporaryFolder.newFolder("paimon_" + UUID.randomUUID()).toString();
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon_catalog WITH ('type'='paimon', 'warehouse'='%s')",
                        warehouse));
        tEnv.executeSql("USE CATALOG paimon_catalog");
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
                                + "  table.properties.bucket: 10\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 4",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        warehouse);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path paimonCdcConnector = TestUtils.getResource("paimon-cdc-pipeline-connector.jar");
        Path hadoopJar = TestUtils.getResource("flink-shade-hadoop.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, paimonCdcConnector, mysqlDriverJar, hadoopJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        String query =
                String.format(
                        "SELECT * FROM %s.%s",
                        mysqlInventoryDatabase.getDatabaseName(), "products");
        validateSinkResult(
                query,
                Arrays.asList(
                        Row.of(
                                101,
                                "scooter",
                                "Small 2-wheel scooter",
                                3.14f,
                                "red",
                                "{\"key1\": \"value1\"}",
                                "{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}"),
                        Row.of(
                                102,
                                "car battery",
                                "12V car battery",
                                8.1f,
                                "white",
                                "{\"key2\": \"value2\"}",
                                "{\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}"),
                        Row.of(
                                103,
                                "12-pack drill bits",
                                "12-pack of drill bits with sizes ranging from #40 to #3",
                                0.8f,
                                "red",
                                "{\"key3\": \"value3\"}",
                                "{\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}"),
                        Row.of(
                                104,
                                "hammer",
                                "12oz carpenter's hammer",
                                0.75f,
                                "white",
                                "{\"key4\": \"value4\"}",
                                "{\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}"),
                        Row.of(
                                105,
                                "hammer",
                                "14oz carpenter's hammer",
                                0.875f,
                                "red",
                                "{\"k1\": \"v1\", \"k2\": \"v2\"}",
                                "{\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}"),
                        Row.of(106, "hammer", "16oz carpenter's hammer", 1.0f, null, null, null),
                        Row.of(107, "rocks", "box of assorted rocks", 5.3f, null, null, null),
                        Row.of(
                                108,
                                "jacket",
                                "water resistent black wind breaker",
                                0.1f,
                                null,
                                null,
                                null),
                        Row.of(109, "spare tire", "24 inch spare tire", 22.2f, null, null, null)));

        query =
                String.format(
                        "SELECT * FROM %s.%s",
                        mysqlInventoryDatabase.getDatabaseName(), "customers");
        validateSinkResult(
                query,
                Arrays.asList(
                        Row.of(101, "user_1", "Shanghai", "123567891234"),
                        Row.of(102, "user_2", "Shanghai", "123567891234"),
                        Row.of(103, "user_3", "Shanghai", "123567891234"),
                        Row.of(104, "user_4", "Shanghai", "123567891234")));

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null);"); // 110
            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");

            // modify table schema
            stat.execute("ALTER TABLE products DROP COLUMN point_c;");
            stat.execute("DELETE FROM products WHERE id=101;");

            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null);"); // 111
            stat.execute(
                    "INSERT INTO products VALUES (default,'finally', null, 2.14, null, null);"); // 112
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
        query =
                String.format(
                        "SELECT * FROM %s.%s",
                        mysqlInventoryDatabase.getDatabaseName(), "products");
        validateSinkResult(
                query,
                Arrays.asList(
                        Row.ofKind(
                                RowKind.INSERT,
                                102,
                                "car battery",
                                "12V car battery",
                                8.1f,
                                "white",
                                "{\"key2\": \"value2\"}"),
                        Row.ofKind(
                                RowKind.INSERT,
                                103,
                                "12-pack drill bits",
                                "12-pack of drill bits with sizes ranging from #40 to #3",
                                0.8f,
                                "red",
                                "{\"key3\": \"value3\"}"),
                        Row.ofKind(
                                RowKind.INSERT,
                                104,
                                "hammer",
                                "12oz carpenter's hammer",
                                0.75f,
                                "white",
                                "{\"key4\": \"value4\"}"),
                        Row.ofKind(
                                RowKind.INSERT,
                                105,
                                "hammer",
                                "14oz carpenter's hammer",
                                0.875f,
                                "red",
                                "{\"k1\": \"v1\", \"k2\": \"v2\"}"),
                        Row.ofKind(
                                RowKind.INSERT,
                                106,
                                "hammer",
                                "18oz carpenter hammer",
                                1.0f,
                                null,
                                null),
                        Row.ofKind(
                                RowKind.INSERT,
                                107,
                                "rocks",
                                "box of assorted rocks",
                                5.1f,
                                null,
                                null),
                        Row.ofKind(
                                RowKind.INSERT,
                                108,
                                "jacket",
                                "water resistent black wind breaker",
                                0.1f,
                                null,
                                null),
                        Row.ofKind(
                                RowKind.INSERT,
                                109,
                                "spare tire",
                                "24 inch spare tire",
                                22.2f,
                                null,
                                null),
                        Row.ofKind(
                                RowKind.INSERT,
                                110,
                                "jacket",
                                "water resistent white wind breaker",
                                0.2f,
                                null,
                                null),
                        Row.ofKind(
                                RowKind.INSERT,
                                111,
                                "scooter",
                                "Big 2-wheel scooter ",
                                5.18f,
                                null,
                                null),
                        Row.ofKind(RowKind.INSERT, 112, "finally", null, 2.14f, null, null)));
    }

    private void validateSinkResult(String query, List<Row> expected) {
        long startWaitingTimestamp = System.currentTimeMillis();
        boolean validateSucceed = false;
        while (!validateSucceed) {
            try {
                List<Row> results = new ArrayList<>();
                tEnv.executeSql(query).collect().forEachRemaining(results::add);
                assertEqualsInAnyOrder(expected, results);
                validateSucceed = true;
            } catch (Throwable e) {
                if (System.currentTimeMillis() - startWaitingTimestamp
                        > TESTCASE_TIMEOUT_SECONDS * 1000_1000L) {
                    throw new RuntimeException("Failed to check result with given query.");
                }
            }
        }
    }

    private static void assertEqualsInAnyOrder(List<Row> expected, List<Row> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        Set<Row> expectedSet = new HashSet<>(expected);
        for (Row row : actual) {
            Assert.assertTrue(expectedSet.contains(row));
        }
    }
}
