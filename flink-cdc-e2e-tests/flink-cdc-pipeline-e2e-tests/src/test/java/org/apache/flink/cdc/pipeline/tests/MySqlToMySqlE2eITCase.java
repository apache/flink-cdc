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
import org.apache.flink.cdc.common.utils.TestCaseUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.connectors.mysql.sink.MySqlSinkTestBase.waitForMySqlContainerToBeReady;

/** E2e test case from MySQL source to MySQL sink (JDBC). */
public class MySqlToMySqlE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlToMySqlE2eITCase.class);
    public static final int TESTCASE_TIMEOUT_SECONDS = 60;
    protected static final String TEST_USERNAME = "mysqluser";
    protected static final String TEST_PASSWORD = "mysqlpw";
    public static final String SOURCE_NETWORK_NAME = "mysqlsource";
    public static final String SINK_NETWORK_NAME = "mysqlsink";
    protected Path jdbcJar;

    public static final MySqlContainer MYSQL_SOURCE =
            (MySqlContainer)
                    new MySqlContainer(MySqlVersion.V8_0)
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases(SOURCE_NETWORK_NAME)
                            .withExposedPorts(3306)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    public static final MySqlContainer MYSQL_SINK =
            (MySqlContainer)
                    new MySqlContainer(MySqlVersion.V8_0)
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases(SINK_NETWORK_NAME)
                            .withExposedPorts(3306)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void initializeContainers() throws Exception {
        Startables.deepStart(MYSQL_SOURCE, MYSQL_SINK).join();
        LOG.info("Starting containers...");
        waitForMySqlContainerToBeReady(MYSQL_SOURCE);
        LOG.info("MySQL Source container started.");
        waitForMySqlContainerToBeReady(MYSQL_SINK);
        LOG.info("MySQL Sink container started.");
    }

    @AfterClass
    public static void destroyContainers() {
        if (MYSQL_SOURCE != null) {
            MYSQL_SOURCE.stop();
        }
        if (MYSQL_SINK != null) {
            MYSQL_SINK.stop();
        }
    }

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL_SOURCE, "mysql_inventory", TEST_USERNAME, TEST_PASSWORD);

    @Before
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();
        // Create sink database manually, as JDBC sink does not support DB auto-creation
        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL_SINK.getJdbcUrl(), TEST_USERNAME, TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute(
                    String.format("CREATE DATABASE %s;", mysqlInventoryDatabase.getDatabaseName()));
        }
        jdbcJar = TestUtils.getResource(getJdbcConnectorResourceName());
    }

    protected String getJdbcConnectorResourceName() {
        return String.format("jdbc-connector_%s.jar", flinkVersion);
    }

    @After
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
    }

    @Test
    public void testSyncWholeDatabase() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "  jdbc.properties.useSSL: false\n"
                                + "  jdbc.properties.allowPublicKeyRetrieval: true\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        SOURCE_NETWORK_NAME,
                        TEST_USERNAME,
                        TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        SINK_NETWORK_NAME,
                        TEST_USERNAME,
                        TEST_PASSWORD,
                        parallelism);

        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, jdbcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateSinkResult(
                "products",
                7,
                Arrays.asList(
                        "101 | scooter | Small 2-wheel scooter | 3.14 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                        "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                        "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                        "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                        "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                        "106 | hammer | 16oz carpenter's hammer | 1.0 | null | null | null",
                        "107 | rocks | box of assorted rocks | 5.3 | null | null | null",
                        "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null",
                        "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null"));
    }

    private void validateSinkResult(String tableName, int columnCount, List<String> expected) {
        TestCaseUtils.repeatedCheckAndValidate(
                () -> {
                    List<String> results = new ArrayList<>();
                    try (Connection conn =
                                    DriverManager.getConnection(
                                            MYSQL_SINK.getJdbcUrl(
                                                    mysqlInventoryDatabase.getDatabaseName()),
                                            TEST_USERNAME,
                                            TEST_PASSWORD);
                            Statement stat = conn.createStatement()) {
                        ResultSet rs =
                                stat.executeQuery(
                                        String.format(
                                                "SELECT * FROM `%s`.`%s`;",
                                                mysqlInventoryDatabase.getDatabaseName(),
                                                tableName));

                        while (rs.next()) {
                            List<String> columns = new ArrayList<>();
                            for (int i = 1; i <= columnCount; i++) {
                                try {
                                    columns.add(rs.getString(i));
                                } catch (SQLException ignored) {
                                    // Column count could change after schema evolution
                                    columns.add(null);
                                }
                            }
                            results.add(String.join(" | ", columns));
                        }
                    }
                    LOG.info("Fetched results: {}", results);
                    return results;
                },
                (results) -> {
                    try {
                        Assertions.assertThat(results)
                                .containsExactlyInAnyOrderElementsOf(expected);
                        LOG.info("Validated successfully.");
                        return true;
                    } catch (AssertionError e) {
                        LOG.warn("Validation failed, waiting for the next turn...", e);
                        return false;
                    }
                },
                Duration.ofSeconds(TESTCASE_TIMEOUT_SECONDS),
                Duration.ofSeconds(1),
                Collections.singletonList(SQLException.class));
    }
}
