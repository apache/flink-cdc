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
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.pipeline.tests.utils.MySqlContainer;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.sink.MySqlSinkTestBase.assertEqualsInAnyOrder;

/** */
@RunWith(Parameterized.class)
public class MySqlToMySqlE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlToMySqlE2eITCase.class);
    public static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;
    public static final int TESTCASE_TIMEOUT_SECONDS = 60;
    public static final String TEST_USERNAME = "root";
    public static final String TEST_PASSWORD = "test";
    public static final String TEST_DB = "db_source";
    // public static final String TEST_SINK_DB = "db_sink";
    public static final String SOURCE_NETWORK_NAME = "mysqlsource";
    public static final String SINK_NETWORK_NAME = "mysqlsink";
    protected Path jdbcJar;

    @ClassRule
    public static final MySqlContainer MYSQL_SOURCE =
            new MySqlContainer(MySqlVersion.V8_0, NETWORK, SOURCE_NETWORK_NAME, TEST_DB);

    @ClassRule
    public static final MySqlContainer MYSQL_SINK =
            new MySqlContainer(MySqlVersion.V8_0, NETWORK, SINK_NETWORK_NAME, TEST_DB);

    @BeforeClass
    public static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_SOURCE)).join();
        Startables.deepStart(Stream.of(MYSQL_SINK)).join();
        LOG.info("Waiting for backends to be available");
        long startWaitingTimestamp = System.currentTimeMillis();

        // MYSQL_SOURCE.waitForLog("", 1, 60);

        while (!MYSQL_SOURCE.checkMySqlAvailability()) {
            try {
                if (System.currentTimeMillis() - startWaitingTimestamp
                        > DEFAULT_STARTUP_TIMEOUT_SECONDS * 1000) {
                    throw new RuntimeException("MYSQL_SOURCE startup timed out.");
                }
                LOG.info("Waiting for MYSQL_SOURCE to be available");
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                // ignore and check next round
            }
        }

        while (!MYSQL_SINK.checkMySqlAvailability()) {
            try {
                if (System.currentTimeMillis() - startWaitingTimestamp
                        > DEFAULT_STARTUP_TIMEOUT_SECONDS * 1000) {
                    throw new RuntimeException("MYSQL_SINK startup timed out.");
                }
                LOG.info("Waiting for MYSQL_SINK to be available");
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                // ignore and check next round
            }
        }
        LOG.info("Containers are started.");
    }

    @Before
    public void before() throws Exception {
        super.before();
        MYSQL_SOURCE.createDatabase(TEST_DB);
        MYSQL_SOURCE.createAndInitialize("mysql_inventory");
        MYSQL_SINK.createDatabase(TEST_DB);
        jdbcJar = TestUtils.getResource(getJdbcConnectorResourceName());
        // MYSQL_SINK.createAndInitialize("mysql_inventory");
    }

    protected String getJdbcConnectorResourceName() {
        return String.format("jdbc-connector_%s.jar", flinkVersion);
    }

    @After
    public void after() {
        super.after();
        MYSQL_SOURCE.dropDatabase(TEST_DB);
        MYSQL_SINK.dropDatabase(TEST_DB);
    }

    @Test
    public void test() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: "
                                + SOURCE_NETWORK_NAME
                                + "\n"
                                + "  port: 3306 \n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "  jdbc.properties.useSSL: false\n"
                                + "  jdbc.properties.allowPublicKeyRetrieval: true\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: mysql-writer\n"
                                + "  hostname: "
                                + SINK_NETWORK_NAME
                                + "\n"
                                + "  port: 3306 \n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        TEST_USERNAME,
                        TEST_PASSWORD,
                        TEST_DB,
                        TEST_USERNAME,
                        TEST_PASSWORD);

        System.out.println(pipelineJob);

        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path jdbcCdcJar = TestUtils.getResource("jdbc-cdc-pipeline-connector.jar");
        Path hikariJar = TestUtils.getResource("HikariCP.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, jdbcCdcJar, jdbcJar, mysqlDriverJar, hikariJar);
        Thread.sleep(1000 * 30);
        // waitUntilJobRunning(Duration.ofSeconds(60));
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

    private void validateSinkResult(String tableName, int columnCount, List<String> expected)
            throws Exception {
        long startWaitingTimestamp = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - startWaitingTimestamp
                    > TESTCASE_TIMEOUT_SECONDS * 1000) {
                throw new RuntimeException("Doris backend startup timed out.");
            }
            List<String> results = new ArrayList<>();
            try (Connection conn =
                            DriverManager.getConnection(
                                    MYSQL_SINK.getJdbcUrl(TEST_DB), TEST_USERNAME, TEST_PASSWORD);
                    Statement stat = conn.createStatement()) {
                ResultSet rs =
                        stat.executeQuery(
                                String.format("SELECT * FROM `%s`.`%s`;", TEST_DB, tableName));

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

                System.out.println();
                if (expected.size() == results.size()) {
                    assertEqualsInAnyOrder(expected, results);
                    break;
                } else {
                    Thread.sleep(1000);
                }
            } catch (SQLException e) {
                LOG.info("Validate sink result failure, waiting for next turn...", e);
                Thread.sleep(1000);
            }
        }
    }
}
