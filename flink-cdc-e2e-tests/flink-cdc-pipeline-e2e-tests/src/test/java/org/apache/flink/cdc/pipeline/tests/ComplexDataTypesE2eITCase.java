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
import org.apache.flink.cdc.connectors.doris.sink.utils.DorisContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** End-to-end tests for complex data types. */
public class ComplexDataTypesE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(ComplexDataTypesE2eITCase.class);

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    public static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;
    public static final int TESTCASE_TIMEOUT_SECONDS = 60;

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

    @ClassRule
    public static final DorisContainer DORIS =
            new DorisContainer(NETWORK)
                    .withNetworkAliases("doris")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected final UniqueDatabase complexDataTypesDatabase =
            new UniqueDatabase(MYSQL, "data_types_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @BeforeClass
    public static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        Startables.deepStart(Stream.of(DORIS)).join();
        LOG.info("Waiting for backends to be available");
        long startWaitingTimestamp = System.currentTimeMillis();

        new LogMessageWaitStrategy()
                .withRegEx(".*get heartbeat from FE.*")
                .withTimes(1)
                .withStartupTimeout(
                        Duration.of(DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS))
                .waitUntilReady(DORIS);

        while (!checkBackendAvailability()) {
            try {
                if (System.currentTimeMillis() - startWaitingTimestamp
                        > DEFAULT_STARTUP_TIMEOUT_SECONDS * 1000) {
                    throw new RuntimeException("Doris backend startup timed out.");
                }
                LOG.info("Waiting for backends to be available");
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
        complexDataTypesDatabase.createAndInitialize();
        createDorisDatabase(complexDataTypesDatabase.getDatabaseName());
    }

    private static boolean checkBackendAvailability() {
        try {
            Container.ExecResult rs =
                    DORIS.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            "-e SHOW BACKENDS\\G");

            if (rs.getExitCode() != 0) {
                return false;
            }
            String output = rs.getStdout();
            LOG.info("Doris backend status:\n{}", output);
            return output.contains("*************************** 1. row ***************************")
                    && !output.contains("AvailCapacity: 1.000 B");
        } catch (Exception e) {
            LOG.info("Failed to check backend status.", e);
            return false;
        }
    }

    @After
    public void after() {
        super.after();
        complexDataTypesDatabase.dropDatabase();
        dropDorisDatabase(complexDataTypesDatabase.getDatabaseName());
    }

    @Test
    public void testComplexDataTypes() throws Exception {
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
                                + "  type: doris\n"
                                + "  fenodes: doris:8030\n"
                                + "  benodes: doris:8040\n"
                                + "  username: %s\n"
                                + "  password: \"%s\"\n"
                                + "  table.create.properties.replication_num: 1\n"
                                + "\n"
                                + "transform:\n"
                                + "  - source-table: %s.DATA_TYPES_TABLE\n"
                                + "    projection: \\*, 'fine' AS FINE\n"
                                + "    filter: id <> 3 AND id <> 4\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        complexDataTypesDatabase.getDatabaseName(),
                        DORIS.getUsername(),
                        DORIS.getPassword(),
                        complexDataTypesDatabase.getDatabaseName());
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path dorisCdcConnector = TestUtils.getResource("doris-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, dorisCdcConnector, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        validateSinkResult(
                complexDataTypesDatabase.getDatabaseName(),
                "DATA_TYPES_TABLE",
                52,
                Collections.singletonList(
                        "1 | 127 | 255 | 255 | 32767 | 65535 | 65535 | 8388607 | 16777215 | 16777215 | 2147483647 | 4294967295 | 4294967295 | 2147483647 | 9223372036854775807 | Hello World | abc | 123.102 | 123.102 | 123.103 | 123.104 | 404.4443 | 404.4444 | 404.4445 | 123.4567 | 123.4568 | 123.4569 | 346 | 34567892.1 | 0 | 1 | 1 | 2020-07-17 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22 | text | EA== | EA== | EA== | EA== | 2021 | red | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0} | {\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0} | {\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0} | fine"));

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        complexDataTypesDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            // Insert id = 2, 3, 4, 5
            for (int i = 2; i < 6; i++) {
                stat.execute(
                        "INSERT INTO DATA_TYPES_TABLE\n"
                                + "VALUES ("
                                + i
                                + ", 127, 255, 255, 32767, 65535, 65535, 8388607, 16777215, 16777215, 2147483647,\n"
                                + "        4294967295, 4294967295, 2147483647, 9223372036854775807,\n"
                                + "        'Hello World', 'abc', 123.102, 123.102, 123.103, 123.104, 404.4443, 404.4444, 404.4445,\n"
                                + "        123.4567, 123.4568, 123.4569, 345.6, 34567892.1, 0, 1, true,\n"
                                + "        '2020-07-17',  '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456', '2020-07-17 18:00:22',\n"
                                + "        'text', UNHEX(HEX(16)), UNHEX(HEX(16)), UNHEX(HEX(16)), UNHEX(HEX(16)), 2021,\n"
                                + "        'red',\n"
                                + "        ST_GeomFromText('POINT(1 1)'),\n"
                                + "        ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),\n"
                                + "        ST_GeomFromText('LINESTRING(3 0, 3 3, 3 5)'),\n"
                                + "        ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),\n"
                                + "        ST_GeomFromText('MULTIPOINT((1 1),(2 2))'),\n"
                                + "        ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))'),\n"
                                + "        ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))'),\n"
                                + "        ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))'));");
            }

            validateSinkResult(
                    complexDataTypesDatabase.getDatabaseName(),
                    "DATA_TYPES_TABLE",
                    52,
                    Arrays.asList(
                            "1 | 127 | 255 | 255 | 32767 | 65535 | 65535 | 8388607 | 16777215 | 16777215 | 2147483647 | 4294967295 | 4294967295 | 2147483647 | 9223372036854775807 | Hello World | abc | 123.102 | 123.102 | 123.103 | 123.104 | 404.4443 | 404.4444 | 404.4445 | 123.4567 | 123.4568 | 123.4569 | 346 | 34567892.1 | 0 | 1 | 1 | 2020-07-17 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22 | text | EA== | EA== | EA== | EA== | 2021 | red | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0} | {\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0} | {\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0} | fine",
                            "2 | 127 | 255 | 255 | 32767 | 65535 | 65535 | 8388607 | 16777215 | 16777215 | 2147483647 | 4294967295 | 4294967295 | 2147483647 | 9223372036854775807 | Hello World | abc | 123.102 | 123.102 | 123.103 | 123.104 | 404.4443 | 404.4444 | 404.4445 | 123.4567 | 123.4568 | 123.4569 | 346 | 34567892.1 | 0 | 1 | 1 | 2020-07-17 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22 | text | EA== | EA== | EA== | EA== | 2021 | red | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0} | {\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0} | {\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0} | fine",
                            "5 | 127 | 255 | 255 | 32767 | 65535 | 65535 | 8388607 | 16777215 | 16777215 | 2147483647 | 4294967295 | 4294967295 | 2147483647 | 9223372036854775807 | Hello World | abc | 123.102 | 123.102 | 123.103 | 123.104 | 404.4443 | 404.4444 | 404.4445 | 123.4567 | 123.4568 | 123.4569 | 346 | 34567892.1 | 0 | 1 | 1 | 2020-07-17 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22 | text | EA== | EA== | EA== | EA== | 2021 | red | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0} | {\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0} | {\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0} | fine"));
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
    }

    public static void createDorisDatabase(String databaseName) {
        try {
            Container.ExecResult rs =
                    DORIS.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            String.format("-e CREATE DATABASE IF NOT EXISTS `%s`;", databaseName));

            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to create database." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create database.", e);
        }
    }

    public static void dropDorisDatabase(String databaseName) {
        try {
            Container.ExecResult rs =
                    DORIS.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            String.format("-e DROP DATABASE IF EXISTS %s;", databaseName));

            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to drop database." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to drop database.", e);
        }
    }

    private void validateSinkResult(
            String databaseName, String tableName, int columnCount, List<String> expected)
            throws Exception {
        long startWaitingTimestamp = System.currentTimeMillis();
        List<String> results = new ArrayList<>();
        while (true) {
            if (System.currentTimeMillis() - startWaitingTimestamp
                    > TESTCASE_TIMEOUT_SECONDS * 1000) {
                LOG.error(
                        "Failed to retrieve enough {} entries before timeout.\nResults for now: {}",
                        expected.size(),
                        results);
                throw new RuntimeException("Failed to retrieve enough entries before timeout.");
            }
            results.clear();
            try (Connection conn =
                            DriverManager.getConnection(
                                    DORIS.getJdbcUrl(databaseName, DORIS.getUsername()));
                    Statement stat = conn.createStatement()) {
                ResultSet rs =
                        stat.executeQuery(
                                String.format("SELECT * FROM `%s`.`%s`;", databaseName, tableName));

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

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }
}
