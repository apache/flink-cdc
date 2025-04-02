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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;

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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** E2e test case from MySQL source to MySQL sink (JDBC). */
class MySqlToJdbcMySqlE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlToJdbcMySqlE2eITCase.class);

    private static final Duration TESTCASE_TIMEOUT = Duration.ofMinutes(5);
    private static final String TEST_USERNAME = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";
    private static final String SOURCE_NETWORK_NAME = "mysqlsource";
    private static final String SINK_NETWORK_NAME = "mysqlsink";

    @Container
    private static final MySqlContainer MYSQL_SOURCE =
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

    @Container
    private static final MySqlContainer MYSQL_SINK =
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

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL_SOURCE, "mysql_inventory", TEST_USERNAME, TEST_PASSWORD);

    protected final UniqueDatabase batchedWriteDatabase =
            new UniqueDatabase(MYSQL_SOURCE, "batched_write", TEST_USERNAME, TEST_PASSWORD);

    @BeforeEach
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
            stat.execute(
                    String.format("CREATE DATABASE %s;", batchedWriteDatabase.getDatabaseName()));
        }
    }

    @AfterEach
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
        batchedWriteDatabase.dropDatabase();
    }

    @Test
    void testSyncSchemaChanges() throws Exception {
        String databaseName = mysqlInventoryDatabase.getDatabaseName();
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
                                + "  type: jdbc\n"
                                + "  url: jdbc:mysql://%s:%d\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        SOURCE_NETWORK_NAME,
                        TEST_USERNAME,
                        TEST_PASSWORD,
                        databaseName,
                        SINK_NETWORK_NAME,
                        MySqlContainer.MYSQL_PORT,
                        TEST_USERNAME,
                        TEST_PASSWORD,
                        parallelism);

        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path jdbcConnectorJar = TestUtils.getResource("jdbc-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, jdbcConnectorJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        // Verify snapshot results
        validateSinkResult(
                databaseName,
                "products",
                "*",
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
        validateSinkSchema(
                databaseName,
                "products",
                Arrays.asList(
                        "id | int | NO | PRI | null",
                        "name | varchar(255) | YES |  | flink",
                        "description | varchar(512) | YES |  | null",
                        "weight | float | YES |  | null",
                        "enum_c | text | YES |  | null",
                        "json_c | text | YES |  | null",
                        "point_c | text | YES |  | null"));

        // Verify incremental binlog results
        runBinlogEvents();
        validateSinkResult(
                databaseName,
                "products",
                "*",
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
                        "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null",
                        "1001 | Alice | a dreaming novelist | 0.017 | red | null | null",
                        "1099 | Zink | the end of everything | 993.0 | white | null | null"));
        validateSinkSchema(
                databaseName,
                "products",
                Arrays.asList(
                        "id | int | NO | PRI | null",
                        "name | varchar(255) | YES |  | flink",
                        "description | varchar(512) | YES |  | null",
                        "weight | float | YES |  | null",
                        "enum_c | text | YES |  | null",
                        "json_c | text | YES |  | null",
                        "point_c | text | YES |  | null"));

        // Verify AddColumnEvent results
        runAddColumnEvents();
        validateSinkResult(
                databaseName,
                "products",
                "*",
                8,
                Arrays.asList(
                        "101 | scooter | null | Small 2-wheel scooter | 3.14 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                        "102 | car battery | null | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                        "103 | 12-pack drill bits | null | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                        "104 | hammer | null | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                        "105 | hammer | null | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                        "106 | hammer | null | 16oz carpenter's hammer | 1.0 | null | null | null",
                        "107 | rocks | null | box of assorted rocks | 5.3 | null | null | null",
                        "108 | jacket | null | water resistent black wind breaker | 0.1 | null | null | null",
                        "109 | spare tire | null | 24 inch spare tire | 22.2 | null | null | null",
                        "1001 | Alice | null | a dreaming novelist | 0.017 | red | null | null",
                        "1099 | Zink | null | the end of everything | 993.0 | white | null | null",
                        "1100 | Romeo | R | loves juliet | 0.0 | red | null | null",
                        "9900 | Juliet | J | loves romeo | 0.0 | white | null | null"));
        validateSinkSchema(
                databaseName,
                "products",
                Arrays.asList(
                        "id | int | NO | PRI | null",
                        "name | varchar(255) | YES |  | flink",
                        "nickname | varchar(17) | YES |  | null",
                        "description | varchar(512) | YES |  | null",
                        "weight | float | YES |  | null",
                        "enum_c | text | YES |  | null",
                        "json_c | text | YES |  | null",
                        "point_c | text | YES |  | null"));

        // Verify AlterColumnTypeEvent results
        runAlterColumnTypeEvents();
        validateSinkResult(
                databaseName,
                "products",
                "*",
                8,
                Arrays.asList(
                        "101 | scooter | null | Small 2-wheel scooter | 3.140000104904175 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                        "102 | car battery | null | 12V car battery | 8.100000381469727 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                        "103 | 12-pack drill bits | null | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.800000011920929 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                        "104 | hammer | null | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                        "105 | hammer | null | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                        "106 | hammer | null | 16oz carpenter's hammer | 1.0 | null | null | null",
                        "107 | rocks | null | box of assorted rocks | 5.300000190734863 | null | null | null",
                        "108 | jacket | null | water resistent black wind breaker | 0.10000000149011612 | null | null | null",
                        "109 | spare tire | null | 24 inch spare tire | 22.200000762939453 | null | null | null",
                        "1001 | Alice | null | a dreaming novelist | 0.017000000923871994 | red | null | null",
                        "1099 | Zink | null | the end of everything | 993.0 | white | null | null",
                        "1100 | Romeo | R | loves juliet | 0.0 | red | null | null",
                        "9900 | Juliet | J | loves romeo | 0.0 | white | null | null",
                        "10001 | Sierra | S | macOS 10.12 | 3.141592653589793 | white | null | null"));
        validateSinkSchema(
                databaseName,
                "products",
                Arrays.asList(
                        "id | int | NO | PRI | null",
                        "name | varchar(255) | YES |  | flink",
                        "nickname | varchar(17) | YES |  | null",
                        "description | varchar(512) | YES |  | null",
                        "weight | double | YES |  | null",
                        "enum_c | text | YES |  | null",
                        "json_c | text | YES |  | null",
                        "point_c | text | YES |  | null"));

        // Verify RenameColumnEvent results
        runRenameColumnEvents();
        validateSinkResult(
                databaseName,
                "products",
                "*",
                8,
                Arrays.asList(
                        "101 | scooter | null | Small 2-wheel scooter | 3.140000104904175 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                        "102 | car battery | null | 12V car battery | 8.100000381469727 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                        "103 | 12-pack drill bits | null | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.800000011920929 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                        "104 | hammer | null | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                        "105 | hammer | null | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                        "106 | hammer | null | 16oz carpenter's hammer | 1.0 | null | null | null",
                        "107 | rocks | null | box of assorted rocks | 5.300000190734863 | null | null | null",
                        "108 | jacket | null | water resistent black wind breaker | 0.10000000149011612 | null | null | null",
                        "109 | spare tire | null | 24 inch spare tire | 22.200000762939453 | null | null | null",
                        "1001 | Alice | null | a dreaming novelist | 0.017000000923871994 | red | null | null",
                        "1099 | Zink | null | the end of everything | 993.0 | white | null | null",
                        "1100 | Romeo | R | loves juliet | 0.0 | red | null | null",
                        "9900 | Juliet | J | loves romeo | 0.0 | white | null | null",
                        "10001 | Sierra | S | macOS 10.12 | 3.141592653589793 | white | null | null",
                        "10002 | High Sierra | HS | macOS 10.13 | 2.718281828 | white | null | null"));
        validateSinkSchema(
                databaseName,
                "products",
                Arrays.asList(
                        "id | int | NO | PRI | null",
                        "name | varchar(255) | YES |  | flink",
                        "nickname | varchar(17) | YES |  | null",
                        "description | varchar(512) | YES |  | null",
                        "weight | double | YES |  | null",
                        "enumerable_c | text | YES |  | null",
                        "json_c | text | YES |  | null",
                        "point_c | text | YES |  | null"));

        // Verify DropColumnEvent results
        runDropColumnEvents();
        validateSinkResult(
                databaseName,
                "products",
                "*",
                7,
                Arrays.asList(
                        "101 | scooter | null | Small 2-wheel scooter | 3.140000104904175 | red | {\"key1\": \"value1\"}",
                        "102 | car battery | null | 12V car battery | 8.100000381469727 | white | {\"key2\": \"value2\"}",
                        "103 | 12-pack drill bits | null | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.800000011920929 | red | {\"key3\": \"value3\"}",
                        "104 | hammer | null | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"}",
                        "105 | hammer | null | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"}",
                        "106 | hammer | null | 16oz carpenter's hammer | 1.0 | null | null",
                        "107 | rocks | null | box of assorted rocks | 5.300000190734863 | null | null",
                        "108 | jacket | null | water resistent black wind breaker | 0.10000000149011612 | null | null",
                        "109 | spare tire | null | 24 inch spare tire | 22.200000762939453 | null | null",
                        "1001 | Alice | null | a dreaming novelist | 0.017000000923871994 | red | null",
                        "1099 | Zink | null | the end of everything | 993.0 | white | null",
                        "1100 | Romeo | R | loves juliet | 0.0 | red | null",
                        "9900 | Juliet | J | loves romeo | 0.0 | white | null",
                        "10001 | Sierra | S | macOS 10.12 | 3.141592653589793 | white | null",
                        "10002 | High Sierra | HS | macOS 10.13 | 2.718281828 | white | null",
                        "10003 | Mojave | M | macOS 10.14 | 1.414 | white | null"));
        validateSinkSchema(
                databaseName,
                "products",
                Arrays.asList(
                        "id | int | NO | PRI | null",
                        "name | varchar(255) | YES |  | flink",
                        "nickname | varchar(17) | YES |  | null",
                        "description | varchar(512) | YES |  | null",
                        "weight | double | YES |  | null",
                        "enumerable_c | text | YES |  | null",
                        "json_c | text | YES |  | null"));
    }

    @ParameterizedTest(name = "hasPK: {0}")
    @ValueSource(booleans = {true, false})
    void testWriteBatchedRecords(boolean hasPrimaryKey) throws Exception {
        String databaseName = batchedWriteDatabase.getDatabaseName();
        int recordsCount = 10240;
        runBatchedDataReadPhaseOne(recordsCount, hasPrimaryKey);
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
                                + "  scan.incremental.snapshot.chunk.key-column: \\.*.\\.*:id\n"
                                + "  jdbc.properties.useSSL: false\n"
                                + "  jdbc.properties.allowPublicKeyRetrieval: true\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: jdbc\n"
                                + "  url: jdbc:mysql://%s:%d\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  write.batch.size: 512\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        SOURCE_NETWORK_NAME,
                        TEST_USERNAME,
                        TEST_PASSWORD,
                        databaseName,
                        SINK_NETWORK_NAME,
                        MySqlContainer.MYSQL_PORT,
                        TEST_USERNAME,
                        TEST_PASSWORD,
                        parallelism);

        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path jdbcConnectorJar = TestUtils.getResource("jdbc-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, jdbcConnectorJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        validateSinkResult(
                databaseName,
                "batch_table",
                "COUNT(id)",
                1,
                Collections.singletonList(String.valueOf(recordsCount)));
        validateSinkSchema(
                databaseName,
                "batch_table",
                Arrays.asList(
                        hasPrimaryKey
                                ? "id | varchar(255) | NO | PRI | null"
                                : "id | varchar(255) | YES |  | null",
                        "number | int | YES |  | null"));

        // Check incremental writing
        runBatchedDataReadPhaseTwo(recordsCount);
        validateSinkResult(
                databaseName,
                "batch_table",
                "COUNT(id)",
                1,
                Collections.singletonList(String.valueOf(recordsCount * 2)));

        // Check updating
        runBatchedDataReadPhaseThree();
        validateSinkResult(
                databaseName,
                "batch_table",
                "SUM(number)",
                1,
                Collections.singletonList(String.valueOf(recordsCount * (recordsCount + 3))));

        runBatchedDataReadPhaseFour();
        validateSinkResult(
                databaseName, "batch_table", "COUNT(*)", 1, Collections.singletonList("0"));
    }

    private void runBinlogEvents() throws Exception {
        executeSql(
                MYSQL_SOURCE,
                mysqlInventoryDatabase.getDatabaseName(),
                TEST_USERNAME,
                TEST_PASSWORD,
                Arrays.asList(
                        "INSERT INTO products VALUES (1001, 'Alice', 'a dreaming novelist', 0.017, 'red', null, null);",
                        "INSERT INTO products VALUES (1099, 'Zink', 'the end of everything', 993, 'white', null, null);"));
    }

    private void runAddColumnEvents() throws Exception {
        executeSql(
                MYSQL_SOURCE,
                mysqlInventoryDatabase.getDatabaseName(),
                TEST_USERNAME,
                TEST_PASSWORD,
                Arrays.asList(
                        "ALTER TABLE products ADD COLUMN nickname VARCHAR(17) NULL AFTER name;",
                        "INSERT INTO products VALUES (1100, 'Romeo', 'R', 'loves juliet', 0, 'red', null, null);",
                        "INSERT INTO products VALUES (9900, 'Juliet', 'J', 'loves romeo', 0, 'white', null, null);"));
    }

    private void runAlterColumnTypeEvents() throws Exception {
        executeSql(
                MYSQL_SOURCE,
                mysqlInventoryDatabase.getDatabaseName(),
                TEST_USERNAME,
                TEST_PASSWORD,
                Arrays.asList(
                        "ALTER TABLE products MODIFY COLUMN weight DOUBLE;",
                        "INSERT INTO products VALUES (10001, 'Sierra', 'S', 'macOS 10.12', 3.141592653589793238, 'white', null, null);"));
    }

    private void runRenameColumnEvents() throws Exception {
        executeSql(
                MYSQL_SOURCE,
                mysqlInventoryDatabase.getDatabaseName(),
                TEST_USERNAME,
                TEST_PASSWORD,
                Arrays.asList(
                        "ALTER TABLE products RENAME COLUMN `enum_c` TO `enumerable_c`;",
                        "INSERT INTO products VALUES (10002, 'High Sierra', 'HS', 'macOS 10.13', 2.718281828, 'white', null, null);"));
    }

    private void runDropColumnEvents() throws Exception {
        executeSql(
                MYSQL_SOURCE,
                mysqlInventoryDatabase.getDatabaseName(),
                TEST_USERNAME,
                TEST_PASSWORD,
                Arrays.asList(
                        "ALTER TABLE products DROP COLUMN `point_c`;",
                        "INSERT INTO products VALUES (10003, 'Mojave', 'M', 'macOS 10.14', 1.414, 'white', null);"));
    }

    private void runBatchedDataReadPhaseOne(int count, boolean hasPrimaryKey) throws Exception {
        String databaseName = batchedWriteDatabase.getDatabaseName();
        executeSql(
                MYSQL_SOURCE,
                "",
                TEST_USERNAME,
                TEST_PASSWORD,
                Collections.singletonList("CREATE DATABASE " + databaseName));

        executeSql(
                MYSQL_SOURCE,
                databaseName,
                TEST_USERNAME,
                TEST_PASSWORD,
                Collections.singletonList(
                        "CREATE TABLE batch_table (id VARCHAR(255) "
                                + (hasPrimaryKey ? "PRIMARY KEY" : "")
                                + ", number INT);"));

        executeSql(
                MYSQL_SOURCE,
                databaseName,
                TEST_USERNAME,
                TEST_PASSWORD,
                IntStream.rangeClosed(1, count)
                        .mapToObj(
                                i ->
                                        String.format(
                                                "INSERT INTO batch_table VALUES ('name%d', %d)",
                                                i, i))
                        .collect(Collectors.toList()));
    }

    private void runBatchedDataReadPhaseTwo(int count) throws Exception {
        String databaseName = batchedWriteDatabase.getDatabaseName();
        executeSql(
                MYSQL_SOURCE,
                databaseName,
                TEST_USERNAME,
                TEST_PASSWORD,
                IntStream.rangeClosed(1, count)
                        .mapToObj(
                                i ->
                                        String.format(
                                                "INSERT INTO batch_table VALUES ('name%d', %d)",
                                                100_000 + i, i))
                        .collect(Collectors.toList()));
    }

    private void runBatchedDataReadPhaseThree() throws Exception {
        String databaseName = batchedWriteDatabase.getDatabaseName();
        executeSql(
                MYSQL_SOURCE,
                databaseName,
                TEST_USERNAME,
                TEST_PASSWORD,
                Collections.singletonList("UPDATE batch_table SET number = number + 1;"));
    }

    private void runBatchedDataReadPhaseFour() throws Exception {
        String databaseName = batchedWriteDatabase.getDatabaseName();
        executeSql(
                MYSQL_SOURCE,
                databaseName,
                TEST_USERNAME,
                TEST_PASSWORD,
                Collections.singletonList("DELETE FROM batch_table;"));
    }

    private void executeSql(
            MySqlContainer container,
            String databaseName,
            String username,
            String password,
            List<String> sql)
            throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                container.getJdbcUrl(databaseName), username, password);
                Statement stmt = conn.createStatement()) {
            for (String s : sql) {
                stmt.execute(s);
            }
        }
    }

    private List<String> validateSinkSchema(
            String databaseName, String tableName, List<String> expected) throws SQLException {
        List<String> results = new ArrayList<>();
        ResultSet rs =
                MYSQL_SINK
                        .createConnection("")
                        .createStatement()
                        .executeQuery(String.format("DESCRIBE `%s`.`%s`", databaseName, tableName));

        while (rs.next()) {
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                columns.add(rs.getString(i));
            }
            results.add(String.join(" | ", columns));
        }

        Assertions.assertThat(results).containsExactlyElementsOf(expected);
        return results;
    }

    private void validateSinkResult(
            String databaseName,
            String tableName,
            String expression,
            int columnCount,
            List<String> expected) {
        TestCaseUtils.repeatedCheckAndValidate(
                () -> {
                    List<String> results = new ArrayList<>();
                    try (Connection conn =
                                    DriverManager.getConnection(
                                            MYSQL_SINK.getJdbcUrl(databaseName),
                                            TEST_USERNAME,
                                            TEST_PASSWORD);
                            Statement stmt = conn.createStatement()) {
                        ResultSet rs =
                                stmt.executeQuery(
                                        String.format(
                                                "SELECT %s FROM `%s`.`%s`;",
                                                expression, databaseName, tableName));

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
                TESTCASE_TIMEOUT,
                Duration.ofSeconds(1),
                Collections.singletonList(SQLException.class));
    }
}
