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
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** End-to-end tests for mysql cdc to Doris pipeline job. */
class MySqlToDorisE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlToDorisE2eITCase.class);
    private static final int DIAGNOSTIC_LOG_TAIL_LINES = 200;

    @Container
    protected static final DorisContainer DORIS =
            new DorisContainer(NETWORK).withNetworkAliases("doris");

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    protected final UniqueDatabase complexDataTypesDatabase =
            new UniqueDatabase(MYSQL, "data_types_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    protected final UniqueDatabase columnCaseDatabase =
            new UniqueDatabase(MYSQL, "mysql_case_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @BeforeAll
    public static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        Startables.deepStart(Stream.of(DORIS)).join();
        LOG.info("Waiting for backends to be available");
        long startWaitingTimestamp = System.currentTimeMillis();

        new LogMessageWaitStrategy()
                .withRegEx(".*get heartbeat from FE.*")
                .withTimes(1)
                .withStartupTimeout(STARTUP_WAITING_TIMEOUT)
                .waitUntilReady(DORIS);

        while (!checkBackendAvailability()) {
            try {
                if (System.currentTimeMillis() - startWaitingTimestamp
                        > STARTUP_WAITING_TIMEOUT.toMillis()) {
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

    @BeforeEach
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();
        createDorisDatabase(mysqlInventoryDatabase.getDatabaseName());

        complexDataTypesDatabase.createAndInitialize();
        createDorisDatabase(complexDataTypesDatabase.getDatabaseName());
    }

    private static boolean checkBackendAvailability() {
        try {
            org.testcontainers.containers.Container.ExecResult rs =
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

    @AfterEach
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
        dropDorisDatabase(mysqlInventoryDatabase.getDatabaseName());

        complexDataTypesDatabase.dropDatabase();
        dropDorisDatabase(complexDataTypesDatabase.getDatabaseName());
    }

    @Test
    void testSyncWholeDatabase() throws Exception {
        String databaseName = mysqlInventoryDatabase.getDatabaseName();
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
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        databaseName,
                        DORIS.getUsername(),
                        DORIS.getPassword(),
                        parallelism);
        Path dorisCdcConnector = TestUtils.getResource("doris-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, dorisCdcConnector);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateSinkSchema(
                databaseName,
                "products",
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "name | VARCHAR(765) | Yes | false | flink",
                        "description | VARCHAR(1536) | Yes | false | null",
                        "weight | FLOAT | Yes | false | null",
                        "enum_c | TEXT | Yes | false | red",
                        "json_c | TEXT | Yes | false | null",
                        "point_c | TEXT | Yes | false | null"));
        validateSinkResult(
                databaseName,
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

        validateSinkSchema(
                databaseName,
                "customers",
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "name | VARCHAR(765) | Yes | false | flink",
                        "address | VARCHAR(3072) | Yes | false | null",
                        "phone_number | VARCHAR(1536) | Yes | false | null"));
        validateSinkResult(
                databaseName,
                "customers",
                4,
                Arrays.asList(
                        "101 | user_1 | Shanghai | 123567891234",
                        "102 | user_2 | Shanghai | 123567891234",
                        "103 | user_3 | Shanghai | 123567891234",
                        "104 | user_4 | Shanghai | 123567891234"));

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), databaseName);
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null);"); // 110

            validateSinkResult(
                    databaseName,
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
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null"));

            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            validateSinkResult(
                    databaseName,
                    "products",
                    7,
                    Arrays.asList(
                            "101 | scooter | Small 2-wheel scooter | 3.14 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                            "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                            "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                            "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                            "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                            "106 | hammer | 18oz carpenter hammer | 1.0 | null | null | null",
                            "107 | rocks | box of assorted rocks | 5.1 | null | null | null",
                            "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null",
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null"));

            // modify table schema
            stat.execute("ALTER TABLE products DROP COLUMN point_c;");
            validateSinkSchema(
                    databaseName,
                    "products",
                    Arrays.asList(
                            "id | INT | Yes | true | null",
                            "name | VARCHAR(765) | Yes | false | flink",
                            "description | VARCHAR(1536) | Yes | false | null",
                            "weight | FLOAT | Yes | false | null",
                            "enum_c | TEXT | Yes | false | red",
                            "json_c | TEXT | Yes | false | null"));

            stat.execute("DELETE FROM products WHERE id=101;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null);"); // 111
            stat.execute(
                    "INSERT INTO products VALUES (default,'finally', null, 2.14, null, null);"); // 112
            validateSinkResult(
                    databaseName,
                    "products",
                    7,
                    Arrays.asList(
                            "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | null",
                            "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | null",
                            "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | null",
                            "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | null",
                            "106 | hammer | 18oz carpenter hammer | 1.0 | null | null | null",
                            "107 | rocks | box of assorted rocks | 5.1 | null | null | null",
                            "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null",
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null",
                            "111 | scooter | Big 2-wheel scooter  | 5.18 | null | null | null",
                            "112 | finally | null | 2.14 | null | null | null"));
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
    }

    @Test
    void testSyncMysqlColumnCaseTablesWithJsonFormat() throws Exception {
        columnCaseDatabase.createAndInitialize();
        createDorisDatabase(columnCaseDatabase.getDatabaseName());
        try {
            String databaseName = columnCaseDatabase.getDatabaseName();
            String pipelineJob =
                    String.format(
                            "source:\n"
                                    + "  type: mysql\n"
                                    + "  hostname: mysql\n"
                                    + "  port: 3306\n"
                                    + "  username: %s\n"
                                    + "  password: %s\n"
                                    + "  tables: %s.mixed_case_customer,%s.upper_case_customer,%s.lower_case_customer\n"
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
                                    + "  sink.properties.format: json\n"
                                    + "\n"
                                    + "pipeline:\n"
                                    + "  parallelism: %d",
                            MYSQL_TEST_USER,
                            MYSQL_TEST_PASSWORD,
                            databaseName,
                            databaseName,
                            databaseName,
                            DORIS.getUsername(),
                            DORIS.getPassword(),
                            parallelism);

            submitMysqlToDorisJob(pipelineJob);

            validateSinkSchema(
                    databaseName,
                    "mixed_case_customer",
                    Arrays.asList(
                            "ID | INT | Yes | true | null",
                            "Name | VARCHAR(765) | Yes | false | null",
                            "phone_Number | VARCHAR(765) | Yes | false | null"));
            validateSinkResult(
                    databaseName,
                    "mixed_case_customer",
                    3,
                    Arrays.asList("101 | Alice | 13900000001", "102 | Bob | 13900000002"));

            validateSinkSchema(
                    databaseName,
                    "upper_case_customer",
                    Arrays.asList(
                            "ID | INT | Yes | true | null",
                            "NAME | VARCHAR(765) | Yes | false | null",
                            "PHONE_NUMBER | VARCHAR(765) | Yes | false | null"));
            validateSinkResult(
                    databaseName,
                    "upper_case_customer",
                    3,
                    Arrays.asList("201 | Carol | 13900000003", "202 | Dave | 13900000004"));

            validateSinkSchema(
                    databaseName,
                    "lower_case_customer",
                    Arrays.asList(
                            "id | INT | Yes | true | null",
                            "name | VARCHAR(765) | Yes | false | null",
                            "phone_number | VARCHAR(765) | Yes | false | null"));
            validateSinkResult(
                    databaseName,
                    "lower_case_customer",
                    3,
                    Arrays.asList("301 | Eve | 13900000005", "302 | Frank | 13900000006"));

            insertColumnCaseIncrementalRows(databaseName);

            validateSinkResult(
                    databaseName,
                    "mixed_case_customer",
                    3,
                    Arrays.asList(
                            "101 | Alice | 13900000001",
                            "102 | Bob | 13900000002",
                            "103 | Cindy | 13900000007"));
            validateSinkResult(
                    databaseName,
                    "upper_case_customer",
                    3,
                    Arrays.asList(
                            "201 | Carol | 13900000003",
                            "202 | Dave | 13900000004",
                            "203 | Eric | 13900000008"));
            validateSinkResult(
                    databaseName,
                    "lower_case_customer",
                    3,
                    Arrays.asList(
                            "301 | Eve | 13900000005",
                            "302 | Frank | 13900000006",
                            "303 | Gina | 13900000009"));

            updateAndDeleteColumnCaseRows(databaseName);

            validateSinkResult(
                    databaseName,
                    "mixed_case_customer",
                    3,
                    Arrays.asList(
                            "101 | Alice-Updated | 13900001001", "103 | Cindy | 13900000007"));
            validateSinkResult(
                    databaseName,
                    "upper_case_customer",
                    3,
                    Arrays.asList("201 | Carol-Updated | 13900001002", "203 | Eric | 13900000008"));
            validateSinkResult(
                    databaseName,
                    "lower_case_customer",
                    3,
                    Arrays.asList("301 | Eve-Updated | 13900001003", "303 | Gina | 13900000009"));
        } finally {
            columnCaseDatabase.dropDatabase();
            dropDorisDatabase(columnCaseDatabase.getDatabaseName());
        }
    }

    @Test
    void testSyncMysqlColumnCaseTablesWithJsonFormatAndUpperColumnNameCase() throws Exception {
        columnCaseDatabase.createAndInitialize();
        createDorisDatabase(columnCaseDatabase.getDatabaseName());
        try {
            String databaseName = columnCaseDatabase.getDatabaseName();
            String pipelineJob =
                    String.format(
                            "source:\n"
                                    + "  type: mysql\n"
                                    + "  hostname: mysql\n"
                                    + "  port: 3306\n"
                                    + "  username: %s\n"
                                    + "  password: %s\n"
                                    + "  tables: %s.mixed_case_customer,%s.upper_case_customer,%s.lower_case_customer\n"
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
                                    + "  sink.properties.format: json\n"
                                    + "\n"
                                    + "pipeline:\n"
                                    + "  parallelism: %d\n"
                                    + "  column-name-case: UPPER",
                            MYSQL_TEST_USER,
                            MYSQL_TEST_PASSWORD,
                            databaseName,
                            databaseName,
                            databaseName,
                            DORIS.getUsername(),
                            DORIS.getPassword(),
                            parallelism);

            submitMysqlToDorisJob(pipelineJob);

            validateSinkSchema(
                    databaseName,
                    "mixed_case_customer",
                    Arrays.asList(
                            "ID | INT | Yes | true | null",
                            "NAME | VARCHAR(765) | Yes | false | null",
                            "PHONE_NUMBER | VARCHAR(765) | Yes | false | null"));
            validateSinkResult(
                    databaseName,
                    "mixed_case_customer",
                    3,
                    Arrays.asList("101 | Alice | 13900000001", "102 | Bob | 13900000002"));

            validateSinkSchema(
                    databaseName,
                    "upper_case_customer",
                    Arrays.asList(
                            "ID | INT | Yes | true | null",
                            "NAME | VARCHAR(765) | Yes | false | null",
                            "PHONE_NUMBER | VARCHAR(765) | Yes | false | null"));
            validateSinkResult(
                    databaseName,
                    "upper_case_customer",
                    3,
                    Arrays.asList("201 | Carol | 13900000003", "202 | Dave | 13900000004"));

            validateSinkSchema(
                    databaseName,
                    "lower_case_customer",
                    Arrays.asList(
                            "ID | INT | Yes | true | null",
                            "NAME | VARCHAR(765) | Yes | false | null",
                            "PHONE_NUMBER | VARCHAR(765) | Yes | false | null"));
            validateSinkResult(
                    databaseName,
                    "lower_case_customer",
                    3,
                    Arrays.asList("301 | Eve | 13900000005", "302 | Frank | 13900000006"));

            insertColumnCaseIncrementalRows(databaseName);

            validateSinkResult(
                    databaseName,
                    "mixed_case_customer",
                    3,
                    Arrays.asList(
                            "101 | Alice | 13900000001",
                            "102 | Bob | 13900000002",
                            "103 | Cindy | 13900000007"));
            validateSinkResult(
                    databaseName,
                    "upper_case_customer",
                    3,
                    Arrays.asList(
                            "201 | Carol | 13900000003",
                            "202 | Dave | 13900000004",
                            "203 | Eric | 13900000008"));
            validateSinkResult(
                    databaseName,
                    "lower_case_customer",
                    3,
                    Arrays.asList(
                            "301 | Eve | 13900000005",
                            "302 | Frank | 13900000006",
                            "303 | Gina | 13900000009"));

            updateAndDeleteColumnCaseRows(databaseName);

            validateSinkResult(
                    databaseName,
                    "mixed_case_customer",
                    3,
                    Arrays.asList(
                            "101 | Alice-Updated | 13900001001", "103 | Cindy | 13900000007"));
            validateSinkResult(
                    databaseName,
                    "upper_case_customer",
                    3,
                    Arrays.asList("201 | Carol-Updated | 13900001002", "203 | Eric | 13900000008"));
            validateSinkResult(
                    databaseName,
                    "lower_case_customer",
                    3,
                    Arrays.asList("301 | Eve-Updated | 13900001003", "303 | Gina | 13900000009"));
        } finally {
            columnCaseDatabase.dropDatabase();
            dropDorisDatabase(columnCaseDatabase.getDatabaseName());
        }
    }

    @Test
    void testSyncMysqlProjectionTransformToDorisWithJsonFormat() throws Exception {
        columnCaseDatabase.createAndInitialize();
        createDorisDatabase(columnCaseDatabase.getDatabaseName());
        try {
            String databaseName = columnCaseDatabase.getDatabaseName();
            String pipelineJob =
                    String.format(
                            "source:\n"
                                    + "  type: mysql\n"
                                    + "  hostname: mysql\n"
                                    + "  port: 3306\n"
                                    + "  username: %s\n"
                                    + "  password: %s\n"
                                    + "  tables: %s.customer\n"
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
                                    + "  sink.properties.format: json\n"
                                    + "\n"
                                    + "transform:\n"
                                    + "  - source-table: %s.customer\n"
                                    + "    projection: id as MY_ID, NAME as name, age as AGE\n"
                                    + "\n"
                                    + "pipeline:\n"
                                    + "  parallelism: %d",
                            MYSQL_TEST_USER,
                            MYSQL_TEST_PASSWORD,
                            databaseName,
                            DORIS.getUsername(),
                            DORIS.getPassword(),
                            databaseName,
                            parallelism);

            submitMysqlToDorisJob(pipelineJob);

            validateSinkSchema(
                    databaseName,
                    "customer",
                    Arrays.asList(
                            "MY_ID | INT | Yes | true | null",
                            "name | VARCHAR(765) | Yes | false | null",
                            "AGE | INT | Yes | false | null"));
            validateSinkResult(
                    databaseName,
                    "customer",
                    3,
                    Arrays.asList("401 | Grace | 18", "402 | Heidi | 19"));

            insertProjectionIncrementalRow(databaseName);

            validateSinkResult(
                    databaseName,
                    "customer",
                    3,
                    Arrays.asList("401 | Grace | 18", "402 | Heidi | 19", "403 | Ivan | 20"));
        } finally {
            columnCaseDatabase.dropDatabase();
            dropDorisDatabase(columnCaseDatabase.getDatabaseName());
        }
    }

    @Test
    void testSyncMysqlProjectionTransformToDorisWithJsonFormatAndUpperColumnNameCase()
            throws Exception {
        columnCaseDatabase.createAndInitialize();
        createDorisDatabase(columnCaseDatabase.getDatabaseName());
        try {
            String databaseName = columnCaseDatabase.getDatabaseName();
            String pipelineJob =
                    String.format(
                            "source:\n"
                                    + "  type: mysql\n"
                                    + "  hostname: mysql\n"
                                    + "  port: 3306\n"
                                    + "  username: %s\n"
                                    + "  password: %s\n"
                                    + "  tables: %s.customer\n"
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
                                    + "  sink.properties.format: json\n"
                                    + "\n"
                                    + "transform:\n"
                                    + "  - source-table: %s.customer\n"
                                    + "    projection: id as my_id, NAME as name, age as AGE\n"
                                    + "\n"
                                    + "pipeline:\n"
                                    + "  parallelism: %d\n"
                                    + "  column-name-case: UPPER",
                            MYSQL_TEST_USER,
                            MYSQL_TEST_PASSWORD,
                            databaseName,
                            DORIS.getUsername(),
                            DORIS.getPassword(),
                            databaseName,
                            parallelism);

            submitMysqlToDorisJob(pipelineJob);

            validateSinkSchema(
                    databaseName,
                    "customer",
                    Arrays.asList(
                            "my_id | INT | Yes | true | null",
                            "name | VARCHAR(765) | Yes | false | null",
                            "AGE | INT | Yes | false | null"));
            validateSinkResult(
                    databaseName,
                    "customer",
                    3,
                    Arrays.asList("401 | Grace | 18", "402 | Heidi | 19"));

            insertProjectionIncrementalRow(databaseName);

            validateSinkResult(
                    databaseName,
                    "customer",
                    3,
                    Arrays.asList("401 | Grace | 18", "402 | Heidi | 19", "403 | Ivan | 20"));
        } finally {
            columnCaseDatabase.dropDatabase();
            dropDorisDatabase(columnCaseDatabase.getDatabaseName());
        }
    }

    @Test
    void testSyncWholeDatabaseInBatchMode() throws Exception {
        String databaseName = mysqlInventoryDatabase.getDatabaseName();
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
                                + "  scan.startup.mode: snapshot\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: doris\n"
                                + "  fenodes: doris:8030\n"
                                + "  benodes: doris:8040\n"
                                + "  username: %s\n"
                                + "  password: \"%s\"\n"
                                + "  table.create.properties.replication_num: 1\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        databaseName,
                        DORIS.getUsername(),
                        DORIS.getPassword(),
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path dorisCdcConnector = TestUtils.getResource("doris-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, dorisCdcConnector, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateSinkSchema(
                databaseName,
                "products",
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "name | VARCHAR(765) | Yes | false | flink",
                        "description | VARCHAR(1536) | Yes | false | null",
                        "weight | FLOAT | Yes | false | null",
                        "enum_c | TEXT | Yes | false | red",
                        "json_c | TEXT | Yes | false | null",
                        "point_c | TEXT | Yes | false | null"));

        validateSinkSchema(
                databaseName,
                "customers",
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "name | VARCHAR(765) | Yes | false | flink",
                        "address | VARCHAR(3072) | Yes | false | null",
                        "phone_number | VARCHAR(1536) | Yes | false | null"));

        validateSinkResult(
                databaseName,
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

        validateSinkResult(
                databaseName,
                "customers",
                4,
                Arrays.asList(
                        "101 | user_1 | Shanghai | 123567891234",
                        "102 | user_2 | Shanghai | 123567891234",
                        "103 | user_3 | Shanghai | 123567891234",
                        "104 | user_4 | Shanghai | 123567891234"));
    }

    @Test
    void testComplexDataTypes() throws Exception {
        String databaseName = complexDataTypesDatabase.getDatabaseName();
        String sinkTableName = "data_types_table";
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
                                + "  parallelism: %d",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        databaseName,
                        DORIS.getUsername(),
                        DORIS.getPassword(),
                        databaseName,
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path dorisCdcConnector = TestUtils.getResource("doris-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, dorisCdcConnector, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        LOG.info(
                "Verifying snapshot stage of {} mapped from source DATA_TYPES_TABLE...",
                sinkTableName);
        validateSinkSchema(
                databaseName,
                sinkTableName,
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "tiny_c | TINYINT | Yes | false | null",
                        "tiny_un_c | SMALLINT | Yes | false | null",
                        "tiny_un_z_c | SMALLINT | Yes | false | null",
                        "small_c | SMALLINT | Yes | false | null",
                        "small_un_c | INT | Yes | false | null",
                        "small_un_z_c | INT | Yes | false | null",
                        "medium_c | INT | Yes | false | null",
                        "medium_un_c | INT | Yes | false | null",
                        "medium_un_z_c | INT | Yes | false | null",
                        "int_c | INT | Yes | false | null",
                        "int_un_c | BIGINT | Yes | false | null",
                        "int_un_z_c | BIGINT | Yes | false | null",
                        "int11_c | INT | Yes | false | null",
                        "big_c | BIGINT | Yes | false | null",
                        "varchar_c | VARCHAR(765) | Yes | false | null",
                        "char_c | CHAR(9) | Yes | false | null",
                        "real_c | DOUBLE | Yes | false | null",
                        "float_c | FLOAT | Yes | false | null",
                        "float_un_c | FLOAT | Yes | false | null",
                        "float_un_z_c | FLOAT | Yes | false | null",
                        "double_c | DOUBLE | Yes | false | null",
                        "double_un_c | DOUBLE | Yes | false | null",
                        "double_un_z_c | DOUBLE | Yes | false | null",
                        "decimal_c | DECIMAL(8, 4) | Yes | false | null",
                        "decimal_un_c | DECIMAL(8, 4) | Yes | false | null",
                        "decimal_un_z_c | DECIMAL(8, 4) | Yes | false | null",
                        "numeric_c | DECIMAL(6, 0) | Yes | false | null",
                        "big_decimal_c | TEXT | Yes | false | null",
                        "bit1_c | BOOLEAN | Yes | false | null",
                        "tiny1_c | BOOLEAN | Yes | false | null",
                        "boolean_c | BOOLEAN | Yes | false | null",
                        "date_c | DATE | Yes | false | null",
                        "datetime3_c | DATETIME(3) | Yes | false | null",
                        "datetime6_c | DATETIME(6) | Yes | false | null",
                        "timestamp_c | DATETIME | Yes | false | null",
                        "time_c | TEXT | Yes | false | null",
                        "time3_c | TEXT | Yes | false | null",
                        "text_c | TEXT | Yes | false | null",
                        "tiny_blob_c | TEXT | Yes | false | null",
                        "blob_c | TEXT | Yes | false | null",
                        "medium_blob_c | TEXT | Yes | false | null",
                        "long_blob_c | TEXT | Yes | false | null",
                        "year_c | INT | Yes | false | null",
                        "enum_c | TEXT | Yes | false | red",
                        "point_c | TEXT | Yes | false | null",
                        "geometry_c | TEXT | Yes | false | null",
                        "linestring_c | TEXT | Yes | false | null",
                        "polygon_c | TEXT | Yes | false | null",
                        "multipoint_c | TEXT | Yes | false | null",
                        "multiline_c | TEXT | Yes | false | null",
                        "multipolygon_c | TEXT | Yes | false | null",
                        "geometrycollection_c | TEXT | Yes | false | null",
                        "FINE | TEXT | Yes | false | null"));
        validateSinkResult(
                databaseName,
                sinkTableName,
                54,
                Collections.singletonList(
                        "1 | 127 | 255 | 255 | 32767 | 65535 | 65535 | 8388607 | 16777215 | 16777215 | 2147483647 | 4294967295 | 4294967295 | 2147483647 | 9223372036854775807 | Hello World | abc | 123.102 | 123.102 | 123.103 | 123.104 | 404.4443 | 404.4444 | 404.4445 | 123.4567 | 123.4568 | 123.4569 | 346 | 34567892.1 | 0 | 1 | 1 | 2020-07-17 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22 | 14:38:07 | 21:49:13.123 | text | EA== | EA== | EA== | EA== | 2021 | red | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0} | {\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0} | {\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0} | fine"));

        LOG.info(
                "Verifying streaming stage of {} mapped from source DATA_TYPES_TABLE...",
                sinkTableName);
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), databaseName);
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
                                + "        '2020-07-17',  '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456', '2020-07-17 18:00:22', '14:38:07', '21:49:13.123',\n"
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
                    databaseName,
                    sinkTableName,
                    54,
                    Arrays.asList(
                            "1 | 127 | 255 | 255 | 32767 | 65535 | 65535 | 8388607 | 16777215 | 16777215 | 2147483647 | 4294967295 | 4294967295 | 2147483647 | 9223372036854775807 | Hello World | abc | 123.102 | 123.102 | 123.103 | 123.104 | 404.4443 | 404.4444 | 404.4445 | 123.4567 | 123.4568 | 123.4569 | 346 | 34567892.1 | 0 | 1 | 1 | 2020-07-17 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22 | 14:38:07 | 21:49:13.123 | text | EA== | EA== | EA== | EA== | 2021 | red | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0} | {\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0} | {\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0} | fine",
                            "2 | 127 | 255 | 255 | 32767 | 65535 | 65535 | 8388607 | 16777215 | 16777215 | 2147483647 | 4294967295 | 4294967295 | 2147483647 | 9223372036854775807 | Hello World | abc | 123.102 | 123.102 | 123.103 | 123.104 | 404.4443 | 404.4444 | 404.4445 | 123.4567 | 123.4568 | 123.4569 | 346 | 34567892.1 | 0 | 1 | 1 | 2020-07-17 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22 | 14:38:07 | 21:49:13.123 | text | EA== | EA== | EA== | EA== | 2021 | red | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0} | {\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0} | {\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0} | fine",
                            "5 | 127 | 255 | 255 | 32767 | 65535 | 65535 | 8388607 | 16777215 | 16777215 | 2147483647 | 4294967295 | 4294967295 | 2147483647 | 9223372036854775807 | Hello World | abc | 123.102 | 123.102 | 123.103 | 123.104 | 404.4443 | 404.4444 | 404.4445 | 123.4567 | 123.4568 | 123.4569 | 346 | 34567892.1 | 0 | 1 | 1 | 2020-07-17 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22 | 14:38:07 | 21:49:13.123 | text | EA== | EA== | EA== | EA== | 2021 | red | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0} | {\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0} | {\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0} | fine"));
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
    }

    @Test
    public void testComplexDataTypesInBatchMode() throws Exception {
        String databaseName = complexDataTypesDatabase.getDatabaseName();
        String sinkTableName = "data_types_table";
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
                                + "  scan.startup.mode: snapshot\n"
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
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        databaseName,
                        DORIS.getUsername(),
                        DORIS.getPassword(),
                        databaseName,
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path dorisCdcConnector = TestUtils.getResource("doris-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, dorisCdcConnector, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        LOG.info(
                "Verifying snapshot stage of {} mapped from source DATA_TYPES_TABLE...",
                sinkTableName);
        validateSinkSchema(
                databaseName,
                sinkTableName,
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "tiny_c | TINYINT | Yes | false | null",
                        "tiny_un_c | SMALLINT | Yes | false | null",
                        "tiny_un_z_c | SMALLINT | Yes | false | null",
                        "small_c | SMALLINT | Yes | false | null",
                        "small_un_c | INT | Yes | false | null",
                        "small_un_z_c | INT | Yes | false | null",
                        "medium_c | INT | Yes | false | null",
                        "medium_un_c | INT | Yes | false | null",
                        "medium_un_z_c | INT | Yes | false | null",
                        "int_c | INT | Yes | false | null",
                        "int_un_c | BIGINT | Yes | false | null",
                        "int_un_z_c | BIGINT | Yes | false | null",
                        "int11_c | INT | Yes | false | null",
                        "big_c | BIGINT | Yes | false | null",
                        "varchar_c | VARCHAR(765) | Yes | false | null",
                        "char_c | CHAR(9) | Yes | false | null",
                        "real_c | DOUBLE | Yes | false | null",
                        "float_c | FLOAT | Yes | false | null",
                        "float_un_c | FLOAT | Yes | false | null",
                        "float_un_z_c | FLOAT | Yes | false | null",
                        "double_c | DOUBLE | Yes | false | null",
                        "double_un_c | DOUBLE | Yes | false | null",
                        "double_un_z_c | DOUBLE | Yes | false | null",
                        "decimal_c | DECIMAL(8, 4) | Yes | false | null",
                        "decimal_un_c | DECIMAL(8, 4) | Yes | false | null",
                        "decimal_un_z_c | DECIMAL(8, 4) | Yes | false | null",
                        "numeric_c | DECIMAL(6, 0) | Yes | false | null",
                        "big_decimal_c | TEXT | Yes | false | null",
                        "bit1_c | BOOLEAN | Yes | false | null",
                        "tiny1_c | BOOLEAN | Yes | false | null",
                        "boolean_c | BOOLEAN | Yes | false | null",
                        "date_c | DATE | Yes | false | null",
                        "datetime3_c | DATETIME(3) | Yes | false | null",
                        "datetime6_c | DATETIME(6) | Yes | false | null",
                        "timestamp_c | DATETIME | Yes | false | null",
                        "time_c | TEXT | Yes | false | null",
                        "time3_c | TEXT | Yes | false | null",
                        "text_c | TEXT | Yes | false | null",
                        "tiny_blob_c | TEXT | Yes | false | null",
                        "blob_c | TEXT | Yes | false | null",
                        "medium_blob_c | TEXT | Yes | false | null",
                        "long_blob_c | TEXT | Yes | false | null",
                        "year_c | INT | Yes | false | null",
                        "enum_c | TEXT | Yes | false | red",
                        "point_c | TEXT | Yes | false | null",
                        "geometry_c | TEXT | Yes | false | null",
                        "linestring_c | TEXT | Yes | false | null",
                        "polygon_c | TEXT | Yes | false | null",
                        "multipoint_c | TEXT | Yes | false | null",
                        "multiline_c | TEXT | Yes | false | null",
                        "multipolygon_c | TEXT | Yes | false | null",
                        "geometrycollection_c | TEXT | Yes | false | null",
                        "FINE | TEXT | Yes | false | null"));
        validateSinkResult(
                databaseName,
                sinkTableName,
                54,
                Collections.singletonList(
                        "1 | 127 | 255 | 255 | 32767 | 65535 | 65535 | 8388607 | 16777215 | 16777215 | 2147483647 | 4294967295 | 4294967295 | 2147483647 | 9223372036854775807 | Hello World | abc | 123.102 | 123.102 | 123.103 | 123.104 | 404.4443 | 404.4444 | 404.4445 | 123.4567 | 123.4568 | 123.4569 | 346 | 34567892.1 | 0 | 1 | 1 | 2020-07-17 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22.0 | 2020-07-17 18:00:22 | 14:38:07 | 21:49:13.123 | text | EA== | EA== | EA== | EA== | 2021 | red | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0} | {\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0} | {\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0} | {\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0} | {\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0} | fine"));
    }

    @Test
    public void testSchemaEvolution() throws Exception {
        String databaseName = mysqlInventoryDatabase.getDatabaseName();
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
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %d",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        databaseName,
                        DORIS.getUsername(),
                        DORIS.getPassword(),
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path dorisCdcConnector = TestUtils.getResource("doris-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, dorisCdcConnector, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        LOG.info("Verifying snapshot data from `products`...");
        validateSinkSchema(
                databaseName,
                "products",
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "name | VARCHAR(765) | Yes | false | flink",
                        "description | VARCHAR(1536) | Yes | false | null",
                        "weight | FLOAT | Yes | false | null",
                        "enum_c | TEXT | Yes | false | red",
                        "json_c | TEXT | Yes | false | null",
                        "point_c | TEXT | Yes | false | null"));
        validateSinkResult(
                databaseName,
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

        LOG.info("Verifying snapshot data from `customers`...");
        validateSinkSchema(
                databaseName,
                "customers",
                Arrays.asList(
                        "id | INT | Yes | true | null",
                        "name | VARCHAR(765) | Yes | false | flink",
                        "address | VARCHAR(3072) | Yes | false | null",
                        "phone_number | VARCHAR(1536) | Yes | false | null"));
        validateSinkResult(
                databaseName,
                "customers",
                4,
                Arrays.asList(
                        "101 | user_1 | Shanghai | 123567891234",
                        "102 | user_2 | Shanghai | 123567891234",
                        "103 | user_3 | Shanghai | 123567891234",
                        "104 | user_4 | Shanghai | 123567891234"));

        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), databaseName);
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {

            LOG.info("Switching to streaming stage...");
            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null);"); // 110

            // Ensure we've entered binlog reading stage
            validateSinkResult(
                    databaseName,
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
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null"));

            // Schema change - Add Column
            LOG.info("Test Schema Change - Add Column...");
            stat.execute("ALTER TABLE products ADD COLUMN extras INT;");
            validateSinkSchema(
                    databaseName,
                    "products",
                    Arrays.asList(
                            "id | INT | Yes | true | null",
                            "name | VARCHAR(765) | Yes | false | flink",
                            "description | VARCHAR(1536) | Yes | false | null",
                            "weight | FLOAT | Yes | false | null",
                            "enum_c | TEXT | Yes | false | red",
                            "json_c | TEXT | Yes | false | null",
                            "point_c | TEXT | Yes | false | null",
                            "extras | INT | Yes | false | null"));
            stat.execute(
                    "INSERT INTO products VALUES (default, 'blt', 'bacon, lettuce and tomato sandwich', 0.2, null, null, null, 17)"); // 111
            validateSinkResult(
                    databaseName,
                    "products",
                    8,
                    Arrays.asList(
                            "101 | scooter | Small 2-wheel scooter | 3.14 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | null",
                            "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0} | null",
                            "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0} | null",
                            "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0} | null",
                            "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0} | null",
                            "106 | hammer | 16oz carpenter's hammer | 1.0 | null | null | null | null",
                            "107 | rocks | box of assorted rocks | 5.3 | null | null | null | null",
                            "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null | null",
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null | null",
                            "111 | blt | bacon, lettuce and tomato sandwich | 0.2 | null | null | null | 17"));

            // Schema change - Rename Column
            LOG.info("Test Schema Change - Rename Column...");
            stat.execute("ALTER TABLE products RENAME COLUMN extras TO extra_col;");
            validateSinkSchema(
                    databaseName,
                    "products",
                    Arrays.asList(
                            "id | INT | Yes | true | null",
                            "name | VARCHAR(765) | Yes | false | flink",
                            "description | VARCHAR(1536) | Yes | false | null",
                            "weight | FLOAT | Yes | false | null",
                            "enum_c | TEXT | Yes | false | red",
                            "json_c | TEXT | Yes | false | null",
                            "point_c | TEXT | Yes | false | null",
                            "extra_col | INT | Yes | false | null"));
            stat.execute(
                    "INSERT INTO products VALUES (default, 'cheeseburger', 'meat patty, cheese slice and onions', 0.1, null, null, null, 18)"); // 112
            validateSinkResult(
                    databaseName,
                    "products",
                    8,
                    Arrays.asList(
                            "101 | scooter | Small 2-wheel scooter | 3.14 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | null",
                            "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0} | null",
                            "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0} | null",
                            "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0} | null",
                            "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0} | null",
                            "106 | hammer | 16oz carpenter's hammer | 1.0 | null | null | null | null",
                            "107 | rocks | box of assorted rocks | 5.3 | null | null | null | null",
                            "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null | null",
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null | null",
                            "111 | blt | bacon, lettuce and tomato sandwich | 0.2 | null | null | null | 17",
                            "112 | cheeseburger | meat patty, cheese slice and onions | 0.1 | null | null | null | 18"));

            // Schema change - Alter Column Type
            LOG.info("Test Schema Change - Alter Column Type...");
            stat.execute("ALTER TABLE products MODIFY COLUMN extra_col DOUBLE;");
            validateSinkSchema(
                    databaseName,
                    "products",
                    Arrays.asList(
                            "id | INT | Yes | true | null",
                            "name | VARCHAR(765) | Yes | false | flink",
                            "description | VARCHAR(1536) | Yes | false | null",
                            "weight | FLOAT | Yes | false | null",
                            "enum_c | TEXT | Yes | false | red",
                            "json_c | TEXT | Yes | false | null",
                            "point_c | TEXT | Yes | false | null",
                            "extra_col | DOUBLE | Yes | false | null"));
            stat.execute(
                    "INSERT INTO products VALUES (default, 'fries', 'potato and salt', 0.05, null, null, null, 19.5)"); // 113
            validateSinkResult(
                    databaseName,
                    "products",
                    8,
                    Arrays.asList(
                            "101 | scooter | Small 2-wheel scooter | 3.14 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0} | null",
                            "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0} | null",
                            "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0} | null",
                            "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0} | null",
                            "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0} | null",
                            "106 | hammer | 16oz carpenter's hammer | 1.0 | null | null | null | null",
                            "107 | rocks | box of assorted rocks | 5.3 | null | null | null | null",
                            "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null | null",
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null | null",
                            "111 | blt | bacon, lettuce and tomato sandwich | 0.2 | null | null | null | 17.0",
                            "112 | cheeseburger | meat patty, cheese slice and onions | 0.1 | null | null | null | 18.0",
                            "113 | fries | potato and salt | 0.05 | null | null | null | 19.5"));

            // Schema change - Drop Column
            LOG.info("Test Schema Change - Drop Column...");
            stat.execute("ALTER TABLE products DROP COLUMN extra_col;");
            validateSinkSchema(
                    databaseName,
                    "products",
                    Arrays.asList(
                            "id | INT | Yes | true | null",
                            "name | VARCHAR(765) | Yes | false | flink",
                            "description | VARCHAR(1536) | Yes | false | null",
                            "weight | FLOAT | Yes | false | null",
                            "enum_c | TEXT | Yes | false | red",
                            "json_c | TEXT | Yes | false | null",
                            "point_c | TEXT | Yes | false | null"));
            stat.execute(
                    "INSERT INTO products VALUES (default, 'mac', 'cheese', 0.025, null, null, null)"); // 114
            validateSinkResult(
                    databaseName,
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
                            "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null",
                            "110 | jacket | water resistent white wind breaker | 0.2 | null | null | null",
                            "111 | blt | bacon, lettuce and tomato sandwich | 0.2 | null | null | null",
                            "112 | cheeseburger | meat patty, cheese slice and onions | 0.1 | null | null | null",
                            "113 | fries | potato and salt | 0.05 | null | null | null",
                            "114 | mac | cheese | 0.025 | null | null | null"));

            stat.execute("TRUNCATE TABLE products;");
            Thread.sleep(5000L);
            stat.execute(
                    "INSERT INTO products VALUES (default, 'pasta', 'noodles', 0, null, null, null);"); // 1, because truncating resets auto_increment id

            validateSinkResult(
                    databaseName,
                    "products",
                    7,
                    Collections.singletonList("1 | pasta | noodles | 0.0 | null | null | null"));

            stat.execute("DROP TABLE products;");
            Thread.sleep(5000L);
            Assertions.assertThatThrownBy(
                            () -> {
                                try (Connection connection =
                                                DriverManager.getConnection(
                                                        DORIS.getJdbcUrl(
                                                                databaseName,
                                                                DORIS.getUsername()));
                                        Statement statement = connection.createStatement()) {
                                    statement.executeQuery("SELECT * FROM products;");
                                }
                            })
                    .isExactlyInstanceOf(SQLSyntaxErrorException.class)
                    .hasMessageContaining("errCode = 2, detailMessage = Unknown table 'products'");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to trigger schema change.", e);
        }
    }

    public static void createDorisDatabase(String databaseName) {
        try {
            org.testcontainers.containers.Container.ExecResult rs =
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
            org.testcontainers.containers.Container.ExecResult rs =
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
        waitAndVerify(
                databaseName,
                "SELECT * FROM " + tableName,
                columnCount,
                expected,
                EVENT_WAITING_TIMEOUT.toMillis(),
                true);
    }

    private void validateSinkSchema(String databaseName, String tableName, List<String> expected)
            throws Exception {
        waitAndVerify(
                databaseName,
                "DESCRIBE " + tableName,
                5,
                expected,
                EVENT_WAITING_TIMEOUT.toMillis(),
                false);
    }

    private void waitAndVerify(
            String databaseName,
            String sql,
            int numberOfColumns,
            List<String> expected,
            long timeoutMilliseconds,
            boolean inAnyOrder)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMilliseconds;
        SQLSyntaxErrorException lastSqlSyntaxException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                List<String> actual = fetchTableContent(databaseName, sql, numberOfColumns);
                if (inAnyOrder) {
                    if (expected.stream()
                            .sorted()
                            .collect(Collectors.toList())
                            .equals(actual.stream().sorted().collect(Collectors.toList()))) {
                        return;
                    }
                } else {
                    if (expected.equals(actual)) {
                        return;
                    }
                }
                LOG.info(
                        "Executing {}::{} didn't get expected results.\nExpected: {}\n  Actual: {}",
                        databaseName,
                        sql,
                        expected,
                        actual);
            } catch (SQLSyntaxErrorException t) {
                lastSqlSyntaxException = t;
                LOG.info("Database {} isn't ready yet. Waiting for the next loop...", databaseName);
            }
            Thread.sleep(1000L);
        }
        logVerificationDiagnostics(databaseName, sql, lastSqlSyntaxException);
        AssertionError assertionError =
                new AssertionError(
                        String.format("Failed to verify content of %s::%s.", databaseName, sql));
        if (lastSqlSyntaxException != null) {
            assertionError.initCause(lastSqlSyntaxException);
        }
        throw assertionError;
    }

    private void logVerificationDiagnostics(
            String databaseName, String sql, SQLSyntaxErrorException sqlSyntaxException) {
        if (sqlSyntaxException != null) {
            LOG.error(
                    "Verification timeout for {}::{} with the latest SQL syntax exception.",
                    databaseName,
                    sql,
                    sqlSyntaxException);
        } else {
            LOG.error("Verification timeout for {}::{}.", databaseName, sql);
        }
        logCurrentJobStatuses();
        logContainerDiagnostics("JobManager", jobManagerConsumer.toUtf8String());
        logContainerDiagnostics("TaskManager", taskManagerConsumer.toUtf8String());
    }

    private void logCurrentJobStatuses() {
        try {
            List<JobStatusMessage> jobs = new ArrayList<>(getRestClusterClient().listJobs().get());
            jobs.sort(Comparator.comparing(JobStatusMessage::getStartTime));
            if (jobs.isEmpty()) {
                LOG.error("No Flink jobs are visible from the REST client.");
                return;
            }
            for (JobStatusMessage job : jobs) {
                LOG.error(
                        "Flink job status: name={}, id={}, state={}",
                        job.getJobName(),
                        job.getJobId(),
                        job.getJobState());
            }
        } catch (Exception e) {
            LOG.error("Failed to fetch Flink job statuses for diagnostics.", e);
        }
    }

    private void logContainerDiagnostics(String component, String rawLogs) {
        LOG.error(
                "{} focused diagnostic log:\n{}",
                component,
                summarizeFocusedDiagnosticLogs(rawLogs));
        LOG.error("{} diagnostic log tail:\n{}", component, summarizeDiagnosticLogs(rawLogs));
    }

    private String summarizeFocusedDiagnosticLogs(String rawLogs) {
        if (rawLogs == null || rawLogs.isEmpty()) {
            return "<empty>";
        }

        List<String> focusedLines =
                Arrays.stream(rawLogs.split("\\R"))
                        .filter(this::isFocusedDiagnosticLine)
                        .collect(Collectors.toList());
        if (focusedLines.isEmpty()) {
            return "<empty>";
        }
        return String.join("\n", focusedLines);
    }

    private String summarizeDiagnosticLogs(String rawLogs) {
        if (rawLogs == null || rawLogs.isEmpty()) {
            return "<empty>";
        }
        List<String> lines = Arrays.asList(rawLogs.split("\\R"));
        List<String> diagnosticLines =
                lines.stream().filter(this::isDiagnosticLine).collect(Collectors.toList());
        List<String> source = diagnosticLines.isEmpty() ? lines : diagnosticLines;
        int fromIndex = Math.max(0, source.size() - DIAGNOSTIC_LOG_TAIL_LINES);
        return String.join("\n", source.subList(fromIndex, source.size()));
    }

    private boolean isDiagnosticLine(String line) {
        String normalized = line.toLowerCase(Locale.ROOT);
        return normalized.contains("error")
                || normalized.contains("exception")
                || normalized.contains("failed")
                || normalized.contains("caused by")
                || normalized.contains("schema")
                || normalized.contains("stream load")
                || normalized.contains("create table")
                || normalized.contains("data_types_table")
                || normalized.contains("doris");
    }

    private boolean isFocusedDiagnosticLine(String line) {
        String normalized = line.toLowerCase(Locale.ROOT);
        return normalized.contains("cdc-doris-diag")
                || normalized.contains("create table")
                || normalized.contains("schemachangerequest")
                || normalized.contains("finished schema change events")
                || normalized.contains("sending the flushevent")
                || normalized.contains("going to request schema change")
                || normalized.contains("successfully applied schema change event")
                || normalized.contains("execute sql:")
                || normalized.contains("stream load started")
                || normalized.contains("load success")
                || normalized.contains("unknown table");
    }

    private void submitMysqlToDorisJob(String pipelineJob) throws Exception {
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path dorisCdcConnector = TestUtils.getResource("doris-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, dorisCdcConnector, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
    }

    private void insertColumnCaseIncrementalRows(String databaseName) throws SQLException {
        try (Connection conn =
                        DriverManager.getConnection(
                                String.format(
                                        "jdbc:mysql://%s:%s/%s",
                                        MYSQL.getHost(), MYSQL.getDatabasePort(), databaseName),
                                MYSQL_TEST_USER,
                                MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("INSERT INTO mixed_case_customer VALUES (103, 'Cindy', '13900000007');");
            stat.execute("INSERT INTO upper_case_customer VALUES (203, 'Eric', '13900000008');");
            stat.execute("INSERT INTO lower_case_customer VALUES (303, 'Gina', '13900000009');");
        }
    }

    private void insertProjectionIncrementalRow(String databaseName) throws SQLException {
        try (Connection conn =
                        DriverManager.getConnection(
                                String.format(
                                        "jdbc:mysql://%s:%s/%s",
                                        MYSQL.getHost(), MYSQL.getDatabasePort(), databaseName),
                                MYSQL_TEST_USER,
                                MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("INSERT INTO customer VALUES (403, 'Ivan', 20, 'Hangzhou');");
        }
    }

    private void updateAndDeleteColumnCaseRows(String databaseName) throws SQLException {
        try (Connection conn =
                        DriverManager.getConnection(
                                String.format(
                                        "jdbc:mysql://%s:%s/%s",
                                        MYSQL.getHost(), MYSQL.getDatabasePort(), databaseName),
                                MYSQL_TEST_USER,
                                MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "UPDATE mixed_case_customer SET `Name` = 'Alice-Updated', `phone_Number` = '13900001001' WHERE `ID` = 101;");
            stat.execute("DELETE FROM mixed_case_customer WHERE `ID` = 102;");
            stat.execute(
                    "UPDATE upper_case_customer SET `NAME` = 'Carol-Updated', `PHONE_NUMBER` = '13900001002' WHERE `ID` = 201;");
            stat.execute("DELETE FROM upper_case_customer WHERE `ID` = 202;");
            stat.execute(
                    "UPDATE lower_case_customer SET `name` = 'Eve-Updated', `phone_number` = '13900001003' WHERE `id` = 301;");
            stat.execute("DELETE FROM lower_case_customer WHERE `id` = 302;");
        }
    }

    private List<String> fetchTableContent(String databaseName, String sql, int columnCount)
            throws Exception {

        List<String> results = new ArrayList<>();
        try (Connection conn =
                        DriverManager.getConnection(
                                DORIS.getJdbcUrl(databaseName, DORIS.getUsername()));
                Statement stat = conn.createStatement()) {
            ResultSet rs = stat.executeQuery(sql);

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
        return results;
    }
}
