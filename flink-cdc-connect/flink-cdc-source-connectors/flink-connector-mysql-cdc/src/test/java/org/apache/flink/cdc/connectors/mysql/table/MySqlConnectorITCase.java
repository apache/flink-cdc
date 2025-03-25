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

package org.apache.flink.cdc.connectors.mysql.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.cdc.connectors.mysql.LegacyMySqlSourceTest.currentMySqlLatestOffset;
import static org.apache.flink.cdc.connectors.mysql.MySqlTestUtils.waitForJobStatus;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Integration tests for MySQL Table source. */
class MySqlConnectorITCase extends MySqlSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlConnectorITCase.class);

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    private static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-gtids/expire-seconds/my.cnf");

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", TEST_USER, TEST_PASSWORD);

    private final UniqueDatabase fullTypesMySql57Database =
            new UniqueDatabase(MYSQL_CONTAINER, "column_type_test", TEST_USER, TEST_PASSWORD);
    private final UniqueDatabase fullTypesMySql8Database =
            new UniqueDatabase(
                    MYSQL8_CONTAINER, "column_type_test_mysql8", TEST_USER, TEST_PASSWORD);

    private final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", TEST_USER, TEST_PASSWORD);
    private static final UniqueDatabase customer3_0Database =
            new UniqueDatabase(MYSQL_CONTAINER, "customer3.0", TEST_USER, TEST_PASSWORD);

    private final UniqueDatabase userDatabase1 =
            new UniqueDatabase(MYSQL_CONTAINER, "user_1", TEST_USER, TEST_PASSWORD);
    private final UniqueDatabase userDatabase2 =
            new UniqueDatabase(MYSQL_CONTAINER, "user_2", TEST_USER, TEST_PASSWORD);

    private final UniqueDatabase inventoryDatabase8 =
            new UniqueDatabase(MYSQL8_CONTAINER, "inventory", TEST_USER, TEST_PASSWORD);

    private final UniqueDatabase binlogDatabase =
            new UniqueDatabase(MYSQL8_CONTAINER, "binlog_metadata_test", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @BeforeAll
    public static void beforeClass() {
        LOG.info("Starting MySql8 containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        LOG.info("Container MySql8 is started.");
    }

    @AfterAll
    public static void afterClass() {
        LOG.info("Stopping MySql8 containers...");
        MYSQL8_CONTAINER.stop();
        LOG.info("Container MySql8 is stopped.");
    }

    void setup(boolean incrementalSnapshot) {
        TestValuesTableFactory.clearAllData();
        if (incrementalSnapshot) {
            env.setParallelism(DEFAULT_PARALLELISM);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testConsumingAllEvents(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        runConsumingAllEventsTest("", incrementalSnapshot);
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testConsumingAllEventsUseSSL(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        runConsumingAllEventsTest(
                ", 'jdbc.properties.useSSL'= 'true',"
                        + " 'jdbc.properties.requireSSL'= 'true',"
                        + " 'jdbc.properties.verifyServerCerticate'= 'false'",
                incrementalSnapshot);
    }

    private void runConsumingAllEventsTest(String otherTableOptions, boolean incrementalSnapshot)
            throws Exception {
        setup(incrementalSnapshot);
        inventoryDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + " %s"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot),
                        otherTableOptions);
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " name STRING,"
                        + " weightSum DECIMAL(10,3),"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT name, SUM(weight) FROM debezium_source GROUP BY name");

        waitForSnapshotStarted("sink");

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }

        waitForSinkSize("sink", 20);

        /*
         * <pre>
         * The final database table looks like this:
         *
         * > SELECT * FROM products;
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | id  | name               | description                                             | weight |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | 101 | scooter            | Small 2-wheel scooter                                   |   3.14 |
         * | 102 | car battery        | 12V car battery                                         |    8.1 |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |    0.8 |
         * | 104 | hammer             | 12oz carpenter's hammer                                 |   0.75 |
         * | 105 | hammer             | 14oz carpenter's hammer                                 |  0.875 |
         * | 106 | hammer             | 18oz carpenter hammer                                   |      1 |
         * | 107 | rocks              | box of assorted rocks                                   |    5.1 |
         * | 108 | jacket             | water resistent black wind breaker                      |    0.1 |
         * | 109 | spare tire         | 24 inch spare tire                                      |   22.2 |
         * | 110 | jacket             | new water resistent white wind breaker                  |    0.5 |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * </pre>
         */

        String[] expected =
                new String[] {
                    "+I[scooter, 3.140]",
                    "+I[car battery, 8.100]",
                    "+I[12-pack drill bits, 0.800]",
                    "+I[hammer, 2.625]",
                    "+I[rocks, 5.100]",
                    "+I[jacket, 0.600]",
                    "+I[spare tire, 22.200]"
                };

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testNoPKTableWithChunkKey(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        runConsumingForNoPKTableTest(
                ", 'scan.incremental.snapshot.chunk.key-column'='type'", incrementalSnapshot);
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testNoPKTableWithoutChunkKey(boolean incrementalSnapshot) {
        setup(incrementalSnapshot);
        Assertions.assertThatThrownBy(() -> runConsumingForNoPKTableTest("", incrementalSnapshot))
                .isExactlyInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "'scan.incremental.snapshot.chunk.key-column' is required for table without primary key when 'scan.incremental.snapshot.enabled' enabled.");
    }

    // This test always enable the incrementalSnapshot
    private void runConsumingForNoPKTableTest(String otherTableOptions, boolean incrementalSnapshot)
            throws Exception {
        inventoryDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " `type` INT,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = 'products_no_pk',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '2'"
                                + " %s"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        getServerId(incrementalSnapshot),
                        otherTableOptions);
        // If there is multi parallelism, the sink must have primary keys
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " `type` INT,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(10,3)"
                        + (incrementalSnapshot ? ", PRIMARY KEY (`type`) NOT ENFORCED" : "")
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        // wait until the snapshot phase finished
        waitForSinkSize("sink", 11);

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE products_no_pk SET description='18oz carpenter hammer' WHERE type=103;");
            statement.execute("UPDATE products_no_pk SET weight='5.1' WHERE type=106;");
            statement.execute(
                    "INSERT INTO products_no_pk VALUES (110,'jacket','water resistent white wind breaker',0.2);");
            statement.execute("DELETE FROM products_no_pk WHERE type=102;");
            statement.execute(
                    "INSERT INTO products_no_pk VALUES (111,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products_no_pk SET description='new water resistent white wind breaker', weight='0.5' WHERE type=110;");
            statement.execute("UPDATE products_no_pk SET weight='5.17' WHERE type=111;");
            statement.execute("DELETE FROM products_no_pk WHERE type=111;");
        }

        waitForSinkSize("sink", incrementalSnapshot ? 25 : 29);

        /*
         * <pre>
         * The final database table looks like this:
         *
         * > SELECT * FROM products_no_pk;
         * +------+--------------------+---------------------------------------------------------+--------+
         * | type | name               | description                                             | weight |
         * +------+--------------------+---------------------------------------------------------+--------+
         * | 100  | scooter            | Small 2-wheel scooter                                   |   3.14 |
         * | 101  | car battery        | 12V car battery                                         |    8.1 |
         * | 103  | hammer             | 18oz carpenter hammer                                   |   0.75 |
         * | 103  | hammer             | 18oz carpenter hammer                                   |  0.875 |
         * | 103  | hammer             | 18oz carpenter hammer                                   |      1 |
         * | 104  | rocks              | box of assorted rocks                                   |    5.1 |
         * | 104  | rocks              | box of assorted rocks                                   |    5.1 |
         * | 104  | rocks              | box of assorted rocks                                   |    5.1 |
         * | 105  | jacket             | water resistent black wind breaker                      |    0.1 |
         * | 106  | spare tire         | 24 inch spare tire                                      |    5.1 |
         * | 110  | jacket             | new water resistent white wind breaker                  |    0.5 |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * </pre>
         */

        String[] expected =
                incrementalSnapshot
                        ? new String[] {
                            "+I[100, scooter, Small 2-wheel scooter, 3.140]",
                            "+I[101, car battery, 12V car battery, 8.100]",
                            "+I[103, hammer, 18oz carpenter hammer, 1.000]",
                            "+I[104, rocks, box of assorted rocks, 5.300]",
                            "+I[105, jacket, water resistent black wind breaker, 0.100]",
                            "+I[106, spare tire, 24 inch spare tire, 5.100]",
                            "+I[110, jacket, new water resistent white wind breaker, 0.500]"
                        }
                        : new String[] {
                            "+I[100, scooter, Small 2-wheel scooter, 3.140]",
                            "+I[101, car battery, 12V car battery, 8.100]",
                            "+I[103, hammer, 18oz carpenter hammer, 0.750]",
                            "+I[103, hammer, 18oz carpenter hammer, 0.875]",
                            "+I[103, hammer, 18oz carpenter hammer, 1.000]",
                            "+I[104, rocks, box of assorted rocks, 5.300]",
                            "+I[104, rocks, box of assorted rocks, 5.300]",
                            "+I[104, rocks, box of assorted rocks, 5.300]",
                            "+I[105, jacket, water resistent black wind breaker, 0.100]",
                            "+I[106, spare tire, 24 inch spare tire, 5.100]",
                            "+I[110, jacket, new water resistent white wind breaker, 0.500]"
                        };

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testCheckpointIsOptionalUnderSingleParallelism(boolean incrementalSnapshot)
            throws Exception {
        setup(incrementalSnapshot);
        assumeThat(incrementalSnapshot).isTrue();
        env.setParallelism(1);
        // check the checkpoint is optional when parallelism is 1
        env.getCheckpointConfig().disableCheckpointing();
        inventoryDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM debezium_source");
        CloseableIterator<Row> iterator = result.collect();
        String[] expectedSnapshot =
                new String[] {
                    "+I[101, scooter, Small 2-wheel scooter, 3.140]",
                    "+I[102, car battery, 12V car battery, 8.100]",
                    "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800]",
                    "+I[104, hammer, 12oz carpenter's hammer, 0.750]",
                    "+I[105, hammer, 14oz carpenter's hammer, 0.875]",
                    "+I[106, hammer, 16oz carpenter's hammer, 1.000]",
                    "+I[107, rocks, box of assorted rocks, 5.300]",
                    "+I[108, jacket, water resistent black wind breaker, 0.100]",
                    "+I[109, spare tire, 24 inch spare tire, 22.200]"
                };
        assertEqualsInAnyOrder(
                Arrays.asList(expectedSnapshot), fetchRows(iterator, expectedSnapshot.length));

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }

        String[] expectedBinlog =
                new String[] {
                    "+I[110, jacket, water resistent white wind breaker, 0.200]",
                    "+I[111, scooter, Big 2-wheel scooter , 5.180]",
                    "-U[110, jacket, water resistent white wind breaker, 0.200]",
                    "+U[110, jacket, new water resistent white wind breaker, 0.500]",
                    "-U[111, scooter, Big 2-wheel scooter , 5.180]",
                    "+U[111, scooter, Big 2-wheel scooter , 5.170]",
                    "-D[111, scooter, Big 2-wheel scooter , 5.170]"
                };
        assertEqualsInOrder(
                Arrays.asList(expectedBinlog), fetchRows(iterator, expectedBinlog.length));
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testMysql57AllDataTypes(boolean incrementalSnapshot) throws Throwable {
        setup(incrementalSnapshot);
        testAllDataTypes(MYSQL_CONTAINER, fullTypesMySql57Database, incrementalSnapshot);
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testMySql8AllDataTypes(boolean incrementalSnapshot) throws Throwable {
        setup(incrementalSnapshot);
        testAllDataTypes(MYSQL8_CONTAINER, fullTypesMySql8Database, incrementalSnapshot);
    }

    void testAllDataTypes(
            MySqlContainer mySqlContainer, UniqueDatabase database, boolean incrementalSnapshot)
            throws Throwable {
        database.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types (\n"
                                + "    `id` INT NOT NULL,\n"
                                + "    tiny_c TINYINT,\n"
                                + "    tiny_un_c SMALLINT ,\n"
                                + "    tiny_un_z_c SMALLINT ,\n"
                                + "    small_c SMALLINT,\n"
                                + "    small_un_c INT,\n"
                                + "    small_un_z_c INT,\n"
                                + "    medium_c INT,\n"
                                + "    medium_un_c INT,\n"
                                + "    medium_un_z_c BIGINT,\n"
                                + "    int_c INT ,\n"
                                + "    int_un_c BIGINT,\n"
                                + "    int_un_z_c BIGINT,\n"
                                + "    int11_c BIGINT,\n"
                                + "    big_c BIGINT,\n"
                                + "    big_un_c DECIMAL(20, 0),\n"
                                + "    big_un_z_c DECIMAL(20, 0),\n"
                                + "    varchar_c VARCHAR(255),\n"
                                + "    char_c CHAR(3),\n"
                                + "    real_c FLOAT,\n"
                                + "    float_c FLOAT,\n"
                                + "    float_un_c FLOAT,\n"
                                + "    float_un_z_c FLOAT,\n"
                                + "    double_c DOUBLE,\n"
                                + "    double_un_c DOUBLE,\n"
                                + "    double_un_z_c DOUBLE,\n"
                                + "    decimal_c DECIMAL(8, 4),\n"
                                + "    decimal_un_c DECIMAL(8, 4),\n"
                                + "    decimal_un_z_c DECIMAL(8, 4),\n"
                                + "    numeric_c DECIMAL(6, 0),\n"
                                + "    big_decimal_c STRING,\n"
                                + "    bit1_c BOOLEAN,\n"
                                + "    tiny1_c BOOLEAN,\n"
                                + "    boolean_c BOOLEAN,\n"
                                + "    date_c DATE,\n"
                                + "    time_c TIME(0),\n"
                                + "    datetime3_c TIMESTAMP(3),\n"
                                + "    datetime6_c TIMESTAMP(6),\n"
                                + "    timestamp_c TIMESTAMP(0),\n"
                                + "    file_uuid BYTES,\n"
                                + "    bit_c BINARY(8),\n"
                                + "    text_c STRING,\n"
                                + "    tiny_blob_c BYTES,\n"
                                + "    blob_c BYTES,\n"
                                + "    medium_blob_c BYTES,\n"
                                + "    long_blob_c BYTES,\n"
                                + "    year_c INT,\n"
                                + "    enum_c STRING,\n"
                                + "    set_c ARRAY<STRING>,\n"
                                + "    json_c STRING,\n"
                                + "    point_c STRING,\n"
                                + "    geometry_c STRING,\n"
                                + "    linestring_c STRING,\n"
                                + "    polygon_c STRING,\n"
                                + "    multipoint_c STRING,\n"
                                + "    multiline_c STRING,\n"
                                + "    multipolygon_c STRING,\n"
                                + "    geometrycollection_c STRING,\n"
                                + "    primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        mySqlContainer.getHost(),
                        mySqlContainer.getDatabasePort(),
                        database.getUsername(),
                        database.getPassword(),
                        database.getDatabaseName(),
                        "full_types",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "SELECT id,\n"
                                + "tiny_c,\n"
                                + "tiny_un_c,\n"
                                + "tiny_un_z_c,\n"
                                + "small_c,\n"
                                + "small_un_c,\n"
                                + "small_un_z_c,\n"
                                + "medium_c, \n"
                                + "medium_un_c, \n"
                                + "medium_un_z_c, \n"
                                + "int_c,\n"
                                + "int_un_c,\n"
                                + "int_un_z_c,\n"
                                + "int11_c,\n"
                                + "big_c,\n"
                                + "big_un_c, \n"
                                + "big_un_z_c, \n"
                                + "varchar_c,\n"
                                + "char_c,\n"
                                + "real_c, \n"
                                + "float_c,\n"
                                + "float_un_c,\n"
                                + "float_un_z_c,\n"
                                + "double_c,\n"
                                + "double_un_c,\n"
                                + "double_un_z_c,\n"
                                + "decimal_c,\n"
                                + "decimal_un_c,\n"
                                + "decimal_un_z_c,\n"
                                + "numeric_c,\n"
                                + "big_decimal_c,\n"
                                + "bit1_c,\n"
                                + "tiny1_c,\n"
                                + "boolean_c,\n"
                                + "date_c,\n"
                                + "time_c,\n"
                                + "datetime3_c,\n"
                                + "datetime6_c,\n"
                                + "timestamp_c,\n"
                                + "TO_BASE64(DECODE(file_uuid, 'UTF-8')),\n"
                                + "bit_c,\n"
                                + "text_c,\n"
                                + "tiny_blob_c,\n"
                                + "blob_c,\n"
                                + "medium_blob_c,\n"
                                + "long_blob_c,\n"
                                + "year_c,\n"
                                + "enum_c,\n"
                                + "set_c,\n"
                                + "json_c, \n"
                                + "point_c, \n"
                                + "geometry_c, \n"
                                + "linestring_c, \n"
                                + "polygon_c, \n"
                                + "multipoint_c, \n"
                                + "multiline_c, \n"
                                + "multipolygon_c, \n"
                                + "geometrycollection_c \n"
                                + " FROM full_types");

        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }
        String expectedPointJsonText = "{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}";
        String expectedGeometryJsonText =
                "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}";
        String expectLinestringJsonText =
                "{\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0}";
        String expectPolygonJsonText =
                "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}";
        String expectMultipointJsonText =
                "{\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0}";
        String expectMultilineJsonText =
                "{\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0}";
        String expectMultipolygonJsonText =
                "{\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0}";
        String expectGeometryCollectionJsonText =
                "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0}";
        String[] expected =
                new String[] {
                    "+I[1, 127, 255, 255, 32767, 65535, 65535, 8388607, 16777215, 16777215, 2147483647, 4294967295, 4294967295,"
                            + " 2147483647, 9223372036854775807, 18446744073709551615, 18446744073709551615, Hello World, abc, 123.102, 123.102, 123.103, 123.104, "
                            + "404.4443, 404.4444, 404.4445, 123.4567, 123.4568, 123.4569, 346, 34567892.1, false, true, true,"
                            + " 2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:00:22, "
                            + "ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=, [4, 4, 4, 4, 4, 4, 4, 4], text, [16], [16], [16], [16], 2021, red, [a, b], "
                            + "{\"key1\": \"value1\"}, "
                            + expectedPointJsonText
                            + ", "
                            + expectedGeometryJsonText
                            + ", "
                            + expectLinestringJsonText
                            + ", "
                            + expectPolygonJsonText
                            + ", "
                            + expectMultipointJsonText
                            + ", "
                            + expectMultilineJsonText
                            + ", "
                            + expectMultipolygonJsonText
                            + ", "
                            + expectGeometryCollectionJsonText
                            + "]",
                    "-U[1, 127, 255, 255, 32767, 65535, 65535, 8388607, 16777215, 16777215, 2147483647, 4294967295, 4294967295,"
                            + " 2147483647, 9223372036854775807, 18446744073709551615, 18446744073709551615, Hello World, abc, 123.102,"
                            + " 123.102, 123.103, 123.104, 404.4443, 404.4444, 404.4445, 123.4567, 123.4568, 123.4569, 346, 34567892.1,"
                            + " false, true, true,"
                            + " 2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:00:22, "
                            + "ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=, [4, 4, 4, 4, 4, 4, 4, 4], text, [16], [16], [16], [16], 2021, red, [a, b], "
                            + "{\"key1\":\"value1\"}, "
                            + expectedPointJsonText
                            + ", "
                            + expectedGeometryJsonText
                            + ", "
                            + expectLinestringJsonText
                            + ", "
                            + expectPolygonJsonText
                            + ", "
                            + expectMultipointJsonText
                            + ", "
                            + expectMultilineJsonText
                            + ", "
                            + expectMultipolygonJsonText
                            + ", "
                            + expectGeometryCollectionJsonText
                            + "]",
                    "+U[1, 127, 255, 255, 32767, 65535, 65535, 8388607, 16777215, 16777215, 2147483647, 4294967295, 4294967295,"
                            + " 2147483647, 9223372036854775807, 18446744073709551615, 18446744073709551615, Hello World, abc, 123.102,"
                            + " 123.102, 123.103, 123.104, 404.4443, 404.4444, 404.4445, 123.4567, 123.4568, 123.4569, 346, 34567892.1,"
                            + " false, true, true,"
                            + " 2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:33:22, "
                            + "ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=, [4, 4, 4, 4, 4, 4, 4, 4], text, [16], [16], [16], [16], 2021, red, [a, b], "
                            + "{\"key1\":\"value1\"}, "
                            + expectedPointJsonText
                            + ", "
                            + expectedGeometryJsonText
                            + ", "
                            + expectLinestringJsonText
                            + ", "
                            + expectPolygonJsonText
                            + ", "
                            + expectMultipointJsonText
                            + ", "
                            + expectMultilineJsonText
                            + ", "
                            + expectMultipolygonJsonText
                            + ", "
                            + expectGeometryCollectionJsonText
                            + "]",
                };

        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testWideTable(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        final int tableColumnCount = 500;
        fullTypesMySql57Database.createAndInitialize();
        try (Connection connection = fullTypesMySql57Database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("USE %s", fullTypesMySql57Database.getDatabaseName()));
            statement.execute(
                    "CREATE TABLE wide_table("
                            + buildColumnsDDL("col", 0, tableColumnCount, "BIGINT")
                            + " PRIMARY KEY (col0) "
                            + ")");
            statement.execute(
                    "INSERT INTO wide_table values("
                            + getIntegerSeqString(0, tableColumnCount)
                            + ")");
        }

        String sourceDDL =
                String.format(
                        "CREATE TABLE wide_table (\n"
                                + buildColumnsDDL("col", 0, tableColumnCount, "BIGINT")
                                + "    primary key (`col0`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        fullTypesMySql57Database.getUsername(),
                        fullTypesMySql57Database.getPassword(),
                        fullTypesMySql57Database.getDatabaseName(),
                        "wide_table",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM wide_table");

        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);

        try (Connection connection = fullTypesMySql57Database.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute("UPDATE wide_table SET col1 = 1024 WHERE col0=0;");
        }

        String[] expected =
                new String[] {
                    "+I[0, 1, " + getIntegerSeqString(2, tableColumnCount) + "]",
                    "-U[0, 1, " + getIntegerSeqString(2, tableColumnCount) + "]",
                    "+U[0, 1024, " + getIntegerSeqString(2, tableColumnCount) + "]"
                };

        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testBigTableWithHugeSplits(boolean incrementalSnapshot) throws Exception {
        // only check when incremental snapshot is enabled
        Assumptions.assumeThat(incrementalSnapshot).isTrue();
        setup(incrementalSnapshot);
        final int tableRowNumber = 10;
        fullTypesMySql57Database.createAndInitialize();
        try (Connection connection = fullTypesMySql57Database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("USE %s", fullTypesMySql57Database.getDatabaseName()));
            statement.execute(
                    "CREATE TABLE big_table1(id BIGINT, str VARCHAR(100), PRIMARY KEY (id))");
            statement.execute(
                    "CREATE TABLE big_table2(id BIGINT, str VARCHAR(100), PRIMARY KEY (id))");

            for (int i = 0; i < tableRowNumber; i++) {
                statement.execute("INSERT INTO big_table1 values(" + i + "," + (i + 100000) + ")");
                statement.execute("INSERT INTO big_table2 values(" + i + "," + (i + 200000) + ")");
            }
        }

        String sourceDDL =
                String.format(
                        "CREATE TABLE big_table (\n"
                                + "    id BIGINT,"
                                + "    str STRING,"
                                + "    primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = 'big_table.*',"
                                + " 'scan.incremental.snapshot.chunk.size' = '2',"
                                + " 'chunk-meta.group.size' = '3',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        fullTypesMySql57Database.getUsername(),
                        fullTypesMySql57Database.getPassword(),
                        fullTypesMySql57Database.getDatabaseName(),
                        getServerId(incrementalSnapshot));
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " `id` BIGINT NOT NULL,"
                        + " str STRING,"
                        + " primary key (`id`) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM big_table");

        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", tableRowNumber * 2);

        try (Connection connection = fullTypesMySql57Database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE big_table1 SET str = '1024' WHERE id=0;");
            statement.execute("UPDATE big_table1 SET str = '1025' WHERE id=1;");
            statement.execute("UPDATE big_table2 SET str = '2048' WHERE id=2;");
            statement.execute("UPDATE big_table2 SET str = '2049' WHERE id=3;");
        }
        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", tableRowNumber * 2 + 4);

        List<String> expected = new ArrayList<>();
        // snapshot result after upsert into the sink
        for (int i = 0; i < tableRowNumber; i++) {
            expected.add("+I[" + i + ", " + (i + 100000) + "]");
            expected.add("+I[" + i + ", " + (i + 200000) + "]");
        }
        // binlog result after upsert into the sink
        expected.addAll(
                Lists.newArrayList("+U[0, 1024]", "+U[1, 1025]", "+U[2, 2048]", "+U[3, 2049]"));

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testMetadataColumns(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        userDatabase1.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE mysql_users ("
                                + " db_name STRING METADATA FROM 'database_name' VIRTUAL,"
                                + " table_name STRING METADATA VIRTUAL,"
                                + " row_kind STRING METADATA FROM 'row_kind' VIRTUAL,"
                                + " `id` DECIMAL(20, 0) NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " email STRING,"
                                + " age INT,"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        userDatabase1.getUsername(),
                        userDatabase1.getPassword(),
                        userDatabase1.getDatabaseName(),
                        "user_table_.*",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " database_name STRING,"
                        + " table_name STRING,"
                        + " row_kind STRING,"
                        + " `id` DECIMAL(20, 0) NOT NULL,"
                        + " name STRING,"
                        + " address STRING,"
                        + " phone_number STRING,"
                        + " email STRING,"
                        + " age INT,"
                        + " primary key (database_name, table_name, id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM mysql_users");

        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", 2);

        try (Connection connection = userDatabase1.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO user_table_1_2 VALUES (200,'user_200','Wuhan',123567891234);");
            statement.execute(
                    "INSERT INTO user_table_1_1 VALUES (300,'user_300','Hangzhou',123567891234, 'user_300@foo.com');");
            statement.execute("UPDATE user_table_1_1 SET address='Beijing' WHERE id=300;");
            statement.execute("UPDATE user_table_1_2 SET phone_number=88888888 WHERE id=121;");
            statement.execute("DELETE FROM user_table_1_1 WHERE id=111;");
        }

        // waiting for binlog finished (5 more events)
        waitForSinkSize("sink", 7);

        List<String> expected =
                Stream.of(
                                "+I[%s, user_table_1_1, +I, 111, user_111, Shanghai, 123567891234, user_111@foo.com, null]",
                                "+I[%s, user_table_1_2, +I, 121, user_121, Shanghai, 123567891234, null, null]",
                                "+I[%s, user_table_1_2, +I, 200, user_200, Wuhan, 123567891234, null, null]",
                                "+I[%s, user_table_1_1, +I, 300, user_300, Hangzhou, 123567891234, user_300@foo.com, null]",
                                "+U[%s, user_table_1_1, +U, 300, user_300, Beijing, 123567891234, user_300@foo.com, null]",
                                "+U[%s, user_table_1_2, +U, 121, user_121, Shanghai, 88888888, null, null]",
                                "-D[%s, user_table_1_1, -D, 111, user_111, Shanghai, 123567891234, user_111@foo.com, null]",
                                "-U[%s, user_table_1_1, -U, 300, user_300, Hangzhou, 123567891234, user_300@foo.com, null]",
                                "-U[%s, user_table_1_2, -U, 121, user_121, Shanghai, 123567891234, null, null]")
                        .map(s -> String.format(s, userDatabase1.getDatabaseName()))
                        .sorted()
                        .collect(Collectors.toList());

        // TODO: we can't assert merged result for incremental-snapshot, because we can't add a
        //  keyby shuffle before "values" upsert sink. We should assert merged result once
        //  https://issues.apache.org/jira/browse/FLINK-24511 is fixed.
        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testStartupFromLatestOffset(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        inventoryDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key(id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'latest-offset',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM debezium_source");

        // wait for the source startup, we don't have a better way to wait it, use sleep for now
        do {
            Thread.sleep(5000L);
        } while (result.getJobClient().get().getJobStatus().get() != RUNNING);

        CloseableIterator<Row> iterator = result.collect();

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }

        String[] expected =
                new String[] {
                    "+I[110, jacket, water resistent white wind breaker, 0.200]",
                    "+I[111, scooter, Big 2-wheel scooter , 5.180]",
                    "-U[110, jacket, water resistent white wind breaker, 0.200]",
                    "+U[110, jacket, new water resistent white wind breaker, 0.500]",
                    "-U[111, scooter, Big 2-wheel scooter , 5.180]",
                    "+U[111, scooter, Big 2-wheel scooter , 5.170]",
                    "-D[111, scooter, Big 2-wheel scooter , 5.170]"
                };
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testPrimaryKeyWithVarbinaryType(boolean incrementalSnapshot) throws Exception {
        Assumptions.assumeThat(incrementalSnapshot).isTrue();
        setup(incrementalSnapshot);
        inventoryDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE varbinary_pk_table ("
                                + " order_id VARBINARY(11),"
                                + " order_date DATE,"
                                + " quantity INT,"
                                + " product_id INT,"
                                + " purchaser STRING,"
                                + " PRIMARY KEY(order_id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "varbinary_pk_table",
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM varbinary_pk_table");

        // wait for the source startup, we don't have a better way to wait it, use sleep for now
        do {
            Thread.sleep(5000L);
        } while (result.getJobClient().get().getJobStatus().get() != RUNNING);

        CloseableIterator<Row> iterator = result.collect();

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO varbinary_pk_table VALUES (b'0000010000000100000001000000010000000100000001000000010000000101','2021-03-08', 30, 500, 'flink');"); // 110
            statement.execute(
                    "INSERT INTO varbinary_pk_table VALUES (b'0000010000000100000001000000010000000100000001000000010000000110','2021-03-08', 30, 500, 'flink-sql');");
            statement.execute(
                    "UPDATE varbinary_pk_table SET quantity=50 WHERE order_id=b'0000010000000100000001000000010000000100000001000000010000000101';");
            statement.execute(
                    "DELETE FROM varbinary_pk_table WHERE order_id=b'0000010000000100000001000000010000000100000001000000010000000110';");
        }

        String[] expected =
                new String[] {
                    // snapshot records
                    "+I[[4, 4, 4, 4, 4, 4, 4, 0], 2021-03-08, 0, 0, flink]",
                    "+I[[4, 4, 4, 4, 4, 4, 4, 1], 2021-03-08, 10, 100, flink]",
                    "+I[[4, 4, 4, 4, 4, 4, 4, 2], 2021-03-08, 20, 200, flink]",
                    "+I[[4, 4, 4, 4, 4, 4, 4, 3], 2021-03-08, 30, 300, flink]",
                    "+I[[4, 4, 4, 4, 4, 4, 4, 4], 2021-03-08, 40, 400, flink]",
                    // binlog records
                    "+I[[4, 4, 4, 4, 4, 4, 4, 5], 2021-03-08, 30, 500, flink]",
                    "+I[[4, 4, 4, 4, 4, 4, 4, 6], 2021-03-08, 30, 500, flink-sql]",
                    "-U[[4, 4, 4, 4, 4, 4, 4, 5], 2021-03-08, 30, 500, flink]",
                    "+U[[4, 4, 4, 4, 4, 4, 4, 5], 2021-03-08, 50, 500, flink]",
                    "-D[[4, 4, 4, 4, 4, 4, 4, 6], 2021-03-08, 30, 500, flink-sql]"
                };
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testPrimaryKeyWithSnowflakeAlgorithm(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        customerDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE address ("
                                + " `id` DECIMAL(20, 0) NOT NULL,"
                                + " country STRING,"
                                + " city STRING,"
                                + " detail_address STRING,"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customerDatabase.getUsername(),
                        customerDatabase.getPassword(),
                        customerDatabase.getDatabaseName(),
                        "address",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);
        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "SELECT id,\n" + "country,\n" + "city,\n" + "detail_address FROM address");

        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);

        try (Connection connection = customerDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE address SET city = 'Hangzhou' WHERE id=416927583791428523;");
            statement.execute(
                    "INSERT INTO address VALUES(418257940021724075, 'Germany', 'Berlin', 'West Town address 3')");
        }

        String[] expected =
                new String[] {
                    "+I[417271541558096811, America, New York, East Town address 2]",
                    "+I[417272886855938987, America, New York, East Town address 3]",
                    "+I[417111867899200427, America, New York, East Town address 1]",
                    "+I[417420106184475563, Germany, Berlin, West Town address 1]",
                    "+I[418161258277847979, Germany, Berlin, West Town address 2]",
                    "+I[416874195632735147, China, Beijing, West Town address 1]",
                    "+I[416927583791428523, China, Beijing, West Town address 2]",
                    "+I[417022095255614379, China, Beijing, West Town address 3]",
                    "-U[416927583791428523, China, Beijing, West Town address 2]",
                    "+U[416927583791428523, China, Hangzhou, West Town address 2]",
                    "+I[418257940021724075, Germany, Berlin, West Town address 3]"
                };
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testReadingWithDotTableName(boolean incrementalSnapshot) throws Exception {
        Assumptions.assumeThat(incrementalSnapshot).isTrue();
        setup(incrementalSnapshot);
        customer3_0Database.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE customers ("
                                + " `id` INTEGER NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customer3_0Database.getUsername(),
                        customer3_0Database.getPassword(),
                        customer3_0Database.getDatabaseName(),
                        "customers3.0",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);
        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "SELECT id,\n" + "name,\n" + "address,\n" + "phone_number FROM customers");

        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);

        try (Connection connection = customer3_0Database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE `customers3.0` SET address = 'Hangzhou' WHERE id=103;");
            statement.execute(
                    "INSERT INTO `customers3.0` VALUES(110, 'newCustomer', 'Berlin', '12345678')");
        }

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[104, user_4, Shanghai, 123567891234]",
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "+I[110, newCustomer, Berlin, 12345678]"
                };
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
        customer3_0Database.dropDatabase();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testReadingWithRegexPattern(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        env.setRestartStrategy(RestartStrategies.noRestart());
        customerDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE customers ("
                                + " `id` INTEGER NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customerDatabase.getUsername(),
                        customerDatabase.getPassword(),
                        // The regular regex from database-name and table-name will be
                        // e.g. 'customer_c2dsd.*\\.customers'. This should only contain the
                        // customer_c2dsd.customers table. And the customer_c2dsd.prefix_customers
                        // table must not be contained.
                        String.format("%s.*", customerDatabase.getDatabaseName()),
                        "customers",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);
        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM customers");

        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testDdlWithDefaultStringValue(boolean incrementalSnapshot) throws Exception {
        Assumptions.assumeThat(incrementalSnapshot).isTrue();
        setup(incrementalSnapshot);
        env.setRestartStrategy(RestartStrategies.noRestart());
        customerDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE default_value_test ("
                                + " id BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number BIGINT,"
                                + " primary key (id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customerDatabase.getUsername(),
                        customerDatabase.getPassword(),
                        customerDatabase.getDatabaseName(),
                        "default_value_test.*",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);
        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM default_value_test");
        JobClient jobClient = result.getJobClient().get();
        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(10)));
        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);
        try (Connection connection = customerDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM default_value_test WHERE id=1;");
        }
        String[] expected =
                new String[] {
                    "+I[1, user1, Shanghai, 123567]",
                    "+I[2, user2, Shanghai, 123567]",
                    "-D[1, user1, Shanghai, 123567]"
                };
        try (Connection connection = customerDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    " CREATE TABLE temp_default_value_test (\n"
                            + "     id INTEGER NOT NULL PRIMARY KEY, \n"
                            + "     tiny_c TINYINT DEFAULT ' 0 ', \n"
                            + "     boolean_c BOOLEAN DEFAULT ' 1 ', \n"
                            + "     tiny_un_z_c TINYINT UNSIGNED ZEROFILL DEFAULT ' 2 ', \n"
                            + "     small_c SMALLINT DEFAULT ' 3 ', \n"
                            + "     small_un_c SMALLINT UNSIGNED DEFAULT ' 4 ',\n"
                            + "     small_un_z_c SMALLINT UNSIGNED ZEROFILL DEFAULT ' 5 ', \n"
                            + "     medium_c MEDIUMINT DEFAULT ' 6 ', \n"
                            + "     medium_un_c MEDIUMINT UNSIGNED DEFAULT ' 7 ', \n"
                            + "     medium_un_z_c MEDIUMINT UNSIGNED ZEROFILL DEFAULT ' 8 ', \n"
                            + "     int_c INTEGER DEFAULT ' 9 ', \n"
                            + "     int_un_c INTEGER UNSIGNED DEFAULT ' 10 ', \n"
                            + "     int_un_z_c INTEGER UNSIGNED ZEROFILL DEFAULT ' 11 ',\n"
                            + "     int11_c INT(11) DEFAULT ' 12 ', \n"
                            + "     big_c BIGINT DEFAULT ' 13 ', \n"
                            + "     big_un_c BIGINT UNSIGNED DEFAULT ' 14 ', \n"
                            + "     big_un_z_c BIGINT UNSIGNED ZEROFILL DEFAULT ' 15 ', \n"
                            + "     decimal_c DECIMAL(8, 4) DEFAULT ' 16  ', \n"
                            + "     decimal_un_c DECIMAL(8, 4) UNSIGNED DEFAULT ' 17 ', \n"
                            + "     decimal_un_z_c DECIMAL(8, 4) UNSIGNED ZEROFILL DEFAULT ' 18 ', \n"
                            + "     numeric_c NUMERIC(6, 0) DEFAULT ' 19 ', \n"
                            + "     big_decimal_c DECIMAL(65, 1) DEFAULT ' 20 ',\n"
                            + "     real_c REAL DEFAULT ' 21.0',\n"
                            + "     float_c FLOAT DEFAULT ' 22.0',\n"
                            + "     float_un_c FLOAT UNSIGNED DEFAULT ' 23',\n"
                            + "     float_un_z_c FLOAT UNSIGNED ZEROFILL DEFAULT ' 24',\n"
                            + "     double_c DOUBLE DEFAULT ' 25',\n"
                            + "     double_un_c DOUBLE UNSIGNED DEFAULT ' 26',\n"
                            + "     double_un_z_c DOUBLE UNSIGNED ZEROFILL DEFAULT ' 27',\n"
                            + "     tiny_un_c TINYINT UNSIGNED DEFAULT ' 28 '"
                            + " );");
            statement.execute(
                    "alter table temp_default_value_test alter column `small_c` SET DEFAULT ' 29 ';");
            statement.execute(
                    "alter table temp_default_value_test add column\n"
                            + "    `new_col` smallint(1) unsigned DEFAULT ' 30 ';");
            statement.execute(
                    "alter table default_value_test add column\n"
                            + "    `new_col` smallint(1) unsigned DEFAULT ' 31 ';");
            statement.execute(
                    " CREATE TABLE default_value_test_ignore (\n"
                            + "     id INTEGER NOT NULL PRIMARY KEY, \n"
                            + "     tiny_c TINYINT DEFAULT ' 0 ', \n"
                            + "     boolean_c BOOLEAN DEFAULT ' 1 ', \n"
                            + "     tiny_un_z_c TINYINT UNSIGNED ZEROFILL DEFAULT ' 2 ', \n"
                            + "     small_c SMALLINT DEFAULT ' 3 ', \n"
                            + "     small_un_c SMALLINT UNSIGNED DEFAULT ' 4 ',\n"
                            + "     small_un_z_c SMALLINT UNSIGNED ZEROFILL DEFAULT ' 5 ', \n"
                            + "     medium_c MEDIUMINT DEFAULT ' 6 ', \n"
                            + "     medium_un_c MEDIUMINT UNSIGNED DEFAULT ' 7 ', \n"
                            + "     medium_un_z_c MEDIUMINT UNSIGNED ZEROFILL DEFAULT ' 8 ', \n"
                            + "     int_c INTEGER DEFAULT ' 9 ', \n"
                            + "     int_un_c INTEGER UNSIGNED DEFAULT ' 10 ', \n"
                            + "     int_un_z_c INTEGER UNSIGNED ZEROFILL DEFAULT ' 11 ',\n"
                            + "     int11_c INT(11) DEFAULT ' 12 ', \n"
                            + "     big_c BIGINT DEFAULT ' 13 ', \n"
                            + "     big_un_c BIGINT UNSIGNED DEFAULT ' 14 ', \n"
                            + "     big_un_z_c BIGINT UNSIGNED ZEROFILL DEFAULT ' 15 ', \n"
                            + "     decimal_c DECIMAL(8, 4) DEFAULT ' 16  ', \n"
                            + "     decimal_un_c DECIMAL(8, 4) UNSIGNED DEFAULT ' 17 ', \n"
                            + "     decimal_un_z_c DECIMAL(8, 4) UNSIGNED ZEROFILL DEFAULT ' 18 ', \n"
                            + "     numeric_c NUMERIC(6, 0) DEFAULT ' 19 ', \n"
                            + "     big_decimal_c DECIMAL(65, 1) DEFAULT ' 20 ',\n"
                            + "     real_c REAL DEFAULT ' 21.0',\n"
                            + "     float_c FLOAT DEFAULT ' 22.0',\n"
                            + "     float_un_c FLOAT UNSIGNED DEFAULT ' 23',\n"
                            + "     float_un_z_c FLOAT UNSIGNED ZEROFILL DEFAULT ' 24',\n"
                            + "     double_c DOUBLE DEFAULT ' 25',\n"
                            + "     double_un_c DOUBLE UNSIGNED DEFAULT ' 26',\n"
                            + "     double_un_z_c DOUBLE UNSIGNED ZEROFILL DEFAULT ' 27',\n"
                            + "     tiny_un_c TINYINT UNSIGNED DEFAULT ' 28 '"
                            + " );");
        }
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        jobClient.cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testAlterWithDefaultStringValue(boolean incrementalSnapshot) throws Exception {
        Assumptions.assumeThat(incrementalSnapshot).isTrue();
        setup(incrementalSnapshot);
        env.setRestartStrategy(RestartStrategies.noRestart());
        customerDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE default_value_test ("
                                + " id BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number BIGINT,"
                                + " primary key (id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customerDatabase.getUsername(),
                        customerDatabase.getPassword(),
                        customerDatabase.getDatabaseName(),
                        "default_value_test",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);
        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM default_value_test");
        JobClient jobClient = result.getJobClient().get();
        waitForJobStatus(
                jobClient,
                Collections.singletonList(RUNNING),
                Deadline.fromNow(Duration.ofSeconds(10)));
        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);
        try (Connection connection = customerDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM default_value_test WHERE id=1;");
        }
        String[] expected =
                new String[] {
                    "+I[1, user1, Shanghai, 123567]",
                    "+I[2, user2, Shanghai, 123567]",
                    "-D[1, user1, Shanghai, 123567]"
                };

        try (Connection connection = customerDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "alter table default_value_test add column `collate_test` INT DEFAULT ' 29 ' COLLATE 'utf8_general_ci';");
            statement.execute(
                    "alter table default_value_test add column `int_test` INT DEFAULT ' 30 ';");
        }
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        jobClient.cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testStartupFromSpecificBinlogFilePos(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        inventoryDatabase.createAndInitialize();

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
        }
        Tuple2<String, Integer> offset =
                currentMySqlLatestOffset(MYSQL_CONTAINER, inventoryDatabase, "products", 9, false);

        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.startup.mode' = 'specific-offset',"
                                + " 'scan.startup.specific-offset.file' = '%s',"
                                + " 'scan.startup.specific-offset.pos' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        offset.f0,
                        offset.f1,
                        incrementalSnapshot);
        String sinkDDL =
                "CREATE TABLE sink "
                        + " WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ") LIKE debezium_source (EXCLUDING OPTIONS)";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
        }

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }

        // We only expect 5 records here as all UPDATE_BEFOREs are ignored with primary key defined
        waitForSinkSize("sink", 5);

        String[] expected =
                new String[] {"+I[110, jacket, new water resistent white wind breaker, 0.500]"};

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testStartupFromSpecificGtidSet(boolean incrementalSnapshot) throws Exception {
        // Unfortunately the legacy MySQL source without incremental snapshot does not support
        // starting from GTID set
        Assumptions.assumeThat(incrementalSnapshot).isTrue();
        setup(incrementalSnapshot);

        inventoryDatabase.createAndInitialize();

        BinlogOffset offset;

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            offset =
                    DebeziumUtils.currentBinlogOffset(
                            DebeziumUtils.createMySqlConnection(
                                    new MySqlSourceConfigFactory()
                                            .hostname(MYSQL_CONTAINER.getHost())
                                            .port(MYSQL_CONTAINER.getDatabasePort())
                                            .username(TEST_USER)
                                            .password(TEST_PASSWORD)
                                            .databaseList(inventoryDatabase.getDatabaseName())
                                            .tableList("products")
                                            .createConfig(0)));
        }

        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.startup.mode' = 'specific-offset',"
                                + " 'scan.startup.specific-offset.gtid-set' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        offset.getGtidSet(),
                        incrementalSnapshot);
        String sinkDDL =
                "CREATE TABLE sink "
                        + " WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ") LIKE debezium_source (EXCLUDING OPTIONS)";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
        }

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }

        // We only expect 5 records here as all UPDATE_BEFOREs are ignored with primary key defined
        waitForSinkSize("sink", 5);

        String[] expected =
                new String[] {"+I[110, jacket, new water resistent white wind breaker, 0.500]"};

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testStartupFromEarliestOffset(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        inventoryDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.startup.mode' = 'earliest-offset',"
                                + " 'scan.incremental.snapshot.enabled' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        incrementalSnapshot);
        String sinkDDL =
                "CREATE TABLE sink "
                        + " WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ") LIKE debezium_source (EXCLUDING OPTIONS)";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");

        waitForSinkSize("sink", 16);

        String[] expected =
                new String[] {
                    "+I[101, scooter, Small 2-wheel scooter, 3.140]",
                    "+I[102, car battery, 12V car battery, 8.100]",
                    "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800]",
                    "+I[104, hammer, 12oz carpenter's hammer, 0.750]",
                    "+I[105, hammer, 14oz carpenter's hammer, 0.875]",
                    "+I[108, jacket, water resistent black wind breaker, 0.100]",
                    "+I[109, spare tire, 24 inch spare tire, 22.200]",
                    "+I[106, hammer, 18oz carpenter hammer, 1.000]",
                    "+I[107, rocks, box of assorted rocks, 5.100]",
                    "+I[110, jacket, new water resistent white wind breaker, 0.500]"
                };

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testStartupFromTimestamp(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        inventoryDatabase.createAndInitialize();

        // Unfortunately we have to sleep here to differ initial and later-generating changes in
        // binlog by timestamp
        Thread.sleep(5000L);

        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.startup.mode' = 'timestamp',"
                                + " 'scan.startup.timestamp-millis' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        System.currentTimeMillis(),
                        incrementalSnapshot);
        String sinkDDL =
                "CREATE TABLE sink "
                        + " WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ") LIKE debezium_source (EXCLUDING OPTIONS)";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        // wait for the source startup, we don't have a better way to wait it, use sleep for now
        Thread.sleep(5000L);

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }

        waitForSinkSize("sink", 5);

        String[] expected =
                new String[] {"+I[110, jacket, new water resistent white wind breaker, 0.500]"};

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testColumnOptionalWithDefaultValue(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        customerDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " `product_no` DECIMAL(20, 4) NOT NULL,"
                                + " product_kind STRING,"
                                + " user_id STRING,"
                                + " description STRING,"
                                + " primary key (`product_no`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customerDatabase.getUsername(),
                        customerDatabase.getPassword(),
                        customerDatabase.getDatabaseName(),
                        "shopping_cart_dec",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);
        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "SELECT product_no,\n"
                                + "product_kind,\n"
                                + "user_id,\n"
                                + "description FROM debezium_source");

        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);

        String[] expected =
                new String[] {
                    "+I[123456.1230, KIND_001, user_1, my shopping cart]",
                    "+I[123457.4560, KIND_002, user_2, my shopping cart]",
                    "+I[123458.6789, KIND_003, user_3, my shopping cart]",
                    "+I[123459.1234, KIND_004, user_4, null]"
                };
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testReadingWithMultiMaxValue(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        assumeThat(incrementalSnapshot).isTrue();
        inventoryDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE multi_max_table ("
                                + " order_id STRING,"
                                + " index INTEGER,"
                                + " desc STRING,"
                                + " PRIMARY KEY(order_id, index) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "multi_max_table",
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM multi_max_table");

        // wait for the source startup, we don't have a better way to wait it, use sleep for now
        do {
            Thread.sleep(5000L);
        } while (result.getJobClient().get().getJobStatus().get() != RUNNING);

        CloseableIterator<Row> iterator = result.collect();

        String[] expected =
                new String[] {
                    // snapshot records
                    "+I[, 0, flink]",
                    "+I[, 1, flink]",
                    "+I[, 2, flink]",
                    "+I[a, 0, flink]",
                    "+I[b, 0, flink]",
                    "+I[c, 0, flink]",
                    "+I[d, 0, flink]",
                    "+I[E, 0, flink]",
                    "+I[E, 1, flink]",
                    "+I[E, 2, flink]",
                    "+I[E, 3, flink]",
                    "+I[e, 4, flink]"
                };
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testServerIdConflict(boolean incrementalSnapshot) {
        setup(incrementalSnapshot);
        Assertions.assertThatThrownBy(
                        () -> {
                            env.setRestartStrategy(RestartStrategies.noRestart());
                            customerDatabase.createAndInitialize();
                            int base = 5400;
                            for (int i = 0; i < 2; i++) {
                                String sourceDDL =
                                        String.format(
                                                "CREATE TABLE debezium_source%d ("
                                                        + " `id` INTEGER NOT NULL,"
                                                        + " `name` STRING,"
                                                        + " `address` STRING,"
                                                        + " `phone_name` STRING,"
                                                        + " primary key (`id`) not enforced"
                                                        + ") WITH ("
                                                        + " 'connector' = 'mysql-cdc',"
                                                        + " 'hostname' = '%s',"
                                                        + " 'port' = '%s',"
                                                        + " 'username' = '%s',"
                                                        + " 'password' = '%s',"
                                                        + " 'database-name' = '%s',"
                                                        + " 'table-name' = '%s',"
                                                        + " 'scan.incremental.snapshot.enabled' = '%s',"
                                                        + " 'server-id' = '%s',"
                                                        + " 'server-time-zone' = 'UTC',"
                                                        + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                                        + ")",
                                                i,
                                                MYSQL_CONTAINER.getHost(),
                                                MYSQL_CONTAINER.getDatabasePort(),
                                                customerDatabase.getUsername(),
                                                customerDatabase.getPassword(),
                                                customerDatabase.getDatabaseName(),
                                                "customers",
                                                incrementalSnapshot,
                                                getServerId(base, incrementalSnapshot),
                                                getSplitSize(incrementalSnapshot));
                                String sinkDDL =
                                        String.format(
                                                "CREATE TABLE blackhole_table%d WITH ('connector' = 'blackhole')\n"
                                                        + " LIKE debezium_source%d (EXCLUDING ALL)",
                                                i, i);
                                tEnv.executeSql(sourceDDL);
                                tEnv.executeSql(sinkDDL);
                            }

                            StreamStatementSet statementSet = tEnv.createStatementSet();
                            statementSet.addInsertSql(
                                    "Insert into blackhole_table0 select * from debezium_source0");
                            statementSet.addInsertSql(
                                    "Insert into blackhole_table1 select * from debezium_source1");
                            statementSet.execute().await();
                        })
                .hasStackTraceContaining(
                        "The 'server-id' in the mysql cdc connector should be globally unique, but conflicts happen now.\n"
                                + "The server id conflict may happen in the following situations: \n"
                                + "1. The server id has been used by other mysql cdc table in the current job.\n"
                                + "2. The server id has been used by the mysql cdc table in other jobs.\n"
                                + "3. The server id has been used by other sync tools like canal, debezium and so on.\n");
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testBinlogTableMetadataDeserialization(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        assumeThat(incrementalSnapshot).isTrue();
        binlogDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE binlog_metadata (\n"
                                + "    id BIGINT NOT NULL,\n"
                                + "    tiny_c TINYINT,\n"
                                + "    tiny_un_c SMALLINT ,\n"
                                + "    tiny_un_z_c SMALLINT ,\n"
                                + "    small_c SMALLINT,\n"
                                + "    small_un_c INT,\n"
                                + "    small_un_z_c INT,\n"
                                + "    year_c INT,\n"
                                + " PRIMARY KEY(id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL8_CONTAINER.getHost(),
                        MYSQL8_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        binlogDatabase.getDatabaseName(),
                        "binlog_metadata",
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM binlog_metadata");

        // wait for the source startup, we don't have a better way to wait it, use sleep for now
        do {
            Thread.sleep(5000L);
        } while (result.getJobClient().get().getJobStatus().get() != RUNNING);

        CloseableIterator<Row> iterator = result.collect();

        try (Connection connection = binlogDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO binlog_metadata VALUES (2, 127, 255, 255, 32767, 65535, 65535, 2024),(3, 127, 255, 255, 32767, 65535, 65535, 2024);");
            statement.execute("DELETE FROM binlog_metadata WHERE id=3;");
        }

        String[] expected =
                new String[] {
                    // snapshot records
                    "+I[1, 127, 255, 255, 32767, 65535, 65535, 2023]",
                    "+I[2, 127, 255, 255, 32767, 65535, 65535, 2024]"
                };
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }

    // ------------------------------------------------------------------------------------

    private String getServerId(boolean incrementalSnapshot) {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        if (incrementalSnapshot) {
            return serverId + "-" + (serverId + env.getParallelism());
        }
        return String.valueOf(serverId);
    }

    protected String getServerId(int base, boolean incrementalSnapshot) {
        if (incrementalSnapshot) {
            return base + "-" + (base + DEFAULT_PARALLELISM);
        }
        return String.valueOf(base);
    }

    private int getSplitSize(boolean incrementalSnapshot) {
        if (incrementalSnapshot) {
            // test parallel read
            return 4;
        }
        return 0;
    }

    private static String buildColumnsDDL(
            String columnPrefix, int start, int end, String dataType) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = start; i < end; i++) {
            stringBuilder.append(columnPrefix).append(i).append(" ").append(dataType).append(",");
        }
        return stringBuilder.toString();
    }

    private static String getIntegerSeqString(int start, int end) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = start; i < end - 1; i++) {
            stringBuilder.append(i).append(", ");
        }
        stringBuilder.append(end - 1);
        return stringBuilder.toString();
    }

    private static void waitForSnapshotStarted(String sinkName) throws InterruptedException {
        while (sinkSize(sinkName) == 0) {
            Thread.sleep(100);
        }
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResultsAsStrings(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    private static void waitForSnapshotStarted(CloseableIterator<Row> iterator) throws Exception {
        while (!iterator.hasNext()) {
            Thread.sleep(100);
        }
    }

    @ParameterizedTest(name = "incrementalSnapshot = {0}")
    @ValueSource(booleans = {true, false})
    void testBinaryHandlingModeWithBase64(boolean incrementalSnapshot) throws Exception {
        assumeThat(incrementalSnapshot).isTrue();
        setup(incrementalSnapshot);
        inventoryDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE varbinary_base64_table ("
                                + " id INT,"
                                + " order_id STRING,"
                                + " order_date DATE,"
                                + " quantity INT,"
                                + " product_id INT,"
                                + " purchaser STRING,"
                                + " PRIMARY KEY(id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s',"
                                + " 'debezium.binary.handling.mode' = 'base64'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "varbinary_base64_table",
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot));
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM varbinary_base64_table");

        // wait for the source startup, we don't have a better way to wait it, use sleep for now
        do {
            Thread.sleep(5000L);
        } while (result.getJobClient().get().getJobStatus().get() != RUNNING);

        CloseableIterator<Row> iterator = result.collect();

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO varbinary_base64_table VALUES "
                            + "(6, b'0000010000000100000001000000010000000100000001000000010000000101','2021-03-08', "
                            + "30, 500, 'flink');");
            statement.execute(
                    "INSERT INTO varbinary_base64_table VALUES "
                            + "(7, b'0000010000000100000001000000010000000100000001000000010000000110','2021-03-08', "
                            + "30, 500, 'flink-sql');");
            statement.execute("UPDATE varbinary_base64_table SET quantity=50 WHERE id=6;");
            statement.execute("DELETE FROM varbinary_base64_table WHERE id= 7;");
        }
        String[] expected =
                new String[] {
                    // snapshot records
                    "+I[1, BAQEBAQEBAA=, 2021-03-08, 0, 0, flink]",
                    "+I[2, BAQEBAQEBAE=, 2021-03-08, 10, 100, flink]",
                    "+I[3, BAQEBAQEBAI=, 2021-03-08, 20, 200, flink]",
                    "+I[4, BAQEBAQEBAM=, 2021-03-08, 30, 300, flink]",
                    "+I[5, BAQEBAQEBAQ=, 2021-03-08, 40, 400, flink]",
                    // binlog records
                    "+I[6, BAQEBAQEBAU=, 2021-03-08, 30, 500, flink]",
                    "+I[7, BAQEBAQEBAY=, 2021-03-08, 30, 500, flink-sql]",
                    "-U[6, BAQEBAQEBAU=, 2021-03-08, 30, 500, flink]",
                    "+U[6, BAQEBAQEBAU=, 2021-03-08, 50, 500, flink]",
                    "-D[7, BAQEBAQEBAY=, 2021-03-08, 30, 500, flink-sql]"
                };
        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));
        result.getJobClient().get().cancel().get();
    }
}
