/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.RateLimiter;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.oracle.source.OracleSourceTestBase.CONNECTOR_PWD;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceTestBase.CONNECTOR_USER;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceTestBase.ORACLE_CONTAINER;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceTestBase.assertEqualsInAnyOrder;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceTestBase.createAndInitialize;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceTestBase.getJdbcConnection;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Integration tests for Oracle redo log SQL source. */
@RunWith(Parameterized.class)
public class OracleConnectorITCase {
    private static final int RECORDS_COUNT = 10_000;
    private static final int WORKERS_COUNT = 4;

    private static final Logger LOG = LoggerFactory.getLogger(OracleConnectorITCase.class);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    // enable the parallelismSnapshot (i.e: The new source OracleParallelSource)
    private final boolean parallelismSnapshot;

    public OracleConnectorITCase(boolean parallelismSnapshot) {
        this.parallelismSnapshot = parallelismSnapshot;
    }

    @Parameterized.Parameters(name = "parallelismSnapshot: {0}")
    public static Object[] parameters() {
        return new Object[][] {new Object[] {true}, new Object[] {false}};
    }

    @Before
    public void before() throws Exception {

        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Containers are started.");

        TestValuesTableFactory.clearAllData();

        if (parallelismSnapshot) {
            env.setParallelism(4);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
    }

    @After
    public void teardown() {
        ORACLE_CONTAINER.stop();
    }

    @Test
    public void testConsumingAllEvents() throws Exception {

        createAndInitialize("product.sql");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " ID INT NOT NULL,"
                                + " NAME STRING,"
                                + " DESCRIPTION STRING,"
                                + " WEIGHT DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'debezium.log.mining.strategy' = 'online_catalog',"
                                + " 'debezium.database.history.store.only.captured.tables.ddl' = 'true',"
                                + " 'scan.incremental.snapshot.chunk.size' = '2',"
                                + " 'database-name' = 'ORCLCDB',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        ORACLE_CONTAINER.getHost(),
                        ORACLE_CONTAINER.getOraclePort(),
                        CONNECTOR_USER,
                        CONNECTOR_PWD,
                        parallelismSnapshot,
                        "debezium",
                        "products");
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
                        "INSERT INTO sink SELECT NAME, SUM(WEIGHT) FROM debezium_source GROUP BY NAME");

        // There are 9 records in the table, wait until the snapshot phase finished
        waitForSinkSize("sink", 9);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106");
            statement.execute("UPDATE debezium.products SET WEIGHT=5.1 WHERE ID=107");
            statement.execute(
                    "INSERT INTO debezium.products VALUES (111,'jacket','water resistent white wind breaker',0.2)"); // 110
            statement.execute(
                    "INSERT INTO debezium.products VALUES (112,'scooter','Big 2-wheel scooter ',5.18)");
            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='new water resistent white wind breaker', WEIGHT=0.5 WHERE ID=111");
            statement.execute("UPDATE debezium.products SET WEIGHT=5.17 WHERE ID=112");
            statement.execute("DELETE FROM debezium.products WHERE ID=112");
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
         * | 111 | jacket             | new water resistent white wind breaker                  |    0.5 |
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

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testConsumingAllEventsByChunkKeyColumn() throws Exception {

        createAndInitialize("product.sql");
        if (!parallelismSnapshot) {
            return;
        }
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " ID INT NOT NULL,"
                                + " NAME STRING,"
                                + " DESCRIPTION STRING,"
                                + " WEIGHT DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.key-column' = 'ID',"
                                + " 'debezium.log.mining.strategy' = 'online_catalog',"
                                + " 'debezium.database.history.store.only.captured.tables.ddl' = 'true',"
                                + " 'scan.incremental.snapshot.chunk.size' = '2',"
                                + " 'database-name' = 'ORCLCDB',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        ORACLE_CONTAINER.getHost(),
                        ORACLE_CONTAINER.getOraclePort(),
                        "dbzuser",
                        "dbz",
                        parallelismSnapshot,
                        "debezium",
                        "products");
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
                        "INSERT INTO sink SELECT NAME, SUM(WEIGHT) FROM debezium_source GROUP BY NAME");

        waitForSinkSize("sink", 9);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106");
            statement.execute("UPDATE debezium.products SET WEIGHT=5.1 WHERE ID=107");
            statement.execute(
                    "INSERT INTO debezium.products VALUES (111,'jacket','water resistent white wind breaker',0.2)"); // 110
            statement.execute(
                    "INSERT INTO debezium.products VALUES (112,'scooter','Big 2-wheel scooter ',5.18)");
            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='new water resistent white wind breaker', WEIGHT=0.5 WHERE ID=111");
            statement.execute("UPDATE debezium.products SET WEIGHT=5.17 WHERE ID=112");
            statement.execute("DELETE FROM debezium.products WHERE ID=112");
        }

        waitForSinkSize("sink", 20);

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

        List<String> actual = TestValuesTableFactory.getResults("sink");
        LOG.info("actual:{}", actual);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testMetadataColumns() throws Throwable {

        createAndInitialize("product.sql");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " DB_NAME STRING METADATA FROM 'database_name' VIRTUAL,"
                                + " SCHEMA_NAME STRING METADATA FROM 'schema_name' VIRTUAL,"
                                + " TABLE_NAME STRING METADATA  FROM 'table_name' VIRTUAL,"
                                + " ID INT NOT NULL,"
                                + " NAME STRING,"
                                + " DESCRIPTION STRING,"
                                + " WEIGHT DECIMAL(10,3),"
                                + " PRIMARY KEY (ID) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'debezium.log.mining.strategy' = 'online_catalog',"
                                // + " 'debezium.database.history.store.only.captured.tables.ddl' =
                                // 'true',"
                                + " 'scan.incremental.snapshot.chunk.size' = '2',"
                                + " 'database-name' = 'ORCLCDB',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        ORACLE_CONTAINER.getHost(),
                        ORACLE_CONTAINER.getOraclePort(),
                        "dbzuser",
                        "dbz",
                        parallelismSnapshot,
                        "debezium",
                        "products");
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " database_name STRING,"
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " id INT,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(10,3),"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");

        waitForSinkSize("sink", 9);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106");
            statement.execute("UPDATE debezium.products SET WEIGHT=5.1 WHERE ID=107");
            statement.execute(
                    "INSERT INTO debezium.products VALUES (111,'jacket','water resistent white wind breaker',0.2)");
            statement.execute(
                    "INSERT INTO debezium.products VALUES (112,'scooter','Big 2-wheel scooter ',5.18)");
            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='new water resistent white wind breaker', WEIGHT=0.5 WHERE ID=111");
            statement.execute("UPDATE debezium.products SET WEIGHT=5.17 WHERE ID=112");
            statement.execute("DELETE FROM debezium.products WHERE ID=112");
        }
        waitForSinkSize("sink", 16);
        List<String> expected =
                Arrays.asList(
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 101, scooter, Small 2-wheel scooter, 3.140]",
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 102, car battery, 12V car battery, 8.100]",
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800]",
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 104, hammer, 12oz carpenters hammer, 0.750]",
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 105, hammer, 14oz carpenters hammer, 0.875]",
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 106, hammer, 16oz carpenters hammer, 1.000]",
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 107, rocks, box of assorted rocks, 5.300]",
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 108, jacket, water resistent black wind breaker, 0.100]",
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 109, spare tire, 24 inch spare tire, 22.200]",
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 111, jacket, water resistent white wind breaker, 0.200]",
                        "+I[ORCLCDB, DEBEZIUM, PRODUCTS, 112, scooter, Big 2-wheel scooter , 5.180]",
                        "+U[ORCLCDB, DEBEZIUM, PRODUCTS, 106, hammer, 18oz carpenter hammer, 1.000]",
                        "+U[ORCLCDB, DEBEZIUM, PRODUCTS, 107, rocks, box of assorted rocks, 5.100]",
                        "+U[ORCLCDB, DEBEZIUM, PRODUCTS, 111, jacket, new water resistent white wind breaker, 0.500]",
                        "+U[ORCLCDB, DEBEZIUM, PRODUCTS, 112, scooter, Big 2-wheel scooter , 5.170]",
                        "-D[ORCLCDB, DEBEZIUM, PRODUCTS, 112, scooter, Big 2-wheel scooter , 5.170]");

        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        Collections.sort(expected);
        Collections.sort(actual);
        assertEquals(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testStartupFromLatestOffset() throws Exception {

        createAndInitialize("product.sql");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " ID INT NOT NULL,"
                                + " NAME STRING,"
                                + " DESCRIPTION STRING,"
                                + " WEIGHT DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'debezium.log.mining.strategy' = 'online_catalog',"
                                + " 'debezium.database.history.store.only.captured.tables.ddl' = 'true',"
                                + " 'database-name' = 'ORCLCDB',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s' ,"
                                + " 'scan.startup.mode' = 'latest-offset'"
                                + ")",
                        ORACLE_CONTAINER.getHost(),
                        ORACLE_CONTAINER.getOraclePort(),
                        "dbzuser",
                        "dbz",
                        parallelismSnapshot,
                        "debezium",
                        "products");
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
        Thread.sleep(10000L);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO debezium.products VALUES (110,'jacket','water resistent white wind breaker',0.2)"); // 110
            statement.execute(
                    "INSERT INTO debezium.products VALUES (111,'scooter','Big 2-wheel scooter ',5.18)");
            statement.execute(
                    "UPDATE debezium.products SET description='new water resistent white wind breaker', weight=0.5 WHERE id=110");
            statement.execute("UPDATE debezium.products SET weight=5.17 WHERE id=111");
            statement.execute("DELETE FROM debezium.products WHERE id=111");
        }

        waitForSinkSize("sink", 7);

        String[] expected =
                new String[] {"+I[110, jacket, new water resistent white wind breaker, 0.500]"};

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertThat(actual, containsInAnyOrder(expected));

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testConsumingNumericColumns() throws Exception {
        // Prepare numeric type data
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE debezium.test_numeric_table ("
                            + " ID NUMBER(18,0),"
                            + " TEST_BOOLEAN NUMBER(1,0),"
                            + " TEST_TINYINT NUMBER(2,0),"
                            + " TEST_SMALLINT NUMBER(4,0),"
                            + " TEST_INT NUMBER(9,0),"
                            + " TEST_BIG_NUMERIC NUMBER(32,0),"
                            + " TEST_DECIMAL NUMBER(20,8),"
                            + " TEST_NUMBER NUMBER,"
                            + " TEST_NUMERIC NUMBER,"
                            + " TEST_FLOAT FLOAT(63),"
                            + " PRIMARY KEY (ID))");
            statement.execute(
                    "INSERT INTO debezium.test_numeric_table "
                            + "VALUES (11000000000, 0, 98, 9998, 987654320, 20000000000000000000, 987654321.12345678, 2147483647, 1024.955, 1024.955)");
            statement.execute(
                    "INSERT INTO debezium.test_numeric_table "
                            + "VALUES (11000000001, 1, 99, 9999, 987654321, 20000000000000000001, 987654321.87654321, 2147483648, 1024.965, 1024.965)");
        }

        String sourceDDL =
                String.format(
                        "CREATE TABLE test_numeric_table ("
                                + " ID BIGINT,"
                                + " TEST_BOOLEAN BOOLEAN,"
                                + " TEST_TINYINT TINYINT,"
                                + " TEST_SMALLINT SMALLINT,"
                                + " TEST_INT INT,"
                                + " TEST_BIG_NUMERIC DECIMAL(32, 0),"
                                + " TEST_DECIMAL DECIMAL(20, 8),"
                                + " TEST_NUMBER BIGINT,"
                                + " TEST_NUMERIC DECIMAL(10, 3),"
                                + " TEST_FLOAT FLOAT,"
                                + " PRIMARY KEY (ID) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'debezium.log.mining.strategy' = 'online_catalog',"
                                + " 'debezium.database.history.store.only.captured.tables.ddl' = 'true',"
                                + " 'database-name' = 'ORCLCDB',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        ORACLE_CONTAINER.getHost(),
                        ORACLE_CONTAINER.getOraclePort(),
                        "dbzuser",
                        "dbz",
                        parallelismSnapshot,
                        "debezium",
                        "test_numeric_table");
        String sinkDDL =
                "CREATE TABLE test_numeric_sink ("
                        + " id BIGINT,"
                        + " test_boolean BOOLEAN,"
                        + " test_tinyint TINYINT,"
                        + " test_smallint SMALLINT,"
                        + " test_int INT,"
                        + " test_big_numeric DECIMAL(32, 0),"
                        + " test_decimal DECIMAL(20, 8),"
                        + " test_number BIGINT,"
                        + " test_numeric DECIMAL(10, 3),"
                        + " test_float FLOAT,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql("INSERT INTO test_numeric_sink SELECT * FROM test_numeric_table");

        waitForSnapshotStarted("test_numeric_sink");

        // waiting for change events finished.
        waitForSinkSize("test_numeric_sink", 2);

        List<String> expected =
                Arrays.asList(
                        "+I[11000000000, false, 98, 9998, 987654320, 20000000000000000000, 987654321.12345678, 2147483647, 1024.955, 1024.955]",
                        "+I[11000000001, true, 99, 9999, 987654321, 20000000000000000001, 987654321.87654321, 2147483648, 1024.965, 1024.965]");

        List<String> actual = TestValuesTableFactory.getRawResults("test_numeric_sink");
        Collections.sort(actual);
        assertEquals(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testAllDataTypes() throws Throwable {
        createAndInitialize("column_type_test.sql");

        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types ("
                                + " ID INT,"
                                + " VAL_VARCHAR STRING,"
                                + " VAL_VARCHAR2 STRING,"
                                + " VAL_NVARCHAR2 STRING,"
                                + " VAL_CHAR STRING,"
                                + " VAL_NCHAR STRING,"
                                + " VAL_BF FLOAT,"
                                + " VAL_BD DOUBLE,"
                                + " VAL_F FLOAT,"
                                + " VAL_F_10 FLOAT,"
                                + " VAL_NUM DECIMAL(10, 6),"
                                + " VAL_DP DOUBLE,"
                                + " VAL_R DECIMAL(38,2),"
                                + " VAL_DECIMAL DECIMAL(10, 6),"
                                + " VAL_NUMERIC DECIMAL(10, 6),"
                                + " VAL_NUM_VS DECIMAL(10, 3),"
                                + " VAL_INT DECIMAL(38,0),"
                                + " VAL_INTEGER DECIMAL(38,0),"
                                + " VAL_SMALLINT DECIMAL(38,0),"
                                + " VAL_NUMBER_38_NO_SCALE DECIMAL(38,0),"
                                + " VAL_NUMBER_38_SCALE_0 DECIMAL(38,0),"
                                + " VAL_NUMBER_1 BOOLEAN,"
                                + " VAL_NUMBER_2 TINYINT,"
                                + " VAL_NUMBER_4 SMALLINT,"
                                + " VAL_NUMBER_9 INT,"
                                + " VAL_NUMBER_18 BIGINT,"
                                + " VAL_NUMBER_2_NEGATIVE_SCALE TINYINT,"
                                + " VAL_NUMBER_4_NEGATIVE_SCALE SMALLINT,"
                                + " VAL_NUMBER_9_NEGATIVE_SCALE INT,"
                                + " VAL_NUMBER_18_NEGATIVE_SCALE BIGINT,"
                                + " VAL_NUMBER_36_NEGATIVE_SCALE DECIMAL(38,0),"
                                + " VAL_DATE TIMESTAMP,"
                                + " VAL_TS TIMESTAMP,"
                                + " VAL_TS_PRECISION2 TIMESTAMP(2 ),"
                                + " VAL_TS_PRECISION4 TIMESTAMP(4),"
                                + " VAL_TS_PRECISION9 TIMESTAMP(6),"
                                + " VAL_TSTZ STRING,"
                                + " VAL_TSLTZ TIMESTAMP_LTZ,"
                                + " VAL_INT_YTM BIGINT,"
                                + " VAL_INT_DTS BIGINT,"
                                + " T15VARCHAR STRING,"
                                + " PRIMARY KEY (ID) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'debezium.log.mining.strategy' = 'online_catalog',"
                                + " 'debezium.database.history.store.only.captured.tables.ddl' = 'true',"
                                + " 'scan.incremental.snapshot.chunk.size' = '2',"
                                + " 'database-name' = 'ORCLCDB',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        ORACLE_CONTAINER.getHost(),
                        ORACLE_CONTAINER.getOraclePort(),
                        CONNECTOR_USER,
                        CONNECTOR_PWD,
                        parallelismSnapshot,
                        "debezium",
                        "full_types");

        String sinkDDL =
                "CREATE TABLE sink "
                        + " WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '2'"
                        + ") LIKE full_types (EXCLUDING OPTIONS)";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM full_types");

        // waiting for change events finished.
        waitForSinkSize("sink", 1);

        String[] expected =
                new String[] {
                    "+I[1, vc2, vc2, nvc2, c  , nc , "
                            + "1.1, 2.22, 3.33, 8.888, 4.444400, 5.555, 6.66, "
                            + "1234.567891, 1234.567891, 77.323, 1, 22, 333, 4444, 5555, "
                            + "true, 99, 9999, 999999999, 999999999999999999, "
                            + "90, 9900, 999999990, 999999999999999900, 99999999999999999999999999999999999900, "
                            + "2022-10-30T00:00, "
                            + "2022-10-30T12:34:56.007890, "
                            + "2022-10-30T12:34:56.130, "
                            + "2022-10-30T12:34:56.125500, "
                            + "2022-10-30T12:34:56.125457, "
                            + "2022-10-30T01:34:56.00789-11:00, "
                            + "2022-10-29T17:34:56.007890Z, "
                            + "-110451600000000, "
                            + "-93784560000, "
                            + "<name>\n"
                            + "  <a id=\"1\" value=\"some values\">test xmlType</a>\n"
                            + "</name>\n]"
                };

        List<String> actual = TestValuesTableFactory.getResults("sink");
        Collections.sort(actual);
        assertEquals(Arrays.asList(expected), actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testSnapshotToStreamingSwitchPendingTransactions() throws Exception {
        Assume.assumeFalse(parallelismSnapshot);

        CompletableFuture<Void> finishFuture = createRecordInserters();

        String sourceDDL =
                String.format(
                        "CREATE TABLE messages ("
                                + " ID INT NOT NULL,"
                                + " CATEGORY_NAME STRING"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = 'category',"
                                + " 'scan.incremental.snapshot.enabled' = 'false',"
                                + " 'debezium.log.mining.strategy' = 'online_catalog',"
                                + " 'debezium.database.history.store.only.captured.tables.ddl' = 'true'"
                                + ")",
                        ORACLE_CONTAINER.getHost(),
                        ORACLE_CONTAINER.getOraclePort(),
                        "dbzuser",
                        "dbz",
                        "ORCLCDB",
                        "debezium");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " ID INT,"
                        + " message STRING"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM messages");

        finishFuture.get(10, TimeUnit.MINUTES);
        LOG.info("all async runners were finished");

        waitForSinkSize("sink", RECORDS_COUNT);

        List<Integer> actual =
                TestValuesTableFactory.getResults("sink").stream()
                        .map(s -> s.replaceFirst("\\+I\\[(\\d+).+", "$1"))
                        .map(Integer::parseInt)
                        .sorted()
                        .collect(Collectors.toList());

        List<Integer> expected =
                IntStream.range(0, RECORDS_COUNT).boxed().collect(Collectors.toList());

        assertEquals(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<Void> createRecordInserters() {
        int requestPerSecondPerThread = 100;
        int recordsChunkSize = RECORDS_COUNT / WORKERS_COUNT;
        int recordsToCommit = recordsChunkSize / 4;

        List<Runnable> runners =
                IntStream.range(0, WORKERS_COUNT)
                        .mapToObj(
                                i ->
                                        createRecordInserter(
                                                requestPerSecondPerThread,
                                                recordsChunkSize * i,
                                                recordsChunkSize,
                                                recordsToCommit))
                        .collect(Collectors.toList());

        ExecutorService executor = Executors.newFixedThreadPool(WORKERS_COUNT);
        CompletableFuture<Void>[] completableFutures =
                runners.stream()
                        .map(runnable -> CompletableFuture.runAsync(runnable, executor))
                        .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(completableFutures);
    }

    private Runnable createRecordInserter(
            int requestPerSecond, int startIndex, int recordsCnt, int recordsToCommit) {
        return () -> {
            Supplier<String> messageSupplier =
                    createRandomSupplier(
                            Lists.newArrayList("msg1", "msg2", "msg3", "msg4", "msg5", "msg6"));

            RateLimiter rateLimiter = RateLimiter.create(requestPerSecond);

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                connection.setAutoCommit(false);
                for (long i = startIndex; i < startIndex + recordsCnt; i++) {
                    rateLimiter.acquire();
                    statement.execute(
                            String.format(
                                    "INSERT INTO %s.%s VALUES (%d,'%s')",
                                    "debezium", "category", i, messageSupplier.get()));
                    if (i % recordsToCommit == 0) {
                        LOG.info("Committing at id {}", i);
                        connection.commit();
                    }
                }

                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        };
    }

    private <T> Supplier<T> createRandomSupplier(List<T> possibleValues) {
        int size = possibleValues.size();
        return () -> possibleValues.get(ThreadLocalRandom.current().nextInt(size));
    }

    // ------------------------------------------------------------------------------------

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
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }
}
