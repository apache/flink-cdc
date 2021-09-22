/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.connectors.mysql.source.MySqlParallelSourceTestBase;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.ververica.cdc.connectors.mysql.MySqlSourceTest.currentMySqlLatestOffset;
import static org.apache.flink.api.common.JobStatus.RUNNING;

/** Integration tests for MySQL binlog SQL source. */
@RunWith(Parameterized.class)
public class MySqlConnectorITCase extends MySqlParallelSourceTestBase {

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", TEST_USER, TEST_PASSWORD);

    private final UniqueDatabase fullTypesDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "column_type_test", TEST_USER, TEST_PASSWORD);

    private final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env,
                    EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

    // the debezium mysql connector use legacy implementation or not
    private final boolean useLegacyDezMySql;

    // enable the incrementalSnapshot (i.e: The new source MySqlParallelSource)
    private final boolean incrementalSnapshot;

    public MySqlConnectorITCase(boolean useLegacyDezMySql, boolean incrementalSnapshot) {
        this.useLegacyDezMySql = useLegacyDezMySql;
        this.incrementalSnapshot = incrementalSnapshot;
    }

    @Parameterized.Parameters(name = "useLegacyDezImpl: {0}, incrementalSnapshot: {1}")
    public static Object[] parameters() {
        return new Object[][] {
            new Object[] {true, false},
            new Object[] {false, false},
            // the incremental snapshot read is base on new Debezium implementation
            new Object[] {false, true}
        };
    }

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        if (incrementalSnapshot) {
            env.setParallelism(DEFAULT_PARALLELISM);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
    }

    @Test
    public void testConsumingAllEvents() throws Exception {
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
                                + " 'debezium.internal.implementation' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        getDezImplementation(),
                        incrementalSnapshot,
                        getServerId(),
                        getSplitSize());
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

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testCheckpointIsOptionalUnderSingleParallelism() throws Exception {
        if (incrementalSnapshot) {
            env.setParallelism(1);
            // check the checkpoint is optional when parallelism is 1
            env.getCheckpointConfig().disableCheckpointing();
        } else {
            return;
        }
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
                                + " 'debezium.internal.implementation' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        getDezImplementation(),
                        incrementalSnapshot,
                        getServerId(),
                        getSplitSize());
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

    @Test
    public void testAllTypes() throws Throwable {
        fullTypesDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types (\n"
                                + "    `id` INT NOT NULL,\n"
                                + "    tiny_c TINYINT,\n"
                                + "    tiny_un_c SMALLINT ,\n"
                                + "    small_c SMALLINT,\n"
                                + "    small_un_c INT,\n"
                                + "    int_c INT ,\n"
                                + "    int_un_c BIGINT,\n"
                                + "    int11_c BIGINT,\n"
                                + "    big_c BIGINT,\n"
                                + "    varchar_c STRING,\n"
                                + "    char_c STRING,\n"
                                + "    float_c FLOAT,\n"
                                + "    double_c DOUBLE,\n"
                                + "    decimal_c DECIMAL(8, 4),\n"
                                + "    numeric_c DECIMAL(6, 0),\n"
                                + "    boolean_c BOOLEAN,\n"
                                + "    date_c DATE,\n"
                                + "    time_c TIME(0),\n"
                                + "    datetime3_c TIMESTAMP(3),\n"
                                + "    datetime6_c TIMESTAMP(6),\n"
                                + "    timestamp_c TIMESTAMP(0),\n"
                                + "    file_uuid BYTES,\n"
                                + "    primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'debezium.internal.implementation' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        fullTypesDatabase.getUsername(),
                        fullTypesDatabase.getPassword(),
                        fullTypesDatabase.getDatabaseName(),
                        "full_types",
                        getDezImplementation(),
                        incrementalSnapshot,
                        getServerId(),
                        getSplitSize());
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "SELECT id,\n"
                                + "tiny_c,\n"
                                + "tiny_un_c,\n"
                                + "small_c,\n"
                                + "small_un_c,\n"
                                + "int_c,\n"
                                + "int_un_c,\n"
                                + "int11_c,\n"
                                + "big_c,\n"
                                + "varchar_c,\n"
                                + "char_c,\n"
                                + "float_c,\n"
                                + "double_c,\n"
                                + "decimal_c,\n"
                                + "numeric_c,\n"
                                + "boolean_c,\n"
                                + "date_c,\n"
                                + "time_c,\n"
                                + "datetime3_c,\n"
                                + "datetime6_c,\n"
                                + "timestamp_c,\n"
                                + "TO_BASE64(DECODE(file_uuid, 'UTF-8')) FROM full_types");

        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);

        try (Connection connection = fullTypesDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }

        String[] expected =
                new String[] {
                    "+I[1, 127, 255, 32767, 65535, 2147483647, 4294967295, 2147483647, 9223372036854775807, Hello World, abc, 123.102, 404.4443, 123.4567, 346, true, 2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:00:22, ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=]",
                    "-U[1, 127, 255, 32767, 65535, 2147483647, 4294967295, 2147483647, 9223372036854775807, Hello World, abc, 123.102, 404.4443, 123.4567, 346, true, 2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:00:22, ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=]",
                    "+U[1, 127, 255, 32767, 65535, 2147483647, 4294967295, 2147483647, 9223372036854775807, Hello World, abc, 123.102, 404.4443, 123.4567, 346, true, 2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:33:22, ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=]"
                };

        assertEqualsInAnyOrder(
                Arrays.asList(expected), fetchRows(result.collect(), expected.length));
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testStartupFromLatestOffset() throws Exception {
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
                                + " 'server-id' = '%s',"
                                + " 'debezium.internal.implementation' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        incrementalSnapshot,
                        getServerId(),
                        getDezImplementation());
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
        assertEqualsInAnyOrder(
                Arrays.asList(expected), fetchRows(result.collect(), expected.length));
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testPrimaryKeyWithSnowflakeAlgorithm() throws Exception {
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
                                + " 'debezium.internal.implementation' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customerDatabase.getUsername(),
                        customerDatabase.getPassword(),
                        customerDatabase.getDatabaseName(),
                        "address",
                        getDezImplementation(),
                        incrementalSnapshot,
                        getServerId(),
                        getSplitSize());
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
        assertEqualsInAnyOrder(
                Arrays.asList(expected), fetchRows(result.collect(), expected.length));
        result.getJobClient().get().cancel().get();
    }

    @Ignore
    @Test
    public void testStartupFromSpecificOffset() throws Exception {
        if (incrementalSnapshot) {
            // not support yet
            return;
        }
        inventoryDatabase.createAndInitialize();

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
        }
        Tuple2<String, Integer> offset =
                currentMySqlLatestOffset(inventoryDatabase, "products", 9, useLegacyDezMySql);

        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
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
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'specific-offset',"
                                + " 'scan.startup.specific-offset.file' = '%s',"
                                + " 'scan.startup.specific-offset.pos' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'debezium.internal.implementation' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        offset.f0,
                        offset.f1,
                        incrementalSnapshot,
                        getDezImplementation());
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

        waitForSinkSize("sink", 7);

        String[] expected =
                new String[] {"+I[110, jacket, new water resistent white wind breaker, 0.500]"};

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);

        result.getJobClient().get().cancel().get();
    }

    @Ignore
    @Test
    public void testStartupFromEarliestOffset() throws Exception {
        if (incrementalSnapshot) {
            // not support yet
            return;
        }
        inventoryDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
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
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'earliest-offset',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'debezium.internal.implementation' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        incrementalSnapshot,
                        getDezImplementation());
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

        waitForSinkSize("sink", 20);

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

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);

        result.getJobClient().get().cancel().get();
    }

    @Test
    @Ignore
    public void testStartupFromTimestamp() throws Exception {
        if (incrementalSnapshot) {
            // not support yet
            return;
        }
        inventoryDatabase.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
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
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'timestamp',"
                                + " 'scan.startup.timestamp-millis' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'debezium.internal.implementation' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        System.currentTimeMillis(),
                        incrementalSnapshot,
                        getDezImplementation());
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

        waitForSinkSize("sink", 7);

        String[] expected =
                new String[] {"+I[110, jacket, new water resistent white wind breaker, 0.500]"};

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);

        result.getJobClient().get().cancel().get();
    }

    // ------------------------------------------------------------------------------------

    private String getDezImplementation() {
        return useLegacyDezMySql ? "legacy" : "";
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        if (incrementalSnapshot) {
            return serverId + "-" + (serverId + env.getParallelism());
        }
        return String.valueOf(serverId);
    }

    private int getSplitSize() {
        if (incrementalSnapshot) {
            // test parallel read
            return 4;
        }
        return 0;
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
                return TestValuesTableFactory.getRawResults(sinkName).size();
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
}
