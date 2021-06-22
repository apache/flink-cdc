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

package com.alibaba.ververica.cdc.connectors.oracle.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import com.alibaba.ververica.cdc.connectors.oracle.OracleTestBase;
import com.alibaba.ververica.cdc.connectors.oracle.utils.UniqueDatabase;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.Container;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/** Integration tests for MySQL binlog SQL source. */
public class OracleConnectorITCase extends OracleTestBase {

    private final UniqueDatabase database =
            new UniqueDatabase(ORACLE_CONTAINER, "debezium", "system", "oracle", "inventory_1");

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env,
                    EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

    @Before
    public void before() throws Exception {
        TestValuesTableFactory.clearAllData();
        Container.ExecResult execResult1 =
                ORACLE_CONTAINER.execInContainer("chmod", "+x", "/etc/logminer_conf.sh");

        execResult1.getStdout();
        execResult1.getStderr();

        // Container.ExecResult execResult12  = ORACLE_CONTAINER.execInContainer("chmod", "-R",
        // "777", "/u01/app/oracle/");
        // execResult12 = ORACLE_CONTAINER.execInContainer("su - oracle");

        // execResult12.getStdout();
        // execResult12.getStderr();
        Container.ExecResult execResult =
                ORACLE_CONTAINER.execInContainer("/bin/sh", "-c", "/etc/logminer_conf.sh");
        execResult.getStdout();
        execResult.getStderr();
        env.setParallelism(1);
    }

    @Test
    public void testConsumingAllEvents()
            throws SQLException, ExecutionException, InterruptedException {
        // inventoryDatabase.createAndInitialize();

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
                                + " 'database-name' = 'XE',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        ORACLE_CONTAINER.getHost(),
                        ORACLE_CONTAINER.getOraclePort(),
                        "dbzuser",
                        "dbz",
                        database.getDatabaseName(),
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

        waitForSnapshotStarted("sink");

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106");
            statement.execute("UPDATE debezium.products SET WEIGHT='5.1' WHERE ID=107");
            statement.execute(
                    "INSERT INTO debezium.products VALUES (111,'jacket','water resistent white wind breaker',0.2)"); // 110
            statement.execute(
                    "INSERT INTO debezium.products VALUES (112,'scooter','Big 2-wheel scooter ',5.18)");
            statement.execute(
                    "UPDATE debezium.products SET DESCRIPTION='new water resistent white wind breaker', WEIGHT='0.5' WHERE ID=111");
            statement.execute("UPDATE debezium.products SET WEIGHT='5.17' WHERE ID=112");
            statement.execute("DELETE FROM debezium.products WHERE ID=112");
        }

        waitForSinkSize("sink", 20);

        // The final database table looks like this:
        //
        // > SELECT * FROM products;
        // +-----+--------------------+---------------------------------------------------------+--------+
        // | id  | name               | description                                             |
        // weight |
        // +-----+--------------------+---------------------------------------------------------+--------+
        // | 101 | scooter            | Small 2-wheel scooter                                   |
        // 3.14 |
        // | 102 | car battery        | 12V car battery                                         |
        // 8.1 |
        // | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |
        // 0.8 |
        // | 104 | hammer             | 12oz carpenter's hammer                                 |
        // 0.75 |
        // | 105 | hammer             | 14oz carpenter's hammer                                 |
        // 0.875 |
        // | 106 | hammer             | 18oz carpenter hammer                                   |
        //   1 |
        // | 107 | rocks              | box of assorted rocks                                   |
        // 5.1 |
        // | 108 | jacket             | water resistent black wind breaker                      |
        // 0.1 |
        // | 109 | spare tire         | 24 inch spare tire                                      |
        // 22.2 |
        // | 110 | jacket             | new water resistent white wind breaker                  |
        // 0.5 |
        // +-----+--------------------+---------------------------------------------------------+--------+

        String[] expected =
                new String[] {
                    "scooter,8.310",
                    "car battery,8.100",
                    "12-pack drill bits,0.800",
                    "hammer,2.625",
                    "rocks,5.100",
                    "jacket,1.100",
                    "spare tire,22.200"
                };

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertThat(actual, containsInAnyOrder(expected));

        result.getJobClient().get().cancel().get();
    }
    /*
        @Test
        public void testAllTypes() throws Throwable {
            //database.createAndInitialize();
            String sourceDDL =
                    String.format(
                            "CREATE TABLE full_types (\n"
                                    + "    id INT NOT NULL,\n"
                                    + "    tiny_c TINYINT,\n"
                                    + "    tiny_un_c SMALLINT ,\n"
                                    + "    small_c SMALLINT,\n"
                                    + "    small_un_c INT,\n"
                                    + "    int_c INT ,\n"
                                    + "    int_un_c BIGINT,\n"
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
                                    + "    file_uuid BYTES\n"
                                    + ") WITH ("
                                    + " 'connector' = 'mysql-cdc',"
                                    + " 'hostname' = '%s',"
                                    + " 'port' = '%s',"
                                    + " 'username' = '%s',"
                                    + " 'password' = '%s',"
                                    + " 'database-name' = '%s',"
                                    + " 'table-name' = '%s'"
                                    + ")",
                            ORACLE_CONTAINER.getHost(),
                            ORACLE_CONTAINER.getOraclePort(),
                            database.getUsername(),
                            database.getPassword(),
                            database.getDatabaseName(),
                            "full_types");
            String sinkDDL =
                    "CREATE TABLE sink (\n"
                            + "    id INT NOT NULL,\n"
                            + "    tiny_c TINYINT,\n"
                            + "    tiny_un_c SMALLINT ,\n"
                            + "    small_c SMALLINT,\n"
                            + "    small_un_c INT,\n"
                            + "    int_c INT ,\n"
                            + "    int_un_c BIGINT,\n"
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
                            + "    file_uuid STRING\n"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")";
            tEnv.executeSql(sourceDDL);
            tEnv.executeSql(sinkDDL);

            // async submit job
            TableResult result =
                    tEnv.executeSql(
                            "INSERT INTO sink SELECT id,\n"
                                    + "tiny_c,\n"
                                    + "tiny_un_c,\n"
                                    + "small_c,\n"
                                    + "small_un_c,\n"
                                    + "int_c,\n"
                                    + "int_un_c,\n"
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

            waitForSnapshotStarted("sink");

            try (Connection connection = database.getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                statement.execute(
                        "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
            }

            waitForSinkSize("sink", 3);

            List<String> expected =
                    Arrays.asList(
                            "+I(1,127,255,32767,65535,2147483647,4294967295,9223372036854775807,Hello World,abc,"
                                    + "123.102,404.4443,123.4567,346,true,2020-07-17,18:00:22,2020-07-17T18:00:22.123,"
                                    + "2020-07-17T18:00:22.123456,2020-07-17T18:00:22,ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=)",
                            "-U(1,127,255,32767,65535,2147483647,4294967295,9223372036854775807,Hello World,abc,"
                                    + "123.102,404.4443,123.4567,346,true,2020-07-17,18:00:22,2020-07-17T18:00:22.123,"
                                    + "2020-07-17T18:00:22.123456,2020-07-17T18:00:22,ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=)",
                            "+U(1,127,255,32767,65535,2147483647,4294967295,9223372036854775807,Hello World,abc,"
                                    + "123.102,404.4443,123.4567,346,true,2020-07-17,18:00:22,2020-07-17T18:00:22.123,"
                                    + "2020-07-17T18:00:22.123456,2020-07-17T18:33:22,ZRrvv70IOQ9I77+977+977+9Nu+/vT57dAA=)");
            List<String> actual = TestValuesTableFactory.getRawResults("sink");
            assertEquals(expected, actual);

            result.getJobClient().get().cancel().get();
        }

    */
    @Test
    public void testStartupFromLatestOffset() throws Exception {
        // database.createAndInitialize();
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
                                + " 'database-name' = 'XE',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s' ,"
                                + " 'scan.startup.mode' = 'latest-offset'"
                                + ")",
                        ORACLE_CONTAINER.getHost(),
                        ORACLE_CONTAINER.getOraclePort(),
                        "dbzuser",
                        "dbz",
                        database.getDatabaseName(),
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
        Thread.sleep(5000L);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "INSERT INTO debezium.products VALUES (110,'jacket','water resistent white wind breaker',0.2)"); // 110
            statement.execute(
                    "INSERT INTO debezium.products VALUES (111,'scooter','Big 2-wheel scooter ',5.18)");
            statement.execute(
                    "UPDATE debezium.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110");
            statement.execute("UPDATE debezium.products SET weight='5.17' WHERE id=111");
            statement.execute("DELETE FROM debezium.products WHERE id=111");
        }

        waitForSinkSize("sink", 7);

        String[] expected =
                new String[] {"110,jacket,new water resistent white wind breaker,0.500"};

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertThat(actual, containsInAnyOrder(expected));

        result.getJobClient().get().cancel().get();
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
