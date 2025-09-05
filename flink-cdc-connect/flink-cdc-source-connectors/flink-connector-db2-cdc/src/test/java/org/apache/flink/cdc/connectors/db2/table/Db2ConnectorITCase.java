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

package org.apache.flink.cdc.connectors.db2.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.db2.Db2TestBase;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.cdc.connectors.utils.StaticExternalResourceProxy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.utils.LegacyRowResource;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.testcontainers.containers.Db2Container.DB2_PORT;

/** Integration tests for DB2 CDC source. */
class Db2ConnectorITCase extends Db2TestBase {

    private static final Logger LOG = LoggerFactory.getLogger(Db2ConnectorITCase.class);

    protected static final int DEFAULT_PARALLELISM = 2;

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @RegisterExtension
    public static StaticExternalResourceProxy<LegacyRowResource> usesLegacyRows =
            new StaticExternalResourceProxy<>(LegacyRowResource.INSTANCE);

    public void setup(boolean incrementalSnapshot) {
        TestValuesTableFactory.clearAllData();
        env.setRestartStrategy(RestartStrategies.noRestart());
        if (incrementalSnapshot) {
            env.setParallelism(DEFAULT_PARALLELISM);
            env.enableCheckpointing(1000);
        } else {
            env.setParallelism(1);
        }
    }

    private void cancelJobIfRunning(TableResult result)
            throws InterruptedException, ExecutionException {
        try {
            result.getJobClient().get().cancel().get();
        } catch (IllegalStateException ignored) {
            // job isn't running, ignore it
        }
    }

    @ParameterizedTest(name = "incrementalSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testConsumingAllEvents(boolean incrementalSnapshot)
            throws SQLException, InterruptedException, ExecutionException {
        setup(incrementalSnapshot);
        initializeDb2Table("inventory", "PRODUCTS");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " ID INT NOT NULL,"
                                + " NAME STRING,"
                                + " DESCRIPTION STRING,"
                                + " WEIGHT DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'db2-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s'"
                                + ")",
                        DB2_CONTAINER.getHost(),
                        DB2_CONTAINER.getMappedPort(DB2_PORT),
                        DB2_CONTAINER.getUsername(),
                        DB2_CONTAINER.getPassword(),
                        DB2_CONTAINER.getDatabaseName(),
                        "DB2INST1.PRODUCTS",
                        incrementalSnapshot);
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

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE DB2INST1.PRODUCTS SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106;");
            statement.execute("UPDATE DB2INST1.PRODUCTS SET WEIGHT='5.1' WHERE ID=107;");
            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS VALUES (110,'jacket','water resistent white wind breaker',0.2);");
            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS VALUES (111,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE DB2INST1.PRODUCTS SET DESCRIPTION='new water resistent white wind breaker', WEIGHT='0.5' WHERE ID=110;");
            statement.execute("UPDATE DB2INST1.PRODUCTS SET WEIGHT='5.17' WHERE ID=111;");
            statement.execute("DELETE FROM DB2INST1.PRODUCTS WHERE ID=111;");
        }

        waitForSinkSize("sink", 20);

        /*
         * <pre>
         * The final database table looks like this:
         *
         * > SELECT * FROM DB2INST1.PRODUCTS;
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | ID  | NAME               | DESCRIPTION                                             | WEIGHT |
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
                    "scooter,3.140",
                    "car battery,8.100",
                    "12-pack drill bits,0.800",
                    "hammer,2.625",
                    "rocks,5.100",
                    "jacket,0.600",
                    "spare tire,22.200"
                };

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        cancelJobIfRunning(result);
    }

    @ParameterizedTest(name = "incrementalSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testAllTypes(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        initializeDb2Table("column_type_test", "FULL_TYPES");
        // NOTE: db2 is not case sensitive by default, the schema returned by debezium
        // is uppercase, thus we need use uppercase when defines a db2 table.
        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types (\n"
                                + "    ID INTEGER NOT NULL,\n"
                                // Debezium cannot track db2 boolean type, see:
                                // https://issues.redhat.com/browse/DBZ-2587
                                //  + "    BOOLEAN_C BOOLEAN NOT NULL,\n"
                                + "    SMALL_C SMALLINT,\n"
                                + "    INT_C INTEGER,\n"
                                + "    BIG_C BIGINT,\n"
                                + "    REAL_C FLOAT,\n"
                                + "    DOUBLE_C DOUBLE,\n"
                                + "    NUMERIC_C DECIMAL(10, 5),\n"
                                + "    DECIMAL_C DECIMAL(10, 1),\n"
                                + "    VARCHAR_C STRING,\n"
                                + "    CHAR_C STRING,\n"
                                + "    CHARACTER_C STRING,\n"
                                + "    TIMESTAMP_C TIMESTAMP(3),\n"
                                + "    DATE_C DATE,\n"
                                + "    TIME_C TIME(0),\n"
                                + "    DEFAULT_NUMERIC_C DECIMAL,\n"
                                + "    TIMESTAMP_PRECISION_C TIMESTAMP(9),\n"
                                + "    PRIMARY KEY (ID) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'db2-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s'"
                                + ")",
                        DB2_CONTAINER.getHost(),
                        DB2_CONTAINER.getMappedPort(DB2_PORT),
                        DB2_CONTAINER.getUsername(),
                        DB2_CONTAINER.getPassword(),
                        DB2_CONTAINER.getDatabaseName(),
                        "DB2INST1.FULL_TYPES",
                        incrementalSnapshot);
        String sinkDDL =
                "CREATE TABLE sink (\n"
                        + "    id INTEGER NOT NULL,\n"
                        + "    small_c SMALLINT,\n"
                        + "    int_c INTEGER,\n"
                        + "    big_c BIGINT,\n"
                        + "    real_c FLOAT,\n"
                        + "    double_c DOUBLE,\n"
                        + "    numeric_c DECIMAL(10, 5),\n"
                        + "    decimal_c DECIMAL(10, 1),\n"
                        + "    varchar_c STRING,\n"
                        + "    char_c STRING,\n"
                        + "    character_c STRING,\n"
                        + "    timestamp_c TIMESTAMP(3),\n"
                        + "    date_c DATE,\n"
                        + "    time_c TIME(0),\n"
                        + "    default_numeric_c DECIMAL,\n"
                        + "    timestamp_precision_c TIMESTAMP(9),\n"
                        + "    PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM full_types");

        waitForSnapshotStarted("sink");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE DB2INST1.FULL_TYPES SET SMALL_C=0 WHERE ID=1;");
        }

        waitForSinkSize("sink", 2);

        List<String> expected =
                Arrays.asList(
                        "+I(1,32767,65535,2147483647,5.5,6.6,123.12345,404.4,Hello World,a,abc,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,2020-07-17T18:00:22.123456789)",
                        "+U(1,0,65535,2147483647,5.5,6.6,123.12345,404.4,Hello World,a,abc,2020-07-17T18:00:22.123,2020-07-17,18:00:22,500,2020-07-17T18:00:22.123456789)");
        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        cancelJobIfRunning(result);
    }

    @ParameterizedTest(name = "incrementalSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testStartupFromLatestOffset(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        initializeDb2Table("inventory", "PRODUCTS");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " ID INT NOT NULL,"
                                + " NAME STRING,"
                                + " DESCRIPTION STRING,"
                                + " WEIGHT DECIMAL(10,3),"
                                + " PRIMARY KEY (ID) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'db2-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s' ,"
                                + " 'scan.startup.mode' = 'latest-offset',"
                                + " 'scan.incremental.snapshot.enabled' = '%s'"
                                + ")",
                        DB2_CONTAINER.getHost(),
                        DB2_CONTAINER.getMappedPort(DB2_PORT),
                        DB2_CONTAINER.getUsername(),
                        DB2_CONTAINER.getPassword(),
                        DB2_CONTAINER.getDatabaseName(),
                        "DB2INST1.PRODUCTS",
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
        do {
            Thread.sleep(5000L);
        } while (result.getJobClient().get().getJobStatus().get() != RUNNING);
        Thread.sleep(30000L);
        LOG.info("Snapshot should end and start to read binlog.");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS VALUES (default,'jacket','water resistent white wind breaker',0.2)");
            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS VALUES (default,'scooter','Big 2-wheel scooter ',5.18)");
            statement.execute(
                    "UPDATE DB2INST1.PRODUCTS SET DESCRIPTION='new water resistent white wind breaker', WEIGHT='0.5' WHERE ID=110");
            statement.execute("UPDATE DB2INST1.PRODUCTS SET WEIGHT='5.17' WHERE ID=111");
            statement.execute("DELETE FROM DB2INST1.PRODUCTS WHERE ID=111");
        }

        waitForSinkSize("sink", 5);

        String[] expected =
                new String[] {"110,jacket,new water resistent white wind breaker,0.500"};

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        cancelJobIfRunning(result);
    }

    @ParameterizedTest(name = "incrementalSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testMetadataColumns(boolean incrementalSnapshot) throws Throwable {
        setup(incrementalSnapshot);
        initializeDb2Table("inventory", "PRODUCTS");
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
                                + " 'connector' = 'db2-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s'"
                                + ")",
                        DB2_CONTAINER.getHost(),
                        DB2_CONTAINER.getMappedPort(DB2_PORT),
                        DB2_CONTAINER.getUsername(),
                        DB2_CONTAINER.getPassword(),
                        DB2_CONTAINER.getDatabaseName(),
                        "DB2INST1.PRODUCTS",
                        incrementalSnapshot);
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " database_name STRING,"
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " id int,"
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

        waitForSnapshotStarted("sink");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE DB2INST1.PRODUCTS SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106;");
            statement.execute("UPDATE DB2INST1.PRODUCTS SET WEIGHT='5.1' WHERE ID=107;");
            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS VALUES (110,'jacket','water resistent white wind breaker',0.2);");
            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS VALUES (111,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE DB2INST1.PRODUCTS SET DESCRIPTION='new water resistent white wind breaker', WEIGHT='0.5' WHERE ID=110;");
            statement.execute("UPDATE DB2INST1.PRODUCTS SET WEIGHT='5.17' WHERE ID=111;");
            statement.execute("DELETE FROM DB2INST1.PRODUCTS WHERE ID=111;");
        }

        waitForSinkSize("sink", 16);

        List<String> expected =
                Arrays.asList(
                        "+I(testdb,DB2INST1,PRODUCTS,101,scooter,Small 2-wheel scooter,3.140)",
                        "+I(testdb,DB2INST1,PRODUCTS,102,car battery,12V car battery,8.100)",
                        "+I(testdb,DB2INST1,PRODUCTS,103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.800)",
                        "+I(testdb,DB2INST1,PRODUCTS,104,hammer,12oz carpenter's hammer,0.750)",
                        "+I(testdb,DB2INST1,PRODUCTS,105,hammer,14oz carpenter's hammer,0.875)",
                        "+I(testdb,DB2INST1,PRODUCTS,106,hammer,16oz carpenter's hammer,1.000)",
                        "+I(testdb,DB2INST1,PRODUCTS,107,rocks,box of assorted rocks,5.300)",
                        "+I(testdb,DB2INST1,PRODUCTS,108,jacket,water resistent black wind breaker,0.100)",
                        "+I(testdb,DB2INST1,PRODUCTS,109,spare tire,24 inch spare tire,22.200)",
                        "+U(testdb,DB2INST1,PRODUCTS,106,hammer,18oz carpenter hammer,1.000)",
                        "+U(testdb,DB2INST1,PRODUCTS,107,rocks,box of assorted rocks,5.100)",
                        "+I(testdb,DB2INST1,PRODUCTS,110,jacket,water resistent white wind breaker,0.200)",
                        "+I(testdb,DB2INST1,PRODUCTS,111,scooter,Big 2-wheel scooter ,5.180)",
                        "+U(testdb,DB2INST1,PRODUCTS,110,jacket,new water resistent white wind breaker,0.500)",
                        "+U(testdb,DB2INST1,PRODUCTS,111,scooter,Big 2-wheel scooter ,5.170)",
                        "-D(testdb,DB2INST1,PRODUCTS,111,scooter,Big 2-wheel scooter ,5.170)");

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        cancelJobIfRunning(result);
    }

    private static void waitForSnapshotStarted(String sinkName) throws InterruptedException {
        while (sinkSize(sinkName) == 0) {
            Thread.sleep(1000L);
        }
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(1000L);
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
}
