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

package org.apache.flink.cdc.connectors.postgres.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.utils.StaticExternalResourceProxy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.utils.LegacyRowResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Integration tests for PostgreSQL Table source. */
class PostgreSQLConnectorITCase extends PostgresTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @RegisterExtension
    public static StaticExternalResourceProxy<LegacyRowResource> usesLegacyRows =
            new StaticExternalResourceProxy<>(LegacyRowResource.INSTANCE);

    void setup(boolean parallelismSnapshot) {
        TestValuesTableFactory.clearAllData();
        env.setRestartStrategy(RestartStrategies.noRestart());
        if (parallelismSnapshot) {
            env.setParallelism(4);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testConsumingAllEvents(boolean parallelismSnapshot)
            throws SQLException, ExecutionException, InterruptedException {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        parallelismSnapshot,
                        getSlotName());
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

        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        waitForSinkSize("sink", 20);

        /*
         * <pre>
         * The final database table looks like this:
         *
         * > SELECT * FROM products;
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
         * | id | name | description | weight |
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
         * | 101 | scooter | Small 2-wheel scooter | 3.14 |
         * | 102 | car battery | 12V car battery | 8.1 |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from
         * #40 to #3 | 0.8 |
         * | 104 | hammer | 12oz carpenter's hammer | 0.75 |
         * | 105 | hammer | 14oz carpenter's hammer | 0.875 |
         * | 106 | hammer | 18oz carpenter hammer | 1 |
         * | 107 | rocks | box of assorted rocks | 5.1 |
         * | 108 | jacket | water resistent black wind breaker | 0.1 |
         * | 109 | spare tire | 24 inch spare tire | 22.2 |
         * | 110 | jacket | new water resistent white wind breaker | 0.5 |
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
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

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true})
    void testStartupFromLatestOffset(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'slot.name' = '%s',"
                                + " 'scan.startup.mode' = 'latest-offset'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        parallelismSnapshot,
                        getSlotName());
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
        // wait for the source startup, we don't have a better way to wait it, use sleep
        // for now
        Thread.sleep(10000L);

        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        waitForSinkSize("sink", 5);

        String[] expected =
                new String[] {"110,jacket,new water resistent white wind breaker,0.500"};

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExceptionForReplicaIdentity(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "replica_identity");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        parallelismSnapshot,
                        getSlotName());
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

        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        Assertions.assertThatThrownBy(result::await)
                .hasStackTraceContaining(
                        "The \"before\" field of UPDATE/DELETE message is null, "
                                + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAllTypes(boolean parallelismSnapshot) throws Throwable {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "column_type_test");

        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types ("
                                + "    id INTEGER NOT NULL,"
                                + "    bytea_c BYTES,"
                                + "    small_c SMALLINT,"
                                + "    int_c INTEGER,"
                                + "    big_c BIGINT,"
                                + "    real_c FLOAT,"
                                + "    double_precision DOUBLE,"
                                + "    numeric_c DECIMAL(10, 5),"
                                + "    decimal_c DECIMAL(10, 1),"
                                + "    boolean_c BOOLEAN,"
                                + "    text_c STRING,"
                                + "    char_c STRING,"
                                + "    character_c STRING,"
                                + "    character_varying_c STRING,"
                                + "    timestamp3_c TIMESTAMP(3),"
                                + "    timestamp6_c TIMESTAMP(6),"
                                + "    date_c DATE,"
                                + "    time_c TIME(0),"
                                + "    default_numeric_c DECIMAL,"
                                + "    geography_c STRING,"
                                + "    geometry_c STRING"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "full_types",
                        parallelismSnapshot,
                        getSlotName());
        String sinkDDL =
                "CREATE TABLE sink ("
                        + "    id INTEGER NOT NULL,"
                        + "    bytea_c BYTES,"
                        + "    small_c SMALLINT,"
                        + "    int_c INTEGER,"
                        + "    big_c BIGINT,"
                        + "    real_c FLOAT,"
                        + "    double_precision DOUBLE,"
                        + "    numeric_c DECIMAL(10, 5),"
                        + "    decimal_c DECIMAL(10, 1),"
                        + "    boolean_c BOOLEAN,"
                        + "    text_c STRING,"
                        + "    char_c STRING,"
                        + "    character_c STRING,"
                        + "    character_varying_c STRING,"
                        + "    timestamp3_c TIMESTAMP(3),"
                        + "    timestamp6_c TIMESTAMP(6),"
                        + "    date_c DATE,"
                        + "    time_c TIME(0),"
                        + "    default_numeric_c DECIMAL,"
                        + "    geography_c STRING,"
                        + "    geometry_c STRING,"
                        + "    PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM full_types");

        waitForSinkSize("sink", 1);
        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE inventory.full_types SET small_c=0 WHERE id=1;");
        }

        waitForSinkSize("sink", 3);

        List<String> expected =
                Arrays.asList(
                        "+I(1,[50],32767,65535,2147483647,5.5,6.6,123.12345,404.4,true,Hello World,a,abc,abcd..xyz,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17,18:00:22,500,{\"hexewkb\":\"0105000020e610000001000000010200000002000000a779c7293a2465400b462575025a46c0c66d3480b7fc6440c3d32b65195246c0\",\"srid\":4326},{\"hexewkb\":\"0101000020730c00001c7c613255de6540787aa52c435c42c0\",\"srid\":3187})",
                        "-D(1,[50],32767,65535,2147483647,5.5,6.6,123.12345,404.4,true,Hello World,a,abc,abcd..xyz,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17,18:00:22,500,{\"hexewkb\":\"0105000020e610000001000000010200000002000000a779c7293a2465400b462575025a46c0c66d3480b7fc6440c3d32b65195246c0\",\"srid\":4326},{\"hexewkb\":\"0101000020730c00001c7c613255de6540787aa52c435c42c0\",\"srid\":3187})",
                        "+I(1,[50],0,65535,2147483647,5.5,6.6,123.12345,404.4,true,Hello World,a,abc,abcd..xyz,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17,18:00:22,500,{\"hexewkb\":\"0105000020e610000001000000010200000002000000a779c7293a2465400b462575025a46c0c66d3480b7fc6440c3d32b65195246c0\",\"srid\":4326},{\"hexewkb\":\"0101000020730c00001c7c613255de6540787aa52c435c42c0\",\"srid\":3187})");
        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual).isEqualTo(expected);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMetadataColumns(boolean parallelismSnapshot) throws Throwable {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source  ("
                                + " db_name STRING METADATA FROM 'database_name' VIRTUAL,"
                                + " schema_name STRING METADATA VIRTUAL,"
                                + " table_name STRING METADATA VIRTUAL,"
                                + " row_kind STRING METADATA FROM 'row_kind' VIRTUAL,"
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        parallelismSnapshot,
                        getSlotName());

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " database_name STRING,"
                        + " schema_name STRING,"
                        + " table_name STRING,"
                        + " row_kind STRING,"
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

        // sync submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");

        waitForSnapshotStarted("sink");
        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        // waiting for change events finished.
        waitForSinkSize("sink", 16);
        String databaseName = POSTGRES_CONTAINER.getDatabaseName();

        List<String> expected =
                Arrays.asList(
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,101,scooter,Small 2-wheel scooter,3.140)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,102,car battery,12V car battery,8.100)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.800)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,104,hammer,12oz carpenter's hammer,0.750)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,105,hammer,14oz carpenter's hammer,0.875)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,106,hammer,16oz carpenter's hammer,1.000)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,107,rocks,box of assorted rocks,5.300)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,108,jacket,water resistent black wind breaker,0.100)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,109,spare tire,24 inch spare tire,22.200)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,110,jacket,water resistent white wind breaker,0.200)",
                        "+I("
                                + databaseName
                                + ",inventory,products,+I,111,scooter,Big 2-wheel scooter ,5.180)",
                        "+U("
                                + databaseName
                                + ",inventory,products,+U,106,hammer,18oz carpenter hammer,1.000)",
                        "+U("
                                + databaseName
                                + ",inventory,products,+U,107,rocks,box of assorted rocks,5.100)",
                        "+U("
                                + databaseName
                                + ",inventory,products,+U,110,jacket,new water resistent white wind breaker,0.500)",
                        "+U("
                                + databaseName
                                + ",inventory,products,+U,111,scooter,Big 2-wheel scooter ,5.170)",
                        "-D("
                                + databaseName
                                + ",inventory,products,-D,111,scooter,Big 2-wheel scooter ,5.170)");
        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUpsertMode(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        initializePostgresTable(POSTGRES_CONTAINER, "replica_identity");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'slot.name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'changelog-mode' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        getSlotName(),
                        parallelismSnapshot,
                        "upsert");
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

        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE inventory.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.products WHERE id=111;");
        }

        waitForSinkSize("sink", 20);

        /*
         * <pre>
         * The final database table looks like this:
         *
         * > SELECT * FROM products;
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
         * | id | name | description | weight |
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
         * | 101 | scooter | Small 2-wheel scooter | 3.14 |
         * | 102 | car battery | 12V car battery | 8.1 |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from
         * #40 to #3 | 0.8 |
         * | 104 | hammer | 12oz carpenter's hammer | 0.75 |
         * | 105 | hammer | 14oz carpenter's hammer | 0.875 |
         * | 106 | hammer | 18oz carpenter hammer | 1 |
         * | 107 | rocks | box of assorted rocks | 5.1 |
         * | 108 | jacket | water resistent black wind breaker | 0.1 |
         * | 109 | spare tire | 24 inch spare tire | 22.2 |
         * | 110 | jacket | new water resistent white wind breaker | 0.5 |
         * +-----+--------------------+-------------------------------------------------
         * --------+--------+
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

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUniqueIndexIncludingFunction(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        // Clear the influence of usesLegacyRows which set USE_LEGACY_TO_STRING = true.
        // In this test, print +I,-U, +U to see more clearly.
        RowUtils.USE_LEGACY_TO_STRING = false;
        initializePostgresTable(POSTGRES_CONTAINER, "index_type_test");

        String sourceDDL =
                String.format(
                        "CREATE TABLE functional_unique_index ("
                                + "    id INTEGER NOT NULL,"
                                + "    char_c STRING"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                // In the snapshot phase of increment snapshot mode, table without
                                // primary key is not allowed now.Thus, when
                                // scan.incremental.snapshot.enabled = true, use 'latest-offset'
                                // startup mode.
                                + (parallelismSnapshot
                                        ? " 'scan.startup.mode' = 'latest-offset',"
                                        : "")
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGRES_CONTAINER.getHost(),
                        POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGRES_CONTAINER.getUsername(),
                        POSTGRES_CONTAINER.getPassword(),
                        POSTGRES_CONTAINER.getDatabaseName(),
                        "indexes",
                        "functional_unique_index",
                        parallelismSnapshot,
                        getSlotName());
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult tableResult = tEnv.executeSql("SELECT * FROM functional_unique_index");
        List<String> expected = new ArrayList<>();
        if (!parallelismSnapshot) {
            expected.add("+I[1, a]");
        }

        // wait a bit to make sure the replication slot is ready
        Thread.sleep(5000L);

        // generate WAL
        try (Connection connection = getJdbcConnection(POSTGRES_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE indexes.functional_unique_index SET char_c=NULL WHERE id=1;");
        }

        expected.addAll(Arrays.asList("-U[1, a]", "+U[1, null]"));
        CloseableIterator<Row> iterator = tableResult.collect();
        assertEqualsInAnyOrder(expected, fetchRows(iterator, expected.size()));
        tableResult.getJobClient().get().cancel().get();
        RowUtils.USE_LEGACY_TO_STRING = true;
    }
}
