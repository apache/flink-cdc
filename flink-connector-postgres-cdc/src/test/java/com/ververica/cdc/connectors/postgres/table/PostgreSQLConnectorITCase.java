/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.postgres.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.utils.LegacyRowResource;
import org.apache.flink.util.ExceptionUtils;

import com.ververica.cdc.connectors.postgres.PostgresTestBase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Integration tests for PostgreSQL Table source. */
public class PostgreSQLConnectorITCase extends PostgresTestBase {
    private static final String SLOT_NAME = "flinktest";

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @ClassRule public static LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(1);
    }

    @Test
    public void testConsumingAllEvents()
            throws SQLException, ExecutionException, InterruptedException {
        initializePostgresTable("inventory");
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
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGERS_CONTAINER.getHost(),
                        POSTGERS_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGERS_CONTAINER.getUsername(),
                        POSTGERS_CONTAINER.getPassword(),
                        POSTGERS_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        SLOT_NAME);
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

        try (Connection connection = getJdbcConnection();
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
                    "scooter,3.140",
                    "car battery,8.100",
                    "12-pack drill bits,0.800",
                    "hammer,2.625",
                    "rocks,5.100",
                    "jacket,0.600",
                    "spare tire,22.200"
                };

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertThat(actual, containsInAnyOrder(expected));

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testExceptionForReplicaIdentity() throws Exception {
        initializePostgresTable("replica_identity");
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
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGERS_CONTAINER.getHost(),
                        POSTGERS_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGERS_CONTAINER.getUsername(),
                        POSTGERS_CONTAINER.getPassword(),
                        POSTGERS_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        "replica_identity_slot");
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

        try (Connection connection = getJdbcConnection();
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

        try {
            result.await();
        } catch (Exception e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e,
                                    "The \"before\" field of UPDATE/DELETE message is null, "
                                            + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.")
                            .isPresent());
        }
    }

    @Test
    public void testAllTypes() throws Throwable {
        initializePostgresTable("column_type_test");

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
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGERS_CONTAINER.getHost(),
                        POSTGERS_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGERS_CONTAINER.getUsername(),
                        POSTGERS_CONTAINER.getPassword(),
                        POSTGERS_CONTAINER.getDatabaseName(),
                        "inventory",
                        "full_types",
                        SLOT_NAME);
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
                        + "    geometry_c STRING"
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
            statement.execute("UPDATE inventory.full_types SET small_c=0 WHERE id=1;");
        }

        waitForSinkSize("sink", 3);

        List<String> expected =
                Arrays.asList(
                        "+I(1,[50],32767,65535,2147483647,5.5,6.6,123.12345,404.4,true,Hello World,a,abc,abcd..xyz,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17,18:00:22,500,{\"hexewkb\":\"0105000020e610000001000000010200000002000000a779c7293a2465400b462575025a46c0c66d3480b7fc6440c3d32b65195246c0\",\"srid\":4326},{\"hexewkb\":\"0101000020730c00001c7c613255de6540787aa52c435c42c0\",\"srid\":3187})",
                        "-U(1,[50],32767,65535,2147483647,5.5,6.6,123.12345,404.4,true,Hello World,a,abc,abcd..xyz,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17,18:00:22,500,{\"hexewkb\":\"0105000020e610000001000000010200000002000000a779c7293a2465400b462575025a46c0c66d3480b7fc6440c3d32b65195246c0\",\"srid\":4326},{\"hexewkb\":\"0101000020730c00001c7c613255de6540787aa52c435c42c0\",\"srid\":3187})",
                        "+U(1,[50],0,65535,2147483647,5.5,6.6,123.12345,404.4,true,Hello World,a,abc,abcd..xyz,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17,18:00:22,500,{\"hexewkb\":\"0105000020e610000001000000010200000002000000a779c7293a2465400b462575025a46c0c66d3480b7fc6440c3d32b65195246c0\",\"srid\":4326},{\"hexewkb\":\"0101000020730c00001c7c613255de6540787aa52c435c42c0\",\"srid\":3187})");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEquals(expected, actual);

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testMetadataColumns() throws Throwable {
        initializePostgresTable("inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source  ("
                                + " db_name STRING METADATA FROM 'database_name' VIRTUAL,"
                                + " schema_name STRING METADATA VIRTUAL,"
                                + " table_name STRING METADATA VIRTUAL,"
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
                                + " 'slot.name' = '%s'"
                                + ")",
                        POSTGERS_CONTAINER.getHost(),
                        POSTGERS_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGERS_CONTAINER.getUsername(),
                        POSTGERS_CONTAINER.getPassword(),
                        POSTGERS_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        "meta_data_slot");

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

        // sync submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");

        waitForSnapshotStarted("sink");

        try (Connection connection = getJdbcConnection();
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

        List<String> expected =
                Arrays.asList(
                        "+I(postgres,inventory,products,101,scooter,Small 2-wheel scooter,3.140)",
                        "+I(postgres,inventory,products,102,car battery,12V car battery,8.100)",
                        "+I(postgres,inventory,products,103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.800)",
                        "+I(postgres,inventory,products,104,hammer,12oz carpenter's hammer,0.750)",
                        "+I(postgres,inventory,products,105,hammer,14oz carpenter's hammer,0.875)",
                        "+I(postgres,inventory,products,106,hammer,16oz carpenter's hammer,1.000)",
                        "+I(postgres,inventory,products,107,rocks,box of assorted rocks,5.300)",
                        "+I(postgres,inventory,products,108,jacket,water resistent black wind breaker,0.100)",
                        "+I(postgres,inventory,products,109,spare tire,24 inch spare tire,22.200)",
                        "+I(postgres,inventory,products,110,jacket,water resistent white wind breaker,0.200)",
                        "+I(postgres,inventory,products,111,scooter,Big 2-wheel scooter ,5.180)",
                        "+U(postgres,inventory,products,106,hammer,18oz carpenter hammer,1.000)",
                        "+U(postgres,inventory,products,107,rocks,box of assorted rocks,5.100)",
                        "+U(postgres,inventory,products,110,jacket,new water resistent white wind breaker,0.500)",
                        "+U(postgres,inventory,products,111,scooter,Big 2-wheel scooter ,5.170)",
                        "-D(postgres,inventory,products,111,scooter,Big 2-wheel scooter ,5.170)");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        Collections.sort(actual);
        Collections.sort(expected);
        assertEquals(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testUpsertMode() throws Exception {
        initializePostgresTable("replica_identity");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL PRIMARY KEY,"
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
                                + " 'slot.name' = '%s',"
                                + " 'changelog-mode' = '%s'"
                                + ")",
                        POSTGERS_CONTAINER.getHost(),
                        POSTGERS_CONTAINER.getMappedPort(POSTGRESQL_PORT),
                        POSTGERS_CONTAINER.getUsername(),
                        POSTGERS_CONTAINER.getPassword(),
                        POSTGERS_CONTAINER.getDatabaseName(),
                        "inventory",
                        "products",
                        "replica_identity_slot",
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

        try (Connection connection = getJdbcConnection();
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
                    "scooter,3.140",
                    "car battery,8.100",
                    "12-pack drill bits,0.800",
                    "hammer,2.625",
                    "rocks,5.100",
                    "jacket,0.600",
                    "spare tire,22.200"
                };

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertThat(actual, containsInAnyOrder(expected));

        result.getJobClient().get().cancel().get();
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
}
