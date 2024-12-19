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

package org.apache.flink.cdc.connectors.sqlserver.table;

import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.utils.StaticExternalResourceProxy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.utils.LegacyRowResource;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Integration tests for SqlServer Table source. */
class SqlServerConnectorITCase extends SqlServerTestBase {

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
        initializeSqlServerTable("inventory");
        initializeSqlServerTable("product");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'sqlserver-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'debezium.database.history.store.only.captured.tables.ddl' = 'true',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        MSSQL_SERVER_CONTAINER.getHost(),
                        MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT),
                        MSSQL_SERVER_CONTAINER.getUsername(),
                        MSSQL_SERVER_CONTAINER.getPassword(),
                        parallelismSnapshot,
                        "inventory",
                        "dbo.products");
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
                    "UPDATE inventory.dbo.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.dbo.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.dbo.products (name,description,weight) VALUES ('jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.dbo.products (name,description,weight) VALUES ('scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.dbo.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.dbo.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.dbo.products WHERE id=111;");

            statement.execute(
                    "INSERT INTO product.dbo.products (name,description,weight) VALUES ('scooter','Big 2-wheel scooter ',5.18);");
        }

        // test schema change
        // sqlserver online schema update refer:
        // https://debezium.io/documentation/reference/1.9/connectors/sqlserver.html#online-schema-updates
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute("USE inventory;");
            // modify the schema
            statement.execute("ALTER TABLE inventory.dbo.products ADD volume FLOAT;");
            // create the new capture instance
            statement.execute(
                    "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'products', @role_name = NULL, @supports_net_changes = 0, @capture_instance = 'dbo_products_v2';");
            statement.execute("UPDATE inventory.dbo.products SET volume='1.2' WHERE id=110;");
        }

        waitForSinkSize("sink", 20);

        /*
         * <pre>
         * The final database table looks like this:
         *
         * > SELECT * FROM products;
         * +-----+--------------------+---------------------------------------------------------+--------+--------+
         * | id  | name               | description                                             | weight | volume |
         * +-----+--------------------+---------------------------------------------------------+--------+--------|
         * | 101 | scooter            | Small 2-wheel scooter                                   |   3.14 |   null |
         * | 102 | car battery        | 12V car battery                                         |    8.1 |   null |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |    0.8 |   null |
         * | 104 | hammer             | 12oz carpenter's hammer                                 |   0.75 |   null |
         * | 105 | hammer             | 14oz carpenter's hammer                                 |  0.875 |   null |
         * | 106 | hammer             | 18oz carpenter hammer                                   |      1 |   null |
         * | 107 | rocks              | box of assorted rocks                                   |    5.1 |   null |
         * | 108 | jacket             | water resistent black wind breaker                      |    0.1 |   null |
         * | 109 | spare tire         | 24 inch spare tire                                      |   22.2 |   null |
         * | 110 | jacket             | new water resistent white wind breaker                  |    0.5 |   1.2  |
         * +-----+--------------------+---------------------------------------------------------+--------+--------+
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
    void testStartupFromLatestOffset(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        initializeSqlServerTable("inventory");

        Connection connection = getJdbcConnection();
        Statement statement = connection.createStatement();

        // The following two change records will be discarded in the 'latest-offset' mode
        statement.execute(
                "INSERT INTO inventory.dbo.products (name,description,weight) VALUES ('jacket','water resistent white wind breaker',0.2);"); // 110
        statement.execute(
                "INSERT INTO inventory.dbo.products (name,description,weight) VALUES ('scooter','Big 2-wheel scooter ',5.18);");
        Thread.sleep(5000L);

        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3)"
                                + ") WITH ("
                                + " 'connector' = 'sqlserver-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'latest-offset'"
                                + ")",
                        MSSQL_SERVER_CONTAINER.getHost(),
                        MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT),
                        MSSQL_SERVER_CONTAINER.getUsername(),
                        MSSQL_SERVER_CONTAINER.getPassword(),
                        parallelismSnapshot,
                        "inventory",
                        "dbo.products");
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

        statement.execute(
                "INSERT INTO inventory.dbo.products (name,description,weight) VALUES ('hammer','18oz carpenters hammer',1.2);");
        statement.execute(
                "INSERT INTO inventory.dbo.products (name,description,weight) VALUES ('scooter','Big 3-wheel scooter',5.20);");

        waitForSinkSize("sink", 2);

        String[] expected =
                new String[] {
                    "112,hammer,18oz carpenters hammer,1.200",
                    "113,scooter,Big 3-wheel scooter,5.200"
                };

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAllTypes(boolean parallelismSnapshot) throws Throwable {
        setup(parallelismSnapshot);
        initializeSqlServerTable("column_type_test");

        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types (\n"
                                + "    id int NOT NULL,\n"
                                + "    val_char CHAR(3),\n"
                                + "    val_varchar VARCHAR(1000),\n"
                                + "    val_text STRING,\n"
                                + "    val_nchar CHAR(3),\n"
                                + "    val_nvarchar VARCHAR(1000),\n"
                                + "    val_ntext STRING,\n"
                                + "    val_decimal DECIMAL(6,3),\n"
                                + "    val_numeric NUMERIC,\n"
                                + "    val_float DOUBLE,\n"
                                + "    val_real FLOAT,\n"
                                + "    val_smallmoney DECIMAL,\n"
                                + "    val_money DECIMAL,\n"
                                + "    val_bit BOOLEAN,\n"
                                + "    val_tinyint SMALLINT,\n"
                                + "    val_smallint SMALLINT,\n"
                                + "    val_int INT,\n"
                                + "    val_bigint BIGINT,\n"
                                + "    val_date DATE,\n"
                                + "    val_time_p2 TIME(0),\n"
                                + "    val_time TIME(0),\n"
                                + "    val_datetime2 TIMESTAMP,\n"
                                + "    val_datetimeoffset TIMESTAMP_LTZ(3),\n"
                                + "    val_datetime TIMESTAMP,\n"
                                + "    val_smalldatetime TIMESTAMP,\n"
                                + "    val_xml STRING\n"
                                + ") WITH ("
                                + " 'connector' = 'sqlserver-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        MSSQL_SERVER_CONTAINER.getHost(),
                        MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT),
                        MSSQL_SERVER_CONTAINER.getUsername(),
                        MSSQL_SERVER_CONTAINER.getPassword(),
                        parallelismSnapshot,
                        "column_type_test",
                        "dbo.full_types");
        String sinkDDL =
                "CREATE TABLE sink (\n"
                        + "    id int NOT NULL,\n"
                        + "    val_char CHAR(3),\n"
                        + "    val_varchar VARCHAR(1000),\n"
                        + "    val_text STRING,\n"
                        + "    val_nchar CHAR(3),\n"
                        + "    val_nvarchar VARCHAR(1000),\n"
                        + "    val_ntext STRING,\n"
                        + "    val_decimal DECIMAL(6,3),\n"
                        + "    val_numeric NUMERIC,\n"
                        + "    val_float DOUBLE,\n"
                        + "    val_real FLOAT,\n"
                        + "    val_smallmoney DECIMAL,\n"
                        + "    val_money DECIMAL,\n"
                        + "    val_bit BOOLEAN,\n"
                        + "    val_tinyint SMALLINT,\n"
                        + "    val_smallint SMALLINT,\n"
                        + "    val_int INT,\n"
                        + "    val_bigint BIGINT,\n"
                        + "    val_date DATE,\n"
                        + "    val_time_p2 TIME(0),\n"
                        + "    val_time TIME(0),\n"
                        + "    val_datetime2 TIMESTAMP,\n"
                        + "    val_datetimeoffset TIMESTAMP_LTZ(3),\n"
                        + "    val_datetime TIMESTAMP,\n"
                        + "    val_smalldatetime TIMESTAMP,\n"
                        + "    val_xml STRING\n,"
                        + "    PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM full_types");

        waitForSnapshotStarted("sink");

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE column_type_test.dbo.full_types SET val_int=8888 WHERE id=0;");
        }

        waitForSinkSize("sink", 2);

        List<String> expected =
                Arrays.asList(
                        "+I(0,cc ,vcc,tc,cč ,vcč,tč,1.123,2,3.323,4.323,5,6,true,22,333,4444,55555,2018-07-13,10:23:45.680,10:23:45.678,2018-07-13T11:23:45.340,2018-07-13T01:23:45.456Z,2018-07-13T13:23:45.780,2018-07-13T14:24,<a>b</a>)",
                        "+U(0,cc ,vcc,tc,cč ,vcč,tč,1.123,2,3.323,4.323,5,6,true,22,333,8888,55555,2018-07-13,10:23:45.680,10:23:45.679,2018-07-13T11:23:45.340,2018-07-13T01:23:45.456Z,2018-07-13T13:23:45.780,2018-07-13T14:24,<a>b</a>)");
        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMetadataColumns(boolean parallelismSnapshot) throws Throwable {
        setup(parallelismSnapshot);
        initializeSqlServerTable("inventory");
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
                                + " 'connector' = 'sqlserver-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        MSSQL_SERVER_CONTAINER.getHost(),
                        MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT),
                        MSSQL_SERVER_CONTAINER.getUsername(),
                        MSSQL_SERVER_CONTAINER.getPassword(),
                        parallelismSnapshot,
                        "inventory",
                        "dbo.products");

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
                    "UPDATE inventory.dbo.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE inventory.dbo.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO inventory.dbo.products VALUES ('jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO inventory.dbo.products VALUES ('scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE inventory.dbo.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE inventory.dbo.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM inventory.dbo.products WHERE id=111;");
        }

        // waiting for change events finished.
        waitForSinkSize("sink", 16);

        List<String> expected =
                Arrays.asList(
                        "+I(inventory,dbo,products,101,scooter,Small 2-wheel scooter,3.140)",
                        "+I(inventory,dbo,products,102,car battery,12V car battery,8.100)",
                        "+I(inventory,dbo,products,103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.800)",
                        "+I(inventory,dbo,products,104,hammer,12oz carpenter's hammer,0.750)",
                        "+I(inventory,dbo,products,105,hammer,14oz carpenter's hammer,0.875)",
                        "+I(inventory,dbo,products,106,hammer,16oz carpenter's hammer,1.000)",
                        "+I(inventory,dbo,products,107,rocks,box of assorted rocks,5.300)",
                        "+I(inventory,dbo,products,108,jacket,water resistent black wind breaker,0.100)",
                        "+I(inventory,dbo,products,109,spare tire,24 inch spare tire,22.200)",
                        "+I(inventory,dbo,products,110,jacket,water resistent white wind breaker,0.200)",
                        "+I(inventory,dbo,products,111,scooter,Big 2-wheel scooter ,5.180)",
                        "+U(inventory,dbo,products,106,hammer,18oz carpenter hammer,1.000)",
                        "+U(inventory,dbo,products,107,rocks,box of assorted rocks,5.100)",
                        "+U(inventory,dbo,products,110,jacket,new water resistent white wind breaker,0.500)",
                        "+U(inventory,dbo,products,111,scooter,Big 2-wheel scooter ,5.170)",
                        "-D(inventory,dbo,products,111,scooter,Big 2-wheel scooter ,5.170)");
        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true})
    void testCompositePkTableSplitsUnevenlyWithChunkKeyColumn(boolean parallelismSnapshot)
            throws InterruptedException, ExecutionException {
        setup(parallelismSnapshot);
        testUseChunkColumn("product_kind", parallelismSnapshot);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true})
    void testCompositePkTableSplitsEvenlyWithChunkKeyColumn(boolean parallelismSnapshot)
            throws ExecutionException, InterruptedException {
        setup(parallelismSnapshot);
        testUseChunkColumn("product_no", parallelismSnapshot);
    }

    private void testUseChunkColumn(String chunkColumn, boolean parallelismSnapshot)
            throws InterruptedException, ExecutionException {
        initializeSqlServerTable("customer");
        String sourceDDL =
                String.format(
                        "CREATE TABLE evenly_shopping_cart (\n"
                                + "    product_no INT NOT NULL,\n"
                                + "    product_kind VARCHAR(255),\n"
                                + "    user_id VARCHAR(255) NOT NULL,\n"
                                + "    description VARCHAR(255) NOT NULL\n"
                                + ") WITH ("
                                + " 'connector' = 'sqlserver-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.key-column' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        MSSQL_SERVER_CONTAINER.getHost(),
                        MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT),
                        MSSQL_SERVER_CONTAINER.getUsername(),
                        MSSQL_SERVER_CONTAINER.getPassword(),
                        parallelismSnapshot,
                        chunkColumn,
                        4,
                        "customer",
                        "dbo.evenly_shopping_cart");
        String sinkDDL =
                "CREATE TABLE sink "
                        + " WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ") LIKE evenly_shopping_cart (EXCLUDING OPTIONS)";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM evenly_shopping_cart");
        waitForSinkSize("sink", 12);
        result.getJobClient().get().cancel().get();
    }
}
