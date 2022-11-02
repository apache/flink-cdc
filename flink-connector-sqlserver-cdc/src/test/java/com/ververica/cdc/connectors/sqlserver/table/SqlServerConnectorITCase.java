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

package com.ververica.cdc.connectors.sqlserver.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.utils.LegacyRowResource;

import com.ververica.cdc.connectors.sqlserver.SqlServerTestBase;
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
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Integration tests for SqlServer Table source. */
public class SqlServerConnectorITCase extends SqlServerTestBase {

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
        initializeSqlServerTable("inventory");
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
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        MSSQL_SERVER_CONTAINER.getHost(),
                        MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT),
                        MSSQL_SERVER_CONTAINER.getUsername(),
                        MSSQL_SERVER_CONTAINER.getPassword(),
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
    public void testAllTypes() throws Throwable {
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
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        MSSQL_SERVER_CONTAINER.getHost(),
                        MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT),
                        MSSQL_SERVER_CONTAINER.getUsername(),
                        MSSQL_SERVER_CONTAINER.getPassword(),
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
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEquals(expected, actual);

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testMetadataColumns() throws Throwable {
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
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        MSSQL_SERVER_CONTAINER.getHost(),
                        MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT),
                        MSSQL_SERVER_CONTAINER.getUsername(),
                        MSSQL_SERVER_CONTAINER.getPassword(),
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
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        Collections.sort(actual);
        Collections.sort(expected);
        assertEquals(expected, actual);
        result.getJobClient().get().cancel().get();
    }
}
