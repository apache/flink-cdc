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

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.utils.LegacyRowResource;

import com.ververica.cdc.connectors.tidb.TiDBTestBase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Integration tests for TiDB change stream event SQL source. */
public class TiDBConnectorITCase extends TiDBTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBConnectorITCase.class);
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
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
    public void testConsumingAllEvents() throws Exception {
        initializeTidbTable("inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE tidb_source ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'tidb-cdc',"
                                + " 'tikv.grpc.timeout_in_ms' = '20000',"
                                + " 'pd-addresses' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN),
                        "inventory",
                        "products");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM tidb_source");

        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", 9);

        try (Connection connection = getJdbcConnection("inventory");
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

        waitForSinkSize("sink", 16);

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

        List<String> expected =
                Arrays.asList(
                        "+I(101,scooter,Small 2-wheel scooter,3.1400000000)",
                        "+I(102,car battery,12V car battery,8.1000000000)",
                        "+I(103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000)",
                        "+I(104,hammer,12oz carpenter's hammer,0.7500000000)",
                        "+I(105,hammer,14oz carpenter's hammer,0.8750000000)",
                        "+I(106,hammer,16oz carpenter's hammer,1.0000000000)",
                        "+I(107,rocks,box of assorted rocks,5.3000000000)",
                        "+I(108,jacket,water resistent black wind breaker,0.1000000000)",
                        "+I(109,spare tire,24 inch spare tire,22.2000000000)",
                        "+U(106,hammer,18oz carpenter hammer,1.0000000000)",
                        "+U(107,rocks,box of assorted rocks,5.1000000000)",
                        "+I(110,jacket,water resistent white wind breaker,0.2000000000)",
                        "+I(111,scooter,Big 2-wheel scooter ,5.1800000000)",
                        "+U(110,jacket,new water resistent white wind breaker,0.5000000000)",
                        "+U(111,scooter,Big 2-wheel scooter ,5.1700000000)",
                        "-D(111,scooter,Big 2-wheel scooter ,5.1700000000)");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEqualsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testDeleteColumn() throws Exception {
        initializeTidbTable("inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE tidb_source ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'tidb-cdc',"
                                + " 'tikv.grpc.timeout_in_ms' = '20000',"
                                + " 'pd-addresses' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN),
                        "inventory",
                        "products");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM tidb_source");

        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", 9);

        try (Connection connection = getJdbcConnection("inventory");
                Statement statement = connection.createStatement()) {

            statement.execute("ALTER TABLE products DROP COLUMN description");

            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            statement.execute("INSERT INTO products VALUES (default,'jacket',0.2);"); // 110
            statement.execute("INSERT INTO products VALUES (default,'scooter',5.18);"); // 111
            statement.execute("UPDATE products SET name='jacket2', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }

        waitForSinkSize("sink", 15);

        List<String> expected =
                Arrays.asList(
                        "+I(101,scooter,Small 2-wheel scooter,3.1400000000)",
                        "+I(102,car battery,12V car battery,8.1000000000)",
                        "+I(103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000)",
                        "+I(104,hammer,12oz carpenter's hammer,0.7500000000)",
                        "+I(105,hammer,14oz carpenter's hammer,0.8750000000)",
                        "+I(106,hammer,16oz carpenter's hammer,1.0000000000)",
                        "+I(107,rocks,box of assorted rocks,5.3000000000)",
                        "+I(108,jacket,water resistent black wind breaker,0.1000000000)",
                        "+I(109,spare tire,24 inch spare tire,22.2000000000)",
                        "+U(107,rocks,null,5.1000000000)",
                        "+I(110,jacket,null,0.2000000000)",
                        "+I(111,scooter,null,5.1800000000)",
                        "+U(110,jacket2,null,0.5000000000)",
                        "+U(111,scooter,null,5.1700000000)",
                        "-D(111,scooter,null,5.1700000000)");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEqualsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testAddColumn() throws Exception {
        initializeTidbTable("inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE tidb_source ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'tidb-cdc',"
                                + " 'tikv.grpc.timeout_in_ms' = '20000',"
                                + " 'pd-addresses' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN),
                        "inventory",
                        "products");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM tidb_source");

        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", 9);

        try (Connection connection = getJdbcConnection("inventory");
                Statement statement = connection.createStatement()) {

            statement.execute("ALTER TABLE products ADD COLUMN serialnum INTEGER");

            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2,null);"); // 110
            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18,1);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }

        waitForSinkSize("sink", 16);

        List<String> expected =
                Arrays.asList(
                        "+I(101,scooter,Small 2-wheel scooter,3.1400000000)",
                        "+I(102,car battery,12V car battery,8.1000000000)",
                        "+I(103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000)",
                        "+I(104,hammer,12oz carpenter's hammer,0.7500000000)",
                        "+I(105,hammer,14oz carpenter's hammer,0.8750000000)",
                        "+I(106,hammer,16oz carpenter's hammer,1.0000000000)",
                        "+I(107,rocks,box of assorted rocks,5.3000000000)",
                        "+I(108,jacket,water resistent black wind breaker,0.1000000000)",
                        "+I(109,spare tire,24 inch spare tire,22.2000000000)",
                        "+U(106,hammer,18oz carpenter hammer,1.0000000000)",
                        "+U(107,rocks,box of assorted rocks,5.1000000000)",
                        "+I(110,jacket,water resistent white wind breaker,0.2000000000)",
                        "+I(111,scooter,Big 2-wheel scooter ,5.1800000000)",
                        "+U(110,jacket,new water resistent white wind breaker,0.5000000000)",
                        "+U(111,scooter,Big 2-wheel scooter ,5.1700000000)",
                        "-D(111,scooter,Big 2-wheel scooter ,5.1700000000)");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEqualsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testMetadataColumns() throws Exception {
        initializeTidbTable("inventory");

        String sourceDDL =
                String.format(
                        "CREATE TABLE tidb_source ("
                                + " db_name STRING METADATA FROM 'database_name' VIRTUAL,"
                                + " table_name STRING METADATA VIRTUAL,"
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(20, 10),"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'tidb-cdc',"
                                + " 'tikv.grpc.timeout_in_ms' = '20000',"
                                + " 'pd-addresses' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN),
                        "inventory",
                        "products");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " database_name STRING,"
                        + " table_name STRING,"
                        + " `id` DECIMAL(20, 0) NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(20, 10),"
                        + " primary key (database_name, table_name, id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM tidb_source");

        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", 9);

        try (Connection connection = getJdbcConnection("inventory");
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
        }

        waitForSinkSize("sink", 10);

        List<String> expected =
                Arrays.asList(
                        "+I(inventory,products,101,scooter,Small 2-wheel scooter,3.1400000000)",
                        "+I(inventory,products,102,car battery,12V car battery,8.1000000000)",
                        "+I(inventory,products,103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8000000000)",
                        "+I(inventory,products,104,hammer,12oz carpenter's hammer,0.7500000000)",
                        "+I(inventory,products,105,hammer,14oz carpenter's hammer,0.8750000000)",
                        "+I(inventory,products,106,hammer,16oz carpenter's hammer,1.0000000000)",
                        "+I(inventory,products,107,rocks,box of assorted rocks,5.3000000000)",
                        "+I(inventory,products,108,jacket,water resistent black wind breaker,0.1000000000)",
                        "+I(inventory,products,109,spare tire,24 inch spare tire,22.2000000000)",
                        "+U(inventory,products,106,hammer,18oz carpenter hammer,1.0000000000)",
                        "-U(inventory,products,106,hammer,16oz carpenter's hammer,1.0000000000)");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEqualsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testAllDataTypes() throws Throwable {
        try (Connection connection = getJdbcConnection("");
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("SET GLOBAL time_zone = '%s';", "UTC"));
        }
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        initializeTidbTable("column_type_test");
        String sourceDDL =
                String.format(
                        "CREATE TABLE tidb_source (\n"
                                + "    `id` INT NOT NULL,\n"
                                + "    tiny_c TINYINT,\n"
                                + "    tiny_un_c SMALLINT ,\n"
                                + "    small_c SMALLINT,\n"
                                + "    small_un_c INT,\n"
                                + "    medium_c INT,\n"
                                + "    medium_un_c INT,\n"
                                + "    int_c INT ,\n"
                                + "    int_un_c BIGINT,\n"
                                + "    int11_c INT,\n"
                                + "    big_c BIGINT,\n"
                                + "    big_un_c DECIMAL(20, 0),\n"
                                + "    varchar_c VARCHAR(255),\n"
                                + "    char_c CHAR(3),\n"
                                + "    real_c DOUBLE,\n"
                                + "    float_c FLOAT,\n"
                                + "    double_c DOUBLE,\n"
                                + "    decimal_c DECIMAL(8, 4),\n"
                                + "    numeric_c DECIMAL(6, 0),\n"
                                + "    big_decimal_c STRING,\n"
                                + "    bit1_c BOOLEAN,\n"
                                + "    tiny1_c BOOLEAN,\n"
                                + "    boolean_c BOOLEAN,\n"
                                + "    date_c DATE,\n"
                                + "    time_c TIME(0),\n"
                                + "    datetime3_c TIMESTAMP(3),\n"
                                + "    datetime6_c TIMESTAMP(6),\n"
                                + "    timestamp_c TIMESTAMP_LTZ,\n"
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
                                + "    primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'tidb-cdc',"
                                + " 'tikv.grpc.timeout_in_ms' = '20000',"
                                + " 'pd-addresses' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN),
                        "column_type_test",
                        "full_types");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + "    `id` INT NOT NULL,\n"
                        + "    tiny_c TINYINT,\n"
                        + "    tiny_un_c SMALLINT ,\n"
                        + "    small_c SMALLINT,\n"
                        + "    small_un_c INT,\n"
                        + "    medium_c INT,\n"
                        + "    medium_un_c INT,\n"
                        + "    int_c INT ,\n"
                        + "    int_un_c BIGINT,\n"
                        + "    int11_c INT,\n"
                        + "    big_c BIGINT,\n"
                        + "    big_un_c DECIMAL(20, 0),\n"
                        + "    varchar_c VARCHAR(255),\n"
                        + "    char_c CHAR(3),\n"
                        + "    real_c DOUBLE,\n"
                        + "    float_c FLOAT,\n"
                        + "    double_c DOUBLE,\n"
                        + "    decimal_c DECIMAL(8, 4),\n"
                        + "    numeric_c DECIMAL(6, 0),\n"
                        + "    big_decimal_c STRING,\n"
                        + "    bit1_c BOOLEAN,\n"
                        + "    tiny1_c BOOLEAN,\n"
                        + "    boolean_c BOOLEAN,\n"
                        + "    date_c DATE,\n"
                        + "    time_c TIME(0),\n"
                        + "    datetime3_c TIMESTAMP(3),\n"
                        + "    datetime6_c TIMESTAMP(6),\n"
                        + "    timestamp_c TIMESTAMP,\n"
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
                        + "    primary key (`id`) not enforced"
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
                        "INSERT INTO sink SELECT id, tiny_c, tiny_un_c, small_c, small_un_c, medium_c, medium_un_c, int_c, int_un_c, int11_c, big_c, big_un_c, varchar_c, char_c, real_c, float_c, double_c, decimal_c, numeric_c, big_decimal_c, bit1_c, tiny1_c, boolean_c, date_c, time_c, datetime3_c, datetime6_c, cast(timestamp_c as timestamp), file_uuid, bit_c, text_c, tiny_blob_c, blob_c, medium_blob_c, long_blob_c, year_c, enum_c, set_c, json_c FROM tidb_source");

        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", 1);

        try (Connection connection = getJdbcConnection("column_type_test");
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }

        waitForSinkSize("sink", 2);

        List<String> expected =
                Arrays.asList(
                        "+I(1,127,255,32767,65535,8388607,16777215,2147483647,4294967295,2147483647,9223372036854775807,18446744073709551615,Hello World,abc,123.102,123.102,404.4443,123.4567,346,34567892.1,false,true,true,2020-07-17,18:00:22,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17T18:00:22,[101, 26, -17, -65, -67, 8, 57, 15, 72, -17, -65, -67, -17, -65, -67, -17, -65, -67, 54, -17, -65, -67, 62, 123, 116, 0],[4, 4, 4, 4, 4, 4, 4, 4],text,[16],[16],[16],[16],2021,red,[a, b],{\"key1\":\"value1\"})",
                        "+U(1,127,255,32767,65535,8388607,16777215,2147483647,4294967295,2147483647,9223372036854775807,18446744073709551615,Hello World,abc,123.102,123.102,404.4443,123.4567,346,34567892.1,false,true,true,2020-07-17,18:00:22,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17T18:33:22,[101, 26, -17, -65, -67, 8, 57, 15, 72, -17, -65, -67, -17, -65, -67, -17, -65, -67, 54, -17, -65, -67, 62, 123, 116, 0],[4, 4, 4, 4, 4, 4, 4, 4],text,[16],[16],[16],[16],2021,red,[a, b],{\"key1\":\"value1\"})");

        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEqualsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testTiDBServerInBerlin() throws Exception {
        testTiDBServerTimezone("Europe/Berlin");
    }

    @Test
    public void testTiDBServerInShanghai() throws Exception {
        testTiDBServerTimezone("Asia/Shanghai");
    }

    public void testTiDBServerTimezone(String timezone) throws Exception {
        try (Connection connection = getJdbcConnection("");
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("SET GLOBAL time_zone = '%s';", timezone));
        }
        tEnv.getConfig().setLocalTimeZone(ZoneId.of(timezone));
        initializeTidbTable("column_type_test");
        String sourceDDL =
                String.format(
                        "CREATE TABLE tidb_source (\n"
                                + "    `id` INT NOT NULL,\n"
                                + "    date_c DATE,\n"
                                + "    time_c TIME(0),\n"
                                + "    datetime3_c TIMESTAMP(3),\n"
                                + "    datetime6_c TIMESTAMP(6),\n"
                                + "    timestamp_c TIMESTAMP_LTZ,\n"
                                + "    primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'tidb-cdc',"
                                + " 'tikv.grpc.timeout_in_ms' = '20000',"
                                + " 'pd-addresses' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN),
                        "column_type_test",
                        "full_types");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + "    `id` INT NOT NULL,\n"
                        + "    date_c DATE,\n"
                        + "    time_c TIME(0),\n"
                        + "    datetime3_c TIMESTAMP(3),\n"
                        + "    datetime6_c TIMESTAMP(6),\n"
                        + "    timestamp_c TIMESTAMP,\n"
                        + "    primary key (`id`) not enforced"
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
                        "INSERT INTO sink select `id`, date_c, time_c,datetime3_c, datetime6_c, cast(timestamp_c as timestamp) FROM tidb_source t");

        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", 1);

        try (Connection connection = getJdbcConnection("column_type_test");
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }

        waitForSinkSize("sink", 2);

        List<String> expected =
                Arrays.asList(
                        "+I(1,2020-07-17,18:00:22,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17T18:00:22)",
                        "+U(1,2020-07-17,18:00:22,2020-07-17T18:00:22.123,2020-07-17T18:00:22.123456,2020-07-17T18:33:22)");

        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEqualsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
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

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }
}
