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

package com.ververica.cdc.connectors.polardbx;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.connectors.mysql.schema.MySqlSchema;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;

/**
 * Database Polardbx supported the mysql protocol, but there are some different features in ddl. So
 * we added fallback in {@link MySqlSchema} when parsing ddl failed and provided these cases to
 * test.
 */
public class PolardbxSourceITCase extends PolardbxSourceTestBase {
    private static final String DATABASE = "polardbx_ddl_test";

    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        initializePolardbxTables(DATABASE, null);
    }

    @Test
    public void testSingleKey() throws Exception {
        int parallelism = 1;
        String[] captureCustomerTables = new String[] {"orders"};

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        String sourceDDL =
                format(
                        "CREATE TABLE orders_source ("
                                + " id BIGINT NOT NULL,"
                                + " seller_id STRING,"
                                + " order_id STRING,"
                                + " buyer_id STRING,"
                                + " create_time TIMESTAMP,"
                                + " primary key (id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s'"
                                + ")",
                        HOST_NAME,
                        PORT,
                        USER_NAME,
                        PASSWORD,
                        DATABASE,
                        getTableNameRegex(captureCustomerTables),
                        getServerId());

        // first step: check the snapshot data
        String[] snapshotForSingleTable =
                new String[] {
                    "+I[1, 1001, 1, 102, 2022-01-16T00:00]",
                    "+I[2, 1002, 2, 105, 2022-01-16T00:00]",
                    "+I[3, 1004, 3, 109, 2022-01-16T00:00]",
                    "+I[4, 1002, 2, 106, 2022-01-16T00:00]",
                    "+I[5, 1003, 1, 107, 2022-01-16T00:00]",
                };
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from orders_source");
        CloseableIterator<Row> iterator = tableResult.collect();
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        List<String> realSnapshotData = fetchRows(iterator, expectedSnapshotData.size());
        assertEqualsInAnyOrder(expectedSnapshotData, realSnapshotData);

        // second step: check the sink data
        tEnv.executeSql(
                "CREATE TABLE sink ("
                        + " id BIGINT NOT NULL,"
                        + " seller_id STRING,"
                        + " order_id STRING,"
                        + " buyer_id STRING,"
                        + " create_time TIMESTAMP,"
                        + " primary key (id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")");
        tEnv.executeSql("insert into sink select * from orders_source");

        waitForSinkSize("sink", realSnapshotData.size());
        assertEqualsInAnyOrder(expectedSnapshotData, TestValuesTableFactory.getRawResults("sink"));

        // third step: check dml events
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("use " + DATABASE);
            statement.execute("INSERT INTO orders VALUES (6, 1006,1006, 1006,'2022-01-17');");
            statement.execute("INSERT INTO orders VALUES (7,1007, 1007,1007, '2022-01-17');");
            statement.execute("UPDATE orders SET seller_id= 9999, order_id=9999 WHERE id=6;");
            statement.execute("UPDATE orders SET seller_id= 9999, order_id=9999 WHERE id=7;");
            statement.execute("DELETE FROM orders WHERE id=7;");
        }

        String[] expectedBinlog =
                new String[] {
                    "+I[6, 1006, 1006, 1006, 2022-01-17T00:00]",
                    "+I[7, 1007, 1007, 1007, 2022-01-17T00:00]",
                    "-D[6, 1006, 1006, 1006, 2022-01-17T00:00]",
                    "+I[6, 9999, 9999, 1006, 2022-01-17T00:00]",
                    "-D[7, 1007, 1007, 1007, 2022-01-17T00:00]",
                    "+I[7, 9999, 9999, 1007, 2022-01-17T00:00]",
                    "-D[7, 9999, 9999, 1007, 2022-01-17T00:00]"
                };
        List<String> expectedBinlogData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedBinlogData.addAll(Arrays.asList(expectedBinlog));
        }
        List<String> realBinlog = fetchRows(iterator, expectedBinlog.length);
        assertEqualsInOrder(expectedBinlogData, realBinlog);
    }

    @Test
    public void testFullTypesDdl() {
        int parallelism = 1;
        String[] captureCustomerTables = new String[] {"polardbx_full_types"};

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        String sourceDDL =
                String.format(
                        "CREATE TABLE polardbx_full_types (\n"
                                + "    `id` INT NOT NULL,\n"
                                + "    tiny_c TINYINT,\n"
                                + "    tiny_un_c SMALLINT ,\n"
                                + "    small_c SMALLINT,\n"
                                + "    small_un_c INT,\n"
                                + "    medium_c INT,\n"
                                + "    medium_un_c INT,\n"
                                + "    int_c INT ,\n"
                                + "    int_un_c BIGINT,\n"
                                + "    int11_c BIGINT,\n"
                                + "    big_c BIGINT,\n"
                                + "    big_un_c DECIMAL(20, 0),\n"
                                + "    varchar_c VARCHAR(255),\n"
                                + "    char_c CHAR(3),\n"
                                + "    real_c FLOAT,\n"
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
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s'"
                                + ")",
                        HOST_NAME,
                        PORT,
                        USER_NAME,
                        PASSWORD,
                        DATABASE,
                        getTableNameRegex(captureCustomerTables),
                        getServerId());
        tEnv.executeSql(sourceDDL);

        TableResult tableResult = tEnv.executeSql("select * from polardbx_full_types");
        CloseableIterator<Row> iterator = tableResult.collect();
        List<String> realSnapshotData = fetchRows(iterator, 1);
        String[] expectedSnapshotData =
                new String[] {
                    "+I[100001, 127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295, 2147483647, "
                            + "9223372036854775807, 18446744073709551615, Hello World, abc, 123.102, 123.102, 404.4443, 123.4567,"
                            + " 346, 34567892.1, false, true, true, 2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, "
                            + "2020-07-17T18:00:22.123456, 2020-07-17T18:00:22, [101, 26, -19, 8, 57, 15, 72, -109, -78, -15, 54,"
                            + " -110, 62, 123, 116, 0], [4, 4, 4, 4, 4, 4, 4, 4], text, [16], [16], [16], [16], 2021, red, [a, "
                            + "b], {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}, "
                            + "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}, "
                            + "{\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0}, {\"coordinates\":[[[1,"
                            + "1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}, {\"coordinates\":[[1,1],[2,2]],"
                            + "\"type\":\"MultiPoint\",\"srid\":0}, {\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],"
                            + "\"type\":\"MultiLineString\",\"srid\":0}, {\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],"
                            + "[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0}, "
                            + "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\","
                            + "\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],"
                            + "\"type\":\"GeometryCollection\",\"srid\":0}]",
                };
        assertEqualsInAnyOrder(Arrays.asList(expectedSnapshotData), realSnapshotData);
    }

    @Test
    public void testMultiKeys() throws Exception {
        int parallelism = 1;
        String[] captureCustomerTables = new String[] {"orders_with_multi_pks"};

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        String sourceDDL =
                format(
                        "CREATE TABLE orders_with_multi_pks ("
                                + " id BIGINT NOT NULL,"
                                + " seller_id STRING,"
                                + " order_id STRING,"
                                + " buyer_id STRING,"
                                + " create_time TIMESTAMP,"
                                + " primary key (id,order_id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s'"
                                + ")",
                        HOST_NAME,
                        PORT,
                        USER_NAME,
                        PASSWORD,
                        DATABASE,
                        getTableNameRegex(captureCustomerTables),
                        getServerId());

        // first step: check the snapshot data
        String[] snapshotForSingleTable =
                new String[] {
                    "+I[1, 1001, 1, 102, 2022-01-16T00:00]",
                    "+I[2, 1002, 2, 105, 2022-01-16T00:00]",
                    "+I[3, 1004, 3, 109, 2022-01-16T00:00]",
                    "+I[4, 1002, 2, 106, 2022-01-16T00:00]",
                    "+I[5, 1003, 1, 107, 2022-01-16T00:00]",
                };
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from orders_with_multi_pks");
        CloseableIterator<Row> iterator = tableResult.collect();
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        List<String> realSnapshotData = fetchRows(iterator, expectedSnapshotData.size());
        assertEqualsInAnyOrder(expectedSnapshotData, realSnapshotData);

        // second step: check the sink data
        tEnv.executeSql(
                "CREATE TABLE multi_key_sink ("
                        + " id BIGINT NOT NULL,"
                        + " seller_id STRING,"
                        + " order_id STRING,"
                        + " buyer_id STRING,"
                        + " create_time TIMESTAMP,"
                        + " primary key (id,order_id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")");

        tEnv.executeSql("insert into multi_key_sink select * from orders_with_multi_pks");

        waitForSinkSize("multi_key_sink", realSnapshotData.size());
        assertEqualsInAnyOrder(
                expectedSnapshotData, TestValuesTableFactory.getRawResults("multi_key_sink"));

        // third step: check dml events
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("use " + DATABASE);
            statement.execute(
                    "INSERT INTO orders_with_multi_pks VALUES (6, 1006,1006, 1006,'2022-01-17');");
            statement.execute(
                    "INSERT INTO orders_with_multi_pks VALUES (7,1007, 1007,1007, '2022-01-17');");
            statement.execute(
                    "UPDATE orders_with_multi_pks SET seller_id= 9999, order_id=9999 WHERE id=6;");
            statement.execute(
                    "UPDATE orders_with_multi_pks SET seller_id= 9999, order_id=9999 WHERE id=7;");
            statement.execute("DELETE FROM orders_with_multi_pks WHERE id=7;");
        }

        String[] expectedBinlog =
                new String[] {
                    "+I[6, 1006, 1006, 1006, 2022-01-17T00:00]",
                    "+I[7, 1007, 1007, 1007, 2022-01-17T00:00]",
                    "-D[6, 1006, 1006, 1006, 2022-01-17T00:00]",
                    "+I[6, 9999, 9999, 1006, 2022-01-17T00:00]",
                    "-D[7, 1007, 1007, 1007, 2022-01-17T00:00]",
                    "+I[7, 9999, 9999, 1007, 2022-01-17T00:00]",
                    "-D[7, 9999, 9999, 1007, 2022-01-17T00:00]"
                };
        List<String> realBinlog = fetchRows(iterator, expectedBinlog.length);
        assertEqualsInAnyOrder(Arrays.asList(expectedBinlog), realBinlog);
    }
}
