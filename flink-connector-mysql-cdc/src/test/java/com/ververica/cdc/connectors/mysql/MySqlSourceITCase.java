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

package com.ververica.cdc.connectors.mysql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertTrue;

/** Integration tests for {@link MySqlSource}. */
@Ignore
public class MySqlSourceITCase extends MySqlTestBase {

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    private final UniqueDatabase fullTypesDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "column_type_test", "mysqluser", "mysqlpw");

    @Test
    public void testConsumingAllEvents() throws Exception {
        inventoryDatabase.createAndInitialize();
        SourceFunction<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(
                                inventoryDatabase
                                        .getDatabaseName()) // monitor all tables under inventory
                        // database
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .deserializer(new StringDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(sourceFunction).print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }

    @Test
    public void testConsumingAllTypesWithJsonFormat() throws Exception {
        fullTypesDatabase.createAndInitialize();
        SourceFunction<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(
                                fullTypesDatabase
                                        .getDatabaseName()) // monitor all tables under inventory
                        // database
                        .username(fullTypesDatabase.getUsername())
                        .password(fullTypesDatabase.getPassword())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env,
                        EnvironmentSettings.newInstance()
                                .useBlinkPlanner()
                                .inStreamingMode()
                                .build());
        List<String> expectedList = readLines("file/debezium-data-schema-exclude.txt");
        DataStreamSource<String> source = env.addSource(sourceFunction);
        tEnv.createTemporaryView("full_types", source);
        TableResult result = tEnv.executeSql("SELECT * FROM full_types");
        CloseableIterator<Row> snapshot = result.collect();
        waitForSnapshotStarted(snapshot);
        assertTrue(
                compareDebeziumJson(fetchRows(snapshot, 1).get(0).toString(), expectedList.get(0)));
        try (Connection connection = fullTypesDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }
        CloseableIterator<Row> binlog = result.collect();
        assertTrue(
                compareDebeziumJson(fetchRows(binlog, 1).get(0).toString(), expectedList.get(1)));
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testConsumingAllEventsWithIncludeSchemaJsonFormat() throws Exception {
        fullTypesDatabase.createAndInitialize();
        SourceFunction<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(
                                fullTypesDatabase
                                        .getDatabaseName()) // monitor all tables under inventory
                        // database
                        .username(fullTypesDatabase.getUsername())
                        .password(fullTypesDatabase.getPassword())
                        .deserializer(new JsonDebeziumDeserializationSchema(true))
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env,
                        EnvironmentSettings.newInstance()
                                .useBlinkPlanner()
                                .inStreamingMode()
                                .build());
        List<String> expectedList = readLines("file/debezium-data-schema-include.txt");
        DataStreamSource<String> source = env.addSource(sourceFunction);
        tEnv.createTemporaryView("full_types", source);
        TableResult result = tEnv.executeSql("SELECT * FROM full_types");
        CloseableIterator<Row> snapshot = result.collect();
        waitForSnapshotStarted(snapshot);
        assertTrue(
                compareDebeziumJson(fetchRows(snapshot, 1).get(0).toString(), expectedList.get(0)));
        try (Connection connection = fullTypesDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }
        CloseableIterator<Row> binlog = result.collect();
        assertTrue(
                compareDebeziumJson(fetchRows(binlog, 1).get(0).toString(), expectedList.get(1)));
        result.getJobClient().get().cancel().get();
    }

    private static List<Object> fetchRows(Iterator<Row> iter, int size) {
        List<Object> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            // ignore rowKind marker
            rows.add(row.getField(0));
            size--;
        }
        return rows;
    }

    private static void waitForSnapshotStarted(CloseableIterator<Row> iterator) throws Exception {
        while (!iterator.hasNext()) {
            Thread.sleep(100);
        }
    }

    private static List<String> readLines(String resource) throws IOException {
        final URL url = MySqlSourceITCase.class.getClassLoader().getResource(resource);
        assert url != null;
        java.nio.file.Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private static boolean compareDebeziumJson(String actual, String expect) {
        JSONObject actualJsonObject = JSONObject.parseObject(actual);
        JSONObject expectJsonObject = JSONObject.parseObject(expect);
        if (expectJsonObject.getJSONObject("payload") != null
                && actualJsonObject.getJSONObject("payload") != null) {
            expectJsonObject = expectJsonObject.getJSONObject("payload");
            actualJsonObject = actualJsonObject.getJSONObject("payload");
        }
        return Objects.equals(
                        expectJsonObject.getJSONObject("after"),
                        actualJsonObject.getJSONObject("after"))
                && Objects.equals(
                        expectJsonObject.getJSONObject("before"),
                        actualJsonObject.getJSONObject("before"))
                && Objects.equals(expectJsonObject.get("op"), actualJsonObject.get("op"));
    }
}
