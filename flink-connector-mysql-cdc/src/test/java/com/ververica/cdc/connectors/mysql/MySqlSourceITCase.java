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

import com.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

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
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env,
                        EnvironmentSettings.newInstance()
                                .useBlinkPlanner()
                                .inStreamingMode()
                                .build());

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
        tEnv.createTemporaryView("full_types", dataStreamSource);
        TableResult result = tEnv.executeSql("SELECT * FROM full_types");
        CloseableIterator<Row> iterator = result.collect();
        // ignore match `source` field
        String expected =
                "+I[{\"before\":null,\"after\":{\"id\":1,\"tiny_c\":127,\"tiny_un_c\":255,\"small_c\":32767,\"small_un_c\":65535,\"int_c\":2147483647,\"int_un_c\":4294967295,\"int11_c\":2147483647,\"big_c\":9223372036854775807,\"varchar_c\":\"Hello World\",\"char_c\":\"abc\",\"float_c\":123.10199737548828,\"double_c\":404.4443,\"decimal_c\":\"EtaH\",\"numeric_c\":\"AVo=\",\"boolean_c\":1,\"date_c\":18460,\"time_c\":64822000000,\"datetime3_c\":1595008822123,\"datetime6_c\":1595008822123456,\"timestamp_c\":\"2020-07-17T18:00:22Z\",\"file_uuid\":\"ZRrtCDkPSJOy8TaSPnt0AA==\"},";
        assertThat(iterator.next().toString(), startsWith(expected));
        result.getJobClient().get().cancel().get();
    }
}
