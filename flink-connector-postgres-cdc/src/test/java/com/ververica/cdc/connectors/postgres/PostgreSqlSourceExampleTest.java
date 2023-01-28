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

package com.ververica.cdc.connectors.postgres;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.Properties;

public class PostgreSqlSourceExampleTest extends PostgresTestBase {

    @Ignore
    @Test
    public void test() throws Exception {
        initializePostgresTable("column_type_test");
        Properties properties = new Properties();
        properties.put("converters", "flinkcdc-converter");
        properties.put(
                "flinkcdc-converter.type",
                "com.ververica.cdc.debezium.converter.FlinkCDCFormatConverter");
        properties.put("flinkcdc-converter.format.date", "yyyy-MM-dd");
        properties.put("flinkcdc-converter.format.time", "HH:mm:ss");
        properties.put("flinkcdc-converter.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.put("flinkcdc-converter.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        properties.put("flinkcdc-converter.format.timestamp.zone", "Asia/Shanghai");
        DebeziumSourceFunction<String> sourceFunction =
                PostgreSQLSource.<String>builder()
                        .hostname(POSTGERS_CONTAINER.getHost())
                        .port(POSTGERS_CONTAINER.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
                        .database(POSTGERS_CONTAINER.getDatabaseName())
                        .username(POSTGERS_CONTAINER.getUsername())
                        .password(POSTGERS_CONTAINER.getPassword())
                        .schemaList("inventory")
                        .tableList("inventory.full_types")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .debeziumProperties(properties)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(sourceFunction).print().setParallelism(1);

        env.execute("Print PostgreSql Snapshot + Binlog");
    }
}
