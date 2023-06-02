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

package com.ververica.cdc.connectors.sqlserver;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceTestBase;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Test;

/** Example Tests for {@link SqlServerIncrementalSource}. */
public class SqlServerParallelSourceExampleTest extends SqlServerSourceTestBase {

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testSqlServerExampleSource() throws Exception {
        initializeSqlServerTable("inventory");

        SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder()
                        .hostname(MSSQL_SERVER_CONTAINER.getHost())
                        .port(MSSQL_SERVER_CONTAINER.getFirstMappedPort())
                        .databaseList("inventory")
                        .tableList("dbo.products")
                        .username(MSSQL_SERVER_CONTAINER.getUsername())
                        .password(MSSQL_SERVER_CONTAINER.getPassword())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.initial())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
        env.fromSource(
                        sqlServerSource,
                        WatermarkStrategy.noWatermarks(),
                        "SqlServerIncrementalSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute("Print SqlServer Snapshot + Change Stream");
    }
}
