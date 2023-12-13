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

package org.apache.flink.cdc.connectors.db2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.db2.source.Db2SourceBuilder;
import org.apache.flink.cdc.connectors.db2.source.Db2SourceBuilder.Db2IncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Ignore;
import org.junit.Test;

import static org.testcontainers.containers.Db2Container.DB2_PORT;

/** Example Tests for {@link Db2IncrementalSource}. */
public class Db2ParallelSourceExampleTest extends Db2TestBase {

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testDb2ExampleSource() throws Exception {
        initializeDb2Table("customers", "CUSTOMERS");
        Db2IncrementalSource<String> sqlServerSource =
                new Db2SourceBuilder()
                        .hostname(DB2_CONTAINER.getHost())
                        .port(DB2_CONTAINER.getMappedPort(DB2_PORT))
                        .databaseList(DB2_CONTAINER.getDatabaseName())
                        .tableList("DB2INST1.CUSTOMERS")
                        .username(DB2_CONTAINER.getUsername())
                        .password(DB2_CONTAINER.getPassword())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.initial())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
        env.fromSource(sqlServerSource, WatermarkStrategy.noWatermarks(), "Db2IncrementalSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute("Print DB2 Snapshot + Change Stream");
    }
}
