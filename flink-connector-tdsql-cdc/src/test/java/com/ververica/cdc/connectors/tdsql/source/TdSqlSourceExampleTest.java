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

package com.ververica.cdc.connectors.tdsql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;
import com.ververica.cdc.connectors.tdsql.testutils.TdSqlDatabase;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Example test for {@link TdSqlSource}. */
public class TdSqlSourceExampleTest extends TdSqlSourceTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TdSqlSourceExampleTest.class);
    private final TdSqlDatabase inventoryDatabase =
            new TdSqlDatabase(tdSqlSets(), "inventory", "tdsqluser", "tdsqlpw");

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {
        inventoryDatabase.createAndInitialize();

        LOG.info(
                "first container address: {}:{}",
                inventoryDatabase.getHost(0),
                inventoryDatabase.getDatabasePort(0));
        LOG.info(
                "second container address: {}:{}",
                inventoryDatabase.getHost(1),
                inventoryDatabase.getDatabasePort(1));

        TdSqlSource<String> tdSqlSource =
                TdSqlSource.<String>builder()
                        .hostname(inventoryDatabase.getHost(0))
                        .port(inventoryDatabase.getDatabasePort(0))
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + ".products")
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();

        tdSqlSource.setTestSets(
                Stream.of(
                                new TdSqlSet(
                                        "set_1",
                                        inventoryDatabase.getHost(0),
                                        inventoryDatabase.getDatabasePort(0)),
                                new TdSqlSet(
                                        "set_2",
                                        inventoryDatabase.getHost(1),
                                        inventoryDatabase.getDatabasePort(1)))
                        .collect(Collectors.toList()));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        env.fromSource(tdSqlSource, WatermarkStrategy.noWatermarks(), "TdSqlParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print TdSql Snapshot + Binlog");
    }
}
