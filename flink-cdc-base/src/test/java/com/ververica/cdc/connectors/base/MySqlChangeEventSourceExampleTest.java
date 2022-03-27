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

package com.ververica.cdc.connectors.base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import com.ververica.cdc.connectors.base.experimental.MySqlSourceBuilder;
import com.ververica.cdc.connectors.base.source.JdbcIncrementalSource;
import com.ververica.cdc.connectors.base.testutils.MySqlContainer;
import com.ververica.cdc.connectors.base.testutils.MySqlVersion;
import com.ververica.cdc.connectors.base.testutils.UniqueDatabase;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

/** Example Tests for {@link JdbcIncrementalSource}. */
public class MySqlChangeEventSourceExampleTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(MySqlChangeEventSourceExampleTest.class);

    private static final int DEFAULT_PARALLELISM = 4;
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V5_7);

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {
        inventoryDatabase.createAndInitialize();
        JdbcIncrementalSource<String> mySqlChangeEventSource =
                new MySqlSourceBuilder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + ".products")
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        env.fromSource(
                        mySqlChangeEventSource,
                        WatermarkStrategy.noWatermarks(),
                        "MySqlParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return (MySqlContainer)
                new MySqlContainer(version)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withDatabaseName("flink-test")
                        .withUsername("flinkuser")
                        .withPassword("flinkpw")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }
}
