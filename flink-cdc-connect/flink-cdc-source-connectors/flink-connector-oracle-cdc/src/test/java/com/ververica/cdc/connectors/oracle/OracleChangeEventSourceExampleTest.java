/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.oracle;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import com.ververica.cdc.connectors.oracle.source.OracleSourceTestBase;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** Example Tests for {@link JdbcIncrementalSource}. */
public class OracleChangeEventSourceExampleTest extends OracleSourceTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(OracleChangeEventSourceExampleTest.class);

    private static final int DEFAULT_PARALLELISM = 4;
    private static final long DEFAULT_CHECKPOINT_INTERVAL = 1000;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .setConfiguration(new Configuration())
                            .withHaLeadershipControl()
                            .build());

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {

        createAndInitialize("product.sql");

        LOG.info(
                "getOraclePort:{},getUsername:{},getPassword:{}",
                ORACLE_CONTAINER.getOraclePort(),
                ORACLE_CONTAINER.getUsername(),
                ORACLE_CONTAINER.getPassword());

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("log.mining.strategy", "online_catalog");

        JdbcIncrementalSource<String> oracleChangeEventSource =
                new OracleSourceBuilder<String>()
                        .hostname(ORACLE_CONTAINER.getHost())
                        .port(ORACLE_CONTAINER.getOraclePort())
                        .databaseList(ORACLE_DATABASE)
                        .schemaList(ORACLE_SCHEMA)
                        .tableList("DEBEZIUM.PRODUCTS")
                        .username(ORACLE_CONTAINER.getUsername())
                        .password(ORACLE_CONTAINER.getPassword())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(debeziumProperties)
                        .splitSize(2)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL);
        // set the source parallelism to 4
        env.fromSource(
                        oracleChangeEventSource,
                        WatermarkStrategy.noWatermarks(),
                        "OracleParallelSource")
                .setParallelism(DEFAULT_PARALLELISM)
                .print()
                .setParallelism(1);
        env.execute("Print Oracle Snapshot + RedoLog");
    }
}
