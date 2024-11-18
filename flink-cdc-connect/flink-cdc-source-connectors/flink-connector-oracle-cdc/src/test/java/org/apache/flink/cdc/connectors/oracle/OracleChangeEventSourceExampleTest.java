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

package org.apache.flink.cdc.connectors.oracle;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** Example Tests for {@link JdbcIncrementalSource}. */
class OracleChangeEventSourceExampleTest extends OracleSourceTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(OracleChangeEventSourceExampleTest.class);

    private static final int DEFAULT_PARALLELISM = 4;
    private static final long DEFAULT_CHECKPOINT_INTERVAL = 1000;

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .setConfiguration(new Configuration())
                                    .withHaLeadershipControl()
                                    .build()));

    @Test
    @Disabled("Test ignored because it won't stop and is used for manual test")
    void testConsumingAllEvents() throws Exception {

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
