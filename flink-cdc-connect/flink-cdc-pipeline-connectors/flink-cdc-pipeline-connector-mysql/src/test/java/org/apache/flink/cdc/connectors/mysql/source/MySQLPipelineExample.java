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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.getServerId;

/**
 * Basic class for testing MySQL binlog source, this contains a MySQL container which enables
 * binlog.
 */
public class MySQLPipelineExample {
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void test() throws Exception {
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname("bj.yarnmonitortest.r.qiyi.db")
                        .port(3026)
                        .databaseList("yarn_monitor_test") // set captured database, If you need to
                        // synchronize the whole database, Please set
                        // tableList to ".*".
                        .tableList("yarn_monitor_test.app") // set captured table
                        .username("rcp_binlog")
                        .password("$sqT+6!0I3")
                        .startupOptions(StartupOptions.initial())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("Asia/Shanghai")
                        .includeSchemaChanges(SCHEMA_CHANGE_ENABLED.defaultValue());
        // enable checkpoint
        env.enableCheckpointing(3000);

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();

        env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MySqlDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo())
                .print();
        env.execute();
    }
}
