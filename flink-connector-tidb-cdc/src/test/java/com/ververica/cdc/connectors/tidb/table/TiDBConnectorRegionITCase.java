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

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.utils.LegacyRowResource;

import com.ververica.cdc.connectors.tidb.TiDBTestBase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/** Integration tests for TiDB change stream event SQL source. */
public class TiDBConnectorRegionITCase extends TiDBTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBConnectorRegionITCase.class);
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @ClassRule public static LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(1);
    }

    @Test
    public void testRegionChange() throws Exception {
        initializeTidbTable("region_switch_test");
        String sourceDDL =
                String.format(
                        "CREATE TABLE tidb_source ("
                                + " `id` INT NOT NULL,"
                                + " b INT,"
                                + " PRIMARY KEY (`id`) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'tidb-cdc',"
                                + " 'tikv.grpc.timeout_in_ms' = '20000',"
                                + " 'pd-addresses' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN),
                        "region_switch_test",
                        "t1");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " `id` INT NOT NULL,"
                        + " b INT,"
                        + " PRIMARY KEY (`id`) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM tidb_source");

        waitForSinkSize("sink", 1);

        int count = 0;

        try (Connection connection = getJdbcConnection("region_switch_test");
                Statement statement = connection.createStatement()) {
            for (int i = 0; i < 15; i++) {
                statement.execute(
                        "INSERT INTO t1 SELECT NULL, FLOOR(RAND()*1000), RANDOM_BYTES(1024), RANDOM_BYTES"
                                + "(1024), RANDOM_BYTES(1024) FROM t1 a JOIN t1 b JOIN t1 c LIMIT 10000;");
            }

            ResultSet resultSet = statement.executeQuery("SHOW TABLE t1 REGIONS;");
            while (resultSet.next()) {
                String regionId = resultSet.getString(1);
                String leaderStoreId = resultSet.getString(2);
                String peerStoreIds = resultSet.getString(3);
                String regionState = resultSet.getString(4);
                String regionRows = resultSet.getString(5);
                String regionSize = resultSet.getString(6);
                String regionKeys = resultSet.getString(7);
                LOG.info(
                        "regionId: {}, leaderStoreId: {}, peerStoreIds: {}, regionState: {}, regionRows: {}, regionSize: {}, regionKeys: {}",
                        regionId,
                        leaderStoreId,
                        peerStoreIds,
                        regionState,
                        regionRows,
                        regionSize,
                        regionKeys);
            }

            ResultSet resultSetCount = statement.executeQuery("select count(*) from t1;");
            resultSetCount.next();
            count = resultSetCount.getInt(1);
            LOG.info("count: {}", count);
        }

        waitForSinkSize("sink", count);
        result.getJobClient().get().cancel().get();
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }
}
