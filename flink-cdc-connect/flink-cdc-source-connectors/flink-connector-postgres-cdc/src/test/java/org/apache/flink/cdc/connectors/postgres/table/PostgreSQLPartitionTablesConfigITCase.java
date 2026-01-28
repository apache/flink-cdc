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

package org.apache.flink.cdc.connectors.postgres.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.RowUtils;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/**
 * Integration tests for {@code partition.tables} configuration on PostgreSQL 10.
 *
 * <p>This test specifically targets PostgreSQL 10 where:
 *
 * <ul>
 *   <li>Parent tables do NOT have primary keys (unlike PG11+)
 *   <li>publish_via_partition_root is NOT available
 *   <li>partition.tables config is required for routing child tables to parent
 * </ul>
 *
 * <p>For PostgreSQL 11+ with native partition support using publish_via_partition_root, see {@link
 * PostgreSQLConnectorITCase#testConsumingAllEventsForPartitionedTable}.
 */
class PostgreSQLPartitionTablesConfigITCase extends PostgresTestBase {

    /**
     * Tests partition.tables configuration on PG10. This verifies that child partition tables are
     * correctly routed to parent table.
     */
    @Test
    void testPartitionTablesConfigOnPG10() throws Exception {
        PostgreSQLContainer<?> container = getPg10Container();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        boolean useLegacyToString = RowUtils.USE_LEGACY_TO_STRING;
        RowUtils.USE_LEGACY_TO_STRING = true;
        TableResult result = null;
        try {
            TestValuesTableFactory.clearAllData();
            env.setRestartStrategy(RestartStrategies.noRestart());
            env.setParallelism(1);
            env.enableCheckpointing(200);

            initializePostgresTable(container, "inventory_partitioned_pg10");
            String publicationName = "dbz_publication_" + new Random().nextInt(1000);
            String slotName = getSlotName();

            try (Connection connection = getJdbcConnection(container);
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        String.format(
                                "CREATE PUBLICATION %s FOR TABLE "
                                        + "inventory_partitioned.products_us, "
                                        + "inventory_partitioned.products_uk",
                                publicationName));
                statement.execute(
                        String.format(
                                "select pg_create_logical_replication_slot('%s','pgoutput');",
                                slotName));
            }

            String sourceDDL = buildSourceDDL(container, publicationName, slotName);
            String sinkDDL = buildSinkDDL();

            tEnv.executeSql(sourceDDL);
            tEnv.executeSql(sinkDDL);

            result =
                    tEnv.executeSql(
                            "INSERT INTO sink SELECT id, name, description, weight, country "
                                    + "FROM debezium_source");

            // Wait for snapshot to start
            waitForSnapshotStarted("sink");

            // Wait a bit to make sure the replication slot is ready for WAL streaming
            Thread.sleep(5000);

            // Generate WAL events
            try (Connection connection = getJdbcConnection(container);
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        "INSERT INTO inventory_partitioned.products "
                                + "VALUES (default,'jacket','water resistent white wind breaker',0.2,'us');");
                statement.execute(
                        "INSERT INTO inventory_partitioned.products "
                                + "VALUES (default,'scooter','Big 2-wheel scooter ',5.18,'uk');");
            }

            // Wait for all data (9 snapshot + 2 WAL = 11 total)
            List<String> expected =
                    Arrays.asList(
                            "101,scooter,Small 2-wheel scooter,3.140,us",
                            "102,car battery,12V car battery,8.100,us",
                            "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.800,us",
                            "104,hammer,12oz carpenter's hammer,0.750,us",
                            "105,hammer,14oz carpenter's hammer,0.875,us",
                            "106,hammer,16oz carpenter's hammer,1.000,uk",
                            "107,rocks,box of assorted rocks,5.300,uk",
                            "108,jacket,water resistent black wind breaker,0.100,uk",
                            "109,spare tire,24 inch spare tire,22.200,uk",
                            "110,jacket,water resistent white wind breaker,0.200,us",
                            "111,scooter,Big 2-wheel scooter ,5.180,uk");
            waitForSinkResult("sink", expected);
        } finally {
            if (result != null) {
                try {
                    result.getJobClient().get().cancel().get();
                } catch (Exception e) {
                    // Job may have already finished with SuccessException
                }
            }
            RowUtils.USE_LEGACY_TO_STRING = useLegacyToString;
        }
    }

    private String buildSourceDDL(
            PostgreSQLContainer<?> container, String publicationName, String slotName) {
        return String.format(
                "CREATE TABLE debezium_source ("
                        + " id INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(10,3),"
                        + " country STRING"
                        + ") WITH ("
                        + " 'connector' = 'postgres-cdc',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.startup.mode' = 'initial',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'scan.incremental.snapshot.chunk.key-column' = 'id',"
                        + " 'decoding.plugin.name' = 'pgoutput',"
                        + " 'debezium.publication.name' = '%s',"
                        + " 'partition.tables' = '%s',"
                        + " 'slot.name' = '%s'"
                        + ")",
                container.getHost(),
                container.getMappedPort(POSTGRESQL_PORT),
                container.getUsername(),
                container.getPassword(),
                container.getDatabaseName(),
                "inventory_partitioned",
                "products",
                publicationName,
                "inventory_partitioned.products:inventory_partitioned.products_.*",
                slotName);
    }

    private String buildSinkDDL() {
        return "CREATE TABLE sink ("
                + " id INT NOT NULL,"
                + " name STRING,"
                + " description STRING,"
                + " weight DECIMAL(10,3),"
                + " country STRING,"
                + " PRIMARY KEY (id, country) NOT ENFORCED"
                + ") WITH ("
                + " 'connector' = 'values',"
                + " 'sink-insert-only' = 'false',"
                + " 'sink-expected-messages-num' = '20'"
                + ")";
    }
}
