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

package com.ververica.cdc.connectors.postgres.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Test;

import static java.lang.String.format;

/** IT tests for {@link PostgresSourceBuilder.PostgresIncrementalSource}. */
public class PostgresSourceSnapshotITCase extends PostgresSourceBase {

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testPostgresSnapshotParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers"});
    }

    @Test
    public void testReadSingleTableWithMultipleParallelism() throws Exception {
        testPostgresSnapshotParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers"});
    }

    @Test
    public void testReadMultipleTableWithSingleParallelism() throws Exception {
        testPostgresSnapshotParallelSource(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"});
    }

    @Test
    public void testReadMultipleTableWithMultipleParallelism() throws Exception {
        testPostgresSnapshotParallelSource(
                4,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"});
    }

    @Test
    public void testTaskManagerFailover() throws Exception {
        testPostgresSnapshotParallelSource(
                1,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                new String[] {"customers", "customers_1"});
    }

    @Test
    public void testJobManagerFailover() throws Exception {
        testPostgresSnapshotParallelSource(
                1,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                new String[] {"customers", "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverMultipleParallelism() throws Exception {
        testPostgresSnapshotParallelSource(
                4,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                new String[] {"customers", "customers_1"});
    }

    @Test
    public void testJobManagerFailoverMultipleParallelism() throws Exception {
        testPostgresSnapshotParallelSource(
                4,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                new String[] {"customers", "customers_1"});
    }

    private void testPostgresSnapshotParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        testPostgresSnapshotOnlyParallelSource(
                parallelism,
                "snapshot-only",
                failoverType,
                failoverPhase,
                captureCustomerTables,
                RestartStrategies.fixedDelayRestart(1, 0));
    }

    private void testPostgresSnapshotOnlyParallelSource(
            int parallelism,
            String scanStartupMode,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(restartStrategyConfiguration);
        String sourceDDL =
                format(
                        "CREATE TABLE customers ("
                                + " id BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " primary key (id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'postgres-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '2',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        customDatabase.getHost(),
                        customDatabase.getDatabasePort(),
                        customDatabase.getUsername(),
                        customDatabase.getPassword(),
                        customDatabase.getDatabaseName(),
                        SCHEMA_NAME,
                        getTableNameRegex(captureCustomerTables),
                        scanStartupMode,
                        getSlotName());
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from customers");

        checkSnapshotData(tableResult, failoverType, failoverPhase, captureCustomerTables);

        tableResult.getJobClient().get().getJobStatus().get();
    }
}
