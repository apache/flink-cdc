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

package org.apache.flink.cdc.connectors.db2.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.db2.Db2TestBase;
import org.apache.flink.cdc.connectors.db2.source.Db2SourceBuilder.Db2IncrementalSource;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static org.testcontainers.containers.Db2Container.DB2_PORT;

/** IT tests for {@link Db2IncrementalSource}. */
public class Db2SourceITCase extends Db2TestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    protected static final int DEFAULT_PARALLELISM = 4;

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testDb2ParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"DB2INST1.CUSTOMERS"});
    }

    @Test
    public void testReadSingleTableWithMultipleParallelism() throws Exception {
        testDb2ParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"DB2INST1.CUSTOMERS"});
    }

    // Failover tests
    @Test
    public void testTaskManagerFailoverInSnapshotPhase() throws Exception {
        testDb2ParallelSource(
                FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"DB2INST1.CUSTOMERS"});
    }

    @Test
    public void testTaskManagerFailoverInRedoLogsPhase() throws Exception {
        testDb2ParallelSource(
                FailoverType.TM, FailoverPhase.STREAM, new String[] {"DB2INST1.CUSTOMERS"});
    }

    @Test
    public void testJobManagerFailoverInSnapshotPhase() throws Exception {
        testDb2ParallelSource(
                FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"DB2INST1.CUSTOMERS"});
    }

    @Test
    public void testJobManagerFailoverInRedoLogsPhase() throws Exception {
        testDb2ParallelSource(
                FailoverType.JM, FailoverPhase.STREAM, new String[] {"DB2INST1.CUSTOMERS"});
    }

    @Test
    public void testJobManagerFailoverSingleParallelism() throws Exception {
        testDb2ParallelSource(
                1, FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"DB2INST1.CUSTOMERS"});
    }

    @Test
    public void testReadSingleTableWithSingleParallelismAndSkipBackfill() throws Exception {
        testDb2ParallelSource(
                DEFAULT_PARALLELISM,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                new String[] {"DB2INST1.CUSTOMERS"},
                true);
    }

    private void testDb2ParallelSource(
            FailoverType failoverType, FailoverPhase failoverPhase, String[] captureCustomerTables)
            throws Exception {
        testDb2ParallelSource(
                DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerTables);
    }

    private void testDb2ParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        testDb2ParallelSource(
                parallelism, failoverType, failoverPhase, captureCustomerTables, false);
    }

    private void testDb2ParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            boolean skipSnapshotBackfill)
            throws Exception {

        initializeDb2Table("customers", "CUSTOMERS");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        String sourceDDL =
                format(
                        "CREATE TABLE CUSTOMERS ("
                                + " ID INT NOT NULL,"
                                + " NAME STRING,"
                                + " ADDRESS STRING,"
                                + " PHONE_NUMBER STRING,"
                                + " primary key (ID) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'db2-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'scan.incremental.snapshot.chunk.size' = '4',"
                                + " 'scan.incremental.snapshot.backfill.skip' = '%s'"
                                + ")",
                        DB2_CONTAINER.getHost(),
                        DB2_CONTAINER.getMappedPort(DB2_PORT),
                        DB2_CONTAINER.getUsername(),
                        DB2_CONTAINER.getPassword(),
                        DB2_CONTAINER.getDatabaseName(),
                        getTableNameRegex(captureCustomerTables),
                        skipSnapshotBackfill);

        // first step: check the snapshot data
        String[] snapshotForSingleTable =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from CUSTOMERS");
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        // trigger failover after some snapshot splits read finished
        if (failoverPhase == FailoverPhase.SNAPSHOT && iterator.hasNext()) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(100));
        }

        assertEqualsInAnyOrder(
                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));

        // second step: check the change stream data
        for (String tableId : captureCustomerTables) {
            makeFirstPartChangeStreamEvents(tableId);
        }
        if (failoverPhase == FailoverPhase.STREAM) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(200));
        }
        for (String tableId : captureCustomerTables) {
            makeSecondPartRedoLogsEvents(tableId);
        }

        String[] redoLogsForSingleTable =
                new String[] {
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]",
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+U[1010, user_11, Hangzhou, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]"
                };
        List<String> expectedRedoLogsData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedRedoLogsData.addAll(Arrays.asList(redoLogsForSingleTable));
        }
        assertEqualsInAnyOrder(
                expectedRedoLogsData, fetchRows(iterator, expectedRedoLogsData.size()));
        tableResult.getJobClient().get().cancel().get();
    }

    private void makeFirstPartChangeStreamEvents(String tableId) {
        executeSql("UPDATE " + tableId + " SET ADDRESS = 'Hangzhou' where ID = 103");
        executeSql("DELETE FROM " + tableId + " where ID = 102");
        executeSql("INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')");
        executeSql("UPDATE " + tableId + " SET ADDRESS = 'Shanghai' where ID = 103");
    }

    private void makeSecondPartRedoLogsEvents(String tableId) {
        executeSql("UPDATE " + tableId + " SET ADDRESS = 'Hangzhou' where ID = 1010");
        executeSql("INSERT INTO " + tableId + " VALUES(2001, 'user_22','Shanghai','123567891234')");
        executeSql("INSERT INTO " + tableId + " VALUES(2002, 'user_23','Shanghai','123567891234')");
        executeSql("INSERT INTO " + tableId + " VALUES(2003, 'user_24','Shanghai','123567891234')");
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    /** The type of failover. */
    protected enum FailoverType {
        TM,
        JM,
        NONE
    }

    /** The phase of failover. */
    protected enum FailoverPhase {
        SNAPSHOT,
        STREAM,
        NEVER
    }

    protected static void triggerFailover(
            FailoverType type, JobID jobId, MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        switch (type) {
            case TM:
                restartTaskManager(miniCluster, afterFailAction);
                break;
            case JM:
                triggerJobManagerFailover(jobId, miniCluster, afterFailAction);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    protected static void triggerJobManagerFailover(
            JobID jobId, MiniCluster miniCluster, Runnable afterFailAction) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    protected static void restartTaskManager(MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }
}
