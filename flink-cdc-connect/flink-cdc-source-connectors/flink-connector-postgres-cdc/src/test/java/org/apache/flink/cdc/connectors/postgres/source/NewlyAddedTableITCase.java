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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.testutils.PostgresTestUtils;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExceptionUtils;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * IT tests to cover various newly added tables during capture process. Ignore this test because
 * this test will pass until close
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class NewlyAddedTableITCase extends PostgresTestBase {

    private static final String DB_NAME_PREFIX = "postgres";
    private static final String SCHEMA_NAME = "customer";
    protected static final int DEFAULT_PARALLELISM = 4;

    @TempDir private Path tempDir;

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    DB_NAME_PREFIX,
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private String slotName;
    private final ScheduledExecutorService mockWalLogExecutor = Executors.newScheduledThreadPool(1);

    @BeforeEach
    public void before() throws SQLException {
        TestValuesTableFactory.clearAllData();
        customDatabase.createAndInitialize();

        try (PostgresConnection connection = getConnection()) {
            connection.setAutoCommit(false);
            // prepare initial data for given table
            String tableId = SCHEMA_NAME + ".produce_wal_log_table";
            connection.execute(
                    format("CREATE TABLE %s ( id BIGINT PRIMARY KEY, cnt BIGINT);", tableId));
            connection.execute(
                    format("INSERT INTO  %s VALUES (0, 100), (1, 101), (2, 102);", tableId));
            connection.commit();

            // mock continuous wal log during the newly added table capturing process
            mockWalLogExecutor.schedule(
                    () -> {
                        try {
                            connection.execute(
                                    format("UPDATE  %s SET  cnt = cnt +1 WHERE id < 2;", tableId));
                            connection.commit();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    },
                    500,
                    TimeUnit.MICROSECONDS);
        }

        this.slotName = getSlotName();
    }

    @AfterEach
    public void after() throws Exception {
        mockWalLogExecutor.shutdown();
        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
        customDatabase.removeSlot(slotName);
    }

    private PostgresConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hostname", customDatabase.getHost());
        properties.put("port", String.valueOf(customDatabase.getDatabasePort()));
        properties.put("user", customDatabase.getUsername());
        properties.put("password", customDatabase.getPassword());
        properties.put("dbname", customDatabase.getDatabaseName());
        return createConnection(properties);
    }

    @Test
    void testNewlyAddedTableForExistsPipelineOnce() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineOnceWithAheadWalLog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineTwice() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineTwiceWithAheadWalLog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineTwiceWithAheadWalLogAndAutoCloseReader()
            throws Exception {
        Map<String, String> otherOptions = new HashMap<>();
        otherOptions.put("scan.incremental.close-idle-reader.enabled", "true");
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                otherOptions,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineThrice() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai",
                "address_shenzhen");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineThriceWithAheadWalLog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai",
                "address_shenzhen");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineSingleParallelism() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineSingleParallelismWithAheadWalLog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testJobManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                PostgresTestUtils.FailoverType.JM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testJobManagerFailoverForNewlyAddedTableWithAheadWalLog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                PostgresTestUtils.FailoverType.JM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testTaskManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                PostgresTestUtils.FailoverType.TM,
                PostgresTestUtils.FailoverPhase.STREAM,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testTaskManagerFailoverForNewlyAddedTableWithAheadWalLog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                PostgresTestUtils.FailoverType.TM,
                PostgresTestUtils.FailoverPhase.STREAM,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testJobManagerFailoverForRemoveTableSingleParallelism() throws Exception {
        testRemoveTablesOneByOne(
                1,
                PostgresTestUtils.FailoverType.JM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testJobManagerFailoverForRemoveTable() throws Exception {
        testRemoveTablesOneByOne(
                DEFAULT_PARALLELISM,
                PostgresTestUtils.FailoverType.JM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testTaskManagerFailoverForRemoveTableSingleParallelism() throws Exception {
        testRemoveTablesOneByOne(
                1,
                PostgresTestUtils.FailoverType.TM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testTaskManagerFailoverForRemoveTable() throws Exception {
        testRemoveTablesOneByOne(
                DEFAULT_PARALLELISM,
                PostgresTestUtils.FailoverType.TM,
                PostgresTestUtils.FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testRemoveTableSingleParallelism() throws Exception {
        testRemoveTablesOneByOne(
                1,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testRemoveTable() throws Exception {
        testRemoveTablesOneByOne(
                DEFAULT_PARALLELISM,
                PostgresTestUtils.FailoverType.NONE,
                PostgresTestUtils.FailoverPhase.NEVER,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testRemoveAndAddTablesOneByOne() throws Exception {
        testRemoveAndAddTablesOneByOne(
                1, "address_hangzhou", "address_beijing", "address_shanghai");
    }

    private void testRemoveAndAddTablesOneByOne(int parallelism, String... captureAddressTables)
            throws Exception {

        PostgresConnection connection = getConnection();
        // step 1: create postgresql tables with all tables included
        initialAddressTables(connection, captureAddressTables);

        final String savepointDirectory = tempDir.toString();

        // get all expected data
        List<String> fetchedDataList = new ArrayList<>();

        String finishedSavePointPath = null;
        // test removing and adding table one by one
        for (int round = 0; round < captureAddressTables.length; round++) {
            String captureTableThisRound = captureAddressTables[round];
            String cityName = captureTableThisRound.split("_")[1];
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(new HashMap<>(), captureTableThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " table_name STRING,"
                            + " id BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (table_name,id) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // this round's snapshot data
            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    captureTableThisRound, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    captureTableThisRound, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    captureTableThisRound, cityName, cityName)));
            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // step 2: make wal log data for all tables before this round(also includes this round),
            // test whether only this round table's data is captured.
            for (int i = 0; i <= round; i++) {
                String tableName = captureAddressTables[i];
                makeWalLogForAddressTableInRound(connection, tableName, round);
            }
            // this round's wal log data
            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "-U[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    captureTableThisRound, cityName, cityName),
                            format(
                                    "+U[%s, 416874195632735147, China_%s, %s, %s West Town address 1]",
                                    captureTableThisRound, round, cityName, cityName),
                            format(
                                    "+I[%s, %d, China, %s, %s West Town address 4]",
                                    captureTableThisRound,
                                    417022095255614380L + round,
                                    cityName,
                                    cityName)));

            // step 3: assert fetched wal log data in this round
            waitForSinkSize("sink", fetchedDataList.size());

            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));
            // step 4: trigger savepoint
            finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            jobClient.cancel().get();
        }
    }

    private void testRemoveTablesOneByOne(
            int parallelism,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            String... captureAddressTables)
            throws Exception {

        // step 1: create postgresql tables with all tables included
        initialAddressTables(getConnection(), captureAddressTables);

        final String savepointDirectory = tempDir.toString();

        // get all expected data
        List<String> fetchedDataList = new ArrayList<>();
        for (String table : captureAddressTables) {
            String cityName = table.split("_")[1];
            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    table, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    table, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    table, cityName, cityName)));
        }

        String finishedSavePointPath = null;
        // step 2: execute insert and trigger savepoint with all tables added
        {
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(new HashMap<>(), captureAddressTables);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " table_name STRING,"
                            + " id BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (table_name, id) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // trigger failover after some snapshot data read finished
            if (failoverPhase == PostgresTestUtils.FailoverPhase.SNAPSHOT) {
                PostgresTestUtils.triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));
            // sleep 1s to wait for the assign status to INITIAL_ASSIGNING_FINISHED.
            // Otherwise, the restart job won't read newly added tables, and this test will be
            // stuck.
            Thread.sleep(1000L);
            finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            jobClient.cancel().get();
        }

        // test removing table one by one, note that there should be at least one table remaining
        for (int round = 0; round < captureAddressTables.length - 1; round++) {
            String[] captureTablesThisRound =
                    Arrays.asList(captureAddressTables)
                            .subList(round + 1, captureAddressTables.length)
                            .toArray(new String[0]);

            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(new HashMap<>(), captureTablesThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " table_name STRING,"
                            + " id BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (table_name, id) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();

            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // step 3: make wal log data for all tables
            List<String> expectedWalLogDataThisRound = new ArrayList<>();

            for (int i = 0, captureAddressTablesLength = captureAddressTables.length;
                    i < captureAddressTablesLength;
                    i++) {
                String tableName = captureAddressTables[i];
                makeWalLogForAddressTableInRound(getConnection(), tableName, round);
                if (i <= round) {
                    continue;
                }
                String cityName = tableName.split("_")[1];
                expectedWalLogDataThisRound.addAll(
                        Arrays.asList(
                                format(
                                        "-U[%s, 416874195632735147, China%s, %s, %s West Town address 1]",
                                        tableName,
                                        round == 0 ? "" : "_" + (round - 1),
                                        cityName,
                                        cityName),
                                format(
                                        "+U[%s, 416874195632735147, China_%s, %s, %s West Town address 1]",
                                        tableName, round, cityName, cityName),
                                format(
                                        "+I[%s, %d, China, %s, %s West Town address 4]",
                                        tableName,
                                        417022095255614380L + round,
                                        cityName,
                                        cityName)));
            }

            if (failoverPhase == PostgresTestUtils.FailoverPhase.STREAM
                    && TestValuesTableFactory.getRawResultsAsStrings("sink").size()
                            > fetchedDataList.size()) {
                PostgresTestUtils.triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }

            fetchedDataList.addAll(expectedWalLogDataThisRound);
            // step 4: assert fetched wal log data in this round
            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // step 5: trigger savepoint
            finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            jobClient.cancel().get();
        }
    }

    private void testNewlyAddedTableOneByOne(
            int parallelism,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            boolean makeWalLogBeforeCapture,
            String... captureAddressTables)
            throws Exception {
        testNewlyAddedTableOneByOne(
                parallelism,
                new HashMap<>(),
                failoverType,
                failoverPhase,
                makeWalLogBeforeCapture,
                captureAddressTables);
    }

    private void testNewlyAddedTableOneByOne(
            int parallelism,
            Map<String, String> sourceOptions,
            PostgresTestUtils.FailoverType failoverType,
            PostgresTestUtils.FailoverPhase failoverPhase,
            boolean makeWalLogBeforeCapture,
            String... captureAddressTables)
            throws Exception {

        // step 1: create postgres tables with initial data
        initialAddressTables(getConnection(), captureAddressTables);

        final String savepointDirectory = tempDir.toString();

        // test newly added table one by one
        String finishedSavePointPath = null;
        List<String> fetchedDataList = new ArrayList<>();
        for (int round = 0; round < captureAddressTables.length; round++) {
            String[] captureTablesThisRound =
                    Arrays.asList(captureAddressTables)
                            .subList(0, round + 1)
                            .toArray(new String[0]);
            String newlyAddedTable = captureAddressTables[round];
            if (makeWalLogBeforeCapture) {
                makeWalLogBeforeCaptureForAddressTable(getConnection(), newlyAddedTable);
            }
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(sourceOptions, captureTablesThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " table_name STRING,"
                            + " id BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (table_name,id) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // step 2: assert fetched snapshot data in this round
            String cityName = newlyAddedTable.split("_")[1];
            List<String> expectedSnapshotDataThisRound =
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    newlyAddedTable, cityName, cityName));
            if (makeWalLogBeforeCapture) {
                expectedSnapshotDataThisRound =
                        Arrays.asList(
                                format(
                                        "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                        newlyAddedTable, cityName, cityName),
                                format(
                                        "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                        newlyAddedTable, cityName, cityName),
                                format(
                                        "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                        newlyAddedTable, cityName, cityName),
                                format(
                                        "+I[%s, 417022095255614381, China, %s, %s West Town address 5]",
                                        newlyAddedTable, cityName, cityName));
            }

            // trigger failover after some snapshot data read finished
            if (failoverPhase == PostgresTestUtils.FailoverPhase.SNAPSHOT) {
                PostgresTestUtils.triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            fetchedDataList.addAll(expectedSnapshotDataThisRound);
            PostgresTestUtils.waitForUpsertSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getResultsAsStrings("sink"));
            // Wait 1s until snapshot phase finished, make sure the binlog data is not lost.
            Thread.sleep(1000L);

            // step 3: make some wal log data for this round
            makeFirstPartWalLogForAddressTable(getConnection(), newlyAddedTable);
            if (failoverPhase == PostgresTestUtils.FailoverPhase.STREAM) {
                PostgresTestUtils.triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            makeSecondPartWalLogForAddressTable(getConnection(), newlyAddedTable);

            // step 4: assert fetched wal log data in this round
            // retract the old data with id 416874195632735147
            fetchedDataList =
                    fetchedDataList.stream()
                            .filter(
                                    r ->
                                            !r.contains(
                                                    format(
                                                            "%s, 416874195632735147",
                                                            newlyAddedTable)))
                            .collect(Collectors.toList());
            List<String> expectedWalLogUpsertDataThisRound =
                    Arrays.asList(
                            // add the new data with id 416874195632735147
                            format(
                                    "+I[%s, 416874195632735147, CHINA, %s, %s West Town address 1]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614380, China, %s, %s West Town address 4]",
                                    newlyAddedTable, cityName, cityName));

            // step 5: assert fetched wal log data in this round
            fetchedDataList.addAll(expectedWalLogUpsertDataThisRound);

            PostgresTestUtils.waitForUpsertSinkSize("sink", fetchedDataList.size());
            // the result size of sink may arrive fetchedDataList.size() with old data, wait one
            // checkpoint to wait retract old record and send new record
            Thread.sleep(1000);
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getResultsAsStrings("sink"));

            // step 6: trigger savepoint
            if (round != captureAddressTables.length - 1) {
                finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            }
            jobClient.cancel().get();
        }
    }

    private void initialAddressTables(JdbcConnection connection, String[] addressTables)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            for (String tableName : addressTables) {
                // make initial data for given table
                String tableId =
                        customDatabase.getDatabaseName() + '.' + SCHEMA_NAME + '.' + tableName;
                String cityName = tableName.split("_")[1];
                connection.execute(
                        "CREATE TABLE "
                                + tableId
                                + "("
                                + "  id BIGINT NOT NULL PRIMARY KEY,"
                                + "  country VARCHAR(255) NOT NULL,"
                                + "  city VARCHAR(255) NOT NULL,"
                                + "  detail_address VARCHAR(1024)"
                                + ");");
                connection.execute(
                        format(
                                "INSERT INTO  %s "
                                        + "VALUES (416874195632735147, 'China', '%s', '%s West Town address 1'),"
                                        + "       (416927583791428523, 'China', '%s', '%s West Town address 2'),"
                                        + "       (417022095255614379, 'China', '%s', '%s West Town address 3');",
                                tableId, cityName, cityName, cityName, cityName, cityName,
                                cityName));
                connection.execute(format("ALTER TABLE %s REPLICA IDENTITY FULL", tableId));
            }
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeFirstPartWalLogForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make wal log events for the first split
            String tableId = customDatabase.getDatabaseName() + '.' + SCHEMA_NAME + '.' + tableName;
            connection.execute(
                    format(
                            "UPDATE %s SET COUNTRY = 'CHINA' where id = 416874195632735147",
                            tableId));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeSecondPartWalLogForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make wal log events for the second split
            String tableId = customDatabase.getDatabaseName() + '.' + SCHEMA_NAME + '.' + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    format(
                            "INSERT INTO %s VALUES(417022095255614380, 'China','%s','%s West Town address 4')",
                            tableId, cityName, cityName));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeWalLogBeforeCaptureForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make wal log before the capture of the table
            String tableId = customDatabase.getDatabaseName() + '.' + SCHEMA_NAME + '.' + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    format(
                            "INSERT INTO %s VALUES(417022095255614381, 'China','%s','%s West Town address 5')",
                            tableId, cityName, cityName));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeWalLogForAddressTableInRound(
            JdbcConnection connection, String tableName, int round) throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make wal log events for the first split
            String tableId = customDatabase.getDatabaseName() + '.' + SCHEMA_NAME + '.' + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    format(
                            "UPDATE %s SET COUNTRY = 'China_%s' where id = 416874195632735147",
                            tableId, round));
            connection.execute(
                    format(
                            "INSERT INTO %s VALUES(%d, 'China','%s','%s West Town address 4')",
                            tableId, 417022095255614380L + round, cityName, cityName));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws ExecutionException, InterruptedException {
        int retryTimes = 0;
        // retry 600 times, it takes 100 milliseconds per time, at most retry 1 minute
        while (retryTimes < 600) {
            try {
                return jobClient.triggerSavepoint(savepointDirectory).get();
            } catch (Exception e) {
                Optional<CheckpointException> exception =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (exception.isPresent()
                        && exception.get().getMessage().contains("Checkpoint triggering task")) {
                    Thread.sleep(100);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        }
        return null;
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironmentFromSavePoint(
            String finishedSavePointPath, int parallelism) throws Exception {
        Configuration configuration = new Configuration();
        if (finishedSavePointPath != null) {
            configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100L));
        return env;
    }

    private String getCreateTableStatement(
            Map<String, String> otherOptions, String... captureTableNames) {
        return String.format(
                "CREATE TABLE address ("
                        + " table_name STRING METADATA VIRTUAL,"
                        + " id BIGINT NOT NULL,"
                        + " country STRING,"
                        + " city STRING,"
                        + " detail_address STRING,"
                        + " primary key (id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'postgres-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'decoding.plugin.name' = 'pgoutput', "
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'slot.name' = '%s', "
                        + " 'scan.incremental.snapshot.chunk.size' = '2',"
                        + " 'chunk-meta.group.size' = '2',"
                        + " 'scan.newly-added-table.enabled' = 'true'"
                        + " %s"
                        + ")",
                customDatabase.getHost(),
                customDatabase.getDatabasePort(),
                customDatabase.getUsername(),
                customDatabase.getPassword(),
                customDatabase.getDatabaseName(),
                SCHEMA_NAME,
                PostgresTestUtils.getTableNameRegex(captureTableNames),
                slotName,
                otherOptions.isEmpty()
                        ? ""
                        : ","
                                + otherOptions.entrySet().stream()
                                        .map(
                                                e ->
                                                        String.format(
                                                                "'%s'='%s'",
                                                                e.getKey(), e.getValue()))
                                        .collect(Collectors.joining(",")));
    }
}
