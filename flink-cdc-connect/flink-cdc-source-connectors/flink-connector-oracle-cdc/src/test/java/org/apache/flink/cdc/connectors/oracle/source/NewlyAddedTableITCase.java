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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.oracle.testutils.OracleTestUtils.FailoverPhase;
import org.apache.flink.cdc.connectors.oracle.testutils.OracleTestUtils.FailoverType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
import static org.apache.flink.cdc.connectors.oracle.testutils.OracleTestUtils.getTableNameRegex;
import static org.apache.flink.cdc.connectors.oracle.testutils.OracleTestUtils.triggerFailover;
import static org.apache.flink.cdc.connectors.oracle.testutils.OracleTestUtils.waitForSinkSize;
import static org.apache.flink.cdc.connectors.oracle.testutils.OracleTestUtils.waitForUpsertSinkSize;

/** IT tests to cover various newly added tables during capture process. */
@Timeout(value = 600, unit = TimeUnit.SECONDS)
class NewlyAddedTableITCase extends OracleSourceTestBase {

    private final ScheduledExecutorService mockRedoLogExecutor =
            Executors.newScheduledThreadPool(1);

    @TempDir private static Path tempFolder;

    @BeforeAll
    public static void beforeClass() throws SQLException {
        try (Connection dbaConnection = getJdbcConnectionAsDBA();
                Statement dbaStatement = dbaConnection.createStatement()) {
            dbaStatement.execute("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        }
    }

    @BeforeEach
    public void before() throws Exception {
        TestValuesTableFactory.clearAllData();
        createAndInitialize("customer.sql");
        try (Connection connection = getJdbcConnection()) {
            Statement statement = connection.createStatement();
            connection.setAutoCommit(false);
            // prepare initial data for given table
            String tableId = ORACLE_SCHEMA + ".PRODUCE_LOG_TABLE";
            statement.execute(
                    format(
                            "CREATE TABLE %s ( ID NUMBER(19), CNT NUMBER(19), PRIMARY KEY(ID))",
                            tableId));
            statement.execute(format("INSERT INTO  %s VALUES (0, 100)", tableId));
            statement.execute(format("INSERT INTO  %s VALUES (1, 101)", tableId));
            statement.execute(format("INSERT INTO  %s VALUES (2, 102)", tableId));
            connection.commit();

            // mock continuous redo log during the newly added table capturing process
            mockRedoLogExecutor.schedule(
                    () -> {
                        try {
                            executeSql(format("UPDATE %s SET  CNT = CNT +1 WHERE ID < 2", tableId));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    },
                    500,
                    TimeUnit.MICROSECONDS);
        }
    }

    @AfterEach
    public void after() throws Exception {
        mockRedoLogExecutor.shutdown();
        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
    }

    @Test
    void testNewlyAddedTableForExistsPipelineOnce() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineOnceWithAheadRedoLog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineTwice() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineTwiceWithAheadRedoLog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineTwiceWithAheadRedoLogAndAutoCloseReader()
            throws Exception {
        Map<String, String> otherOptions = new HashMap<>();
        otherOptions.put("scan.incremental.close-idle-reader.enabled", "true");
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                otherOptions,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineThrice() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI",
                "ADDRESS_SHENZHEN");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineThriceWithAheadRedoLog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI",
                "ADDRESS_SHENZHEN");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineSingleParallelism() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING");
    }

    @Test
    void testNewlyAddedTableForExistsPipelineSingleParallelismWithAheadRedoLog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING");
    }

    @Test
    void testJobManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                false,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING");
    }

    @Test
    void testJobManagerFailoverForNewlyAddedTableWithAheadRedoLog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                true,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING");
    }

    @Test
    void testTaskManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.REDO_LOG,
                false,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING");
    }

    @Test
    void testTaskManagerFailoverForNewlyAddedTableWithAheadRedoLog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.REDO_LOG,
                false,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING");
    }

    @Test
    void testJobManagerFailoverForRemoveTableSingleParallelism() throws Exception {
        testRemoveTablesOneByOne(
                1,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI");
    }

    @Test
    void testJobManagerFailoverForRemoveTable() throws Exception {
        testRemoveTablesOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI");
    }

    @Test
    void testTaskManagerFailoverForRemoveTableSingleParallelism() throws Exception {
        testRemoveTablesOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI");
    }

    @Test
    void testTaskManagerFailoverForRemoveTable() throws Exception {
        testRemoveTablesOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI");
    }

    @Test
    void testRemoveTableSingleParallelism() throws Exception {
        testRemoveTablesOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI");
    }

    @Test
    void testRemoveTable() throws Exception {
        testRemoveTablesOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                "ADDRESS_HANGZHOU",
                "ADDRESS_BEIJING",
                "ADDRESS_SHANGHAI");
    }

    @Test
    void testRemoveAndAddTablesOneByOne() throws Exception {
        testRemoveAndAddTablesOneByOne(
                1, "ADDRESS_HANGZHOU", "ADDRESS_BEIJING", "ADDRESS_SHANGHAI");
    }

    private void testRemoveAndAddTablesOneByOne(int parallelism, String... captureAddressTables)
            throws Exception {

        Connection connection = getJdbcConnection();
        // step 1: create tables with all tables included
        initialAddressTables(connection, captureAddressTables);

        final String savepointDirectory = tempFolder.toString();

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
                            + " TABLE_NAME STRING,"
                            + " ID BIGINT,"
                            + " COUNTRY STRING,"
                            + " CITY STRING,"
                            + " DETAIL_ADDRESS STRING,"
                            + " primary key (CITY, ID) not enforced"
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

            // step 2: make redo log data for all tables before this round(also includes this
            // round),
            // test whether only this round table's data is captured.
            for (int i = 0; i <= round; i++) {
                String tableName = captureAddressTables[i];
                makeRedoLogForAddressTableInRound(tableName, round);
            }
            // this round's redo log data
            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "+U[%s, 416874195632735147, CHINA_%s, %s, %s West Town address 1]",
                                    captureTableThisRound, round, cityName, cityName),
                            format(
                                    "+I[%s, %d, China, %s, %s West Town address 4]",
                                    captureTableThisRound,
                                    417022095255614380L + round,
                                    cityName,
                                    cityName)));

            // step 3: assert fetched redo log data in this round
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
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String... captureAddressTables)
            throws Exception {

        // step 1: create oracle tables with all tables included
        initialAddressTables(getJdbcConnection(), captureAddressTables);

        final String savepointDirectory = tempFolder.toString();

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
                            + " TABLE_NAME STRING,"
                            + " ID BIGINT,"
                            + " COUNTRY STRING,"
                            + " CITY STRING,"
                            + " DETAIL_ADDRESS STRING,"
                            + " primary key (CITY, ID) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // trigger failover after some snapshot data read finished
            if (failoverPhase == FailoverPhase.SNAPSHOT) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));
            // sleep 10s to wait for the assign status to INITIAL_ASSIGNING_FINISHED.
            // Otherwise, the restart job won't read newly added tables, and this test will be
            // stuck.
            sleepMs(10000);
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
                            + " TABLE_NAME STRING,"
                            + " ID BIGINT,"
                            + " COUNTRY STRING,"
                            + " CITY STRING,"
                            + " DETAIL_ADDRESS STRING,"
                            + " primary key (CITY, ID) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult = tEnv.executeSql("insert into sink select * from address");
            JobClient jobClient = tableResult.getJobClient().get();

            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // step 3: make redo log data for all tables
            List<String> expectedRedoLogDataThisRound = new ArrayList<>();

            for (int i = 0, captureAddressTablesLength = captureAddressTables.length;
                    i < captureAddressTablesLength;
                    i++) {
                String tableName = captureAddressTables[i];
                makeRedoLogForAddressTableInRound(tableName, round);
                if (i <= round) {
                    continue;
                }
                String cityName = tableName.split("_")[1];

                expectedRedoLogDataThisRound.addAll(
                        Arrays.asList(
                                format(
                                        "+U[%s, 416874195632735147, CHINA_%s, %s, %s West Town address 1]",
                                        tableName, round, cityName, cityName),
                                format(
                                        "+I[%s, %d, China, %s, %s West Town address 4]",
                                        tableName,
                                        417022095255614380L + round,
                                        cityName,
                                        cityName)));
            }

            if (failoverPhase == FailoverPhase.REDO_LOG
                    && TestValuesTableFactory.getRawResultsAsStrings("sink").size()
                            > fetchedDataList.size()) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }

            fetchedDataList.addAll(expectedRedoLogDataThisRound);
            // step 4: assert fetched redo log data in this round
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
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            boolean makeRedoLogBeforeCapture,
            String... captureAddressTables)
            throws Exception {
        testNewlyAddedTableOneByOne(
                parallelism,
                new HashMap<>(),
                failoverType,
                failoverPhase,
                makeRedoLogBeforeCapture,
                captureAddressTables);
    }

    private void testNewlyAddedTableOneByOne(
            int parallelism,
            Map<String, String> sourceOptions,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            boolean makeRedoLogBeforeCapture,
            String... captureAddressTables)
            throws Exception {

        // step 1: create oracle tables with initial data
        initialAddressTables(getJdbcConnection(), captureAddressTables);

        final String savepointDirectory = tempFolder.toString();

        // test newly added table one by one
        String finishedSavePointPath = null;
        List<String> fetchedDataList = new ArrayList<>();
        for (int round = 0; round < captureAddressTables.length; round++) {
            String[] captureTablesThisRound =
                    Arrays.asList(captureAddressTables)
                            .subList(0, round + 1)
                            .toArray(new String[0]);
            String newlyAddedTable = captureAddressTables[round];
            if (makeRedoLogBeforeCapture) {
                makeRedoLogBeforeCaptureForAddressTable(newlyAddedTable);
            }
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(sourceOptions, captureTablesThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " TABLE_NAME STRING,"
                            + " ID BIGINT,"
                            + " COUNTRY STRING,"
                            + " CITY STRING,"
                            + " DETAIL_ADDRESS STRING,"
                            + " primary key (CITY, ID) not enforced"
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
            if (makeRedoLogBeforeCapture) {
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
            if (failoverPhase == FailoverPhase.SNAPSHOT) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            fetchedDataList.addAll(expectedSnapshotDataThisRound);
            waitForUpsertSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getResultsAsStrings("sink"));
            // Wait 1s until snapshot phase finished, make sure the binlog data is not lost.
            Thread.sleep(1000L);

            // step 3: make some redo log data for this round
            makeFirstPartRedoLogForAddressTable(newlyAddedTable);
            if (failoverPhase == FailoverPhase.REDO_LOG) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            makeSecondPartRedoLogForAddressTable(newlyAddedTable);

            // step 4: assert fetched redo log data in this round
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
            List<String> expectedRedoLogUpsertDataThisRound =
                    Arrays.asList(
                            // add the new data with id 416874195632735147
                            format(
                                    "+I[%s, 416874195632735147, CHINA, %s, %s West Town address 1]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614380, China, %s, %s West Town address 4]",
                                    newlyAddedTable, cityName, cityName));

            // step 5: assert fetched redo log data in this round
            fetchedDataList.addAll(expectedRedoLogUpsertDataThisRound);

            waitForUpsertSinkSize("sink", fetchedDataList.size());
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

    private void initialAddressTables(Connection connection, String[] addressTables)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String tableName : addressTables) {
                // make initial data for given table
                String tableId = ORACLE_SCHEMA + '.' + tableName;
                String cityName = tableName.split("_")[1];
                statement.execute(
                        "CREATE TABLE "
                                + tableId
                                + "("
                                + "  ID NUMBER(19) NOT NULL,"
                                + "  COUNTRY VARCHAR(255) NOT NULL,"
                                + "  CITY VARCHAR(255) NOT NULL,"
                                + "  DETAIL_ADDRESS VARCHAR(1024),"
                                + "  PRIMARY KEY(ID)"
                                + ")");
                statement.execute(
                        format(
                                "INSERT INTO  %s "
                                        + "VALUES (416874195632735147, 'China', '%s', '%s West Town address 1')",
                                tableId, cityName, cityName));
                statement.execute(
                        format(
                                "INSERT INTO  %s "
                                        + "VALUES (416927583791428523, 'China', '%s', '%s West Town address 2')",
                                tableId, cityName, cityName));
                statement.execute(
                        format(
                                "INSERT INTO  %s "
                                        + "VALUES (417022095255614379, 'China', '%s', '%s West Town address 3')",
                                tableId, cityName, cityName));
            }
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeFirstPartRedoLogForAddressTable(String tableName) throws Exception {
        String tableId = ORACLE_SCHEMA + '.' + tableName;
        executeSql(
                format("UPDATE %s SET COUNTRY = 'CHINA' where ID = 416874195632735147", tableId));
    }

    private void makeSecondPartRedoLogForAddressTable(String tableName) throws Exception {
        String tableId = ORACLE_SCHEMA + '.' + tableName;
        String cityName = tableName.split("_")[1];
        executeSql(
                format(
                        "INSERT INTO %s VALUES(417022095255614380, 'China','%s','%s West Town address 4')",
                        tableId, cityName, cityName));
    }

    private void makeRedoLogBeforeCaptureForAddressTable(String tableName) throws Exception {
        String tableId = ORACLE_SCHEMA + '.' + tableName;
        String cityName = tableName.split("_")[1];
        executeSql(
                format(
                        "INSERT INTO %s VALUES(417022095255614381, 'China','%s','%s West Town address 5')",
                        tableId, cityName, cityName));
    }

    private void makeRedoLogForAddressTableInRound(String tableName, int round) throws Exception {
        String tableId = ORACLE_SCHEMA + '.' + tableName;
        String cityName = tableName.split("_")[1];
        executeSql(
                format(
                        "UPDATE %s SET COUNTRY = 'CHINA_%s' where id = 416874195632735147",
                        tableId, round));
        executeSql(
                format(
                        "INSERT INTO %s VALUES(%d, 'China','%s','%s West Town address 4')",
                        tableId, 417022095255614380L + round, cityName, cityName));
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
                        + " ID BIGINT NOT NULL,"
                        + " COUNTRY STRING,"
                        + " CITY STRING,"
                        + " DETAIL_ADDRESS STRING,"
                        + " primary key (CITY, ID) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'oracle-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'debezium.log.mining.strategy' = 'online_catalog',"
                        + " 'debezium.database.history.store.only.captured.tables.ddl' = 'true',"
                        + " 'scan.incremental.snapshot.chunk.size' = '2',"
                        + " 'scan.newly-added-table.enabled' = 'true',"
                        + " 'chunk-meta.group.size' = '2'"
                        + " %s"
                        + ")",
                ORACLE_CONTAINER.getHost(),
                ORACLE_CONTAINER.getOraclePort(),
                // To analyze table for approximate rowCnt computation, use admin user before chunk
                // splitting.
                TOP_USER,
                TOP_SECRET,
                ORACLE_DATABASE,
                ORACLE_SCHEMA,
                getTableNameRegex(captureTableNames),
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

    private void executeSql(String sql) throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
