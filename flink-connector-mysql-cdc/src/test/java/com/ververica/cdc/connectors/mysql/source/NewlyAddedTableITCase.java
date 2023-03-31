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

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.ExceptionUtils;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;

/** IT tests to cover various newly added tables during capture process. */
public class NewlyAddedTableITCase extends MySqlSourceTestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    private final ScheduledExecutorService mockBinlogExecutor = Executors.newScheduledThreadPool(1);

    @Before
    public void before() throws SQLException {
        TestValuesTableFactory.clearAllData();
        customDatabase.createAndInitialize();

        try (MySqlConnection connection = getConnection()) {
            connection.setAutoCommit(false);
            // prepare initial data for given table
            String tableId = customDatabase.getDatabaseName() + ".produce_binlog_table";
            connection.execute(
                    format("CREATE TABLE %s ( id BIGINT PRIMARY KEY, cnt BIGINT);", tableId));
            connection.execute(
                    format("INSERT INTO  %s VALUES (0, 100), (1, 101), (2, 102);", tableId));
            connection.commit();

            // mock continuous binlog during the newly added table capturing process
            mockBinlogExecutor.schedule(
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
    }

    @After
    public void after() {
        mockBinlogExecutor.shutdown();
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineOnce() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineOnceWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineTwice() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineTwiceWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineSingleParallelism() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineSingleParallelismWithAheadBinlog()
            throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testJobManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testJobManagerFailoverForNewlyAddedTableWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testTaskManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.BINLOG,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testTaskManagerFailoverForNewlyAddedTableWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.BINLOG,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    private void testNewlyAddedTableOneByOne(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            boolean makeBinlogBeforeCapture,
            String... captureAddressTables)
            throws Exception {

        // step 1: create mysql tables with initial data
        initialAddressTables(getConnection(), captureAddressTables);

        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final String savepointDirectory = temporaryFolder.newFolder().toURI().toString();

        // test newly added table one by one
        String finishedSavePointPath = null;
        List<String> fetchedDataList = new ArrayList<>();
        for (int round = 0; round < captureAddressTables.length; round++) {
            String[] captureTablesThisRound =
                    Arrays.asList(captureAddressTables)
                            .subList(0, round + 1)
                            .toArray(new String[0]);
            String newlyAddedTable = captureAddressTables[round];
            if (makeBinlogBeforeCapture) {
                makeBinlogBeforeCaptureForAddressTable(getConnection(), newlyAddedTable);
            }
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironment(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement = getCreateTableStatement(captureTablesThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " table_name STRING,"
                            + " id BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (city, id) not enforced"
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
            if (makeBinlogBeforeCapture) {
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
                        miniClusterResource.getMiniCluster(),
                        () -> sleepMs(100));
            }
            fetchedDataList.addAll(expectedSnapshotDataThisRound);
            waitForUpsertSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(fetchedDataList, TestValuesTableFactory.getResults("sink"));

            // step 3: make some binlog data for this round
            makeFirstPartBinlogForAddressTable(getConnection(), newlyAddedTable);
            if (failoverPhase == FailoverPhase.BINLOG) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.getMiniCluster(),
                        () -> sleepMs(100));
            }
            makeSecondPartBinlogForAddressTable(getConnection(), newlyAddedTable);

            // step 4: assert fetched binlog data in this round

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
            List<String> expectedBinlogUpsertDataThisRound =
                    Arrays.asList(
                            // add the new data with id 416874195632735147
                            format(
                                    "+I[%s, 416874195632735147, CHINA, %s, %s West Town address 1]",
                                    newlyAddedTable, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614380, China, %s, %s West Town address 4]",
                                    newlyAddedTable, cityName, cityName));

            // step 5: assert fetched binlog data in this round
            fetchedDataList.addAll(expectedBinlogUpsertDataThisRound);

            waitForUpsertSinkSize("sink", fetchedDataList.size());
            // the result size of sink may arrive fetchedDataList.size() with old data, wait one
            // checkpoint to wait retract old record and send new record
            Thread.sleep(1000);
            assertEqualsInAnyOrder(fetchedDataList, TestValuesTableFactory.getResults("sink"));

            // step 6: trigger savepoint
            if (round != captureAddressTables.length - 1) {
                finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            }
            jobClient.cancel().get();
        }
    }

    private String getCreateTableStatement(String... captureTableNames) {
        return format(
                "CREATE TABLE address ("
                        + " table_name STRING METADATA VIRTUAL,"
                        + " id BIGINT NOT NULL,"
                        + " country STRING,"
                        + " city STRING,"
                        + " detail_address STRING,"
                        + " primary key (city, id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'mysql-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.incremental.snapshot.chunk.size' = '2',"
                        + " 'server-time-zone' = 'UTC',"
                        + " 'server-id' = '%s',"
                        + " 'scan.newly-added-table.enabled' = 'true'"
                        + ")",
                MYSQL_CONTAINER.getHost(),
                MYSQL_CONTAINER.getDatabasePort(),
                customDatabase.getUsername(),
                customDatabase.getPassword(),
                customDatabase.getDatabaseName(),
                getTableNameRegex(captureTableNames),
                getServerId());
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment(
            String finishedSavePointPath, int parallelism) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (finishedSavePointPath != null) {
            // restore from savepoint
            // hack for test to visit protected TestStreamEnvironment#getConfiguration() method
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz =
                    classLoader.loadClass(
                            "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment");
            Field field = clazz.getDeclaredField("configuration");
            field.setAccessible(true);
            Configuration configuration = (Configuration) field.get(env);
            configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100L));
        return env;
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

    private String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return format("(%s)", StringUtils.join(captureCustomerTables, "|"));
        }
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + DEFAULT_PARALLELISM);
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private void initialAddressTables(JdbcConnection connection, String[] addressTables)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            for (String tableName : addressTables) {
                // make initial data for given table
                String tableId = customDatabase.getDatabaseName() + "." + tableName;
                String cityName = tableName.split("_")[1];
                connection.execute(
                        "CREATE TABLE "
                                + tableId
                                + "("
                                + "  id BIGINT UNSIGNED NOT NULL PRIMARY KEY,"
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
            }
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeFirstPartBinlogForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog events for the first split
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
            String cityName = tableName.split("_")[1];
            connection.execute(
                    format(
                            "UPDATE %s SET COUNTRY = 'CHINA' where id = 416874195632735147",
                            tableId));
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeSecondPartBinlogForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog events for the second split
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
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

    private void makeBinlogBeforeCaptureForAddressTable(JdbcConnection connection, String tableName)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog before the capture of the table
            String tableId = customDatabase.getDatabaseName() + "." + tableName;
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

    private MySqlConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        return DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }
}
