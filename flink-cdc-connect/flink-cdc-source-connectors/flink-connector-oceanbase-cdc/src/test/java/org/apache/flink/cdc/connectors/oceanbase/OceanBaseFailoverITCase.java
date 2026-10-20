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

package org.apache.flink.cdc.connectors.oceanbase;

import org.apache.flink.api.common.JobID;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.StringUtils;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.flink.api.common.JobStatus.RUNNING;

/** failover IT tests for oceanbase. */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
public class OceanBaseFailoverITCase extends OceanBaseSourceTestBase {

    private static final String DEFAULT_SCAN_STARTUP_MODE = "initial";
    private static final String DDL_FILE = "oceanbase_ddl_test";
    protected static final int DEFAULT_PARALLELISM = 4;
    private String testDatabase = "customer_" + getRandomSuffix();

    private final List<String> firstPartBinlogEvents =
            Arrays.asList(
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]");

    private final List<String> secondPartBinlogEvents =
            Arrays.asList(
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]",
                    "+U[1010, user_11, Hangzhou, 123567891234]");

    /** The snapshot rows expected from a single customer table, rendered as collect() strings. */
    private static final List<String> SNAPSHOT_ROWS_FOR_SINGLE_TABLE =
            Arrays.asList(
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
                    "+I[2000, user_21, Shanghai, 123567891234]");

    public static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of("customers", null, "false"),
                Arguments.of("customers", "id", "true"),
                Arguments.of("customers_no_pk", "id", "true"));
    }

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

    @BeforeEach
    public void setup() throws InterruptedException {
        testDatabase = "customer_" + getRandomSuffix();
        initializeOceanBaseTables(
                DDL_FILE,
                testDatabase,
                s -> !StringUtils.isNullOrWhitespaceOnly(s) && (s.contains("customers")));
    }

    @AfterEach
    public void clean() {
        dropDatabase(testDatabase);
    }

    // Failover tests
    @ParameterizedTest
    @MethodSource("parameters")
    public void testTaskManagerFailoverInSnapshotPhase(
            String tableName, String chunkColumnName, String assignEndingFirst) throws Exception {
        testMySqlParallelSource(
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                new String[] {tableName, "customers_1"},
                tableName,
                chunkColumnName,
                Collections.singletonMap(
                        "scan.incremental.snapshot.unbounded-chunk-first.enabled",
                        assignEndingFirst));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testTaskManagerFailoverInBinlogPhase(
            String tableName, String chunkColumnName, String assignEndingFirst) throws Exception {
        testMySqlParallelSource(
                FailoverType.TM,
                FailoverPhase.BINLOG,
                new String[] {tableName, "customers_1"},
                tableName,
                chunkColumnName,
                Collections.singletonMap(
                        "scan.incremental.snapshot.unbounded-chunk-first.enabled",
                        assignEndingFirst));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testTaskManagerFailoverFromLatestOffset(
            String tableName, String chunkColumnName, String assignEndingFirst) throws Exception {
        testMySqlParallelSource(
                DEFAULT_PARALLELISM,
                "latest-offset",
                FailoverType.TM,
                FailoverPhase.BINLOG,
                new String[] {tableName, "customers_1"},
                1,
                0,
                tableName,
                chunkColumnName,
                Collections.singletonMap(
                        "scan.incremental.snapshot.unbounded-chunk-first.enabled",
                        assignEndingFirst));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testJobManagerFailoverInSnapshotPhase(
            String tableName, String chunkColumnName, String assignEndingFirst) throws Exception {
        testMySqlParallelSource(
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                new String[] {tableName, "customers_1"},
                tableName,
                chunkColumnName,
                Collections.singletonMap(
                        "scan.incremental.snapshot.unbounded-chunk-first.enabled",
                        assignEndingFirst));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testJobManagerFailoverInBinlogPhase(
            String tableName, String chunkColumnName, String assignEndingFirst) throws Exception {
        testMySqlParallelSource(
                FailoverType.JM,
                FailoverPhase.BINLOG,
                new String[] {tableName, "customers_1"},
                tableName,
                chunkColumnName,
                Collections.singletonMap(
                        "scan.incremental.snapshot.unbounded-chunk-first.enabled",
                        assignEndingFirst));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testJobManagerFailoverFromLatestOffset(
            String tableName, String chunkColumnName, String assignEndingFirst) throws Exception {
        testMySqlParallelSource(
                DEFAULT_PARALLELISM,
                "latest-offset",
                FailoverType.JM,
                FailoverPhase.BINLOG,
                new String[] {tableName, "customers_1"},
                1,
                0,
                tableName,
                chunkColumnName,
                Collections.singletonMap(
                        "scan.incremental.snapshot.unbounded-chunk-first.enabled",
                        assignEndingFirst));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testTaskManagerFailoverSingleParallelism(
            String tableName, String chunkColumnName, String assignEndingFirst) throws Exception {
        testMySqlParallelSource(
                1,
                FailoverType.TM,
                FailoverPhase.SNAPSHOT,
                new String[] {tableName},
                tableName,
                chunkColumnName,
                Collections.singletonMap(
                        "scan.incremental.snapshot.unbounded-chunk-first.enabled",
                        assignEndingFirst));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testJobManagerFailoverSingleParallelism(
            String tableName, String chunkColumnName, String assignEndingFirst) throws Exception {
        testMySqlParallelSource(
                1,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                new String[] {tableName},
                tableName,
                chunkColumnName,
                Collections.singletonMap(
                        "scan.incremental.snapshot.unbounded-chunk-first.enabled",
                        assignEndingFirst));
    }

    private void testMySqlParallelSource(
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            String tableName,
            String chunkColumnName,
            Map<String, String> otherOptions)
            throws Exception {
        testMySqlParallelSource(
                DEFAULT_PARALLELISM,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                tableName,
                chunkColumnName,
                otherOptions);
    }

    private void testMySqlParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            String tableName,
            String chunkColumnName,
            Map<String, String> otherOptions)
            throws Exception {
        testMySqlParallelSource(
                parallelism,
                DEFAULT_SCAN_STARTUP_MODE,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                1,
                0,
                tableName,
                chunkColumnName,
                otherOptions);
    }

    private void testMySqlParallelSource(
            int parallelism,
            String scanStartupMode,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            int restartAttempts,
            long delayBetweenAttempts,
            String tableName,
            String chunkColumnName,
            Map<String, String> otherOptions)
            throws Exception {
        testMySqlParallelSource(
                parallelism,
                scanStartupMode,
                failoverType,
                failoverPhase,
                captureCustomerTables,
                restartAttempts,
                delayBetweenAttempts,
                false,
                tableName,
                chunkColumnName,
                otherOptions);
    }

    private void testMySqlParallelSource(
            int parallelism,
            String scanStartupMode,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables,
            int restartAttempts,
            long delayBetweenAttempts,
            boolean skipSnapshotBackfill,
            String tableName,
            String chunkColumnName,
            Map<String, String> otherOptions)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(
                env, restartAttempts, delayBetweenAttempts);
        String sourceDDL =
                format(
                        "CREATE TABLE customers ("
                                + " id BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING"
                                + ("customers_no_pk".equals(tableName)
                                        ? ""
                                        : ", primary key (id) not enforced")
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'scan.incremental.snapshot.backfill.skip' = '%s',"
                                + " 'server-time-zone' = 'Asia/Shanghai',"
                                + " 'server-id' = '%s'"
                                + " %s"
                                + " %s"
                                + ")",
                        getHost(),
                        getPort(),
                        getUserName(),
                        getPassword(),
                        testDatabase,
                        getTableNameRegex(captureCustomerTables),
                        scanStartupMode,
                        skipSnapshotBackfill,
                        getServerId(),
                        chunkColumnName == null
                                ? ""
                                : String.format(
                                        ", 'scan.incremental.snapshot.chunk.key-column' = '%s'",
                                        chunkColumnName),
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
        tEnv.executeSql(sourceDDL);
        if (!DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            // In latest-offset mode the job must not resolve its start offset before the rows
            // written during setup() are materialized by the OceanBase binlog service,
            // otherwise they are read back as +I events and break the assertions.
            waitForBinlogServiceCaughtUp();
        }
        TableResult tableResult = tEnv.executeSql("select * from customers");

        // first step: check the snapshot data
        if (DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            checkSnapshotData(tableResult, failoverType, failoverPhase, captureCustomerTables);
        }

        // second step: check the binlog data
        checkBinlogData(tableResult, failoverType, failoverPhase, captureCustomerTables);

        sleepMs(3000);
        tableResult.getJobClient().get().cancel().get();
    }

    private void checkSnapshotData(
            TableResult tableResult,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedSnapshotData.addAll(SNAPSHOT_ROWS_FOR_SINGLE_TABLE);
        }

        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();

        // trigger failover after some snapshot splits read finished
        if (failoverPhase == FailoverPhase.SNAPSHOT && iterator.hasNext()) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(100));
        }

        boolean capturedTableHasNoPrimaryKey =
                Arrays.stream(captureCustomerTables).anyMatch(table -> table.contains("no_pk"));
        if (capturedTableHasNoPrimaryKey) {
            // Tables without a primary key only provide at-least-once delivery across a failover,
            // so the snapshot stream may replay duplicate +I rows after the TaskManager restarts.
            // Verify every expected row is observed with at least its expected multiplicity and
            // that no unexpected row ever appears, instead of requiring an exact 1:1 row count.
            // Any trailing replayed rows are consumed later by the binlog phase (see
            // fetchBinlogRowsSkippingReplayedSnapshotRows), so no draining is needed here.
            assertSnapshotDataAllowingDuplicates(iterator, expectedSnapshotData);
        } else {
            assertEqualsInAnyOrder(
                    expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));
        }
    }

    private void assertSnapshotDataAllowingDuplicates(
            CloseableIterator<Row> iterator, List<String> expectedSnapshotData) {
        Map<String, Long> outstanding =
                expectedSnapshotData.stream()
                        .collect(Collectors.groupingBy(row -> row, Collectors.counting()));
        long remaining = expectedSnapshotData.size();
        // Blocking reads: the restarted snapshot re-reads each split in full, so every expected
        // row is guaranteed to (re)appear, which makes this loop terminate.
        while (remaining > 0) {
            String value = iterator.next().toString();
            Assertions.assertThat(outstanding)
                    .withFailMessage("Unexpected snapshot row: %s", value)
                    .containsKey(value);
            long left = outstanding.get(value);
            if (left > 0) {
                outstanding.put(value, left - 1);
                remaining--;
            }
        }
    }

    private void checkBinlogData(
            TableResult tableResult,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        waitUntilJobRunning(tableResult);
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();

        for (String tableId : captureCustomerTables) {
            makeFirstPartBinlogEvents(getConnection(), testDatabase + '.' + tableId);
        }

        // wait for the binlog reading
        Thread.sleep(3_000L);

        if (failoverPhase == FailoverPhase.BINLOG) {
            triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(200));
            waitUntilJobRunning(tableResult);
        }
        for (String tableId : captureCustomerTables) {
            makeSecondPartBinlogEvents(getConnection(), testDatabase + '.' + tableId);
        }

        List<String> expectedBinlogData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedBinlogData.addAll(firstPartBinlogEvents);
            expectedBinlogData.addAll(secondPartBinlogEvents);
        }
        sleepMs(3_000);
        boolean capturedTableHasNoPrimaryKey =
                Arrays.stream(captureCustomerTables).anyMatch(table -> table.contains("no_pk"));
        List<String> binlogData =
                capturedTableHasNoPrimaryKey
                        ? fetchBinlogRowsSkippingReplayedSnapshotRows(
                                iterator, expectedBinlogData.size())
                        : fetchRows(iterator, expectedBinlogData.size());
        assertEqualsInAnyOrder(expectedBinlogData, binlogData);
        Assertions.assertThat(hasNextData(iterator)).isFalse();
    }

    /**
     * For tables without a primary key the snapshot re-read triggered by an at-least-once failover
     * may replay duplicate +I rows. These replayed snapshot rows always precede the changelog
     * events, because the source finishes the whole snapshot phase before reading binlog. Drop the
     * leading rows that match a snapshot row, then collect the expected binlog changelog. This runs
     * entirely on the calling thread, so it never leaves an in-flight {@code hasNext()} racing with
     * subsequent reads of the same collect iterator.
     */
    private List<String> fetchBinlogRowsSkippingReplayedSnapshotRows(
            CloseableIterator<Row> iterator, int size) {
        List<String> rows = new ArrayList<>(size);
        boolean skippingReplayedSnapshotRows = true;
        while (rows.size() < size) {
            String value = iterator.next().toString();
            if (skippingReplayedSnapshotRows && SNAPSHOT_ROWS_FOR_SINGLE_TABLE.contains(value)) {
                continue;
            }
            // The first changelog event (a -U/-D/+U, or a +I that is not a snapshot row) marks the
            // end of any replayed snapshot rows; from here on every row belongs to the binlog
            // phase.
            skippingReplayedSnapshotRows = false;
            rows.add(value);
        }
        return rows;
    }

    private void waitUntilJobRunning(TableResult tableResult)
            throws InterruptedException, ExecutionException {
        do {
            Thread.sleep(5000L);
        } while (tableResult.getJobClient().get().getJobStatus().get() != RUNNING);
    }

    private boolean hasNextData(final CloseableIterator<?> iterator)
            throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            FutureTask<Boolean> future = new FutureTask(iterator::hasNext);
            executor.execute(future);
            return future.get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return false;
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Make some changes on the specified customer table. Changelog in string could be accessed by
     * {@link #firstPartBinlogEvents}.
     */
    private void makeFirstPartBinlogEvents(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make binlog events for the first split
            connection.execute(
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableId + " where id = 102",
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                    "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    /**
     * Make some other changes on the specified customer table. Changelog in string could be
     * accessed by {@link #secondPartBinlogEvents}.
     */
    private void makeSecondPartBinlogEvents(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make binlog events for split-1
            connection.execute("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 1010");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2001, 'user_22','Shanghai','123567891234'),"
                            + " (2002, 'user_23','Shanghai','123567891234'),"
                            + "(2003, 'user_24','Shanghai','123567891234')");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    /**
     * The OceanBase binlog service materializes committed transactions asynchronously. In
     * latest-offset mode the source must not resolve its start offset before the rows written
     * during {@link #setup()} are materialized, otherwise they are read back as +I events and break
     * the assertions. This writes a marker into a non-captured table and waits until the binlog
     * offset moves past it and stops advancing, which guarantees all earlier writes are visible.
     */
    private void waitForBinlogServiceCaughtUp() throws Exception {
        String markerTable = testDatabase + ".binlog_sync_marker";
        MySqlConnection connection = getConnection();
        try {
            BinlogOffset before = DebeziumUtils.currentBinlogOffset(connection);
            connection.setAutoCommit(false);
            connection.execute("CREATE TABLE IF NOT EXISTS " + markerTable + " (id INT)");
            connection.execute("INSERT INTO " + markerTable + " VALUES (1)");
            connection.commit();

            long deadline = System.currentTimeMillis() + 60_000L;
            BinlogOffset previous = null;
            int stableTimes = 0;
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(500L);
                BinlogOffset current = DebeziumUtils.currentBinlogOffset(connection);
                if (previous != null
                        && current.isAfter(before)
                        && current.compareTo(previous) == 0) {
                    if (++stableTimes >= 2) {
                        return;
                    }
                } else {
                    stableTimes = 0;
                }
                previous = current;
            }
            throw new IllegalStateException(
                    "OceanBase binlog service did not catch up with setup writes in time.");
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
}
