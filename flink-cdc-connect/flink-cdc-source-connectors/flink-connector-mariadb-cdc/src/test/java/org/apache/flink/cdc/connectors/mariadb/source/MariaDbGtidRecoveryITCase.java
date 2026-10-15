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

package org.apache.flink.cdc.connectors.mariadb.source;

import org.apache.flink.cdc.connectors.mariadb.testutils.MariaDbContainer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.ExceptionUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * IT case proving the standalone {@code mariadb-cdc} connector resumes binlog streaming from a
 * MariaDB GTID offset that was serialized into a savepoint, and that the resume is driven by the
 * GTID itself rather than merely by within-transaction event/row skipping.
 *
 * <p>The delicate part is <em>which</em> GTID the savepoint captures. Debezium only advances the
 * restartable GTID set once a transaction commits, and MariaDB emits the commit as a trailing
 * {@code XID} event; because Flink CDC does not surface {@code XID} events to the reader, a
 * transaction's completed GTID only reaches the Flink split state when the <em>next</em> emitted
 * record carries it. So a savepoint taken right after transaction A's row still holds the GTID from
 * <em>before</em> A. To move the committed GTID strictly past A, this test drives a second captured
 * transaction A' and waits for its row to arrive. That row carries the restart offset from before
 * A''s own commit is surfaced to the reader, so the split state's restartable GTID set then
 * includes A but not yet A' (even though A' has already autocommitted on the server). The scenario:
 *
 * <ol>
 *   <li>Consume the two snapshot rows (source enters the binlog phase).
 *   <li>Insert transaction A (id=100) and wait for its row.
 *   <li>Insert transaction A' (id=101) and wait for its row; the split state's committed GTID is
 *       now past A.
 *   <li>Take a savepoint (serializing that GTID) and cancel the job.
 *   <li>Restart from the savepoint. On restore the connector reconnects from a GTID set that
 *       already contains A ({@code checkMariadbGtidSet} + {@code connectMariaDb} with {@code
 *       @mariadb_slave_capability=4}), so the server never re-sends A.
 *   <li>Insert transaction B (id=200) and assert the sink ends up with exactly {@code {1, 2, 100,
 *       101, 200}} — A is not replayed (proving GTID-based resume), and A' is de-duplicated by the
 *       offset's event/row skip.
 * </ol>
 *
 * <p>This exercises the PR-2 code paths end to end: {@code currentBinlogOffset}, the MariaDB-flavor
 * {@code BinlogOffset.compareTo}, {@code checkMariadbGtidSet}, {@code connectMariaDb}, and the
 * {@code MariaDBBinaryLogClient} capability handshake that makes the per-transaction GTID advance.
 */
class MariaDbGtidRecoveryITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MariaDbGtidRecoveryITCase.class);

    private static final String TABLE = "gtid_recovery";
    private static final String SINK = "sink";

    /**
     * A table the connector does not capture. Writing to it advances the server's GTID position
     * without producing any change event, which lets a test move {@code @@gtid_binlog_pos} to a
     * known value at a deterministic point in time.
     */
    private static final String MARKER_TABLE = "failover_marker";

    private final MariaDbContainer mariaDb = new MariaDbContainer();

    @BeforeEach
    void before() throws Exception {
        LOG.info("Starting MariaDB container...");
        Startables.deepStart(Stream.of(mariaDb)).join();
        LOG.info("MariaDB container started.");
        TestValuesTableFactory.clearAllData();
        initializeTable();
    }

    @AfterEach
    void after() {
        mariaDb.stop();
    }

    @Test
    @Timeout(180)
    void testGtidRecoveryAfterBinlogSavepoint(@TempDir Path tempDir) throws Exception {
        final String savepointDirectory = tempDir.toUri().toString();

        // Phase 1: advance the committed GTID past transaction A, then savepoint it.
        String savepointPath = runJobUntilBinlogAndSavepoint(savepointDirectory);

        // Phase 2: restore, insert transaction B, and prove A is not replayed.
        runJobFromSavepointAndInsertB(savepointPath);

        // The sink accumulates across both phases; a GTID resume past A appends only transaction B
        // (A' is de-duplicated by the offset skip), while a wrongful replay would duplicate A here.
        Assertions.assertThat(TestValuesTableFactory.getRawResultsAsStrings(SINK))
                .containsExactlyInAnyOrder("+I[1]", "+I[2]", "+I[100]", "+I[101]", "+I[200]");
    }

    /**
     * Simulates reconnecting to a different physical node after a failover. The savepoint's GTID
     * carries the original server id, while the server records subsequent transactions under a new
     * server id in the same replication domain (MariaDB allows {@code server_id} to change at
     * runtime). Recovery must still succeed because {@code MariaDbGtidComparator} keys the
     * containment check on domain + sequence and ignores the server id. A server-id-sensitive
     * comparison would find no entry for the original server id in the server's GTID set, fail
     * containment, and the task would fail fast instead of resuming — so this test discriminates
     * between the two implementations.
     */
    @Test
    @Timeout(180)
    void testGtidRecoveryAcrossServerIdChange(@TempDir Path tempDir) throws Exception {
        final String savepointDirectory = tempDir.toUri().toString();

        String savepointPath = runJobUntilBinlogAndSavepoint(savepointDirectory);
        String gtidBeforeFailover = queryScalar("SELECT @@gtid_binlog_pos");
        Assertions.assertThat(gtidBeforeFailover).startsWith("0-223344-");

        // The "promoted node" keeps the same domain and history but a different server id.
        // Changing server_id alone does NOT move @@gtid_binlog_pos — only the next committed
        // transaction does. Commit one here, into a table the connector does not capture, so the
        // server's GTID set deterministically carries the NEW server id *before* the restored job
        // performs its recovery check. Without this the check could race transaction B and still
        // observe the old server id, which would let a server-id-sensitive comparison pass.
        executeSql("SET GLOBAL server_id = 654321");
        executeSql("INSERT INTO " + MARKER_TABLE + " VALUES (1)");
        String gtidAfterFailover = queryScalar("SELECT @@gtid_binlog_pos");
        Assertions.assertThat(gtidAfterFailover).startsWith("0-654321-");

        runJobFromSavepointAndInsertB(savepointPath);

        Assertions.assertThat(TestValuesTableFactory.getRawResultsAsStrings(SINK))
                .containsExactlyInAnyOrder("+I[1]", "+I[2]", "+I[100]", "+I[101]", "+I[200]");
    }

    /**
     * A MySQL {@code uuid:interval} GTID set must be rejected with an actionable message rather
     * than surfacing the comparator's raw {@link IllegalArgumentException} from deep inside the
     * offset comparison. This covers the guard wired into {@code
     * StatefulTaskContext#checkMariadbGtidSet}, not just the comparator in isolation.
     */
    @Test
    @Timeout(180)
    void testMySqlFormatGtidIsRejectedWithActionableError() {
        StreamExecutionEnvironment env = getExecutionEnvironment(null);
        // Without this the task would fail, restart, and fail again forever, so the job never
        // reaches a terminal state and result.await() would hang until the test times out.
        RestartStrategyUtils.configureNoRestartStrategy(env);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s ("
                                + " id INT NOT NULL,"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mariadb-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '5601-5604',"
                                + " 'scan.startup.mode' = 'specific-offset',"
                                // A MySQL-style GTID set, which MariaDB CDC cannot resume from.
                                + " 'scan.startup.specific-offset.gtid-set' = 'abcd:1-4'"
                                + ")",
                        TABLE,
                        mariaDb.getHost(),
                        mariaDb.getDatabasePort(),
                        mariaDb.getUsername(),
                        mariaDb.getPassword(),
                        mariaDb.getDatabaseName(),
                        TABLE));
        createSinkTable(tEnv);

        TableResult result = tEnv.executeSql("INSERT INTO " + SINK + " SELECT id FROM " + TABLE);
        Assertions.assertThatThrownBy(() -> result.await())
                .hasStackTraceContaining("domain-server-sequence")
                .hasStackTraceContaining("state taken from the mysql-cdc connector");
    }

    private String runJobUntilBinlogAndSavepoint(String savepointDirectory) throws Exception {
        StreamExecutionEnvironment env = getExecutionEnvironment(null);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        createCdcTable(tEnv);
        createSinkTable(tEnv);

        TableResult result = tEnv.executeSql("INSERT INTO " + SINK + " SELECT id FROM " + TABLE);
        JobClient jobClient = result.getJobClient().orElseThrow(IllegalStateException::new);
        try {
            // Two snapshot rows must reach the sink before the source enters the binlog phase.
            waitForSinkSize(2);

            // Transaction A: its row reaches the sink, but the committed GTID is still before A.
            executeSql("INSERT INTO " + TABLE + " VALUES (100)");
            waitForSinkSize(3);

            // Transaction A': observing its row proves A has committed, so the split state's
            // restartable GTID set now includes A (A' is not surfaced to the reader until its own
            // commit is carried by a later record). The savepoint therefore captures a GTID past A.
            executeSql("INSERT INTO " + TABLE + " VALUES (101)");
            waitForSinkSize(4);

            return triggerSavepointWithRetry(jobClient, savepointDirectory);
        } finally {
            jobClient.cancel().get();
        }
    }

    private void runJobFromSavepointAndInsertB(String savepointPath) throws Exception {
        StreamExecutionEnvironment env = getExecutionEnvironment(savepointPath);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        createCdcTable(tEnv);
        createSinkTable(tEnv);

        TableResult result = tEnv.executeSql("INSERT INTO " + SINK + " SELECT id FROM " + TABLE);
        JobClient jobClient = result.getJobClient().orElseThrow(IllegalStateException::new);
        try {
            // Transaction B: its GTID is past the saved offset, so it must be delivered once.
            executeSql("INSERT INTO " + TABLE + " VALUES (200)");
            waitForSinkSize(5);
        } finally {
            jobClient.cancel().get();
        }
    }

    private StreamExecutionEnvironment getExecutionEnvironment(String savepointPath) {
        Configuration configuration = new Configuration();
        if (savepointPath != null) {
            configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        return env;
    }

    private void createCdcTable(StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s ("
                                + " id INT NOT NULL,"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mariadb-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '5501-5504',"
                                + " 'scan.startup.mode' = 'initial'"
                                + ")",
                        TABLE,
                        mariaDb.getHost(),
                        mariaDb.getDatabasePort(),
                        mariaDb.getUsername(),
                        mariaDb.getPassword(),
                        mariaDb.getDatabaseName(),
                        TABLE));
    }

    private void createSinkTable(StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "CREATE TABLE "
                        + SINK
                        + " ("
                        + " id INT NOT NULL,"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")");
    }

    private static void waitForSinkSize(int expectedSize) throws InterruptedException {
        while (sinkSize() < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize() {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResultsAsStrings(SINK).size();
            } catch (IllegalArgumentException e) {
                // The job is not started yet.
                return 0;
            }
        }
    }

    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws Exception {
        int retryTimes = 0;
        while (retryTimes < 600) {
            try {
                return jobClient
                        .triggerSavepoint(savepointDirectory, SavepointFormatType.DEFAULT)
                        .get();
            } catch (Exception e) {
                Optional<CheckpointException> checkpointException =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (checkpointException.isPresent()
                        && checkpointException
                                .get()
                                .getMessage()
                                .contains("Checkpoint triggering task")) {
                    Thread.sleep(100);
                    retryTimes++;
                    continue;
                }
                throw e;
            }
        }
        throw new RuntimeException("Failed to trigger savepoint after " + retryTimes + " retries");
    }

    private void initializeTable() throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                mariaDb.getJdbcUrl(),
                                mariaDb.getUsername(),
                                mariaDb.getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + TABLE + " (id INT NOT NULL PRIMARY KEY)");
            stmt.execute("INSERT INTO " + TABLE + " VALUES (1), (2)");
            // Created up front so the failover test never issues DDL while the source is streaming.
            stmt.execute("CREATE TABLE " + MARKER_TABLE + " (id INT NOT NULL PRIMARY KEY)");
        }
    }

    private String queryScalar(String sql) throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                mariaDb.getJdbcUrl(),
                                mariaDb.getUsername(),
                                mariaDb.getPassword());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private void executeSql(String sql) throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                mariaDb.getJdbcUrl(),
                                mariaDb.getUsername(),
                                mariaDb.getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
