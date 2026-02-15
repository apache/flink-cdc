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
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.dispatcher.UnavailableDispatcherOperationException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import io.debezium.connector.mysql.MySqlConnection;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * IT test for FLINK-38334: MySQL CDC source gets stuck in INITIAL_ASSIGNING state when a table is
 * excluded from configuration after splits have been assigned but before they are finished.
 *
 * <p>NOTE: The test uses a JUnit timeout as a CI safeguard. The test logic is deterministic: it
 * either receives a binlog record (proving streaming mode) or blocks forever (bug exists).
 *
 * <p>When the bug exists, the test will timeout because:
 *
 * <ol>
 *   <li>The savepoint captures table "a" splits in assignedSplits (assigned but not finished)
 *   <li>After restart with table "a" excluded, the reader skips table "a" splits
 *   <li>The enumerator waits for table "a" splits to be reported as finished (they never will be)
 *   <li>allSnapshotSplitsFinished() never returns true → job stuck in INITIAL_ASSIGNING
 * </ol>
 *
 * <p>When the fix is applied, the test passes because excluded table splits are cleaned up on
 * restore, allowing the job to transition to streaming mode.
 */
public class TableExclusionDuringSnapshotIT extends MySqlSourceTestBase {
    private static final UniqueDatabase DATABASE =
            new UniqueDatabase(MYSQL_CONTAINER, "table_exclusion_snapshot", "mysqluser", "mysqlpw");
    private static final DataType DATA_TYPE = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()));
    private static final RowRowConverter ROW_CONVERTER = RowRowConverter.create(DATA_TYPE);
    private static final RowDataDebeziumDeserializeSchema DESERIALIZER =
            RowDataDebeziumDeserializeSchema.newBuilder()
                    .setPhysicalRowType((RowType) DATA_TYPE.getLogicalType())
                    .setResultTypeInfo(
                            InternalTypeInfo.of(TypeConversions.fromDataToLogicalType(DATA_TYPE)))
                    .build();

    @BeforeEach
    void setUp() {
        DATABASE.createAndInitialize();
    }

    // Latches for coordinating between test thread and snapshot hook
    // These are static because the hook is serialized and deserialized
    private static volatile CountDownLatch hookTriggeredLatch;
    private static volatile CountDownLatch savepointTakenLatch;

    /**
     * Tests that excluding a table from configuration during INITIAL_ASSIGNING phase doesn't cause
     * the source to get stuck.
     *
     * <p>Scenario:
     *
     * <ol>
     *   <li>Start job capturing tables "a" and "b" with a blocking hook on table "a"
     *   <li>Take savepoint while table "a" is being snapshotted (splits assigned but not finished)
     *   <li>Restart with configuration excluding table "a"
     *   <li>Insert a new record into table "b"
     *   <li>Verify we receive the new record (proves job transitioned to streaming)
     * </ol>
     *
     * <p>If the bug exists, the job will be stuck in INITIAL_ASSIGNING because the enumerator waits
     * for table "a" splits to be reported as finished, but the reader skips them.
     */
    @Test
    @Timeout(120)
    void testTableExclusionDuringInitialAssigning(@TempDir Path tempDir) throws Exception {
        final String savepointDirectory = tempDir.toUri().toString();

        executeSql("INSERT INTO a VALUES (1)");
        executeSql("INSERT INTO b VALUES (1)");
        executeSql("INSERT INTO b VALUES (2)");

        // Phase 1: Take savepoint while table "a" splits are assigned but not finished
        String savepointPath =
                runJobAndSavepointDuringInitialAssigning(savepointDirectory, "a", "b");

        // Phase 2: Restart with only table "b", verify streaming mode works
        String binlogRecord = runJobFromSavepointAndVerifyStreaming(savepointPath, "b");
        Assertions.assertThat(binlogRecord).isEqualTo("+I[200]");
    }

    /** Starts a job and takes a savepoint while splits are assigned but not finished. */
    private String runJobAndSavepointDuringInitialAssigning(
            String savepointDirectory, String... tableNames) throws Exception {
        hookTriggeredLatch = new CountDownLatch(1);
        savepointTakenLatch = new CountDownLatch(1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);

        MySqlSource<RowData> source = createSourceWithBlockingHook(tableNames);
        DataStreamSource<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");
        stream.print();

        JobClient jobClient = env.executeAsync("Snapshot phase");

        hookTriggeredLatch.await();
        String savepointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
        savepointTakenLatch.countDown();

        jobClient.cancel().get();
        return savepointPath;
    }

    /** Restarts job from savepoint and verifies it transitions to streaming mode. */
    private String runJobFromSavepointAndVerifyStreaming(String savepointPath, String... tableNames)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        MySqlSource<RowData> source = createSourceBuilder(tableNames).build();
        DataStreamSource<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");

        try (CloseableIterator<RowData> iterator = stream.executeAndCollect()) {
            // Consume snapshot records from table "b" (2 rows)
            for (int i = 0; i < 2; i++) {
                iterator.next();
            }

            // Insert a new record - if job is in streaming mode, we'll receive it
            executeSql("INSERT INTO b VALUES (200)");

            // This blocks forever if the job is stuck in INITIAL_ASSIGNING
            return ROW_CONVERTER.toExternal(iterator.next()).toString();
        }
    }

    private MySqlSourceBuilder<RowData> createSourceBuilder(String... tableNames) {
        return MySqlSource.<RowData>builder()
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .databaseList(DATABASE.getDatabaseName())
                .serverTimeZone("UTC")
                .tableList(
                        Arrays.stream(tableNames)
                                .map(t -> DATABASE.getDatabaseName() + "." + t)
                                .toArray(String[]::new))
                .username(DATABASE.getUsername())
                .password(DATABASE.getPassword())
                .deserializer(DESERIALIZER)
                .scanNewlyAddedTableEnabled(true);
    }

    /** Creates a source with a hook that blocks on table "a" until the savepoint is taken. */
    private MySqlSource<RowData> createSourceWithBlockingHook(String... tableNames) {
        MySqlSource<RowData> source = createSourceBuilder(tableNames).build();

        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        hooks.setPostLowWatermarkAction(
                (connection, split) -> {
                    MySqlSnapshotSplit snapshotSplit = (MySqlSnapshotSplit) split;
                    if (!snapshotSplit.getTableId().table().equals("a")) {
                        return;
                    }
                    hookTriggeredLatch.countDown();
                    try {
                        savepointTakenLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
        source.setSnapshotHooks(hooks);

        return source;
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
                // Retry if checkpoint triggering task is not yet ready
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
                // Retry if job is still initializing
                Optional<UnavailableDispatcherOperationException> dispatcherException =
                        ExceptionUtils.findThrowable(
                                e, UnavailableDispatcherOperationException.class);
                if (dispatcherException.isPresent()) {
                    Thread.sleep(100);
                    retryTimes++;
                    continue;
                }
                throw e;
            }
        }
        throw new RuntimeException("Failed to trigger savepoint after " + retryTimes + " retries");
    }

    private void executeSql(String... statements) throws SQLException {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", DATABASE.getUsername());
        properties.put("database.password", DATABASE.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);

        try (MySqlConnection connection =
                DebeziumUtils.createMySqlConnection(configuration, new Properties())) {
            connection.execute("USE " + DATABASE.getDatabaseName());
            connection.execute(statements);
        }
    }
}
