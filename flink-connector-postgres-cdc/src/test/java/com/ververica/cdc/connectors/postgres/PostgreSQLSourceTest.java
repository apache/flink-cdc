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

package com.ververica.cdc.connectors.postgres;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.jayway.jsonpath.JsonPath;
import com.ververica.cdc.connectors.utils.TestSourceContext;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.ververica.cdc.connectors.utils.AssertUtils.assertDelete;
import static com.ververica.cdc.connectors.utils.AssertUtils.assertInsert;
import static com.ververica.cdc.connectors.utils.AssertUtils.assertRead;
import static com.ververica.cdc.connectors.utils.AssertUtils.assertUpdate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Tests for {@link PostgreSQLSource} which also heavily tests {@link DebeziumSourceFunction}. */
public class PostgreSQLSourceTest extends PostgresTestBase {
    private static final String SLOT_NAME = "flink";

    @Before
    public void before() {
        initializePostgresTable("inventory");
    }

    @Test
    public void testConsumingAllEvents() throws Exception {
        DebeziumSourceFunction<SourceRecord> source = createPostgreSqlSourceWithHeartbeatDisabled();
        TestSourceContext<SourceRecord> sourceContext = new TestSourceContext<>();

        setupSource(source);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            // start the source
            final CheckedThread runThread =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source.run(sourceContext);
                        }
                    };
            runThread.start();

            List<SourceRecord> records = drain(sourceContext, 9);
            assertEquals(9, records.size());
            for (int i = 0; i < records.size(); i++) {
                assertRead(records.get(i), "id", 101 + i);
            }

            statement.execute(
                    "INSERT INTO inventory.products VALUES (default,'robot','Toy robot',1.304)"); // 110
            records = drain(sourceContext, 1);
            assertInsert(records.get(0), "id", 110);

            statement.execute(
                    "INSERT INTO inventory.products VALUES (1001,'roy','old robot',1234.56)"); // 1001
            records = drain(sourceContext, 1);
            assertInsert(records.get(0), "id", 1001);

            // ---------------------------------------------------------------------------------------------------------------
            // Changing the primary key of a row should result in 2 events: INSERT, DELETE
            // (TOMBSTONE is dropped)
            // ---------------------------------------------------------------------------------------------------------------
            statement.execute(
                    "UPDATE inventory.products SET id=2001, description='really old robot' WHERE id=1001");
            records = drain(sourceContext, 2);
            assertDelete(records.get(0), "id", 1001);
            assertInsert(records.get(1), "id", 2001);

            // ---------------------------------------------------------------------------------------------------------------
            // Simple UPDATE (with no schema changes)
            // ---------------------------------------------------------------------------------------------------------------
            statement.execute("UPDATE inventory.products SET weight=1345.67 WHERE id=2001");
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "id", 2001);

            // ---------------------------------------------------------------------------------------------------------------
            // Change our schema with a fully-qualified name; we should still see this event
            // ---------------------------------------------------------------------------------------------------------------
            // Add a column with default to the 'products' table and explicitly update one record
            // ...
            statement.execute(
                    "ALTER TABLE inventory.products ADD COLUMN volume FLOAT, ADD COLUMN alias VARCHAR(30) NULL");
            statement.execute("UPDATE inventory.products SET volume=13.5 WHERE id=2001");
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "id", 2001);

            // cleanup
            source.cancel();
            source.close();
            runThread.sync();
        }
    }

    @Test
    public void testCheckpointAndRestore() throws Exception {
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();
        int prevLsn = 0;
        {
            // ---------------------------------------------------------------------------
            // Step-1: start the source from empty state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source =
                    createPostgreSqlSourceWithHeartbeatDisabled();
            // we use blocking context to block the source to emit before last snapshot record
            final BlockingSourceContext<SourceRecord> sourceContext =
                    new BlockingSourceContext<>(8);
            // setup source with empty state
            setupSource(source, false, offsetState, historyState, true, 0, 1);

            final CheckedThread runThread =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source.run(sourceContext);
                        }
                    };
            runThread.start();

            // wait until consumer is started
            int received = drain(sourceContext, 2).size();
            assertEquals(2, received);

            // we can't perform checkpoint during DB snapshot
            assertFalse(
                    waitForCheckpointLock(
                            sourceContext.getCheckpointLock(), Duration.ofSeconds(3)));

            // unblock the source context to continue the processing
            sourceContext.blocker.release();
            // wait until the source finishes the database snapshot
            List<SourceRecord> records = drain(sourceContext, 9 - received);
            assertEquals(9, records.size() + received);

            // state is still empty
            assertEquals(0, offsetState.list.size());
            assertEquals(0, historyState.list.size());

            // ---------------------------------------------------------------------------
            // Step-2: trigger checkpoint-1 after snapshot finished
            // ---------------------------------------------------------------------------
            synchronized (sourceContext.getCheckpointLock()) {
                // trigger checkpoint-1
                source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
            }

            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertEquals("postgres_cdc_source", JsonPath.read(state, "$.sourcePartition.server"));
            assertEquals("557", JsonPath.read(state, "$.sourceOffset.txId").toString());
            assertEquals(
                    "true", JsonPath.read(state, "$.sourceOffset.last_snapshot_record").toString());
            assertEquals("true", JsonPath.read(state, "$.sourceOffset.snapshot").toString());
            assertTrue(state.contains("ts_usec"));
            int lsn = JsonPath.read(state, "$.sourceOffset.lsn");
            assertTrue(lsn > prevLsn);
            prevLsn = lsn;

            source.cancel();
            source.close();
            runThread.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-3: restore the source from state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source2 =
                    createPostgreSqlSourceWithHeartbeatDisabled();
            final TestSourceContext<SourceRecord> sourceContext2 = new TestSourceContext<>();
            setupSource(source2, true, offsetState, historyState, true, 0, 1);
            final CheckedThread runThread2 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source2.run(sourceContext2);
                        }
                    };
            runThread2.start();

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(5), sourceContext2));

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                statement.execute(
                        "INSERT INTO inventory.products VALUES (default,'robot','Toy robot',1.304)"); // 110
                List<SourceRecord> records = drain(sourceContext2, 1);
                assertEquals(1, records.size());
                assertInsert(records.get(0), "id", 110);

                // ---------------------------------------------------------------------------
                // Step-4: trigger checkpoint-2 during DML operations
                // ---------------------------------------------------------------------------
                synchronized (sourceContext2.getCheckpointLock()) {
                    // trigger checkpoint-1
                    source2.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));
                }

                assertEquals(1, offsetState.list.size());
                String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
                assertEquals(
                        "postgres_cdc_source", JsonPath.read(state, "$.sourcePartition.server"));
                assertEquals("558", JsonPath.read(state, "$.sourceOffset.txId").toString());
                assertTrue(state.contains("ts_usec"));
                assertFalse(state.contains("snapshot"));
                int lsn = JsonPath.read(state, "$.sourceOffset.lsn");
                assertTrue(lsn > prevLsn);
                prevLsn = lsn;

                // execute 2 more DMLs to have more binlog
                statement.execute(
                        "INSERT INTO inventory.products VALUES (1001,'roy','old robot',1234.56)"); // 1001
                statement.execute("UPDATE inventory.products SET weight=1345.67 WHERE id=1001");
            }

            // cancel the source
            source2.cancel();
            source2.close();
            runThread2.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-5: restore the source from checkpoint-2
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source3 =
                    createPostgreSqlSourceWithHeartbeatDisabled();
            final TestSourceContext<SourceRecord> sourceContext3 = new TestSourceContext<>();
            setupSource(source3, true, offsetState, historyState, true, 0, 1);

            // restart the source
            final CheckedThread runThread3 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source3.run(sourceContext3);
                        }
                    };
            runThread3.start();

            // consume the unconsumed binlog
            List<SourceRecord> records = drain(sourceContext3, 2);
            assertInsert(records.get(0), "id", 1001);
            assertUpdate(records.get(1), "id", 1001);

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(3), sourceContext3));

            // can continue to receive new events
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute("DELETE FROM inventory.products WHERE id=1001");
            }
            records = drain(sourceContext3, 1);
            assertDelete(records.get(0), "id", 1001);

            // ---------------------------------------------------------------------------
            // Step-6: trigger checkpoint-2 to make sure we can continue to to further checkpoints
            // ---------------------------------------------------------------------------
            synchronized (sourceContext3.getCheckpointLock()) {
                // checkpoint 3
                source3.snapshotState(new StateSnapshotContextSynchronousImpl(233, 233));
            }
            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertEquals("postgres_cdc_source", JsonPath.read(state, "$.sourcePartition.server"));
            assertEquals("561", JsonPath.read(state, "$.sourceOffset.txId").toString());
            assertTrue(state.contains("ts_usec"));
            assertFalse(state.contains("snapshot"));
            int lsn = JsonPath.read(state, "$.sourceOffset.lsn");
            assertTrue(lsn > prevLsn);

            source3.cancel();
            source3.close();
            runThread3.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-7: restore the source from checkpoint-3
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source4 =
                    createPostgreSqlSourceWithHeartbeatDisabled();
            final TestSourceContext<SourceRecord> sourceContext4 = new TestSourceContext<>();
            setupSource(source4, true, offsetState, historyState, true, 0, 1);

            // restart the source
            final CheckedThread runThread4 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source4.run(sourceContext4);
                        }
                    };
            runThread4.start();

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(5), sourceContext4));

            // ---------------------------------------------------------------------------
            // Step-8: trigger checkpoint-2 to make sure we can continue to to further checkpoints
            // ---------------------------------------------------------------------------
            synchronized (sourceContext4.getCheckpointLock()) {
                // checkpoint 3
                source4.snapshotState(new StateSnapshotContextSynchronousImpl(254, 254));
            }
            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertEquals("postgres_cdc_source", JsonPath.read(state, "$.sourcePartition.server"));
            assertEquals("561", JsonPath.read(state, "$.sourceOffset.txId").toString());
            assertTrue(state.contains("ts_usec"));
            assertFalse(state.contains("snapshot"));
            int lsn = JsonPath.read(state, "$.sourceOffset.lsn");
            assertTrue(lsn > prevLsn);
            prevLsn = lsn;

            source4.cancel();
            source4.close();
            runThread4.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-9: insert partial and alter table
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source5 =
                    createPostgreSqlSourceWithHeartbeatDisabled();
            final TestSourceContext<SourceRecord> sourceContext5 = new TestSourceContext<>();
            setupSource(source5, true, offsetState, historyState, true, 0, 1);

            // restart the source
            final CheckedThread runThread5 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source5.run(sourceContext5);
                        }
                    };
            runThread5.start();

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                statement.execute(
                        "INSERT INTO inventory.products(id, description, weight) VALUES (default, 'Go go go', 111.1)");
                statement.execute(
                        "ALTER TABLE inventory.products ADD comment_col VARCHAR(100) DEFAULT 'cdc'");
                List<SourceRecord> records = drain(sourceContext5, 1);
                assertInsert(records.get(0), "id", 111);
            }

            // ---------------------------------------------------------------------------
            // Step-10: trigger checkpoint-4
            // ---------------------------------------------------------------------------
            synchronized (sourceContext5.getCheckpointLock()) {
                // trigger checkpoint-4
                source5.snapshotState(new StateSnapshotContextSynchronousImpl(300, 300));
            }
            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertEquals("postgres_cdc_source", JsonPath.read(state, "$.sourcePartition.server"));
            assertEquals("562", JsonPath.read(state, "$.sourceOffset.txId").toString());
            assertTrue(state.contains("ts_usec"));
            assertFalse(state.contains("snapshot"));
            int pos = JsonPath.read(state, "$.sourceOffset.lsn");
            assertTrue(pos > prevLsn);

            source5.cancel();
            source5.close();
            runThread5.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-11: restore from the checkpoint-4 and insert the partial value
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source6 =
                    createPostgreSqlSourceWithHeartbeatDisabled();
            final TestSourceContext<SourceRecord> sourceContext6 = new TestSourceContext<>();
            setupSource(source6, true, offsetState, historyState, true, 0, 1);

            // restart the source
            final CheckedThread runThread6 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source6.run(sourceContext6);
                        }
                    };
            runThread6.start();
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                statement.execute(
                        "INSERT INTO inventory.products(id, description, weight) VALUES (default, 'Run!', 22.2)");
                List<SourceRecord> records = drain(sourceContext6, 1);
                assertInsert(records.get(0), "id", 112);
            }

            source6.cancel();
            source6.close();
            runThread6.sync();
        }
    }

    @Test
    public void testFlushLsn() throws Exception {
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();
        final LinkedHashSet<String> flushLsn = new LinkedHashSet<>();
        {
            // ---------------------------------------------------------------------------
            // Step-1: start the source from empty state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source =
                    createPostgreSqlSourceWithHeartbeatEnabled();
            final TestSourceContext<SourceRecord> sourceContext = new TestSourceContext<>();
            // setup source with empty state
            setupSource(source, false, offsetState, historyState, true, 0, 1);

            final CheckedThread runThread =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source.run(sourceContext);
                        }
                    };
            runThread.start();

            // wait until consumer is started
            int received = drain(sourceContext, 9).size();
            assertEquals(9, received);

            // ---------------------------------------------------------------------------
            // Step-2: trigger checkpoint-1 after snapshot finished
            // ---------------------------------------------------------------------------
            synchronized (sourceContext.getCheckpointLock()) {
                // trigger checkpoint-1
                source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
            }
            source.notifyCheckpointComplete(101);
            assertTrue(flushLsn.add(getConfirmedFlushLsn()));

            batchInsertAndCheckpoint(5, source, sourceContext, 201);
            assertEquals(1, source.getPendingOffsetsToCommit().size());
            source.notifyCheckpointComplete(201);
            assertEquals(0, source.getPendingOffsetsToCommit().size());
            assertTrue(flushLsn.add(getConfirmedFlushLsn()));

            batchInsertAndCheckpoint(1, source, sourceContext, 301);
            // do not notify checkpoint complete to see the LSN is not advanced.
            assertFalse(flushLsn.add(getConfirmedFlushLsn()));

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(3), sourceContext));

            source.cancel();
            source.close();
            runThread.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-3: restore the source from state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source2 =
                    createPostgreSqlSourceWithHeartbeatEnabled();
            final TestSourceContext<SourceRecord> sourceContext2 = new TestSourceContext<>();
            // setup source with empty state
            setupSource(source2, true, offsetState, historyState, true, 0, 1);

            final CheckedThread runThread =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            source2.run(sourceContext2);
                        }
                    };
            runThread.start();

            assertFalse(flushLsn.add(getConfirmedFlushLsn()));

            batchInsertAndCheckpoint(0, source2, sourceContext2, 401);
            Thread.sleep(3_000); // waiting heartbeat events, we have set 1s heartbeat interval
            // trigger checkpoint once again to make sure ChangeConsumer is initialized
            batchInsertAndCheckpoint(0, source2, sourceContext2, 402);
            source2.notifyCheckpointComplete(402);
            assertTrue(flushLsn.add(getConfirmedFlushLsn()));

            // verify LSN is advanced even if there is no changes on the table
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                // we have to do some transactions which is not related to the monitored table
                statement.execute("CREATE TABLE dummy (a int)");
            }
            Thread.sleep(3_000);
            batchInsertAndCheckpoint(0, source2, sourceContext2, 404);
            source2.notifyCheckpointComplete(404);
            assertTrue(flushLsn.add(getConfirmedFlushLsn()));

            batchInsertAndCheckpoint(3, source2, sourceContext2, 501);
            batchInsertAndCheckpoint(2, source2, sourceContext2, 502);
            batchInsertAndCheckpoint(1, source2, sourceContext2, 503);
            assertEquals(3, source2.getPendingOffsetsToCommit().size());
            source2.notifyCheckpointComplete(503);
            assertTrue(flushLsn.add(getConfirmedFlushLsn()));
            assertEquals(0, source2.getPendingOffsetsToCommit().size());

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(3), sourceContext2));

            source2.cancel();
            source2.close();
            runThread.sync();
        }

        assertEquals(5, flushLsn.size());
    }

    private void batchInsertAndCheckpoint(
            int num,
            DebeziumSourceFunction<SourceRecord> source,
            TestSourceContext<SourceRecord> sourceContext,
            long checkpointId)
            throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            for (int i = 0; i < num; i++) {
                statement.execute(
                        "INSERT INTO inventory.products VALUES (default,'dummy','My Dummy',1.1)");
            }
        }
        assertEquals(num, drain(sourceContext, num).size());
        synchronized (sourceContext.getCheckpointLock()) {
            // trigger checkpoint-1
            source.snapshotState(
                    new StateSnapshotContextSynchronousImpl(checkpointId, checkpointId));
        }
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private DebeziumSourceFunction<SourceRecord> createPostgreSqlSourceWithHeartbeatDisabled() {
        return createPostgreSqlSource(0);
    }

    private DebeziumSourceFunction<SourceRecord> createPostgreSqlSourceWithHeartbeatEnabled() {
        return createPostgreSqlSource(1000);
    }

    private DebeziumSourceFunction<SourceRecord> createPostgreSqlSource(int heartbeatInterval) {
        Properties properties = new Properties();
        properties.setProperty("heartbeat.interval.ms", String.valueOf(heartbeatInterval));
        return PostgreSQLSource.<SourceRecord>builder()
                .hostname(POSTGERS_CONTAINER.getHost())
                .port(POSTGERS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                .database(POSTGERS_CONTAINER.getDatabaseName())
                .username(POSTGERS_CONTAINER.getUsername())
                .password(POSTGERS_CONTAINER.getPassword())
                .schemaList("inventory")
                .tableList("inventory.products")
                .deserializer(new ForwardDeserializeSchema())
                .slotName(SLOT_NAME)
                .debeziumProperties(properties)
                .build();
    }

    private String getConfirmedFlushLsn() throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            ResultSet rs =
                    statement.executeQuery(
                            String.format(
                                    "select * from pg_replication_slots where slot_name = '%s' and database = '%s' and plugin = '%s'",
                                    SLOT_NAME,
                                    POSTGERS_CONTAINER.getDatabaseName(),
                                    "decoderbufs"));
            if (rs.next()) {
                return rs.getString("confirmed_flush_lsn");
            } else {
                fail("No replication slot info available");
            }
            return null;
        }
    }

    private <T> List<T> drain(TestSourceContext<T> sourceContext, int expectedRecordCount)
            throws Exception {
        List<T> allRecords = new ArrayList<>();
        LinkedBlockingQueue<StreamRecord<T>> queue = sourceContext.getCollectedOutputs();
        while (allRecords.size() < expectedRecordCount) {
            StreamRecord<T> record = queue.poll(100, TimeUnit.SECONDS);
            if (record != null) {
                allRecords.add(record.getValue());
            } else {
                throw new RuntimeException(
                        "Can't receive " + expectedRecordCount + " elements before timeout.");
            }
        }

        return allRecords;
    }

    private boolean waitForCheckpointLock(Object checkpointLock, Duration timeout)
            throws Exception {
        final Semaphore semaphore = new Semaphore(0);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(
                () -> {
                    synchronized (checkpointLock) {
                        semaphore.release();
                    }
                });
        boolean result = semaphore.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
        executor.shutdownNow();
        return result;
    }

    /**
     * Wait for a maximum amount of time until the first record is available.
     *
     * @param timeout the maximum amount of time to wait; must not be negative
     * @return {@code true} if records are available, or {@code false} if the timeout occurred and
     *     no records are available
     */
    private boolean waitForAvailableRecords(Duration timeout, TestSourceContext<?> sourceContext)
            throws InterruptedException {
        long now = System.currentTimeMillis();
        long stop = now + timeout.toMillis();
        while (System.currentTimeMillis() < stop) {
            if (!sourceContext.getCollectedOutputs().isEmpty()) {
                break;
            }
            Thread.sleep(10); // save CPU
        }
        return !sourceContext.getCollectedOutputs().isEmpty();
    }

    private static <T> void setupSource(DebeziumSourceFunction<T> source) throws Exception {
        setupSource(
                source, false, null, null,
                true, // enable checkpointing; auto commit should be ignored
                0, 1);
    }

    private static <T, S1, S2> void setupSource(
            DebeziumSourceFunction<T> source,
            boolean isRestored,
            ListState<S1> restoredOffsetState,
            ListState<S2> restoredHistoryState,
            boolean isCheckpointingEnabled,
            int subtaskIndex,
            int totalNumSubtasks)
            throws Exception {

        // run setup procedure in operator life cycle
        source.setRuntimeContext(
                new MockStreamingRuntimeContext(
                        isCheckpointingEnabled, totalNumSubtasks, subtaskIndex));
        source.initializeState(
                new MockFunctionInitializationContext(
                        isRestored,
                        new MockOperatorStateStore(restoredOffsetState, restoredHistoryState)));
        source.open(new Configuration());
    }

    private static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 2975058057832211228L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) throws Exception {
            out.collect(record);
        }

        @Override
        public TypeInformation<SourceRecord> getProducedType() {
            return TypeInformation.of(SourceRecord.class);
        }
    }

    private static class MockOperatorStateStore implements OperatorStateStore {

        private final ListState<?> restoredOffsetListState;
        private final ListState<?> restoredHistoryListState;

        private MockOperatorStateStore(
                ListState<?> restoredOffsetListState, ListState<?> restoredHistoryListState) {
            this.restoredOffsetListState = restoredOffsetListState;
            this.restoredHistoryListState = restoredHistoryListState;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor)
                throws Exception {
            if (stateDescriptor.getName().equals(DebeziumSourceFunction.OFFSETS_STATE_NAME)) {
                return (ListState<S>) restoredOffsetListState;
            } else if (stateDescriptor
                    .getName()
                    .equals(DebeziumSourceFunction.HISTORY_RECORDS_STATE_NAME)) {
                return (ListState<S>) restoredHistoryListState;
            } else {
                throw new IllegalStateException("Unknown state.");
            }
        }

        @Override
        public <K, V> BroadcastState<K, V> getBroadcastState(
                MapStateDescriptor<K, V> stateDescriptor) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getRegisteredStateNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getRegisteredBroadcastStateNames() {
            throw new UnsupportedOperationException();
        }
    }

    private static class MockFunctionInitializationContext
            implements FunctionInitializationContext {

        private final boolean isRestored;
        private final OperatorStateStore operatorStateStore;

        private MockFunctionInitializationContext(
                boolean isRestored, OperatorStateStore operatorStateStore) {
            this.isRestored = isRestored;
            this.operatorStateStore = operatorStateStore;
        }

        @Override
        public boolean isRestored() {
            return isRestored;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public OperatorStateStore getOperatorStateStore() {
            return operatorStateStore;
        }

        @Override
        public KeyedStateStore getKeyedStateStore() {
            throw new UnsupportedOperationException();
        }
    }

    private static class BlockingSourceContext<T> extends TestSourceContext<T> {

        private final Semaphore blocker = new Semaphore(0);
        private final int expectedCount;
        private int currentCount = 0;

        private BlockingSourceContext(int expectedCount) {
            this.expectedCount = expectedCount;
        }

        @Override
        public void collect(T t) {
            super.collect(t);
            currentCount++;
            if (currentCount == expectedCount) {
                try {
                    // block the source to emit records
                    blocker.acquire();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    private static final class TestingListState<T> implements ListState<T> {

        private final List<T> list = new ArrayList<>();
        private boolean clearCalled = false;

        @Override
        public void clear() {
            list.clear();
            clearCalled = true;
        }

        @Override
        public Iterable<T> get() throws Exception {
            return list;
        }

        @Override
        public void add(T value) throws Exception {
            Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
            list.add(value);
        }

        public List<T> getList() {
            return list;
        }

        boolean isClearCalled() {
            return clearCalled;
        }

        @Override
        public void update(List<T> values) throws Exception {
            clear();

            addAll(values);
        }

        @Override
        public void addAll(List<T> values) throws Exception {
            if (values != null) {
                values.forEach(
                        v -> Preconditions.checkNotNull(v, "You cannot add null to a ListState."));

                list.addAll(values);
            }
        }
    }
}
