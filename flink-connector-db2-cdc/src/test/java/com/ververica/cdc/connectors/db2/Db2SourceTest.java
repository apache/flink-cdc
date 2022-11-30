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

package com.ververica.cdc.connectors.db2;

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
import org.junit.Test;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
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
import static org.testcontainers.containers.Db2Container.DB2_PORT;

/** Test for {@link Db2Source} which also heavily tests {@link DebeziumSourceFunction}. */
public class Db2SourceTest extends Db2TestBase {

    @Test
    public void testConsumingAllEvents() throws Exception {
        DebeziumSourceFunction<SourceRecord> source = createDb2Source("DB2INST1.PRODUCTS1");
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
                assertRead(records.get(i), "ID", 101 + i);
            }

            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS1 VALUES (default,'robot','Toy robot',1.304)"); // 110
            records = drain(sourceContext, 1);
            assertInsert(records.get(0), "ID", 110);

            statement.execute(
                    "INSERT INTO DB2INST1.PRODUCTS1 VALUES (1001,'roy','old robot',1234.56)"); // 1001
            records = drain(sourceContext, 1);
            assertInsert(records.get(0), "ID", 1001);

            // ---------------------------------------------------------------------------------------------------------------
            // Changing the primary key of a row should result in 2 events: INSERT, DELETE
            // (TOMBSTONE is dropped)
            // ---------------------------------------------------------------------------------------------------------------
            statement.execute(
                    "UPDATE DB2INST1.PRODUCTS1 SET ID=2001, DESCRIPTION='really old robot' WHERE ID=1001");
            records = drain(sourceContext, 2);
            assertDelete(records.get(0), "ID", 1001);
            assertInsert(records.get(1), "ID", 2001);

            // ---------------------------------------------------------------------------------------------------------------
            // Simple UPDATE (with no schema changes)
            // ---------------------------------------------------------------------------------------------------------------
            statement.execute("UPDATE DB2INST1.PRODUCTS1 SET WEIGHT=1345.67 WHERE ID=2001");
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "ID", 2001);

            // ---------------------------------------------------------------------------------------------------------------
            // Change our schema with a fully-qualified name; we should still see this event
            // ---------------------------------------------------------------------------------------------------------------
            // Add a column with default to the 'products' table and explicitly update one record
            // ...
            statement.execute(
                    "ALTER TABLE DB2INST1.PRODUCTS1 ADD COLUMN VOLUME FLOAT ADD COLUMN ALIAS VARCHAR(30) NULL");
            statement.execute("UPDATE DB2INST1.PRODUCTS1 SET VOLUME=13.5 WHERE ID=2001");
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "ID", 2001);

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
        String prevLsn = "";
        {
            // ---------------------------------------------------------------------------
            // Step-1: start the source from empty state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source =
                    createDb2Source("DB2INST1.PRODUCTS2");
            // we use blocking context to block the source to emit before last snapshot record
            final BlockingSourceContext<SourceRecord> sourceContext =
                    new BlockingSourceContext<>(8);
            // setup source with empty state
            setupSource(source, null, offsetState, historyState, true, 0, 1);

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
            assertEquals("db2_cdc_source", JsonPath.read(state, "$.sourcePartition.server"));

            String lsn = JsonPath.read(state, "$.sourceOffset.commit_lsn");
            assertTrue(lsn.compareTo(prevLsn) > 0);
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
                    createDb2Source("DB2INST1.PRODUCTS2");
            final TestSourceContext<SourceRecord> sourceContext2 = new TestSourceContext<>();
            setupSource(source2, 1L, offsetState, historyState, true, 0, 1);
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
                        "INSERT INTO DB2INST1.PRODUCTS2 VALUES (default,'robot','Toy robot',1.304)"); // 110
                List<SourceRecord> records = drain(sourceContext2, 1);
                assertEquals(1, records.size());
                assertInsert(records.get(0), "ID", 110);

                // ---------------------------------------------------------------------------
                // Step-4: trigger checkpoint-2 during DML operations
                // ---------------------------------------------------------------------------
                synchronized (sourceContext2.getCheckpointLock()) {
                    // trigger checkpoint-1
                    source2.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));
                }

                assertEquals(1, offsetState.list.size());
                String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
                assertEquals("db2_cdc_source", JsonPath.read(state, "$.sourcePartition.server"));
                String lsn = JsonPath.read(state, "$.sourceOffset.commit_lsn");
                assertTrue(lsn.compareTo(prevLsn) > 0);

                // execute 2 more DMLs to have more binlog
                statement.execute(
                        "INSERT INTO DB2INST1.PRODUCTS2 VALUES (1001,'roy','old robot',1234.56)"); // 1001
                statement.execute("UPDATE DB2INST1.PRODUCTS2 SET WEIGHT=1345.67 WHERE ID=1001");
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
                    createDb2Source("DB2INST1.PRODUCTS2");
            final TestSourceContext<SourceRecord> sourceContext3 = new TestSourceContext<>();
            setupSource(source3, 2L, offsetState, historyState, true, 0, 1);

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
            assertInsert(records.get(0), "ID", 1001);
            assertUpdate(records.get(1), "ID", 1001);

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(3), sourceContext3));

            // can continue to receive new events
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute("DELETE FROM DB2INST1.PRODUCTS2 WHERE ID=1001");
            }
            records = drain(sourceContext3, 1);
            assertDelete(records.get(0), "ID", 1001);

            // ---------------------------------------------------------------------------
            // Step-6: trigger checkpoint-2 to make sure we can continue to further checkpoints
            // ---------------------------------------------------------------------------
            synchronized (sourceContext3.getCheckpointLock()) {
                // checkpoint 3
                source3.snapshotState(new StateSnapshotContextSynchronousImpl(233, 233));
            }
            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertEquals("db2_cdc_source", JsonPath.read(state, "$.sourcePartition.server"));
            String lsn = JsonPath.read(state, "$.sourceOffset.commit_lsn");
            assertTrue(lsn.compareTo(prevLsn) > 0);

            source3.cancel();
            source3.close();
            runThread3.sync();
        }
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private DebeziumSourceFunction<SourceRecord> createDb2Source(String tableName) {
        return Db2Source.<SourceRecord>builder()
                .hostname(DB2_CONTAINER.getHost())
                .port(DB2_CONTAINER.getMappedPort(DB2_PORT))
                .database(DB2_CONTAINER.getDatabaseName())
                .username(DB2_CONTAINER.getUsername())
                .password(DB2_CONTAINER.getPassword())
                .tableList(tableName)
                .deserializer(new ForwardDeserializeSchema())
                .build();
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
                source, null, null, null,
                true, // enable checkpointing; auto commit should be ignored
                0, 1);
    }

    private static <T, S1, S2> void setupSource(
            DebeziumSourceFunction<T> source,
            @Nullable Long restoredCheckpointId,
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
                        restoredCheckpointId,
                        new MockOperatorStateStore(restoredOffsetState, restoredHistoryState)));
        source.open(new Configuration());
    }

    private static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 1L;

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

        private final Long restoredCheckpointId;
        private final OperatorStateStore operatorStateStore;

        private MockFunctionInitializationContext(
                Long restoredCheckpointId, OperatorStateStore operatorStateStore) {
            this.restoredCheckpointId = restoredCheckpointId;
            this.operatorStateStore = operatorStateStore;
        }

        @Override
        public boolean isRestored() {
            return restoredCheckpointId != null;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            if (restoredCheckpointId == null) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(restoredCheckpointId);
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
