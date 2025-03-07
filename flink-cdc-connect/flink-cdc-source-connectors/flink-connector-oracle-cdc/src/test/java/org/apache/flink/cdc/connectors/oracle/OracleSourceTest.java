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

package org.apache.flink.cdc.connectors.oracle;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase;
import org.apache.flink.cdc.connectors.utils.TestSourceContext;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.cdc.connectors.utils.AssertUtils.assertDelete;
import static org.apache.flink.cdc.connectors.utils.AssertUtils.assertInsert;
import static org.apache.flink.cdc.connectors.utils.AssertUtils.assertRead;
import static org.apache.flink.cdc.connectors.utils.AssertUtils.assertUpdate;

/** Tests for {@link OracleSource} which also heavily tests {@link DebeziumSourceFunction}. */
class OracleSourceTest extends OracleSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSourceTest.class);

    @Test
    void testConsumingAllEvents() throws Exception {

        createAndInitialize("product.sql");

        DebeziumSourceFunction<SourceRecord> source = createOracleLogminerSource();
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
            Assertions.assertThat(records).hasSize(9);
            for (int i = 0; i < records.size(); i++) {
                assertRead(records.get(i), "ID", 101 + i);
            }

            statement.execute(
                    "INSERT INTO debezium.products VALUES (110,'robot','Toy robot',1.304)"); // 110
            records = drain(sourceContext, 1);
            assertInsert(records.get(0), "ID", 110);

            statement.execute(
                    "INSERT INTO debezium.products VALUES (1001,'roy','old robot',1234.56)"); // 1001
            records = drain(sourceContext, 1);
            assertInsert(records.get(0), "ID", 1001);

            // ---------------------------------------------------------------------------------------------------------------
            // Changing the primary key of a row should result in 2 events: INSERT, DELETE
            // (TOMBSTONE is dropped)
            // ---------------------------------------------------------------------------------------------------------------
            statement.execute(
                    "UPDATE debezium.products SET id=2001, description='really old robot' WHERE id=1001");
            records = drain(sourceContext, 2);
            assertDelete(records.get(0), "ID", 1001);
            assertInsert(records.get(1), "ID", 2001);

            // ---------------------------------------------------------------------------------------------------------------
            // Simple UPDATE (with no schema changes)
            // ---------------------------------------------------------------------------------------------------------------
            statement.execute("UPDATE debezium.products SET weight=1345.67 WHERE id=2001");
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "ID", 2001);

            // ---------------------------------------------------------------------------------------------------------------
            // Change our schema with a fully-qualified name; we should still see this event
            // ---------------------------------------------------------------------------------------------------------------
            // Add a column with default to the 'products' table and explicitly update one record
            // ...
            statement.execute(
                    String.format("ALTER TABLE %s.products ADD  volume FLOAT", "debezium"));
            statement.execute("UPDATE debezium.products SET volume=13.5 WHERE id=2001");
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "ID", 2001);

            // cleanup
            source.close();
            runThread.sync();
        }
    }

    @Test
    @Disabled("It can be open until DBZ-5245 and DBZ-4936 fix")
    void testCheckpointAndRestore() throws Exception {

        createAndInitialize("product.sql");

        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();
        {
            // ---------------------------------------------------------------------------
            // Step-1: start the source from empty state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source = createOracleLogminerSource();
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
            Assertions.assertThat(received).isEqualTo(2);

            // we can't perform checkpoint during DB snapshot
            Assertions.assertThat(
                            waitForCheckpointLock(
                                    sourceContext.getCheckpointLock(), Duration.ofSeconds(3)))
                    .isFalse();

            // unblock the source context to continue the processing
            sourceContext.blocker.release();
            // wait until the source finishes the database snapshot
            List<SourceRecord> records = drain(sourceContext, 9 - received);
            Assertions.assertThat(records.size() + received).isEqualTo(9);

            // state is still empty
            Assertions.assertThat(offsetState.list).isEmpty();
            Assertions.assertThat(historyState.list).isEmpty();

            // ---------------------------------------------------------------------------
            // Step-2: trigger checkpoint-1 after snapshot finished
            // ---------------------------------------------------------------------------
            synchronized (sourceContext.getCheckpointLock()) {
                // trigger checkpoint-1
                source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
            }

            assertHistoryState(historyState);
            Assertions.assertThat(offsetState.list).hasSize(1);
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            Assertions.assertThat(JsonPath.<String>read(state, "$.sourcePartition.server"))
                    .isEqualTo("oracle_logminer");

            Assertions.assertThat(state)
                    .doesNotContain("row")
                    .doesNotContain("server_id")
                    .doesNotContain("event");

            source.close();
            runThread.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-3: restore the source from state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source2 = createOracleLogminerSource();
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
            Assertions.assertThat(waitForAvailableRecords(Duration.ofSeconds(5), sourceContext2))
                    .isFalse();

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                statement.execute(
                        "INSERT INTO debezium.products VALUES (110,'robot','Toy robot',1.304)"); // 110
                List<SourceRecord> records = drain(sourceContext2, 1);
                Assertions.assertThat(records).hasSize(1);
                assertInsert(records.get(0), "ID", 110);

                // ---------------------------------------------------------------------------
                // Step-4: trigger checkpoint-2 during DML operations
                // ---------------------------------------------------------------------------
                synchronized (sourceContext2.getCheckpointLock()) {
                    // trigger checkpoint-1
                    source2.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));
                }

                assertHistoryState(historyState); // assert the DDL is stored in the history state
                Assertions.assertThat(offsetState.list).hasSize(1);
                String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
                Assertions.assertThat(JsonPath.<String>read(state, "$.sourcePartition.server"))
                        .isEqualTo("oracle_logminer");

                // execute 2 more DMLs to have more redo log
                statement.execute(
                        "INSERT INTO debezium.products VALUES (1001,'roy','old robot',1234.56)"); // 1001
                statement.execute("UPDATE debezium.products SET weight=1345.67 WHERE id=1001");
            }

            // cancel the source
            source2.close();
            runThread2.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-5: restore the source from checkpoint-2
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source3 = createOracleLogminerSource();
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

            // consume the unconsumed redo log
            List<SourceRecord> records = drain(sourceContext3, 2);
            assertInsert(records.get(0), "ID", 1001);
            assertUpdate(records.get(1), "ID", 1001);

            // make sure there is no more events
            Assertions.assertThat(waitForAvailableRecords(Duration.ofSeconds(3), sourceContext3))
                    .isFalse();

            // can continue to receive new events
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute("DELETE FROM debezium.products WHERE id=1001");
            }
            records = drain(sourceContext3, 1);
            assertDelete(records.get(0), "ID", 1001);

            // ---------------------------------------------------------------------------
            // Step-6: trigger checkpoint-2 to make sure we can continue to to further checkpoints
            // ---------------------------------------------------------------------------
            synchronized (sourceContext3.getCheckpointLock()) {
                // checkpoint 3
                source3.snapshotState(new StateSnapshotContextSynchronousImpl(233, 233));
            }
            assertHistoryState(historyState); // assert the DDL is stored in the history state
            Assertions.assertThat(offsetState.list).hasSize(1);
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            Assertions.assertThat(JsonPath.<String>read(state, "$.sourcePartition.server"))
                    .isEqualTo("oracle_logminer");

            source3.close();
            runThread3.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-7: restore the source from checkpoint-3
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source4 = createOracleLogminerSource();
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
            Assertions.assertThat(waitForAvailableRecords(Duration.ofSeconds(5), sourceContext4))
                    .isFalse();

            // ---------------------------------------------------------------------------
            // Step-8: trigger checkpoint-3 to make sure we can continue to to further checkpoints
            // ---------------------------------------------------------------------------
            synchronized (sourceContext4.getCheckpointLock()) {
                // checkpoint 4
                source4.snapshotState(new StateSnapshotContextSynchronousImpl(254, 254));
            }
            assertHistoryState(historyState); // assert the DDL is stored in the history state
            Assertions.assertThat(offsetState.list).hasSize(1);
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            Assertions.assertThat(JsonPath.<String>read(state, "$.sourcePartition.server"))
                    .isEqualTo("oracle_logminer");

            source4.close();
            runThread4.sync();
        }
    }

    @Test
    @Disabled("Debezium Oracle connector don't monitor unknown tables since 1.6, see DBZ-3612")
    void testRecoverFromRenameOperation() throws Exception {

        createAndInitialize("product.sql");
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();

        {
            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                // Step-1: start the source from empty state
                final DebeziumSourceFunction<SourceRecord> source = createOracleLogminerSource();
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

                // wait until the source finishes the database snapshot
                List<SourceRecord> records = drain(sourceContext, 9);
                Assertions.assertThat(records).hasSize(9);

                // state is still empty
                Assertions.assertThat(offsetState.list).isEmpty();
                Assertions.assertThat(historyState.list).isEmpty();

                // create temporary tables which are not in the whitelist
                statement.execute(
                        "CREATE TABLE debezium.tp_001_ogt_products as (select * from debezium.products WHERE 1=2)");
                // do some renames
                statement.execute("ALTER TABLE DEBEZIUM.PRODUCTS RENAME TO tp_001_del_products");

                statement.execute("ALTER TABLE debezium.tp_001_ogt_products RENAME TO PRODUCTS");
                statement.execute(
                        "INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT) VALUES (110,'robot','Toy robot',1.304)"); // 110
                statement.execute(
                        "INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT) VALUES (111,'stream train','Town stream train',1.304)"); // 111
                statement.execute(
                        "INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT) VALUES (112,'cargo train','City cargo train',1.304)"); // 112

                int received = drain(sourceContext, 3).size();
                Assertions.assertThat(received).isEqualTo(3);

                // Step-2: trigger a checkpoint
                synchronized (sourceContext.getCheckpointLock()) {
                    // trigger checkpoint-1
                    source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
                }

                Assertions.assertThat(historyState.list).isNotEmpty();
                Assertions.assertThat(offsetState.list).isNotEmpty();

                source.close();
                runThread.sync();
            }
        }

        {
            // Step-3: restore the source from state
            final DebeziumSourceFunction<SourceRecord> source2 = createOracleLogminerSource();
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
            Assertions.assertThat(waitForAvailableRecords(Duration.ofSeconds(5), sourceContext2))
                    .isFalse();

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        "INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT) VALUES (113,'Airplane','Toy airplane',1.304)"); // 113
                List<SourceRecord> records = drain(sourceContext2, 1);
                Assertions.assertThat(records).hasSize(1);
                assertInsert(records.get(0), "ID", 113);

                source2.close();
                runThread2.sync();
            }
        }
    }

    @Test
    void testConsumingEmptyTable() throws Exception {

        createAndInitialize("product.sql");
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();
        int prevPos = 0;
        {
            // ---------------------------------------------------------------------------
            // Step-1: start the source from empty state
            // ---------------------------------------------------------------------------
            DebeziumSourceFunction<SourceRecord> source =
                    basicSourceBuilder().tableList("debezium.category").build();
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

            // wait until Debezium is started
            while (!source.getDebeziumStarted()) {
                Thread.sleep(100);
            }
            // ---------------------------------------------------------------------------
            // Step-2: trigger checkpoint-1
            // ---------------------------------------------------------------------------
            synchronized (sourceContext.getCheckpointLock()) {
                source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
            }

            // state is still empty
            Assertions.assertThat(offsetState.list).isEmpty();

            // make sure there is no more events
            Assertions.assertThat(waitForAvailableRecords(Duration.ofSeconds(5), sourceContext))
                    .isFalse();

            try (Connection connection = getJdbcConnection();
                    Statement statement = connection.createStatement()) {

                statement.execute("INSERT INTO debezium.category VALUES (1, 'book')");
                statement.execute("INSERT INTO debezium.category VALUES (2, 'shoes')");
                statement.execute("UPDATE debezium.category SET category_name='books' WHERE id=1");
                List<SourceRecord> records = drain(sourceContext, 3);
                Assertions.assertThat(records).hasSize(3);
                assertInsert(records.get(0), "ID", 1);
                assertInsert(records.get(1), "ID", 2);
                assertUpdate(records.get(2), "ID", 1);

                // ---------------------------------------------------------------------------
                // Step-4: trigger checkpoint-2 during DML operations
                // ---------------------------------------------------------------------------
                synchronized (sourceContext.getCheckpointLock()) {
                    // trigger checkpoint-1
                    source.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));
                }

                assertHistoryState(historyState); // assert the DDL is stored in the history state
                Assertions.assertThat(offsetState.list).hasSize(1);
                String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
                Assertions.assertThat(JsonPath.<String>read(state, "$.sourcePartition.server"))
                        .isEqualTo("oracle_logminer");
            }

            source.close();
            runThread.sync();
        }
    }

    private void assertHistoryState(TestingListState<String> historyState) {
        // assert the DDL is stored in the history state
        Assertions.assertThat(historyState.list).isNotEmpty();
        boolean hasTable =
                historyState.list.stream()
                        .skip(1)
                        .anyMatch(
                                history ->
                                        !((Map<?, ?>) JsonPath.read(history, "$.table")).isEmpty()
                                                && (JsonPath.read(history, "$.type")
                                                                .toString()
                                                                .equals("CREATE")
                                                        || JsonPath.read(history, "$.type")
                                                                .toString()
                                                                .equals("ALTER")));
        Assertions.assertThat(hasTable).isTrue();
    }

    // ------------------------------------------------------------------------------------------
    // Public Utilities
    // ------------------------------------------------------------------------------------------

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private DebeziumSourceFunction<SourceRecord> createOracleLogminerSource() {
        return basicSourceBuilder().build();
    }

    private OracleSource.Builder<SourceRecord> basicSourceBuilder() {
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("debezium.log.mining.strategy", "online_catalog");
        // ignore APEX ORCLCDB system tables changes
        debeziumProperties.setProperty("database.history.store.only.captured.tables.ddl", "true");
        return OracleSource.<SourceRecord>builder()
                .hostname(ORACLE_CONTAINER.getHost())
                .port(ORACLE_CONTAINER.getOraclePort())
                .database("ORCLCDB")
                .tableList("debezium" + "." + "products") // monitor table "products"
                .username(ORACLE_CONTAINER.getUsername())
                .password(ORACLE_CONTAINER.getPassword())
                .debeziumProperties(debeziumProperties)
                .deserializer(new ForwardDeserializeSchema());
    }

    private static <T> List<T> drain(TestSourceContext<T> sourceContext, int expectedRecordCount)
            throws Exception {
        List<T> allRecords = new ArrayList<>();
        LinkedBlockingQueue<StreamRecord<T>> queue = sourceContext.getCollectedOutputs();
        while (allRecords.size() < expectedRecordCount) {
            StreamRecord<T> record = queue.poll(200, TimeUnit.SECONDS);
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

    /**
     * A simple implementation of {@link DebeziumDeserializationSchema} which just forward the
     * {@link SourceRecord}.
     */
    public static class ForwardDeserializeSchema
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
