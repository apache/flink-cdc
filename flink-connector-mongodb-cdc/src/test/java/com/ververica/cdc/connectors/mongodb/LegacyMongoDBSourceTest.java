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

package com.ververica.cdc.connectors.mongodb;

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
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.ververica.cdc.connectors.utils.TestSourceContext;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
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

import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.buildConnectionString;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBAssertUtils.assertDelete;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBAssertUtils.assertInsert;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBAssertUtils.assertObjectIdEquals;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBAssertUtils.assertReplace;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBAssertUtils.assertUpdate;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link MongoDBSource} which also heavily tests {@link DebeziumSourceFunction}. */
public class LegacyMongoDBSourceTest extends LegacyMongoDBTestBase {

    @Test
    public void testConsumingAllEvents() throws Exception {
        String database = MONGODB_CONTAINER.executeCommandFileInSeparateDatabase("inventory");

        DebeziumSourceFunction<SourceRecord> source = createMongoDBSource(database);
        TestSourceContext<SourceRecord> sourceContext = new TestSourceContext<>();

        MongoCollection<Document> products = getMongoDatabase(database).getCollection("products");

        setupSource(source);

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

        assertTrue(
                waitForCheckpointLock(sourceContext.getCheckpointLock(), Duration.ofSeconds(15)));

        // ---------------------------------------------------------------------------------------------------------------
        // Simple INSERT
        // ---------------------------------------------------------------------------------------------------------------
        products.insertOne(productDocOf(null, "description", "Toy robot", 1.304));
        records = drain(sourceContext, 1);
        assertInsert(records.get(0), true);

        products.insertOne(productDocOf("000000000000000000001001", "roy", "old robot", 1234.56));
        records = drain(sourceContext, 1);
        assertInsert(records.get(0), true);
        assertObjectIdEquals("000000000000000000001001", records.get(0));

        // -------------------------------------------------------------------------------------------------------------
        // Changing the description of a row should result in 1 events: UPDATE
        // -------------------------------------------------------------------------------------------------------------
        products.updateOne(
                Filters.eq("_id", new ObjectId("000000000000000000001001")),
                Updates.set("description", "really old robot"));
        records = drain(sourceContext, 1);
        assertUpdate(records.get(0), "000000000000000000001001");

        // -------------------------------------------------------------------------------------------------------------
        // Replace the document of a row should result in 1 events: UPDATE
        // -------------------------------------------------------------------------------------------------------------
        products.replaceOne(
                Filters.eq("_id", new ObjectId("000000000000000000001001")),
                productDocOf(
                        "000000000000000000001001", "roy_replace", "old robot replace", 1234.57));
        records = drain(sourceContext, 1);
        assertReplace(records.get(0), "000000000000000000001001");

        // -------------------------------------------------------------------------------------------------------------
        // Simple DELETE
        // -------------------------------------------------------------------------------------------------------------
        products.deleteOne(Filters.eq("_id", new ObjectId("000000000000000000001001")));
        records = drain(sourceContext, 1);
        assertDelete(records.get(0), "000000000000000000001001");

        // cleanup
        source.cancel();
        source.close();
        runThread.sync();
    }

    @Test
    public void testCheckpointAndRestore() throws Exception {
        String database = MONGODB_CONTAINER.executeCommandFileInSeparateDatabase("inventory");

        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();
        {
            // ---------------------------------------------------------------------------
            // Step-1: start the source from empty state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source = createMongoDBSource(database);
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
            List<SourceRecord> records = drain(sourceContext, 2);
            int received = records.size();
            assertEquals(2, received);

            // we can't perform checkpoint during DB snapshot
            assertFalse(
                    waitForCheckpointLock(
                            sourceContext.getCheckpointLock(), Duration.ofSeconds(3)));

            // unblock the source context to continue the processing
            sourceContext.blocker.release();
            // wait until the source finishes the database snapshot
            records = drain(sourceContext, 9 - received);
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
            assertTrue(state.contains("sourcePartition"));
            assertTrue(state.contains("sourceOffset"));

            // ---------------------------------------------------------------------------
            // Step-3: trigger checkpoint-2 after snapshot finished and streaming data received
            // ---------------------------------------------------------------------------
            MongoCollection<Document> products =
                    getMongoDatabase(database).getCollection("products");

            products.updateOne(
                    Filters.eq("_id", new ObjectId("100000000000000000000102")),
                    Updates.set("description", "really old robot 1002"));
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "100000000000000000000102");

            synchronized (sourceContext.getCheckpointLock()) {
                // trigger checkpoint-2
                source.snapshotState(new StateSnapshotContextSynchronousImpl(102, 102));
            }

            assertEquals(1, offsetState.list.size());

            state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertTrue(state.contains("sourcePartition"));
            assertTrue(state.contains("sourceOffset"));
            String resumeToken = JsonPath.read(state, "$.sourceOffset._id");
            assertTrue(resumeToken.contains("_data"));

            source.cancel();
            source.close();
            runThread.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-3: restore the source from state
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source2 = createMongoDBSource(database);
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

            MongoCollection<Document> products =
                    getMongoDatabase(database).getCollection("products");

            products.insertOne(
                    productDocOf("000000000000000000001002", "robot", "Toy robot", 1.304));

            List<SourceRecord> records = drain(sourceContext2, 1);
            assertEquals(1, records.size());
            assertInsert(records.get(0), true);
            assertObjectIdEquals("000000000000000000001002", records.get(0));

            // ---------------------------------------------------------------------------
            // Step-4: trigger checkpoint-2 during DML operations
            // ---------------------------------------------------------------------------
            synchronized (sourceContext2.getCheckpointLock()) {
                // trigger checkpoint-1
                source2.snapshotState(new StateSnapshotContextSynchronousImpl(138, 138));
            }

            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertTrue(state.contains("sourcePartition"));
            assertTrue(state.contains("sourceOffset"));
            String resumeToken = JsonPath.read(state, "$.sourceOffset._id");
            assertTrue(resumeToken.contains("_data"));

            // execute 2 more DMLs to have more change log
            products.insertOne(
                    productDocOf("000000000000000000001003", "roy", "old robot", 1234.56));
            products.updateOne(
                    Filters.eq("_id", new ObjectId("000000000000000000001002")),
                    Updates.set("weight", 1345.67));

            // cancel the source
            source2.cancel();
            source2.close();
            runThread2.sync();
        }

        {
            // ---------------------------------------------------------------------------
            // Step-5: restore the source from checkpoint-2
            // ---------------------------------------------------------------------------
            final DebeziumSourceFunction<SourceRecord> source3 = createMongoDBSource(database);
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

            // consume the unconsumed oplog
            List<SourceRecord> records = drain(sourceContext3, 2);
            assertInsert(records.get(0), true);
            assertObjectIdEquals("000000000000000000001003", records.get(0));
            assertUpdate(records.get(1), "000000000000000000001002");

            // make sure there is no more events
            assertFalse(waitForAvailableRecords(Duration.ofSeconds(3), sourceContext3));

            MongoCollection<Document> products =
                    getMongoDatabase(database).getCollection("products");

            products.deleteOne(Filters.eq("_id", new ObjectId("000000000000000000001002")));
            records = drain(sourceContext3, 1);
            assertDelete(records.get(0), "000000000000000000001002");

            products.deleteOne(Filters.eq("_id", new ObjectId("000000000000000000001003")));
            records = drain(sourceContext3, 1);
            assertDelete(records.get(0), "000000000000000000001003");

            // ---------------------------------------------------------------------------
            // Step-6: trigger checkpoint-2 to make sure we can continue to to further checkpoints
            // ---------------------------------------------------------------------------
            synchronized (sourceContext3.getCheckpointLock()) {
                // checkpoint 3
                source3.snapshotState(new StateSnapshotContextSynchronousImpl(233, 233));
            }

            assertEquals(1, offsetState.list.size());
            String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
            assertTrue(state.contains("sourcePartition"));
            assertTrue(state.contains("sourceOffset"));
            String resumeToken = JsonPath.read(state, "$.sourceOffset._id");
            assertTrue(resumeToken.contains("_data"));

            source3.cancel();
            source3.close();
            runThread3.sync();
        }
    }

    @Test
    public void testConnectionUri() {
        String hosts = MONGODB_CONTAINER.getHostAndPort();

        ConnectionString case0 = buildConnectionString(null, null, hosts, null);
        assertEquals(String.format("mongodb://%s", hosts), case0.toString());

        ConnectionString case1 = buildConnectionString("", null, hosts, null);
        assertEquals(String.format("mongodb://%s", hosts), case1.toString());

        ConnectionString case2 = buildConnectionString(null, "", hosts, null);
        assertEquals(String.format("mongodb://%s", hosts), case2.toString());

        ConnectionString case3 =
                buildConnectionString(FLINK_USER, FLINK_USER_PASSWORD, hosts, null);
        assertEquals(FLINK_USER, case3.getUsername());
        assertEquals(FLINK_USER_PASSWORD, new String(case3.getPassword()));
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private DebeziumSourceFunction<SourceRecord> createMongoDBSource(String database) {
        return MongoDBSource.<SourceRecord>builder()
                .hosts(MONGODB_CONTAINER.getHostAndPort())
                .username(FLINK_USER)
                .password(FLINK_USER_PASSWORD)
                .databaseList(database)
                .collectionList(database + ".products")
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

    private Document productDocOf(String id, String name, String description, Double weight) {
        Document document = new Document();
        if (id != null) {
            document.put("_id", new ObjectId(id));
        }
        document.put("name", name);
        document.put("description", description);
        document.put("weight", weight);
        return document;
    }
}
