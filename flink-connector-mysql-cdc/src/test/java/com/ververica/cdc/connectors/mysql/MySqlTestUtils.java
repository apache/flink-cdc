/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.jayway.jsonpath.JsonPath;
import com.ververica.cdc.connectors.mysql.source.utils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import com.ververica.cdc.connectors.utils.TestSourceContext;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/** Utils to help test. */
public class MySqlTestUtils {

    /** Gets the latest offset of current MySQL server. */
    public static Tuple2<String, Integer> currentMySQLLatestOffset(
            UniqueDatabase database,
            String table,
            int expectedRecordCount,
            boolean useLegacyImplementation)
            throws Exception {
        DebeziumSourceFunction<SourceRecord> source =
                MySqlSource.<SourceRecord>builder()
                        .hostname(database.getHost())
                        .port(database.getDatabasePort())
                        .databaseList(database.getDatabaseName())
                        .tableList(database.getDatabaseName() + "." + table)
                        .username(database.getUsername())
                        .password(database.getPassword())
                        .deserializer(new ForwardDeserializeSchema())
                        .debeziumProperties(createDebeziumProperties(useLegacyImplementation))
                        .build();
        final TestingListState<byte[]> offsetState = new TestingListState<>();
        final TestingListState<String> historyState = new TestingListState<>();

        // ---------------------------------------------------------------------------
        // Step-1: start source
        // ---------------------------------------------------------------------------
        TestSourceContext<SourceRecord> sourceContext = new TestSourceContext<>();
        setupSource(source, false, offsetState, historyState, true, 0, 1);
        final CheckedThread runThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        source.run(sourceContext);
                    }
                };
        runThread.start();

        drain(sourceContext, expectedRecordCount);

        // ---------------------------------------------------------------------------
        // Step-2: trigger checkpoint-1 after snapshot finished
        // ---------------------------------------------------------------------------
        synchronized (sourceContext.getCheckpointLock()) {
            // trigger checkpoint-1
            source.snapshotState(new StateSnapshotContextSynchronousImpl(101, 101));
        }

        assertEquals(1, offsetState.list.size());
        String state = new String(offsetState.list.get(0), StandardCharsets.UTF_8);
        String offsetFile = JsonPath.read(state, "$.sourceOffset.file");
        int offsetPos = JsonPath.read(state, "$.sourceOffset.pos");

        source.cancel();
        source.close();
        runThread.sync();

        return Tuple2.of(offsetFile, offsetPos);
    }

    public static MySqlSource.Builder<SourceRecord> basicSourceBuilder(
            MySqlContainer container, UniqueDatabase database, boolean useLegacyImplementation) {
        Properties debeziumProps = createDebeziumProperties(useLegacyImplementation);
        return MySqlSource.<SourceRecord>builder()
                .hostname(container.getHost())
                .port(container.getDatabasePort())
                .databaseList(database.getDatabaseName())
                .tableList(
                        database.getDatabaseName() + "." + "products") // monitor table "products"
                .username(container.getUsername())
                .password(container.getPassword())
                .deserializer(new ForwardDeserializeSchema())
                .debeziumProperties(debeziumProps);
    }

    public static <T> void setupSource(DebeziumSourceFunction<T> source) throws Exception {
        setupSource(
                source, false, null, null,
                true, // enable checkpointing; auto commit should be ignored
                0, 1);
    }

    static <T, S1, S2> void setupSource(
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

    public static <T> List<T> drain(TestSourceContext<T> sourceContext, int expectedRecordCount)
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

    private static Properties createDebeziumProperties(boolean useLegacyImplementation) {
        Properties debeziumProps = new Properties();
        if (useLegacyImplementation) {
            debeziumProps.put("internal.implementation", "legacy");
            // check legacy mysql record type
            debeziumProps.put("transforms", "snapshotasinsert");
            debeziumProps.put(
                    "transforms.snapshotasinsert.type",
                    "io.debezium.connector.mysql.transforms.ReadToInsertEvent");
        }
        return debeziumProps;
    }

    /**
     * A simple implementation of {@link DebeziumDeserializationSchema} which just forward the
     * {@link SourceRecord}.
     */
    public static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 2975058057832211228L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) {
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
        public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) {
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
                MapStateDescriptor<K, V> stateDescriptor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) {
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
        public OperatorStateStore getOperatorStateStore() {
            return operatorStateStore;
        }

        @Override
        public KeyedStateStore getKeyedStateStore() {
            throw new UnsupportedOperationException();
        }
    }

    public static final class TestingListState<T> implements ListState<T> {

        public final List<T> list = new ArrayList<>();
        private boolean clearCalled = false;

        @Override
        public void clear() {
            list.clear();
            clearCalled = true;
        }

        @Override
        public Iterable<T> get() {
            return list;
        }

        @Override
        public void add(T value) {
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
        public void update(List<T> values) {
            clear();

            addAll(values);
        }

        @Override
        public void addAll(List<T> values) {
            if (values != null) {
                values.forEach(
                        v -> Preconditions.checkNotNull(v, "You cannot add null to a ListState."));

                list.addAll(values);
            }
        }
    }
}
