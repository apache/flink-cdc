/*
 * Copyright 2023 Ververica Inc.
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

package com.vervetica.cdc.connectors.vitess;

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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.utils.TestSourceContext;
import com.ververica.cdc.connectors.vitess.VitessSource;
import com.ververica.cdc.connectors.vitess.config.TabletType;
import com.ververica.cdc.connectors.vitess.config.VtctldConfig;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.ververica.cdc.connectors.utils.AssertUtils.assertDelete;
import static com.ververica.cdc.connectors.utils.AssertUtils.assertInsert;
import static com.ververica.cdc.connectors.utils.AssertUtils.assertUpdate;

/** Tests for {@link VitessSource} which also heavily tests {@link DebeziumSourceFunction}. */
public class VitessSourceTest extends VitessTestBase {

    @Before
    public void before() {
        initializeTable("inventory");
    }

    @Test
    public void testConsumingAllEvents() throws Exception {
        DebeziumSourceFunction<SourceRecord> source = createVitessSqlSource(0);
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

            waitForSourceToStart(Duration.ofSeconds(60), source);
            List<SourceRecord> records;

            statement.execute(
                    "INSERT INTO test.products VALUES (default,'robot','Toy robot',1.304)"); // 110
            records = drain(sourceContext, 1);
            assertInsert(records.get(0), "id", 101);

            statement.execute(
                    "INSERT INTO test.products VALUES (1001,'roy','old robot',1234.56)"); // 1001
            records = drain(sourceContext, 1);
            assertInsert(records.get(0), "id", 1001);

            // ---------------------------------------------------------------------------------------------------------------
            // Changing the primary key of a row should result in 2 events: INSERT, DELETE
            // (TOMBSTONE is dropped)
            // ---------------------------------------------------------------------------------------------------------------
            statement.execute(
                    "UPDATE test.products SET id=2001, description='really old robot' WHERE id=1001");
            records = drain(sourceContext, 2);
            assertDelete(records.get(0), "id", 1001);
            assertInsert(records.get(1), "id", 2001);

            // ---------------------------------------------------------------------------------------------------------------
            // Simple UPDATE (with no schema changes)
            // ---------------------------------------------------------------------------------------------------------------
            statement.execute("UPDATE test.products SET weight=1345.67 WHERE id=2001");
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "id", 2001);

            // ---------------------------------------------------------------------------------------------------------------
            // Change our schema with a fully-qualified name; we should still see this event
            // ---------------------------------------------------------------------------------------------------------------
            // Add a column with default to the 'products' table and explicitly update one record
            // ...
            statement.execute(
                    "ALTER TABLE test.products ADD COLUMN volume FLOAT, ADD COLUMN alias VARCHAR(30) NULL");

            // Vitess schema change has eventual consistency, wait few seconds.
            Thread.sleep(5000);
            statement.execute("UPDATE test.products SET volume=13.5 WHERE id=2001");
            records = drain(sourceContext, 1);
            assertUpdate(records.get(0), "id", 2001);

            // cleanup
            source.cancel();
            source.close();
            runThread.sync();
        }
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private DebeziumSourceFunction<SourceRecord> createVitessSqlSource(int heartbeatInterval) {
        Properties properties = new Properties();
        properties.setProperty("heartbeat.interval.ms", String.valueOf(heartbeatInterval));
        return VitessSource.<SourceRecord>builder()
                .hostname(VITESS_CONTAINER.getHost())
                .port(VITESS_CONTAINER.getGrpcPort())
                .keyspace(VITESS_CONTAINER.getKeyspace())
                .tabletType(TabletType.MASTER)
                .tableIncludeList("test.products")
                .vtctldConfig(
                        VtctldConfig.builder()
                                .hostname(VITESS_CONTAINER.getHost())
                                .port(VITESS_CONTAINER.getVtctldGrpcPort())
                                .build())
                .deserializer(new ForwardDeserializeSchema())
                .debeziumProperties(properties)
                .build();
    }

    private <T> List<T> drain(TestSourceContext<T> sourceContext, int expectedRecordCount)
            throws Exception {
        List<T> allRecords = new ArrayList<>();
        LinkedBlockingQueue<StreamRecord<T>> queue = sourceContext.getCollectedOutputs();
        while (allRecords.size() < expectedRecordCount) {
            StreamRecord<T> record = queue.poll(1000, TimeUnit.SECONDS);
            if (record != null) {
                allRecords.add(record.getValue());
            } else {
                throw new RuntimeException(
                        "Can't receive " + expectedRecordCount + " elements before timeout.");
            }
        }

        return allRecords;
    }

    private boolean waitForSourceToStart(
            Duration timeout, DebeziumSourceFunction<SourceRecord> source)
            throws InterruptedException {
        long now = System.currentTimeMillis();
        long stop = now + timeout.toMillis();
        while (System.currentTimeMillis() < stop) {
            if (source.getDebeziumStarted()) {
                break;
            }
            Thread.sleep(10); // save CPU
        }
        Thread.sleep(10000); // Wait for full start
        return source.getDebeziumStarted();
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
}
