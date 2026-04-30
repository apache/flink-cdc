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

package org.apache.flink.cdc.connectors.dws.sink;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import com.huaweicloud.dws.client.DwsConfig;
import com.huaweicloud.dws.client.config.DwsClientConfigs;
import com.huaweicloud.dws.client.model.WriteMode;
import com.huaweicloud.dws.connectors.flink.config.DwsConnectionOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DwsSinkFunction}. */
class DwsSinkFunctionTest {

    private static final Event TEST_EVENT =
            new FlushEvent(
                    0,
                    Collections.singletonList(TableId.tableId("test_db", "public", "test_table")),
                    SchemaChangeEventType.CREATE_TABLE);

    @Test
    void testDelegateCheckpointLifecycleToWrappedSinkFunction() throws Exception {
        TrackingSinkFunction delegate = new TrackingSinkFunction();
        DwsSinkFunction sinkFunction =
                new DwsSinkFunction(
                        null,
                        ZoneId.of("UTC"),
                        false,
                        "public",
                        1,
                        Duration.ofSeconds(1),
                        true,
                        true,
                        null,
                        delegate);

        sinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(true, 1, 0));
        sinkFunction.initializeState(new TestingFunctionInitializationContext());
        sinkFunction.open(new Configuration());
        sinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(101L, 101L));
        sinkFunction.notifyCheckpointComplete(101L);
        sinkFunction.notifyCheckpointAborted(102L);
        sinkFunction.close();

        assertThat(delegate.runtimeContextWasSetInInitializeState).isTrue();
        assertThat(delegate.openCalled).isTrue();
        assertThat(delegate.initializeStateCalled).isTrue();
        assertThat(delegate.snapshotStateCalled).isTrue();
        assertThat(delegate.completedCheckpointId).isEqualTo(101L);
        assertThat(delegate.abortedCheckpointId).isEqualTo(102L);
        assertThat(delegate.closeCalled).isTrue();
    }

    @Test
    void testDelegateInvokeLifecycleToWrappedSinkFunction() throws Exception {
        ForwardingSinkFunction delegate = new ForwardingSinkFunction();
        DwsSinkFunction sinkFunction =
                new DwsSinkFunction(
                        null,
                        ZoneId.of("UTC"),
                        false,
                        "public",
                        1,
                        Duration.ofSeconds(1),
                        true,
                        true,
                        null,
                        delegate);

        sinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
        sinkFunction.open(new Configuration());
        sinkFunction.invoke(TEST_EVENT, null);
        sinkFunction.close();

        assertThat(delegate.runtimeContextWasSetInOpen).isTrue();
        assertThat(delegate.openCalled).isTrue();
        assertThat(delegate.lastInvokedEvent).isSameAs(TEST_EVENT);
        assertThat(delegate.closeCalled).isTrue();
    }

    @Test
    void testIgnoreCheckpointCallbacksForNonCheckpointedWrappedSinkFunction() throws Exception {
        ForwardingSinkFunction delegate = new ForwardingSinkFunction();
        DwsSinkFunction sinkFunction =
                new DwsSinkFunction(
                        null,
                        ZoneId.of("UTC"),
                        false,
                        "public",
                        1,
                        Duration.ofSeconds(1),
                        true,
                        true,
                        null,
                        delegate);

        sinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(true, 1, 0));
        sinkFunction.initializeState(new TestingFunctionInitializationContext());
        sinkFunction.open(new Configuration());
        sinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(101L, 101L));
        sinkFunction.notifyCheckpointComplete(101L);
        sinkFunction.notifyCheckpointAborted(102L);
        sinkFunction.invoke(TEST_EVENT, null);
        sinkFunction.close();

        assertThat(delegate.runtimeContextWasSetInOpen).isTrue();
        assertThat(delegate.openCalled).isTrue();
        assertThat(delegate.lastInvokedEvent).isSameAs(TEST_EVENT);
        assertThat(delegate.closeCalled).isTrue();
    }

    @Test
    void testBuildQualifiedTableNameUsesDefaultSchema() {
        DwsSinkFunction sinkFunction = createSinkFunction(false, "ods", true, null);

        assertThat(sinkFunction.buildQualifiedTableName(TableId.tableId("orders")))
                .isEqualTo("ods.orders");
        assertThat(
                        sinkFunction.buildQualifiedTableName(
                                TableId.tableId("catalog", "custom", "orders")))
                .isEqualTo("custom.orders");
    }

    @Test
    void testBuildQualifiedTableNameUsesPublicSchemaWhenDefaultSchemaIsBlank() {
        DwsSinkFunction sinkFunction = createSinkFunction(false, "   ", true, null);

        assertThat(sinkFunction.buildQualifiedTableName(TableId.tableId("orders")))
                .isEqualTo("public.orders");
    }

    @Test
    void testCreateDwsConfigDisablesAutoFlushWhenConfigured() {
        DwsSinkFunction sinkFunction = createSinkFunction(false, "public", false, null);

        DwsConfig dwsConfig = sinkFunction.createDwsConfig();
        assertThat(dwsConfig.get(DwsClientConfigs.WRITE_AUTO_FLUSH_BATCH_SIZE))
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(dwsConfig.get(DwsClientConfigs.WRITE_AUTO_FLUSH_MAX_INTERVAL))
                .isEqualTo(Duration.ZERO);
    }

    @Test
    void testCreateDwsConfigPreservesNativeWriteTuningOptions() {
        DwsConfig nativeConfig =
                DwsConfig.builder()
                        .with(DwsClientConfigs.WRITE_THREAD_SIZE, 6)
                        .with(DwsClientConfigs.WRITE_USE_COPY_BATCH_SIZE, 2048)
                        .with(DwsClientConfigs.WRITE_FORCE_FLUSH_BATCH_SIZE, 4096)
                        .build();

        DwsSinkFunction sinkFunction =
                new DwsSinkFunction(
                        minimalConnectionOptions(nativeConfig),
                        ZoneId.of("UTC"),
                        false,
                        "public",
                        128,
                        Duration.ofSeconds(5),
                        true,
                        true,
                        WriteMode.AUTO,
                        new ForwardingSinkFunction());

        DwsConfig dwsConfig = sinkFunction.createDwsConfig();
        assertThat(dwsConfig.get(DwsClientConfigs.WRITE_THREAD_SIZE)).isEqualTo(6);
        assertThat(dwsConfig.get(DwsClientConfigs.WRITE_USE_COPY_BATCH_SIZE)).isEqualTo(2048);
        assertThat(dwsConfig.get(DwsClientConfigs.WRITE_FORCE_FLUSH_BATCH_SIZE)).isEqualTo(4096);
    }

    @Test
    void testCreateDwsConfigUsesCopyMergeWriteModeByDefaultForCaseInsensitive() throws Exception {
        DwsSinkFunction sinkFunction = createSinkFunction(false, "public", true, null);

        assertThat(readField(sinkFunction, "writeMode")).isEqualTo(WriteMode.COPY_MERGE);
    }

    @Test
    void testCreateDwsConfigUsesAutoWriteModeByDefaultForCaseSensitive() throws Exception {
        DwsSinkFunction sinkFunction = createSinkFunction(true, "public", true, null);

        assertThat(readField(sinkFunction, "writeMode")).isEqualTo(WriteMode.AUTO);
    }

    @Test
    void testCreateDwsConfigUsesConfiguredWriteMode() throws Exception {
        DwsSinkFunction sinkFunction =
                createSinkFunction(false, "public", true, WriteMode.UPDATE_AUTO);

        assertThat(readField(sinkFunction, "writeMode")).isEqualTo(WriteMode.UPDATE_AUTO);
    }

    @Test
    void testCreateDwsConfigPreservesRetryConfiguration() {
        DwsConfig nativeConfig =
                DwsConfig.builder().with(DwsClientConfigs.RETRY_MAX_TIMES, 9).build();

        DwsSinkFunction sinkFunction =
                new DwsSinkFunction(
                        minimalConnectionOptions(nativeConfig),
                        ZoneId.of("UTC"),
                        false,
                        "public",
                        128,
                        Duration.ofSeconds(5),
                        true,
                        false,
                        WriteMode.AUTO,
                        new ForwardingSinkFunction());

        DwsConfig dwsConfig = sinkFunction.createDwsConfig();
        assertThat(dwsConfig.get(DwsClientConfigs.RETRY_MAX_TIMES)).isEqualTo(9);
    }

    private static final class TrackingSinkFunction extends RichSinkFunction<Event>
            implements CheckpointedFunction, CheckpointListener {

        private boolean runtimeContextWasSetInInitializeState;
        private boolean initializeStateCalled;
        private boolean openCalled;
        private boolean snapshotStateCalled;
        private boolean closeCalled;
        private long completedCheckpointId = -1L;
        private long abortedCheckpointId = -1L;

        @Override
        public void initializeState(
                org.apache.flink.runtime.state.FunctionInitializationContext context) {
            runtimeContextWasSetInInitializeState = getRuntimeContext() != null;
            initializeStateCalled = true;
        }

        @Override
        public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext context) {
            snapshotStateCalled = true;
        }

        @Override
        public void open(Configuration parameters) {
            openCalled = true;
        }

        @Override
        public void close() {
            closeCalled = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            completedCheckpointId = checkpointId;
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
            abortedCheckpointId = checkpointId;
        }
    }

    private static final class ForwardingSinkFunction extends RichSinkFunction<Event> {

        private boolean runtimeContextWasSetInOpen;
        private boolean openCalled;
        private boolean closeCalled;
        private Event lastInvokedEvent;

        @Override
        public void open(Configuration parameters) {
            runtimeContextWasSetInOpen = getRuntimeContext() != null;
            openCalled = true;
        }

        @Override
        public void invoke(Event value, Context context) {
            lastInvokedEvent = value;
        }

        @Override
        public void close() {
            closeCalled = true;
        }
    }

    private static final class TestingFunctionInitializationContext
            implements FunctionInitializationContext {

        @Override
        public boolean isRestored() {
            return false;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public OperatorStateStore getOperatorStateStore() {
            return new TestingOperatorStateStore();
        }

        @Override
        public KeyedStateStore getKeyedStateStore() {
            return null;
        }
    }

    private static final class TestingOperatorStateStore implements OperatorStateStore {

        @Override
        public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K, V> BroadcastState<K, V> getBroadcastState(
                MapStateDescriptor<K, V> stateDescriptor) {
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

    private static DwsConnectionOptions minimalConnectionOptions() {
        return minimalConnectionOptions(null);
    }

    private static DwsConnectionOptions minimalConnectionOptions(DwsConfig config) {
        return DwsConnectionOptions.builder()
                .withUrl("jdbc:gaussdb://localhost:8000/test")
                .withUsername("user")
                .withPassword("password")
                .withConfig(config)
                .build();
    }

    private static DwsSinkFunction createSinkFunction(
            boolean caseSensitive,
            String defaultSchema,
            boolean enableAutoFlush,
            WriteMode writeMode) {
        return new DwsSinkFunction(
                minimalConnectionOptions(),
                ZoneId.of("UTC"),
                caseSensitive,
                defaultSchema,
                128,
                Duration.ofSeconds(5),
                enableAutoFlush,
                true,
                writeMode,
                new ForwardingSinkFunction());
    }

    private static Object readField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
