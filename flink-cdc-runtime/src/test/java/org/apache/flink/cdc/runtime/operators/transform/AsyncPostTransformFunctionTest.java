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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AsyncPostTransformFunction}. */
class AsyncPostTransformFunctionTest {

    private static final TableId TABLE_ID = TableId.tableId("ns", "schema", "customers");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();
    private static final Schema SCHEMA_AFTER_ADD_COLUMN =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("region", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @AfterEach
    void releaseBlockingInvocation() {
        BlockingFunction.releaseFirstInvocation();
    }

    @Test
    void testDataChangesExecuteConcurrentlyAndEmitInOrder() throws Exception {
        BlockingFunction.reset(true);
        try (OneInputStreamOperatorTestHarness<Event, Event> harness =
                createHarness(TABLE_ID.identifier(), "*, block(id) AS blocked", 10_000L, 2)) {
            harness.setup(EventSerializer.INSTANCE);
            harness.open();
            harness.processElement(new StreamRecord<>(new CreateTableEvent(TABLE_ID, SCHEMA)));
            waitUntilOutputSize(harness, 1);

            DataChangeEvent first = insert(SCHEMA, 1, "Alice");
            DataChangeEvent second = insert(SCHEMA, 2, "Bob");
            harness.processElement(new StreamRecord<>(first));
            assertThat(BlockingFunction.awaitFirstInvocation()).isTrue();
            harness.processElement(new StreamRecord<>(second));

            assertThat(BlockingFunction.awaitSecondInvocation()).isTrue();
            drainMailbox(harness);
            assertThat(harness.getOutput()).hasSize(1);

            BlockingFunction.releaseFirstInvocation();
            waitUntilOutputSize(harness, 3);

            Schema outputSchema =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT().notNull())
                            .physicalColumn("name", DataTypes.STRING())
                            .physicalColumn("blocked", DataTypes.INT())
                            .primaryKey("id")
                            .build();
            assertThat(harness.extractOutputValues())
                    .containsExactly(
                            new CreateTableEvent(TABLE_ID, outputSchema),
                            insert(outputSchema, 1, "Alice", 1),
                            insert(outputSchema, 2, "Bob", 2));
        }
    }

    @Test
    void testSchemaChangeWaitsForPreviousDataAndBlocksFollowingData() throws Exception {
        BlockingFunction.reset(true);
        try (OneInputStreamOperatorTestHarness<Event, Event> harness =
                createHarness(TABLE_ID.identifier(), "*, block(id) AS blocked", 10_000L, 2)) {
            harness.setup(EventSerializer.INSTANCE);
            harness.open();
            harness.processElement(new StreamRecord<>(new CreateTableEvent(TABLE_ID, SCHEMA)));
            waitUntilOutputSize(harness, 1);

            harness.processElement(new StreamRecord<>(insert(SCHEMA, 1, "Alice")));
            assertThat(BlockingFunction.awaitFirstInvocation()).isTrue();
            AddColumnEvent addColumnEvent = addRegionColumnEvent();
            harness.processElement(new StreamRecord<>(addColumnEvent));
            harness.processElement(
                    new StreamRecord<>(insert(SCHEMA_AFTER_ADD_COLUMN, 2, "Bob", "Berlin")));

            assertThat(BlockingFunction.awaitSecondInvocation(Duration.ofMillis(100))).isFalse();
            BlockingFunction.releaseFirstInvocation();
            assertThat(BlockingFunction.awaitSecondInvocation()).isTrue();
            waitUntilOutputSize(harness, 4);

            assertThat(harness.extractOutputValues().get(1)).isInstanceOf(DataChangeEvent.class);
            assertThat(harness.extractOutputValues().get(2))
                    .isEqualTo(
                            new AddColumnEvent(
                                    TABLE_ID,
                                    Collections.singletonList(
                                            AddColumnEvent.after(
                                                    Column.physicalColumn(
                                                            "region", DataTypes.STRING()),
                                                    "blocked"))));
            assertThat(harness.extractOutputValues().get(3)).isInstanceOf(DataChangeEvent.class);
        }
    }

    @Test
    void testTimeoutIsPropagated() throws Exception {
        BlockingFunction.reset(true);
        try (OneInputStreamOperatorTestHarness<Event, Event> harness =
                createHarness(TABLE_ID.identifier(), "*, block(id) AS blocked", 50L, 1)) {
            harness.setup(EventSerializer.INSTANCE);
            harness.getEnvironment().setExpectedExternalFailureCause(Throwable.class);
            harness.open();
            harness.processElement(new StreamRecord<>(new CreateTableEvent(TABLE_ID, SCHEMA)));
            waitUntilOutputSize(harness, 1);
            DataChangeEvent event = insert(SCHEMA, 1, "Alice");
            harness.processElement(new StreamRecord<>(event));
            assertThat(BlockingFunction.awaitFirstInvocation()).isTrue();

            harness.setProcessingTime(100L);
            assertThat(waitUntilExternalFailure(harness))
                    .rootCause()
                    .hasMessageContaining("Async post-transform timed out for event");
            BlockingFunction.releaseFirstInvocation();
        }
    }

    @Test
    void testTransformExceptionIsPropagated() throws Exception {
        try (OneInputStreamOperatorTestHarness<Event, Event> harness =
                createHarness(TABLE_ID.identifier(), "*, fail(id) AS failed", 10_000L, 1)) {
            harness.setup(EventSerializer.INSTANCE);
            harness.getEnvironment().setExpectedExternalFailureCause(Throwable.class);
            harness.open();
            harness.processElement(new StreamRecord<>(new CreateTableEvent(TABLE_ID, SCHEMA)));
            waitUntilOutputSize(harness, 1);
            harness.processElement(new StreamRecord<>(insert(SCHEMA, 1, "Alice")));

            assertThat(waitUntilExternalFailure(harness))
                    .rootCause()
                    .hasMessage("expected transform failure");
        }
    }

    @Test
    void testCheckpointRestoreEmitsLatestCreateTableEventOnlyOnce() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        AddColumnEvent addColumnEvent = addRegionColumnEvent();

        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<Event, Event> harness = createHarness()) {
            harness.setup(EventSerializer.INSTANCE);
            harness.open();
            harness.processElement(new StreamRecord<>(createTableEvent));
            waitUntilOutputSize(harness, 1);
            harness.processElement(new StreamRecord<>(addColumnEvent));
            waitUntilOutputSize(harness, 2);
            snapshot = snapshot(harness, 1L, 1L);
        }

        try (OneInputStreamOperatorTestHarness<Event, Event> restoredHarness = createHarness()) {
            restoredHarness.setup(EventSerializer.INSTANCE);
            restoredHarness.initializeState(snapshot);
            restoredHarness.open();
            DataChangeEvent first = insert(SCHEMA_AFTER_ADD_COLUMN, 1, "Alice", "Paris");
            DataChangeEvent second = insert(SCHEMA_AFTER_ADD_COLUMN, 2, "Bob", "Berlin");
            restoredHarness.processElement(new StreamRecord<>(first));
            restoredHarness.processElement(new StreamRecord<>(second));
            waitUntilOutputSize(restoredHarness, 3);

            assertThat(restoredHarness.extractOutputValues())
                    .containsExactly(
                            new CreateTableEvent(TABLE_ID, SCHEMA_AFTER_ADD_COLUMN), first, second);
        }
    }

    @Test
    void testCheckpointRestoreEmitsPassthroughCreateTableEventOnlyOnce() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);

        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<Event, Event> harness =
                createHarness("not_matching_table", "*", 10_000L, 2)) {
            harness.setup(EventSerializer.INSTANCE);
            harness.open();
            harness.processElement(new StreamRecord<>(createTableEvent));
            waitUntilOutputSize(harness, 1);
            snapshot = snapshot(harness, 1L, 1L);
        }

        try (OneInputStreamOperatorTestHarness<Event, Event> restoredHarness =
                createHarness("not_matching_table", "*", 10_000L, 2)) {
            restoredHarness.setup(EventSerializer.INSTANCE);
            restoredHarness.initializeState(snapshot);
            restoredHarness.open();
            DataChangeEvent first = insert(SCHEMA, 1, "Alice");
            DataChangeEvent second = insert(SCHEMA, 2, "Bob");
            restoredHarness.processElement(new StreamRecord<>(first));
            restoredHarness.processElement(new StreamRecord<>(second));
            waitUntilOutputSize(restoredHarness, 3);

            assertThat(restoredHarness.extractOutputValues())
                    .containsExactly(createTableEvent, first, second);
        }
    }

    @Test
    void testCheckpointAfterCompletedSchemaChangeRestoresConsistently() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        AddColumnEvent addColumnEvent = addRegionColumnEvent();

        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<Event, Event> harness = createHarness()) {
            harness.setup(EventSerializer.INSTANCE);
            harness.open();
            harness.processElement(new StreamRecord<>(createTableEvent));
            waitUntilOutputSize(harness, 1);

            // The schema change has completed in the async function, but its mailbox result has
            // not been emitted yet when the checkpoint starts.
            harness.processElement(new StreamRecord<>(addColumnEvent));
            snapshot = snapshot(harness, 1L, 1L);

            assertThat(harness.extractOutputValues())
                    .containsExactly(
                            createTableEvent,
                            new AddColumnEvent(
                                    TABLE_ID,
                                    Collections.singletonList(
                                            AddColumnEvent.after(
                                                    Column.physicalColumn(
                                                            "region", DataTypes.STRING()),
                                                    "name"))));
        }

        try (OneInputStreamOperatorTestHarness<Event, Event> restoredHarness = createHarness()) {
            restoredHarness.setup(EventSerializer.INSTANCE);
            restoredHarness.initializeState(snapshot);
            restoredHarness.open();
            DataChangeEvent dataEvent = insert(SCHEMA_AFTER_ADD_COLUMN, 1, "Alice", "Paris");
            restoredHarness.processElement(new StreamRecord<>(dataEvent));
            waitUntilOutputSize(restoredHarness, 2);

            assertThat(restoredHarness.extractOutputValues())
                    .containsExactly(
                            new CreateTableEvent(TABLE_ID, SCHEMA_AFTER_ADD_COLUMN), dataEvent);
        }
    }

    @Test
    void testSameParallelismSavepointWaitsForInFlightEventsAndRestoresState() throws Exception {
        BlockingFunction.reset(true);
        OperatorSubtaskState savepoint;
        DataChangeEvent first = insert(SCHEMA, 1, "Alice");
        DataChangeEvent second = insert(SCHEMA, 2, "Bob");
        try (OneInputStreamOperatorTestHarness<Event, Event> harness =
                createHarness(TABLE_ID.identifier(), "*, block(id) AS blocked", 10_000L, 2)) {
            harness.setup(EventSerializer.INSTANCE);
            harness.open();
            harness.processElement(new StreamRecord<>(new CreateTableEvent(TABLE_ID, SCHEMA)));
            waitUntilOutputSize(harness, 1);
            harness.processElement(new StreamRecord<>(first));
            assertThat(BlockingFunction.awaitFirstInvocation()).isTrue();
            harness.processElement(new StreamRecord<>(second));
            assertThat(BlockingFunction.awaitSecondInvocation()).isTrue();

            CompletableFuture<Void> releaseFuture =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    Thread.sleep(100L);
                                    BlockingFunction.releaseFirstInvocation();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }
                            });
            savepoint = snapshot(harness, 2L, 2L);
            releaseFuture.get(10, TimeUnit.SECONDS);
            waitUntilOutputSize(harness, 3);
        }

        BlockingFunction.reset(false);
        try (OneInputStreamOperatorTestHarness<Event, Event> restoredHarness =
                createHarness(TABLE_ID.identifier(), "*, block(id) AS blocked", 10_000L, 2)) {
            restoredHarness.setup(EventSerializer.INSTANCE);
            restoredHarness.initializeState(savepoint);
            restoredHarness.open();

            Schema outputSchema =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT().notNull())
                            .physicalColumn("name", DataTypes.STRING())
                            .physicalColumn("blocked", DataTypes.INT())
                            .primaryKey("id")
                            .build();
            DataChangeEvent third = insert(SCHEMA, 3, "Carol");
            restoredHarness.processElement(new StreamRecord<>(third));
            waitUntilOutputSize(restoredHarness, 2);
            assertThat(restoredHarness.extractOutputValues())
                    .containsExactly(
                            new CreateTableEvent(TABLE_ID, outputSchema),
                            insert(outputSchema, 3, "Carol", 3));
        }
    }

    private OneInputStreamOperatorTestHarness<Event, Event> createHarness() throws Exception {
        return createHarness(TABLE_ID.identifier(), "*", 10_000L, 2);
    }

    private OneInputStreamOperatorTestHarness<Event, Event> createHarness(
            String tableInclusion, String projection, long timeout, int workerThreads)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new AsyncPostTransformOperatorFactory(
                        createFunction(tableInclusion, projection, workerThreads), timeout, 10),
                EventSerializer.INSTANCE);
    }

    private AsyncPostTransformFunction createFunction(
            String tableInclusion, String projection, int workerThreads) {
        AsyncPostTransformFunctionBuilder builder =
                AsyncPostTransformFunction.newBuilder()
                        .addTransform(
                                tableInclusion,
                                projection,
                                null,
                                null,
                                null,
                                null,
                                null,
                                new SupportedMetadataColumn[0])
                        .addTimezone("UTC")
                        .addAsyncWorkerThreads(workerThreads);
        if (projection.contains("block(")) {
            builder.addUdfFunctions(
                    Collections.singletonList(
                            Tuple3.of(
                                    "block",
                                    BlockingFunction.class.getName(),
                                    Collections.emptyMap())));
        } else if (projection.contains("fail(")) {
            builder.addUdfFunctions(
                    Collections.singletonList(
                            Tuple3.of(
                                    "fail",
                                    FailingFunction.class.getName(),
                                    Collections.emptyMap())));
        }
        return builder.build();
    }

    private static AddColumnEvent addRegionColumnEvent() {
        return new AddColumnEvent(
                TABLE_ID,
                Collections.singletonList(
                        AddColumnEvent.last(Column.physicalColumn("region", DataTypes.STRING()))));
    }

    private static DataChangeEvent insert(Schema schema, Object... values) {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator((RowType) schema.toRowDataType());
        Object[] binaryValues = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            binaryValues[i] =
                    values[i] instanceof String
                            ? BinaryStringData.fromString((String) values[i])
                            : values[i];
        }
        BinaryRecordData record = generator.generate(binaryValues);
        return DataChangeEvent.insertEvent(TABLE_ID, record);
    }

    private static void waitUntilOutputSize(
            OneInputStreamOperatorTestHarness<Event, Event> harness, int expectedSize)
            throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            drainMailbox(harness);
            if (harness.getOutput().size() >= expectedSize) {
                return;
            }
            Thread.sleep(10L);
        }
        assertThat(harness.getOutput()).hasSize(expectedSize);
    }

    private static OperatorSubtaskState snapshot(
            OneInputStreamOperatorTestHarness<Event, Event> harness,
            long checkpointId,
            long timestamp)
            throws Exception {
        harness.getOperator().prepareSnapshotPreBarrier(checkpointId);
        return harness.snapshot(checkpointId, timestamp);
    }

    private static Throwable waitUntilExternalFailure(
            OneInputStreamOperatorTestHarness<Event, Event> harness) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        Optional<? extends Throwable> failure;
        while (System.nanoTime() < deadline) {
            drainMailbox(harness);
            failure = harness.getEnvironment().getActualExternalFailureCause();
            if (failure.isPresent()) {
                return failure.get();
            }
            Thread.sleep(10L);
        }
        failure = harness.getEnvironment().getActualExternalFailureCause();
        assertThat(failure).isPresent();
        return failure.get();
    }

    private static void drainMailbox(OneInputStreamOperatorTestHarness<Event, Event> harness)
            throws Exception {
        while (true) {
            Mail mail = harness.getTaskMailbox().tryTake(TaskMailbox.MIN_PRIORITY).orElse(null);
            if (mail == null) {
                return;
            }
            mail.run();
        }
    }

    /** Test UDF that allows assertions about worker execution order. */
    public static class BlockingFunction implements UserDefinedFunction {

        private static volatile boolean blockFirst;
        private static volatile CountDownLatch firstInvocationStarted = new CountDownLatch(1);
        private static volatile CountDownLatch secondInvocationStarted = new CountDownLatch(1);
        private static volatile CountDownLatch firstInvocationRelease = new CountDownLatch(1);

        public Integer eval(Integer value) {
            if (value == 1) {
                firstInvocationStarted.countDown();
                if (blockFirst) {
                    try {
                        firstInvocationRelease.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            } else if (value == 2) {
                secondInvocationStarted.countDown();
            }
            return value;
        }

        private static void reset(boolean shouldBlockFirst) {
            blockFirst = shouldBlockFirst;
            firstInvocationStarted = new CountDownLatch(1);
            secondInvocationStarted = new CountDownLatch(1);
            firstInvocationRelease = new CountDownLatch(1);
        }

        private static boolean awaitFirstInvocation() throws InterruptedException {
            return firstInvocationStarted.await(10, TimeUnit.SECONDS);
        }

        private static boolean awaitSecondInvocation() throws InterruptedException {
            return awaitSecondInvocation(Duration.ofSeconds(10));
        }

        private static boolean awaitSecondInvocation(Duration timeout) throws InterruptedException {
            return secondInvocationStarted.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        private static void releaseFirstInvocation() {
            firstInvocationRelease.countDown();
        }
    }

    /** Test UDF that always fails. */
    public static class FailingFunction implements UserDefinedFunction {

        public Integer eval(Integer value) {
            throw new IllegalStateException("expected transform failure");
        }
    }
}
