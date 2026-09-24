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

package org.apache.flink.cdc.runtime.operators.schema.distributed;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.pipeline.RouteMode;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.distributed.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.distributed.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.testutils.operators.MockedOperatorCoordinatorContext;
import org.apache.flink.cdc.runtime.testutils.schema.CollectingMetadataApplier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils.unwrap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SchemaCoordinator}. */
class SchemaCoordinatorTest {

    private static final TableId TABLE_ID = TableId.parse("db.schema_recovery");
    private static final Schema INITIAL_SCHEMA =
            Schema.newBuilder().physicalColumn("id", DataTypes.INT().notNull()).build();
    private static final AddColumnEvent ADD_EXTRA_V2 =
            new AddColumnEvent(
                    TABLE_ID,
                    Collections.singletonList(
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("extra_v2", DataTypes.STRING()),
                                    AddColumnEvent.ColumnPosition.LAST,
                                    null)));
    private static final Schema EVOLVED_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("extra_v2", DataTypes.STRING())
                    .build();

    @Test
    void resetsFailedSchemaEvolutionBeforeReplayingSchemaEvents() throws Exception {
        FailOnceMetadataApplier metadataApplier = new FailOnceMetadataApplier();
        ExecutorService coordinatorExecutor = Executors.newSingleThreadExecutor();
        SchemaCoordinator coordinator =
                new SchemaCoordinator(
                        "schema-coordinator",
                        new MockedOperatorCoordinatorContext(
                                new OperatorID(), Thread.currentThread().getContextClassLoader()),
                        coordinatorExecutor,
                        metadataApplier,
                        Collections.emptyList(),
                        RouteMode.ALL_MATCH,
                        SchemaChangeBehavior.LENIENT,
                        Duration.ofSeconds(10));
        coordinator.start();

        try {
            requestSchemaChange(coordinator, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));
            byte[] checkpoint = checkpoint(coordinator);

            CompletableFuture<CoordinationResponse> failedRequest =
                    submitSchemaChange(coordinator, ADD_EXTRA_V2);
            assertThat(metadataApplier.failedAdd.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> failedRequest.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);

            coordinator.resetToCheckpoint(1L, checkpoint);

            requestSchemaChange(coordinator, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));
            SchemaChangeResponse response = requestSchemaChange(coordinator, ADD_EXTRA_V2);

            assertThat(response.getEvolvedSchemas()).containsEntry(TABLE_ID, EVOLVED_SCHEMA);
            assertThat(response.getEvolvedSchemaChangeEvents()).containsExactly(ADD_EXTRA_V2);
            assertThat(metadataApplier.physicalSchemas).containsEntry(TABLE_ID, EVOLVED_SCHEMA);
        } finally {
            coordinator.close();
        }
    }

    @Test
    void nullCheckpointClearsSchemaStateBeforeReplay() throws Exception {
        FailOnceMetadataApplier metadataApplier = new FailOnceMetadataApplier(false);
        SchemaCoordinator coordinator = createCoordinator(metadataApplier, Duration.ofSeconds(10));
        coordinator.start();

        try {
            requestSchemaChange(coordinator, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));
            coordinator.resetToCheckpoint(1L, null);

            SchemaChangeResponse response =
                    requestSchemaChange(
                            coordinator, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));

            assertThat(response.getEvolvedSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));
            assertThat(metadataApplier.appliedEvents)
                    .containsExactly(
                            new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA),
                            new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));
        } finally {
            coordinator.close();
        }
    }

    @Test
    void restoresCheckpointBeforeStart() throws Exception {
        FailOnceMetadataApplier sourceApplier = new FailOnceMetadataApplier(false);
        SchemaCoordinator source = createCoordinator(sourceApplier, Duration.ofSeconds(10));
        source.start();
        byte[] checkpoint;
        try {
            requestSchemaChange(source, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));
            checkpoint = checkpoint(source);
        } finally {
            source.close();
        }

        SchemaCoordinator restored =
                createCoordinator(new FailOnceMetadataApplier(false), Duration.ofSeconds(10));
        restored.resetToCheckpoint(1L, checkpoint);
        restored.start();
        try {
            GetEvolvedSchemaResponse response =
                    unwrap(
                            restored.handleCoordinationRequest(
                                            new GetEvolvedSchemaRequest(
                                                    TABLE_ID,
                                                    GetEvolvedSchemaRequest.LATEST_SCHEMA_VERSION))
                                    .get(10, TimeUnit.SECONDS));

            assertThat(response.getSchema()).contains(INITIAL_SCHEMA);
        } finally {
            restored.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void resetWaitsForAnUninterruptibleSchemaChangeBeforeRetrying(boolean failLate)
            throws Exception {
        FailOnceMetadataApplier metadataApplier =
                new FailOnceMetadataApplier(false, true, failLate);
        CountingCoordinatorContext context = new CountingCoordinatorContext();
        SchemaCoordinator coordinator =
                createCoordinator(metadataApplier, context, Duration.ofMillis(200));
        coordinator.start();

        try {
            requestSchemaChange(coordinator, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));
            CompletableFuture<CoordinationResponse> oldRequest =
                    submitSchemaChange(coordinator, ADD_EXTRA_V2);
            assertThat(metadataApplier.blockedAdd.await(10, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() -> coordinator.resetToCheckpoint(1L, null))
                    .isInstanceOf(TimeoutException.class);
            CompletableFuture<CoordinationResponse> rejectedRequest =
                    coordinator.handleCoordinationRequest(
                            new GetEvolvedSchemaRequest(
                                    TABLE_ID, GetEvolvedSchemaRequest.LATEST_SCHEMA_VERSION));
            CompletableFuture<byte[]> rejectedCheckpoint = new CompletableFuture<>();
            coordinator.checkpointCoordinator(2L, rejectedCheckpoint);
            assertThatThrownBy(() -> rejectedRequest.get(1, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            assertThatThrownBy(() -> rejectedCheckpoint.get(1, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            assertThat(metadataApplier.appliedEvents).hasSize(1);

            metadataApplier.releaseAdd.countDown();
            coordinator.resetToCheckpoint(1L, null);
            if (failLate) {
                assertThatThrownBy(() -> oldRequest.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);
            } else {
                oldRequest.get(10, TimeUnit.SECONDS);
            }
            assertThat(context.failures.get()).isZero();

            requestSchemaChange(coordinator, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));
            requestSchemaChange(coordinator, ADD_EXTRA_V2);
        } finally {
            metadataApplier.releaseAdd.countDown();
            coordinator.close();
        }
    }

    @Test
    void rejectsQueuedRequestAndCheckpointAcrossReset() throws Exception {
        CountDownLatch releaseExecutor = new CountDownLatch(1);
        CountingExecutor coordinatorExecutor = new CountingExecutor(releaseExecutor);
        FailOnceMetadataApplier metadataApplier = new FailOnceMetadataApplier(false);
        SchemaCoordinator coordinator =
                new SchemaCoordinator(
                        "schema-coordinator",
                        new CountingCoordinatorContext(),
                        coordinatorExecutor,
                        metadataApplier,
                        Collections.emptyList(),
                        RouteMode.ALL_MATCH,
                        SchemaChangeBehavior.LENIENT,
                        Duration.ofSeconds(2));
        AtomicReference<Throwable> resetFailure = new AtomicReference<>();
        SchemaCoordinator resettingCoordinator = coordinator;
        Thread resetThread =
                new Thread(
                        () -> {
                            try {
                                resettingCoordinator.resetToCheckpoint(1L, null);
                            } catch (Throwable t) {
                                resetFailure.set(t);
                            }
                        });
        try {
            coordinator.start();
            CompletableFuture<CoordinationResponse> request =
                    coordinator.handleCoordinationRequest(
                            new SchemaChangeRequest(
                                    0, 0, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA)));
            CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
            coordinator.checkpointCoordinator(1L, checkpoint);
            resetThread.start();
            assertThat(coordinatorExecutor.barrierSubmitted.await(10, TimeUnit.SECONDS)).isTrue();
            releaseExecutor.countDown();
            resetThread.join(10_000);
            assertThat(resetThread.isAlive()).isFalse();
            assertThat(resetFailure.get()).isNull();
            assertThatThrownBy(() -> request.get(1, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            assertThatThrownBy(() -> checkpoint.get(1, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            assertThat(metadataApplier.appliedEvents).isEmpty();
        } finally {
            releaseExecutor.countDown();
            resetThread.join(10_000);
            coordinator.close();
        }
    }

    @Test
    void testIgnoreSubtaskResetWithoutOngoingSchemaEvolution() throws Exception {
        MockedOperatorCoordinatorContext context = createContext();
        SchemaCoordinator coordinator = createCoordinator(context);
        coordinator.start();

        try {
            coordinator.executionAttemptFailed(
                    0, 0, new RuntimeException("failure outside schema evolution"));
            coordinator.subtaskReset(0, 123L);

            Assertions.assertThat(context.isJobFailed()).isFalse();
        } finally {
            coordinator.close();
        }
    }

    @Test
    void testDoNotEscalateSubtaskResetWhenFailureReasonIsMissing() throws Exception {
        MockedOperatorCoordinatorContext context = createContext();
        SchemaCoordinator coordinator = createCoordinator(context);
        coordinator.start();

        try {
            CompletableFuture<CoordinationResponse> requestFuture =
                    coordinator.handleCoordinationRequest(createSchemaChangeRequest());
            waitUntil(coordinator::isSchemaEvolutionInProgress);

            coordinator.subtaskReset(0, 123L);
            coordinator.subtaskReset(0, 124L);

            Assertions.assertThat(context.isJobFailed()).isFalse();
            Assertions.assertThat(requestFuture).isNotDone();
        } finally {
            coordinator.close();
        }
    }

    @Test
    void testPreserveFailureFromAnotherSubtaskWithoutEscalatingReset() throws Exception {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(
                        new OperatorID(), 2, Thread.currentThread().getContextClassLoader());
        SchemaCoordinator coordinator = createCoordinator(context);
        coordinator.start();

        try {
            CompletableFuture<CoordinationResponse> requestFromSubtaskZero =
                    coordinator.handleCoordinationRequest(createSchemaChangeRequest(0));
            coordinator.handleCoordinationRequest(createSchemaChangeRequest(1));
            waitUntil(coordinator::isSchemaEvolutionInProgress);

            RuntimeException originalFailure = new RuntimeException("other subtask failure");
            coordinator.executionAttemptFailed(1, 0, originalFailure);
            coordinator.subtaskReset(1, 123L);

            Assertions.assertThat(context.isJobFailed()).isFalse();
            coordinator.executionAttemptReady(1, 1, null);
            CompletableFuture<CoordinationResponse> retriedRequest =
                    coordinator.handleCoordinationRequest(createSchemaChangeRequest(0));

            waitUntil(context::isJobFailed);
            Assertions.assertThat(context.getJobFailureReason()).isSameAs(originalFailure);
            Assertions.assertThatThrownBy(() -> retriedRequest.get(5, TimeUnit.SECONDS))
                    .hasCause(originalFailure);
            assertUnexpectedEvolvingStatusSuppressed(originalFailure);
            Assertions.assertThat(requestFromSubtaskZero).isNotDone();
        } finally {
            coordinator.close();
        }
    }

    @Test
    void testPreserveOriginalFailureDuringPartialFailover() throws Exception {
        MockedOperatorCoordinatorContext context = createContext();
        SchemaCoordinator coordinator = createCoordinator(context);
        coordinator.start();

        try {
            SchemaChangeRequest request = createSchemaChangeRequest();

            CompletableFuture<CoordinationResponse> firstRequest =
                    coordinator.handleCoordinationRequest(request);
            waitUntil(coordinator::isSchemaEvolutionInProgress);

            RuntimeException originalFailure = new RuntimeException("original task failure");
            coordinator.executionAttemptFailed(0, 0, originalFailure);
            coordinator.subtaskReset(0, 123L);

            Assertions.assertThat(context.isJobFailed()).isFalse();
            coordinator.executionAttemptReady(0, 1, null);
            CompletableFuture<CoordinationResponse> retriedRequest =
                    coordinator.handleCoordinationRequest(request);

            waitUntil(context::isJobFailed);
            Assertions.assertThat(context.getFailureCause()).isSameAs(originalFailure);
            Assertions.assertThatThrownBy(() -> retriedRequest.get(5, TimeUnit.SECONDS))
                    .hasCause(originalFailure);
            assertUnexpectedEvolvingStatusSuppressed(originalFailure);
            Assertions.assertThat(firstRequest).isNotDone();
        } finally {
            coordinator.close();
        }
    }

    @Test
    void testPreserveFirstFailureAcrossMultipleAttempts() throws Exception {
        MockedOperatorCoordinatorContext context = createContext();
        SchemaCoordinator coordinator = createCoordinator(context);
        coordinator.start();

        try {
            SchemaChangeRequest request = createSchemaChangeRequest();

            CompletableFuture<CoordinationResponse> firstRequest =
                    coordinator.handleCoordinationRequest(request);
            waitUntil(coordinator::isSchemaEvolutionInProgress);

            RuntimeException firstFailure = new RuntimeException("first attempt failure");
            coordinator.executionAttemptFailed(0, 0, firstFailure);
            coordinator.subtaskReset(0, 123L);

            RuntimeException secondFailure = new RuntimeException("second attempt failure");
            coordinator.executionAttemptFailed(0, 1, secondFailure);
            coordinator.subtaskReset(0, 124L);

            coordinator.executionAttemptReady(0, 2, null);
            CompletableFuture<CoordinationResponse> retriedRequest =
                    coordinator.handleCoordinationRequest(request);

            waitUntil(context::isJobFailed);
            Assertions.assertThat(context.getFailureCause()).isSameAs(firstFailure);
            Assertions.assertThatThrownBy(() -> retriedRequest.get(5, TimeUnit.SECONDS))
                    .hasCause(firstFailure);
            assertUnexpectedEvolvingStatusSuppressed(firstFailure);
            Assertions.assertThat(firstRequest).isNotDone();
        } finally {
            coordinator.close();
        }
    }

    @Test
    void testRetainStateErrorWhenOriginalFailureIsMissing() throws Exception {
        MockedOperatorCoordinatorContext context = createContext();
        SchemaCoordinator coordinator = createCoordinator(context);
        coordinator.start();

        try {
            SchemaChangeRequest request = createSchemaChangeRequest();
            coordinator.handleCoordinationRequest(request);
            waitUntil(coordinator::isSchemaEvolutionInProgress);

            coordinator.handleCoordinationRequest(request);

            waitUntil(context::isJobFailed);
            Assertions.assertThat(context.getFailureCause())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Unexpected evolving status: EVOLVING");
        } finally {
            coordinator.close();
        }
    }

    @Test
    void testClearStaleFailureWhenStartingNewSchemaEvolution() throws Exception {
        MockedOperatorCoordinatorContext context = createContext();
        SchemaCoordinator coordinator = createCoordinator(context);
        coordinator.start();

        try {
            RuntimeException staleFailure = new RuntimeException("stale task failure");
            coordinator.executionAttemptFailed(0, 0, staleFailure);

            SchemaChangeRequest request = createSchemaChangeRequest();
            coordinator.handleCoordinationRequest(request);
            waitUntil(coordinator::isSchemaEvolutionInProgress);

            coordinator.handleCoordinationRequest(request);

            waitUntil(context::isJobFailed);
            Assertions.assertThat(context.getFailureCause())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Unexpected evolving status: EVOLVING")
                    .isNotSameAs(staleFailure);
            Assertions.assertThat(staleFailure.getSuppressed()).isEmpty();
        } finally {
            coordinator.close();
        }
    }

    private static SchemaCoordinator createCoordinator(
            FailOnceMetadataApplier metadataApplier, Duration rpcTimeout) {
        return createCoordinator(
                metadataApplier,
                new MockedOperatorCoordinatorContext(
                        new OperatorID(), Thread.currentThread().getContextClassLoader()),
                rpcTimeout);
    }

    private static SchemaCoordinator createCoordinator(
            FailOnceMetadataApplier metadataApplier,
            OperatorCoordinator.Context context,
            Duration rpcTimeout) {
        return new SchemaCoordinator(
                "schema-coordinator",
                context,
                Executors.newSingleThreadExecutor(),
                metadataApplier,
                Collections.emptyList(),
                RouteMode.ALL_MATCH,
                SchemaChangeBehavior.LENIENT,
                rpcTimeout);
    }

    private static byte[] checkpoint(SchemaCoordinator coordinator) throws Exception {
        CompletableFuture<byte[]> checkpoint = new CompletableFuture<>();
        coordinator.checkpointCoordinator(1L, checkpoint);
        return checkpoint.get(10, TimeUnit.SECONDS);
    }

    private static SchemaChangeResponse requestSchemaChange(
            SchemaCoordinator coordinator, SchemaChangeEvent event) throws Exception {
        return unwrap(submitSchemaChange(coordinator, event).get(10, TimeUnit.SECONDS));
    }

    private static CompletableFuture<CoordinationResponse> submitSchemaChange(
            SchemaCoordinator coordinator, SchemaChangeEvent event) {
        CompletableFuture<CoordinationResponse> response =
                coordinator.handleCoordinationRequest(new SchemaChangeRequest(0, 0, event));
        coordinator.handleEventFromOperator(0, 0, new FlushSuccessEvent(0, 0));
        return response;
    }

    private static MockedOperatorCoordinatorContext createContext() {
        return new MockedOperatorCoordinatorContext(
                new OperatorID(), Thread.currentThread().getContextClassLoader());
    }

    private static SchemaCoordinator createCoordinator(OperatorCoordinator.Context context) {
        return new SchemaCoordinator(
                "SchemaCoordinator",
                context,
                Executors.newSingleThreadExecutor(),
                new CollectingMetadataApplier(Duration.ZERO),
                Collections.emptyList(),
                RouteMode.ALL_MATCH,
                SchemaChangeBehavior.LENIENT,
                Duration.ofMinutes(1));
    }

    private static SchemaChangeRequest createSchemaChangeRequest() {
        return createSchemaChangeRequest(0);
    }

    private static SchemaChangeRequest createSchemaChangeRequest(int sinkSubTaskId) {
        TableId tableId = TableId.tableId("inventory", "products");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        return new SchemaChangeRequest(0, sinkSubTaskId, new CreateTableEvent(tableId, schema));
    }

    private static void assertUnexpectedEvolvingStatusSuppressed(Throwable originalFailure) {
        Assertions.assertThat(originalFailure.getSuppressed()).hasSize(1);
        Assertions.assertThat(originalFailure.getSuppressed()[0])
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected evolving status: EVOLVING");
    }

    private static void waitUntil(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Condition was not met within the timeout.");
            }
            Thread.sleep(10);
        }
    }

    private static final class FailOnceMetadataApplier implements MetadataApplier {
        private final AtomicBoolean failNextAdd;
        private final CountDownLatch failedAdd = new CountDownLatch(1);
        private final CountDownLatch blockedAdd = new CountDownLatch(1);
        private final CountDownLatch releaseAdd = new CountDownLatch(1);
        private final Map<TableId, Schema> physicalSchemas = new ConcurrentHashMap<>();
        private final List<SchemaChangeEvent> appliedEvents = new ArrayList<>();
        private final boolean blockAdd;
        private final AtomicBoolean failLate;

        private FailOnceMetadataApplier() {
            this(true);
        }

        private FailOnceMetadataApplier(boolean failNextAdd) {
            this(failNextAdd, false);
        }

        private FailOnceMetadataApplier(boolean failNextAdd, boolean blockAdd) {
            this(failNextAdd, blockAdd, false);
        }

        private FailOnceMetadataApplier(boolean failNextAdd, boolean blockAdd, boolean failLate) {
            this.failNextAdd = new AtomicBoolean(failNextAdd);
            this.blockAdd = blockAdd;
            this.failLate = new AtomicBoolean(failLate);
        }

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
            if (schemaChangeEvent instanceof AddColumnEvent
                    && failNextAdd.compareAndSet(true, false)) {
                failedAdd.countDown();
                throw new SchemaEvolveException(schemaChangeEvent, "Injected metadata failure");
            }
            if (schemaChangeEvent instanceof AddColumnEvent && blockAdd) {
                blockedAdd.countDown();
                while (releaseAdd.getCount() > 0) {
                    try {
                        releaseAdd.await();
                    } catch (InterruptedException ignored) {
                        // Simulates an external DDL call that ignores interruption.
                    }
                }
            }
            if (schemaChangeEvent instanceof AddColumnEvent
                    && failLate.compareAndSet(true, false)) {
                throw new SchemaEvolveException(
                        schemaChangeEvent, "Injected late metadata failure");
            }
            physicalSchemas.put(
                    schemaChangeEvent.tableId(),
                    SchemaUtils.applySchemaChangeEvent(
                            physicalSchemas.get(schemaChangeEvent.tableId()), schemaChangeEvent));
            appliedEvents.add(schemaChangeEvent);
        }
    }

    private static final class CountingCoordinatorContext extends MockedOperatorCoordinatorContext {
        private final AtomicInteger failures = new AtomicInteger();

        private CountingCoordinatorContext() {
            super(new OperatorID(), Thread.currentThread().getContextClassLoader());
        }

        @Override
        public void failJob(Throwable cause) {
            failures.incrementAndGet();
            super.failJob(cause);
        }
    }

    private static final class CountingExecutor extends ThreadPoolExecutor {
        private final AtomicInteger submissions = new AtomicInteger();
        private final CountDownLatch barrierSubmitted = new CountDownLatch(1);

        private CountingExecutor(CountDownLatch release) {
            super(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            execute(
                    () -> {
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
        }

        @Override
        public void execute(Runnable command) {
            if (submissions.incrementAndGet() == 4) {
                barrierSubmitted.countDown();
            }
            super.execute(command);
        }
    }
}
