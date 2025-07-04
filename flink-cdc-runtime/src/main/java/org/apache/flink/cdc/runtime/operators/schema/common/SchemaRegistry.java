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

package org.apache.flink.cdc.runtime.operators.schema.common;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.common.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetOriginalSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetOriginalSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.common.event.SinkWriterRegisterEvent;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import static org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils.wrap;

/**
 * An abstract centralized schema registry that accepts requests from {@link SchemaEvolutionClient}.
 * A legit schema registry should be able to:
 *
 * <ul>
 *   <li>Handle schema retrieval requests from {@link SchemaEvolutionClient}s
 *   <li>Accept and trace sink writers' registering events
 *   <li>Snapshot & Restore its state during checkpoints
 * </ul>
 *
 * <br>
 * These abilities are done by overriding abstract methods of {@link SchemaRegistry}. All methods
 * will run in given {@link ExecutorService} asynchronously except {@code SchemaRegistry#restore}.
 */
@Internal
public abstract class SchemaRegistry implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistry.class);

    // -------------------------
    // Static fields that got bind after construction
    // -------------------------
    protected final OperatorCoordinator.Context context;
    protected final String operatorName;
    protected final ExecutorService coordinatorExecutor;
    protected final MetadataApplier metadataApplier;
    protected final Duration rpcTimeout;
    protected final List<RouteRule> routingRules;
    protected final SchemaChangeBehavior behavior;

    // -------------------------
    // Dynamically initialized transient fields (after coordinator starts)
    // -------------------------
    protected transient int currentParallelism;
    protected transient Set<Integer> activeSinkWriters;
    protected transient Map<Integer, Throwable> failedReasons;
    protected transient SchemaManager schemaManager;
    protected transient TableIdRouter router;

    protected SchemaRegistry(
            OperatorCoordinator.Context context,
            String operatorName,
            ExecutorService coordinatorExecutor,
            MetadataApplier metadataApplier,
            List<RouteRule> routingRules,
            SchemaChangeBehavior schemaChangeBehavior,
            Duration rpcTimeout) {
        this.context = context;
        this.operatorName = operatorName;
        this.coordinatorExecutor = coordinatorExecutor;
        this.metadataApplier = metadataApplier;
        this.routingRules = routingRules;
        this.rpcTimeout = rpcTimeout;
        this.behavior = schemaChangeBehavior;
    }

    // ---------------
    // Lifecycle hooks
    // ---------------
    @Override
    public void start() throws Exception {
        LOG.info("Starting SchemaRegistry - {}.", operatorName);
        this.currentParallelism = context.currentParallelism();
        this.activeSinkWriters = ConcurrentHashMap.newKeySet();
        this.failedReasons = new ConcurrentHashMap<>();
        if (this.schemaManager == null) {
            this.schemaManager = new SchemaManager();
        }
        this.router = new TableIdRouter(routingRules);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing SchemaRegistry - {}.", operatorName);
        coordinatorExecutor.shutdown();
        try {
            metadataApplier.close();
        } catch (Exception e) {
            LOG.error("Failed to close metadata applier.", e);
            throw new IOException("Failed to close metadata applier.", e);
        }
    }

    // ------------------------------
    // Overridable checkpoint methods
    // ------------------------------
    /** Snapshot current schema registry state in byte array form. */
    protected abstract void snapshot(CompletableFuture<byte[]> resultFuture) throws Exception;

    /** Restore schema registry state from byte array. */
    protected abstract void restore(byte[] checkpointData) throws Exception;

    // ------------------------------------
    // Overridable event & request handlers
    // ------------------------------------

    /** Overridable handler for {@link SinkWriterRegisterEvent}s. */
    protected void handleSinkWriterRegisterEvent(SinkWriterRegisterEvent event) throws Exception {
        LOG.info("Sink subtask {} already registered.", event.getSubtask());
        activeSinkWriters.add(event.getSubtask());
    }

    /** Overridable handler for {@link FlushSuccessEvent}s. */
    protected abstract void handleFlushSuccessEvent(FlushSuccessEvent event) throws Exception;

    /** Overridable handler for {@link GetEvolvedSchemaRequest}s. */
    protected void handleGetEvolvedSchemaRequest(
            GetEvolvedSchemaRequest request, CompletableFuture<CoordinationResponse> responseFuture)
            throws Exception {
        LOG.info("Handling evolved schema request: {}", request);
        int schemaVersion = request.getSchemaVersion();
        TableId tableId = request.getTableId();
        if (schemaVersion == GetEvolvedSchemaRequest.LATEST_SCHEMA_VERSION) {
            responseFuture.complete(
                    wrap(
                            new GetEvolvedSchemaResponse(
                                    schemaManager.getLatestEvolvedSchema(tableId).orElse(null))));
        } else {
            try {
                responseFuture.complete(
                        wrap(
                                new GetEvolvedSchemaResponse(
                                        schemaManager.getEvolvedSchema(tableId, schemaVersion))));
            } catch (IllegalArgumentException iae) {
                LOG.warn(
                        "Some client is requesting an non-existed evolved schema for table {} with version {}",
                        tableId,
                        schemaVersion);
                responseFuture.complete(wrap(new GetEvolvedSchemaResponse(null)));
            }
        }
    }

    /** Overridable handler for {@link GetOriginalSchemaRequest}s. */
    protected void handleGetOriginalSchemaRequest(
            GetOriginalSchemaRequest request,
            CompletableFuture<CoordinationResponse> responseFuture)
            throws Exception {
        LOG.info("Handling original schema request: {}", request);
        int schemaVersion = request.getSchemaVersion();
        TableId tableId = request.getTableId();
        if (schemaVersion == GetOriginalSchemaRequest.LATEST_SCHEMA_VERSION) {
            responseFuture.complete(
                    wrap(
                            new GetOriginalSchemaResponse(
                                    schemaManager.getLatestOriginalSchema(tableId).orElse(null))));
        } else {
            try {
                responseFuture.complete(
                        wrap(
                                new GetOriginalSchemaResponse(
                                        schemaManager.getOriginalSchema(tableId, schemaVersion))));
            } catch (IllegalArgumentException iae) {
                LOG.warn(
                        "Some client is requesting an non-existed original schema for table {} with version {}",
                        tableId,
                        schemaVersion);
                responseFuture.complete(wrap(new GetOriginalSchemaResponse(null)));
            }
        }
    }

    /** Coordination handler for customized {@link CoordinationRequest}s. */
    protected abstract void handleCustomCoordinationRequest(
            CoordinationRequest request, CompletableFuture<CoordinationResponse> responseFuture)
            throws Exception;

    /** Last chance to execute codes before job fails globally. */
    protected void handleUnrecoverableError(String taskDescription, Throwable t) {
        LOG.error(
                "Uncaught exception in the Schema Registry ({}) event loop for {}.",
                operatorName,
                taskDescription,
                t);
        LOG.error("\tCurrent schema manager state: {}", schemaManager);
    }

    // ---------------------------------
    // Event & Request Dispatching Stuff
    // ---------------------------------

    @Override
    public final CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
        runInEventLoop(
                () -> {
                    if (request instanceof GetEvolvedSchemaRequest) {
                        handleGetEvolvedSchemaRequest((GetEvolvedSchemaRequest) request, future);
                    } else if (request instanceof GetOriginalSchemaRequest) {
                        handleGetOriginalSchemaRequest((GetOriginalSchemaRequest) request, future);
                    } else {
                        handleCustomCoordinationRequest(request, future);
                    }
                },
                "Handling request - %s",
                request);
        return future;
    }

    @Override
    public final void handleEventFromOperator(
            int subTaskId, int attemptNumber, OperatorEvent event) {
        runInEventLoop(
                () -> {
                    if (event instanceof FlushSuccessEvent) {
                        handleFlushSuccessEvent((FlushSuccessEvent) event);
                    } else if (event instanceof SinkWriterRegisterEvent) {
                        handleSinkWriterRegisterEvent((SinkWriterRegisterEvent) event);
                    } else {
                        throw new FlinkRuntimeException("Unrecognized Operator Event: " + event);
                    }
                },
                "Handling event - %s (from subTask %d)",
                event,
                subTaskId);
    }

    // --------------------------
    // Gateway registration stuff
    // --------------------------

    @Override
    public final void subtaskReset(int subTaskId, long checkpointId) {
        Throwable rootCause = failedReasons.get(subTaskId);
        LOG.error("Subtask {} reset at checkpoint {}.", subTaskId, checkpointId, rootCause);
    }

    @Override
    public final void executionAttemptFailed(
            int subTaskId, int attemptNumber, @Nullable Throwable reason) {
        if (reason != null) {
            failedReasons.put(subTaskId, reason);
        }
    }

    @Override
    public final void executionAttemptReady(
            int subTaskId, int attemptNumber, SubtaskGateway gateway) {
        // Needless to do anything. SchemaRegistry does not post message to the coordinator
        // spontaneously.
    }

    // ---------------------------
    // Checkpointing related stuff
    // ---------------------------
    @Override
    public final void checkpointCoordinator(
            long checkpointId, CompletableFuture<byte[]> completableFuture) throws Exception {
        LOG.info("Going to start checkpoint No.{}", checkpointId);
        runInEventLoop(() -> snapshot(completableFuture), "Taking checkpoint - %d", checkpointId);
    }

    @Override
    public final void notifyCheckpointComplete(long checkpointId) {
        LOG.info("Successfully completed checkpoint No.{}", checkpointId);
    }

    @Override
    public final void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        LOG.info("Going to restore from checkpoint No.{}", checkpointId);
        if (checkpointData == null) {
            return;
        }
        restore(checkpointData);
    }

    // ---------------------------
    // Utility functions
    // ---------------------------
    /**
     * Run a time-consuming task in given {@link ExecutorService}. All overridable functions have
     * been wrapped inside already, so there's no need to call this method again. However, if you're
     * overriding methods from {@link OperatorCoordinator} or {@link CoordinationRequestHandler}
     * directly, make sure you're running heavy logics inside, or the entire job might hang!
     */
    protected void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {
        coordinatorExecutor.execute(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        // if we have a JVM critical error, promote it immediately, there is a good
                        // chance the logging or job failing will not succeed anymore
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                        handleUnrecoverableError(
                                String.format(actionName, actionNameFormatParameters), t);
                        context.failJob(t);
                    }
                });
    }

    /**
     * Keeps checking if {@code conditionChecker} is satisfied. If not, emit a message and retry.
     */
    protected void loopUntil(
            BooleanSupplier conditionChecker, Runnable message, Duration timeout, Duration interval)
            throws TimeoutException {
        loopWhen(() -> !conditionChecker.getAsBoolean(), message, timeout, interval);
    }

    /**
     * Keeps checking if {@code conditionChecker} is satisfied. Otherwise, emit a message and retry.
     */
    protected void loopWhen(
            BooleanSupplier conditionChecker, Runnable message, Duration timeout, Duration interval)
            throws TimeoutException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        long intervalMs = interval.toMillis();
        while (conditionChecker.getAsBoolean()) {
            message.run();
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutException("Loop checking time limit has exceeded.");
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    protected <T extends Throwable> void failJob(String taskDescription, T t) {
        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
        LOG.error("An exception was triggered from {}. Job will fail now.", taskDescription, t);
        handleUnrecoverableError(taskDescription, t);
        context.failJob(t);
    }

    // ------------------------
    // Visible just for testing
    // ------------------------

    @VisibleForTesting
    public void emplaceOriginalSchema(TableId tableId, Schema schema) {
        schemaManager.registerNewOriginalSchema(tableId, schema);
    }

    @VisibleForTesting
    public void emplaceEvolvedSchema(TableId tableId, Schema schema) {
        schemaManager.registerNewEvolvedSchema(tableId, schema);
    }
}
