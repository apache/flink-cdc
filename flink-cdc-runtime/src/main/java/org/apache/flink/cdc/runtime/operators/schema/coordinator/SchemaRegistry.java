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

package org.apache.flink.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.GetOriginalSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.GetOriginalSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SinkWriterRegisterEvent;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils.wrap;

/**
 * The implementation of the {@link OperatorCoordinator} for the {@link SchemaOperator}.
 *
 * <p>The <code>SchemaRegister</code> provides an event loop style thread model to interact with the
 * Flink runtime. The coordinator ensures that all the state manipulations are made by its event
 * loop thread.
 *
 * <p>This <code>SchemaRegister</code> is responsible for:
 *
 * <ul>
 *   <li>Apply schema changes when receiving the {@link SchemaChangeRequest} from {@link
 *       SchemaOperator}
 *   <li>Notify {@link SchemaOperator} to continue to push data for the table after receiving {@link
 *       FlushSuccessEvent} from its registered sink writer
 * </ul>
 */
public class SchemaRegistry implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistry.class);

    /** The context of the coordinator. */
    private final OperatorCoordinator.Context context;
    /** The name of the operator this SchemaOperatorCoordinator is associated with. */
    private final String operatorName;

    /** A single-thread executor to handle async execution of the coordinator. */
    private final ExecutorService coordinatorExecutor;

    /**
     * Tracks the subtask failed reason to throw a more meaningful exception in {@link
     * #subtaskReset}.
     */
    private final Map<Integer, Throwable> failedReasons;

    /** Metadata applier for applying schema changes to external system. */
    private final MetadataApplier metadataApplier;

    private final List<RouteRule> routes;

    /** The request handler that handle all requests and events. */
    private SchemaRegistryRequestHandler requestHandler;

    /** Schema manager for tracking schemas of all tables. */
    private SchemaManager schemaManager;

    private SchemaDerivation schemaDerivation;

    private SchemaChangeBehavior schemaChangeBehavior;

    /**
     * Current parallelism. Use this to verify if Schema Registry has collected enough flush success
     * events from sink operators.
     */
    private int currentParallelism;

    public SchemaRegistry(
            String operatorName,
            OperatorCoordinator.Context context,
            ExecutorService executorService,
            MetadataApplier metadataApplier,
            List<RouteRule> routes) {
        this(
                operatorName,
                context,
                executorService,
                metadataApplier,
                routes,
                SchemaChangeBehavior.LENIENT);
    }

    public SchemaRegistry(
            String operatorName,
            OperatorCoordinator.Context context,
            ExecutorService coordinatorExecutor,
            MetadataApplier metadataApplier,
            List<RouteRule> routes,
            SchemaChangeBehavior schemaChangeBehavior) {
        this.context = context;
        this.coordinatorExecutor = coordinatorExecutor;
        this.operatorName = operatorName;
        this.failedReasons = new HashMap<>();
        this.metadataApplier = metadataApplier;
        this.routes = routes;
        this.schemaManager = new SchemaManager(schemaChangeBehavior);
        this.schemaDerivation = new SchemaDerivation(schemaManager, routes, new HashMap<>());
        this.requestHandler =
                new SchemaRegistryRequestHandler(
                        metadataApplier,
                        schemaManager,
                        schemaDerivation,
                        schemaChangeBehavior,
                        context);
        this.schemaChangeBehavior = schemaChangeBehavior;
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting SchemaRegistry for {}.", operatorName);
        this.failedReasons.clear();
        this.currentParallelism = context.currentParallelism();
        LOG.info(
                "Started SchemaRegistry for {}. Parallelism: {}", operatorName, currentParallelism);
    }

    @Override
    public void close() throws Exception {
        LOG.info("SchemaRegistry for {} closed.", operatorName);
        coordinatorExecutor.shutdown();
        requestHandler.close();
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        runInEventLoop(
                () -> {
                    try {
                        if (event instanceof FlushSuccessEvent) {
                            FlushSuccessEvent flushSuccessEvent = (FlushSuccessEvent) event;
                            LOG.info(
                                    "Sink subtask {} succeed flushing for table {}.",
                                    flushSuccessEvent.getSubtask(),
                                    flushSuccessEvent.getTableId().toString());
                            requestHandler.flushSuccess(
                                    flushSuccessEvent.getTableId(),
                                    flushSuccessEvent.getSubtask(),
                                    currentParallelism);
                        } else if (event instanceof SinkWriterRegisterEvent) {
                            requestHandler.registerSinkWriter(
                                    ((SinkWriterRegisterEvent) event).getSubtask());
                        } else {
                            throw new FlinkException("Unrecognized Operator Event: " + event);
                        }
                    } catch (Throwable t) {
                        context.failJob(t);
                        throw t;
                    }
                },
                "handling event %s from subTask %d",
                event,
                subtask);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
        // we generate checkpoint in an async thread to not block the JobManager's main thread, the
        // coordinator state might be large if there are many schema changes and monitor many
        // tables.
        runInEventLoop(
                () -> {
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            DataOutputStream out = new DataOutputStream(baos)) {
                        // Serialize SchemaManager
                        int schemaManagerSerializerVersion = SchemaManager.SERIALIZER.getVersion();
                        out.writeInt(schemaManagerSerializerVersion);
                        byte[] serializedSchemaManager =
                                SchemaManager.SERIALIZER.serialize(schemaManager);
                        out.writeInt(serializedSchemaManager.length);
                        out.write(serializedSchemaManager);
                        // Serialize SchemaDerivation mapping
                        SchemaDerivation.serializeDerivationMapping(schemaDerivation, out);
                        resultFuture.complete(baos.toByteArray());
                    } catch (Throwable t) {
                        context.failJob(t);
                        throw t;
                    }
                },
                "taking checkpoint %d",
                checkpointId);
    }

    private void runInEventLoop(
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

                        final String actionString =
                                String.format(actionName, actionNameFormatParameters);
                        LOG.error(
                                "Uncaught exception in the SchemaEvolutionCoordinator for {} while {}. Triggering job failover.",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        CompletableFuture<CoordinationResponse> responseFuture = new CompletableFuture<>();
        runInEventLoop(
                () -> {
                    try {
                        if (request instanceof SchemaChangeRequest) {
                            SchemaChangeRequest schemaChangeRequest = (SchemaChangeRequest) request;
                            requestHandler.handleSchemaChangeRequest(
                                    schemaChangeRequest, responseFuture);
                        } else if (request instanceof SchemaChangeResultRequest) {
                            requestHandler.getSchemaChangeResult(responseFuture);
                        } else if (request instanceof GetEvolvedSchemaRequest) {
                            handleGetEvolvedSchemaRequest(
                                    ((GetEvolvedSchemaRequest) request), responseFuture);
                        } else if (request instanceof GetOriginalSchemaRequest) {
                            handleGetOriginalSchemaRequest(
                                    (GetOriginalSchemaRequest) request, responseFuture);
                        } else {
                            throw new IllegalArgumentException(
                                    "Unrecognized CoordinationRequest type: " + request);
                        }
                    } catch (Throwable t) {
                        context.failJob(t);
                        throw t;
                    }
                },
                "handling coordination request %s",
                request);
        return responseFuture;
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        if (checkpointData == null) {
            return;
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
                DataInputStream in = new DataInputStream(bais)) {
            int schemaManagerSerializerVersion = in.readInt();

            switch (schemaManagerSerializerVersion) {
                case 0:
                    {
                        int length = in.readInt();
                        byte[] serializedSchemaManager = new byte[length];
                        in.readFully(serializedSchemaManager);
                        schemaManager =
                                SchemaManager.SERIALIZER.deserialize(
                                        schemaManagerSerializerVersion, serializedSchemaManager);
                        schemaDerivation =
                                new SchemaDerivation(schemaManager, routes, Collections.emptyMap());
                        requestHandler =
                                new SchemaRegistryRequestHandler(
                                        metadataApplier,
                                        schemaManager,
                                        schemaDerivation,
                                        schemaManager.getBehavior(),
                                        context);
                        break;
                    }
                case 1:
                case 2:
                    {
                        int length = in.readInt();
                        byte[] serializedSchemaManager = new byte[length];
                        in.readFully(serializedSchemaManager);
                        schemaManager =
                                SchemaManager.SERIALIZER.deserialize(
                                        schemaManagerSerializerVersion, serializedSchemaManager);
                        Map<TableId, Set<TableId>> derivationMapping =
                                SchemaDerivation.deserializerDerivationMapping(in);
                        schemaDerivation =
                                new SchemaDerivation(schemaManager, routes, derivationMapping);
                        requestHandler =
                                new SchemaRegistryRequestHandler(
                                        metadataApplier,
                                        schemaManager,
                                        schemaDerivation,
                                        schemaChangeBehavior,
                                        context);
                        break;
                    }
                default:
                    throw new IOException(
                            "Unrecognized serialization version " + schemaManagerSerializerVersion);
            }
        } catch (Throwable t) {
            context.failJob(t);
            throw t;
        }
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        Throwable rootCause = failedReasons.get(subtask);
        LOG.error(
                String.format("Subtask %d reset at checkpoint %d.", subtask, checkpointId),
                rootCause);
    }

    @Override
    public void executionAttemptFailed(
            int subtask, int attemptNumber, @Nullable Throwable throwable) {
        failedReasons.put(subtask, throwable);
    }

    @Override
    public void executionAttemptReady(
            int subtask, int attemptNumber, SubtaskGateway subtaskGateway) {
        // do nothing
    }

    private void handleGetEvolvedSchemaRequest(
            GetEvolvedSchemaRequest getEvolvedSchemaRequest,
            CompletableFuture<CoordinationResponse> response) {
        LOG.info("Handling evolved schema request: {}", getEvolvedSchemaRequest);
        int schemaVersion = getEvolvedSchemaRequest.getSchemaVersion();
        TableId tableId = getEvolvedSchemaRequest.getTableId();
        if (schemaVersion == GetEvolvedSchemaRequest.LATEST_SCHEMA_VERSION) {
            response.complete(
                    wrap(
                            new GetEvolvedSchemaResponse(
                                    schemaManager.getLatestEvolvedSchema(tableId).orElse(null))));
        } else {
            try {
                response.complete(
                        wrap(
                                new GetEvolvedSchemaResponse(
                                        schemaManager.getEvolvedSchema(tableId, schemaVersion))));
            } catch (IllegalArgumentException iae) {
                LOG.warn(
                        "Some client is requesting an non-existed evolved schema for table {} with version {}",
                        tableId,
                        schemaVersion);
                response.complete(wrap(new GetEvolvedSchemaResponse(null)));
            }
        }
    }

    private void handleGetOriginalSchemaRequest(
            GetOriginalSchemaRequest getOriginalSchemaRequest,
            CompletableFuture<CoordinationResponse> response) {
        LOG.info("Handling original schema request: {}", getOriginalSchemaRequest);
        int schemaVersion = getOriginalSchemaRequest.getSchemaVersion();
        TableId tableId = getOriginalSchemaRequest.getTableId();
        if (schemaVersion == GetOriginalSchemaRequest.LATEST_SCHEMA_VERSION) {
            response.complete(
                    wrap(
                            new GetOriginalSchemaResponse(
                                    schemaManager.getLatestOriginalSchema(tableId).orElse(null))));
        } else {
            try {
                response.complete(
                        wrap(
                                new GetOriginalSchemaResponse(
                                        schemaManager.getOriginalSchema(tableId, schemaVersion))));
            } catch (IllegalArgumentException iae) {
                LOG.warn(
                        "Some client is requesting an non-existed original schema for table {} with version {}",
                        tableId,
                        schemaVersion);
                response.complete(wrap(new GetOriginalSchemaResponse(null)));
            }
        }
    }

    // --------------------Only visible for test -----------------

    @VisibleForTesting
    public void handleApplyOriginalSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        schemaManager.applyOriginalSchemaChange(schemaChangeEvent);
    }

    @VisibleForTesting
    public void handleApplyEvolvedSchemaChangeRequest(SchemaChangeEvent schemaChangeEvent) {
        schemaManager.applyEvolvedSchemaChange(schemaChangeEvent);
    }
}
