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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaDerivator;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaManager;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaRegistry;
import org.apache.flink.cdc.runtime.operators.schema.common.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetOriginalSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.distributed.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.distributed.event.SchemaChangeResponse;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava31.com.google.common.collect.HashMultimap;
import org.apache.flink.shaded.guava31.com.google.common.collect.Multimap;
import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils.wrap;

/** Coordinator node for {@link SchemaOperator}. Registry actor in distributed topology. */
public class SchemaCoordinator extends SchemaRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaCoordinator.class);

    /** Atomic finite state machine to track global schema evolving state. */
    private transient AtomicReference<RequestStatus> evolvingStatus;

    /** Request futures from pending schema mappers. */
    private transient Map<
                    Integer, Tuple2<SchemaChangeRequest, CompletableFuture<CoordinationResponse>>>
            pendingRequests;

    /** Tracing sink writers that have flushed successfully. */
    protected transient Set<Integer> flushedSinkWriters;

    /**
     * Transient upstream table schema. The second arity is source partition ID, because in
     * distributed topology, schemas might vary among partitions, so we can't rely on {@code
     * schemaManager} to store original schemas.
     */
    private transient Table<TableId, Integer, Schema> upstreamSchemaTable;

    /**
     * In distributed topology, one schema change event will be broadcast N-times (N = downstream
     * parallelism). We need to effectively ignore duplicate ones since not all {@link
     * SchemaChangeEvent}s are idempotent.
     */
    private transient Multimap<Tuple2<Integer, SchemaChangeEvent>, Integer>
            alreadyHandledSchemaChangeEvents;

    /** Executor service to execute schema change. */
    private final ExecutorService schemaChangeThreadPool;

    public SchemaCoordinator(
            String operatorName,
            OperatorCoordinator.Context context,
            ExecutorService coordinatorExecutor,
            MetadataApplier metadataApplier,
            List<RouteRule> routingRules,
            SchemaChangeBehavior schemaChangeBehavior,
            Duration rpcTimeout) {
        super(
                context,
                operatorName,
                coordinatorExecutor,
                metadataApplier,
                routingRules,
                schemaChangeBehavior,
                rpcTimeout);
        this.schemaChangeThreadPool = Executors.newSingleThreadExecutor();
    }

    // -----------------
    // Lifecycle methods
    // -----------------
    @Override
    public void start() throws Exception {
        super.start();
        this.evolvingStatus = new AtomicReference<>(RequestStatus.IDLE);
        this.pendingRequests = new ConcurrentHashMap<>();
        this.flushedSinkWriters = ConcurrentHashMap.newKeySet();
        this.upstreamSchemaTable = HashBasedTable.create();
        this.alreadyHandledSchemaChangeEvents = HashMultimap.create();
        LOG.info(
                "Started SchemaRegistry for {}. Parallelism: {}", operatorName, currentParallelism);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (schemaChangeThreadPool != null && !schemaChangeThreadPool.isShutdown()) {
            schemaChangeThreadPool.shutdownNow();
        }
    }

    // --------------------------
    // Checkpoint related methods
    // --------------------------
    @Override
    protected void snapshot(CompletableFuture<byte[]> resultFuture) throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            // Serialize SchemaManager
            int schemaManagerSerializerVersion = SchemaManager.SERIALIZER.getVersion();
            out.writeInt(schemaManagerSerializerVersion);
            byte[] serializedSchemaManager = SchemaManager.SERIALIZER.serialize(schemaManager);
            out.writeInt(serializedSchemaManager.length);
            out.write(serializedSchemaManager);
            resultFuture.complete(baos.toByteArray());
        }
    }

    @Override
    protected void restore(byte[] checkpointData) throws Exception {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
                DataInputStream in = new DataInputStream(bais)) {
            int schemaManagerSerializerVersion = in.readInt();
            int length = in.readInt();
            byte[] serializedSchemaManager = new byte[length];
            in.readFully(serializedSchemaManager);
            schemaManager =
                    SchemaManager.SERIALIZER.deserialize(
                            schemaManagerSerializerVersion, serializedSchemaManager);
        }
    }

    // -------------------------
    // Event handler entrances (for schema mappers and sink operators)
    // -------------------------
    @Override
    protected void handleGetOriginalSchemaRequest(
            GetOriginalSchemaRequest request,
            CompletableFuture<CoordinationResponse> responseFuture) {
        throw new UnsupportedOperationException(
                "In distributed topology, there's no centralized upstream schema table since they may evolve independently in various partitions.");
    }

    @Override
    protected void handleCustomCoordinationRequest(
            CoordinationRequest request, CompletableFuture<CoordinationResponse> responseFuture)
            throws Exception {
        if (request instanceof SchemaChangeRequest) {
            handleSchemaEvolveRequest((SchemaChangeRequest) request, responseFuture);
        } else {
            throw new UnsupportedOperationException(
                    "Unknown coordination request type: " + request);
        }
    }

    @Override
    protected void handleFlushSuccessEvent(FlushSuccessEvent event) throws Exception {
        LOG.info("Sink subtask {} succeed flushing.", event.getSinkSubTaskId());
        flushedSinkWriters.add(event.getSinkSubTaskId());
    }

    @Override
    protected void handleUnrecoverableError(String taskDescription, Throwable t) {
        super.handleUnrecoverableError(taskDescription, t);
        LOG.info("Current upstream table state: {}", upstreamSchemaTable);
        pendingRequests.forEach(
                (index, tuple) -> {
                    tuple.f1.completeExceptionally(t);
                });
    }

    // -------------------------
    // Schema evolving logic
    // -------------------------

    private void handleSchemaEvolveRequest(
            SchemaChangeRequest request, CompletableFuture<CoordinationResponse> responseFuture)
            throws Exception {
        LOG.info("Coordinator received schema change request {}.", request);
        if (!request.isNoOpRequest()) {
            LOG.info("It's not an align request, will try to deduplicate.");
            int eventSourcePartitionId = request.getSourceSubTaskId();
            int handlingSinkSubTaskId = request.getSinkSubTaskId();
            SchemaChangeEvent schemaChangeEvent = request.getSchemaChangeEvent();
            Tuple2<Integer, SchemaChangeEvent> uniqueKey =
                    Tuple2.of(eventSourcePartitionId, schemaChangeEvent);
            // Due to the existence of Partitioning Operator, any upstream event will be broadcast
            // to sink N (N = sink parallelism) times.
            // Only the first one should take effect, so we rewrite any other duplicated requests as
            // a no-op align request.
            alreadyHandledSchemaChangeEvents.put(uniqueKey, handlingSinkSubTaskId);
            Collection<Integer> reportedSinkSubTasks =
                    alreadyHandledSchemaChangeEvents.get(uniqueKey);
            if (reportedSinkSubTasks.size() == 1) {
                LOG.info("It's a new request for {}, will handle it", uniqueKey);
                updateUpstreamSchemaTable(
                        schemaChangeEvent.tableId(),
                        request.getSourceSubTaskId(),
                        schemaChangeEvent);
            } else {
                LOG.info(
                        "It's an already handled event {}. It has been handled by {}",
                        uniqueKey,
                        reportedSinkSubTasks);
                request = SchemaChangeRequest.createNoOpRequest(handlingSinkSubTaskId);
            }
            // Moreover, if we've collected all sink subTasks' request, remove it from memory since
            // no more will be possible.
            if (reportedSinkSubTasks.size() == currentParallelism) {
                LOG.info(
                        "All sink subTasks ({}) have already reported request {}. Remove it out of tracking.",
                        reportedSinkSubTasks,
                        uniqueKey);
                alreadyHandledSchemaChangeEvents.removeAll(uniqueKey);
            }
        }

        pendingRequests.put(request.getSinkSubTaskId(), Tuple2.of(request, responseFuture));

        if (pendingRequests.size() == 1) {
            Preconditions.checkState(
                    evolvingStatus.compareAndSet(
                            RequestStatus.IDLE, RequestStatus.WAITING_FOR_FLUSH),
                    "Unexpected evolving status: " + evolvingStatus.get());
            LOG.info(
                    "Received the very-first schema change request {}. Switching from IDLE to WAITING_FOR_FLUSH.",
                    request);
        }

        // No else if, since currentParallelism might be == 1
        if (pendingRequests.size() == currentParallelism) {
            Preconditions.checkState(
                    evolvingStatus.compareAndSet(
                            RequestStatus.WAITING_FOR_FLUSH, RequestStatus.EVOLVING),
                    "Unexpected evolving status: " + evolvingStatus.get());
            LOG.info(
                    "Received the last required schema change request {}. Switching from WAITING_FOR_FLUSH to EVOLVING.",
                    request);

            schemaChangeThreadPool.submit(
                    () -> {
                        try {
                            startSchemaChange();
                        } catch (Throwable t) {
                            failJob(
                                    "Schema change applying task",
                                    new FlinkRuntimeException(
                                            "Failed to apply schema change event.", t));
                            throw new FlinkRuntimeException(
                                    "Failed to apply schema change event.", t);
                        }
                    });
        }
    }

    /**
     * Tries to apply schema change event {@code schemaChangeEvent} to the combination of {@code
     * tableId} and {@code sourcePartition}. Returns {@code true} if schema got changed, or {@code
     * false} if nothing gets touched.
     */
    private void updateUpstreamSchemaTable(
            TableId tableId, int sourcePartition, SchemaChangeEvent schemaChangeEvent) {
        Schema oldSchema = upstreamSchemaTable.get(tableId, sourcePartition);
        upstreamSchemaTable.put(
                tableId,
                sourcePartition,
                SchemaUtils.applySchemaChangeEvent(oldSchema, schemaChangeEvent));
    }

    private void startSchemaChange() throws TimeoutException {
        LOG.info("Starting to evolve schema.");
        loopWhen(
                () -> flushedSinkWriters.size() < currentParallelism,
                () ->
                        LOG.info(
                                "Not all sink writers have successfully flushed. Expected {}, actual {}",
                                currentParallelism,
                                flushedSinkWriters),
                rpcTimeout,
                Duration.ofMillis(100));

        LOG.info("All flushed. Going to evolve schema for pending requests: {}", pendingRequests);
        flushedSinkWriters.clear();

        // Deduce what schema change events should be applied to sink table, and affected sink
        // tables' schema
        Tuple2<Set<TableId>, List<SchemaChangeEvent>> deduceSummary = deduceEvolvedSchemaChanges();

        // And tries to apply it to external system
        List<SchemaChangeEvent> successfullyAppliedSchemaChangeEvents = new ArrayList<>();
        for (SchemaChangeEvent appliedSchemaChangeEvent : deduceSummary.f1) {
            if (applyAndUpdateEvolvedSchemaChange(appliedSchemaChangeEvent)) {
                successfullyAppliedSchemaChangeEvents.add(appliedSchemaChangeEvent);
            }
        }

        // Fetch refreshed view for affected tables. We can't rely on operator clients to do this
        // because it might not have a complete schema view after restoring from previous states.
        Set<TableId> affectedTableIds = deduceSummary.f0;
        Map<TableId, Schema> evolvedSchemaView = new HashMap<>();
        for (TableId tableId : affectedTableIds) {
            schemaManager
                    .getLatestEvolvedSchema(tableId)
                    .ifPresent(schema -> evolvedSchemaView.put(tableId, schema));
        }

        List<Tuple2<SchemaChangeRequest, CompletableFuture<CoordinationResponse>>> futures =
                new ArrayList<>(pendingRequests.values());

        // Restore coordinator internal states first...
        pendingRequests.clear();

        LOG.info("Finished schema evolving. Switching from EVOLVING to IDLE.");
        Preconditions.checkState(
                evolvingStatus.compareAndSet(RequestStatus.EVOLVING, RequestStatus.IDLE),
                "RequestStatus should be EVOLVING when schema evolving finishes.");

        // ... and broadcast affected schema changes to mapper and release upstream then.
        // Make sure we've cleaned-up internal state before this, or we may receive new requests in
        // a dirty state.
        futures.forEach(
                tuple -> {
                    LOG.info(
                            "Coordinator finishes pending future from {}",
                            tuple.f0.getSinkSubTaskId());
                    tuple.f1.complete(
                            wrap(
                                    new SchemaChangeResponse(
                                            evolvedSchemaView,
                                            successfullyAppliedSchemaChangeEvents)));
                });
    }

    private Tuple2<Set<TableId>, List<SchemaChangeEvent>> deduceEvolvedSchemaChanges() {
        List<SchemaChangeRequest> validSchemaChangeRequests =
                pendingRequests.values().stream()
                        .map(e -> e.f0)
                        .filter(
                                request ->
                                        !request.isNoOpRequest()) // Ignore alignment only requests
                        .collect(Collectors.toList());
        LOG.info(
                "Step 1 - Start deducing evolved schema change events for {}",
                validSchemaChangeRequests);

        // Firstly, based on changed upstream tables, infer a set of sink tables that might be
        // affected by this event. Schema changes will be derived individually for each sink table.
        Set<TableId> affectedSinkTableIds =
                SchemaDerivator.getAffectedEvolvedTables(
                        router,
                        validSchemaChangeRequests.stream()
                                .map(rsr -> rsr.getSchemaChangeEvent().tableId())
                                .collect(Collectors.toSet()));
        LOG.info("Step 2 - Affected sink tables are: {}", affectedSinkTableIds);

        List<SchemaChangeEvent> evolvedSchemaChanges = new ArrayList<>();

        // For each affected sink table, we may...
        for (TableId affectedSinkTableId : affectedSinkTableIds) {
            Schema currentSinkSchema =
                    schemaManager.getLatestEvolvedSchema(affectedSinkTableId).orElse(null);
            LOG.info(
                    "Step 3.1 - For affected sink table {} with schema {}...",
                    affectedSinkTableId,
                    currentSinkSchema);

            // ... reversely look up this affected sink table's upstream dependency.
            Set<TableId> upstreamDependencies =
                    SchemaDerivator.reverseLookupDependingUpstreamTables(
                            router, affectedSinkTableId, upstreamSchemaTable);
            Preconditions.checkState(
                    !upstreamDependencies.isEmpty(),
                    "An affected sink table's upstream dependency cannot be empty.");
            LOG.info("Step 3.2 - upstream dependency tables are: {}", upstreamDependencies);

            // Then, grab all upstream schemas from all known partitions and merge them.
            Set<Schema> toBeMergedSchemas =
                    SchemaDerivator.reverseLookupDependingUpstreamSchemas(
                            router, affectedSinkTableId, upstreamSchemaTable);
            LOG.info("Step 3.3 - Upstream dependency schemas are: {}.", toBeMergedSchemas);

            // In distributed topology, schema will never be narrowed because current schema is
            // in the merging base. Notice that current schema might be NULL if it's the first
            // time we met a CreateTableEvent.
            Schema mergedSchema = currentSinkSchema;
            for (Schema toBeMergedSchema : toBeMergedSchemas) {
                mergedSchema =
                        SchemaMergingUtils.getLeastCommonSchema(mergedSchema, toBeMergedSchema);
            }
            LOG.info("Step 3.4 - Deduced widest schema is: {}.", mergedSchema);

            // Detect what schema changes we need to apply to get expected sink table.
            List<SchemaChangeEvent> localEvolvedSchemaChanges =
                    SchemaMergingUtils.getSchemaDifference(
                            affectedSinkTableId, currentSinkSchema, mergedSchema);
            LOG.info("Step 3.5 - Corresponding schema changes are: {}.", localEvolvedSchemaChanges);

            // Finally, we normalize schema change events, including rewriting events by current
            // schema change behavior configuration, dropping explicitly excluded schema change
            // event types.
            List<SchemaChangeEvent> normalizedEvents =
                    SchemaDerivator.normalizeSchemaChangeEvents(
                            currentSinkSchema,
                            localEvolvedSchemaChanges,
                            behavior,
                            metadataApplier);
            LOG.info(
                    "Step 3.6 - After being normalized with {} behavior, final schema change events are: {}",
                    behavior,
                    normalizedEvents);
            evolvedSchemaChanges.addAll(normalizedEvents);
        }

        return Tuple2.of(affectedSinkTableIds, evolvedSchemaChanges);
    }

    private boolean applyAndUpdateEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        try {
            metadataApplier.applySchemaChange(schemaChangeEvent);
            schemaManager.applyEvolvedSchemaChange(schemaChangeEvent);
            LOG.info(
                    "Successfully applied schema change event {} to external system.",
                    schemaChangeEvent);
            return true;
        } catch (Throwable t) {
            handleUnrecoverableError(
                    "Apply schema change event - " + schemaChangeEvent,
                    new FlinkRuntimeException(
                            "Failed to apply schema change event " + schemaChangeEvent + ".", t));
            context.failJob(t);
            throw t;
        }
    }

    // -------------------------
    // Utilities
    // -------------------------

    /**
     * {@code IDLE}: Initial idling state, ready for requests. <br>
     * {@code WAITING_FOR_FLUSH}: Waiting for all mappers to block & collecting enough FlushEvents.
     * <br>
     * {@code EVOLVING}: Applying schema change to sink.
     */
    private enum RequestStatus {
        IDLE,
        WAITING_FOR_FLUSH,
        EVOLVING
    }

    @VisibleForTesting
    public void emplaceOriginalSchema(TableId tableId, Integer subTaskId, Schema schema) {
        upstreamSchemaTable.put(tableId, subTaskId, schema);
    }
}
