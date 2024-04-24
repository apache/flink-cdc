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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.event.RefreshPendingListsResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.ReleaseUpstreamRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.ReleaseUpstreamResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeProcessingResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistryRequestHandler.RequestStatus.RECEIVED_RELEASE_REQUEST;
import static org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils.wrap;

/** A handler to deal with all requests and events for {@link SchemaRegistry}. */
@Internal
@NotThreadSafe
public class SchemaRegistryRequestHandler implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryRequestHandler.class);

    /** The {@link MetadataApplier} for every table. */
    private final MetadataApplier metadataApplier;
    /** All active sink writers. */
    private final Set<Integer> activeSinkWriters;
    /** Schema manager holding schema for all tables. */
    private final SchemaManager schemaManager;

    private final SchemaDerivation schemaDerivation;

    /**
     * Not applied SchemaChangeRequest before receiving all flush success events for its table from
     * sink writers.
     */
    private final List<PendingSchemaChange> pendingSchemaChanges;
    /** Sink writers which have sent flush success events for the request. */
    private final Set<Integer> flushedSinkWriters;

    /** Status of the execution of current schema change request. */
    private boolean isSchemaChangeApplying;
    /** Actual exception if failed to apply schema change. */
    private Exception schemaChangeException;
    /** Executor service to execute schema change. */
    private final ExecutorService schemaChangeThreadPool;

    public SchemaRegistryRequestHandler(
            MetadataApplier metadataApplier,
            SchemaManager schemaManager,
            SchemaDerivation schemaDerivation) {
        this.metadataApplier = metadataApplier;
        this.activeSinkWriters = new HashSet<>();
        this.flushedSinkWriters = new HashSet<>();
        this.pendingSchemaChanges = new LinkedList<>();
        this.schemaManager = schemaManager;
        this.schemaDerivation = schemaDerivation;
        schemaChangeThreadPool = Executors.newSingleThreadExecutor();
        isSchemaChangeApplying = false;
    }

    /**
     * Apply the schema change to the external system.
     *
     * @param tableId the table need to change schema
     * @param derivedSchemaChangeEvents list of the schema changes
     */
    private void applySchemaChange(
            TableId tableId, List<SchemaChangeEvent> derivedSchemaChangeEvents) {
        isSchemaChangeApplying = true;
        schemaChangeException = null;
        try {
            for (SchemaChangeEvent changeEvent : derivedSchemaChangeEvents) {
                metadataApplier.applySchemaChange(changeEvent);
                LOG.debug("Apply schema change {} to table {}.", changeEvent, tableId);
            }
            PendingSchemaChange waitFlushSuccess = pendingSchemaChanges.get(0);
            if (RECEIVED_RELEASE_REQUEST.equals(waitFlushSuccess.getStatus())) {
                startNextSchemaChangeRequest();
            }
        } catch (Exception e) {
            this.schemaChangeException = e;
        } finally {
            this.isSchemaChangeApplying = false;
        }
    }

    /**
     * Handle the {@link SchemaChangeRequest} and wait for all sink subtasks flushing.
     *
     * @param request the received SchemaChangeRequest
     */
    public CompletableFuture<CoordinationResponse> handleSchemaChangeRequest(
            SchemaChangeRequest request) {
        if (pendingSchemaChanges.isEmpty()) {
            LOG.info(
                    "Received schema change event request from table {}. Start to buffer requests for others.",
                    request.getTableId().toString());
            if (request.getSchemaChangeEvent() instanceof CreateTableEvent
                    && schemaManager.schemaExists(request.getTableId())) {
                return CompletableFuture.completedFuture(
                        wrap(new SchemaChangeResponse(Collections.emptyList())));
            }
            schemaManager.applySchemaChange(request.getSchemaChangeEvent());
            List<SchemaChangeEvent> derivedSchemaChangeEvents =
                    schemaDerivation.applySchemaChange(request.getSchemaChangeEvent());
            CompletableFuture<CoordinationResponse> response =
                    CompletableFuture.completedFuture(
                            wrap(new SchemaChangeResponse(derivedSchemaChangeEvents)));
            if (!derivedSchemaChangeEvents.isEmpty()) {
                PendingSchemaChange pendingSchemaChange =
                        new PendingSchemaChange(request, response);
                pendingSchemaChange.derivedSchemaChangeEvents = derivedSchemaChangeEvents;
                pendingSchemaChanges.add(pendingSchemaChange);
                pendingSchemaChanges.get(0).startToWaitForReleaseRequest();
            }
            return response;
        } else {
            LOG.info("There are already processing requests. Wait for processing.");
            CompletableFuture<CoordinationResponse> response = new CompletableFuture<>();
            pendingSchemaChanges.add(new PendingSchemaChange(request, response));
            return response;
        }
    }

    /** Handle the {@link ReleaseUpstreamRequest} and wait for all sink subtasks flushing. */
    public CompletableFuture<CoordinationResponse> handleReleaseUpstreamRequest() {
        CompletableFuture<CoordinationResponse> response =
                pendingSchemaChanges.get(0).getResponseFuture();
        if (response.isDone() && !isSchemaChangeApplying) {
            startNextSchemaChangeRequest();
        } else {
            pendingSchemaChanges.get(0).receiveReleaseRequest();
        }
        return response;
    }

    /**
     * Register a sink subtask.
     *
     * @param sinkSubtask the sink subtask to register
     */
    public void registerSinkWriter(int sinkSubtask) {
        LOG.info("Register sink subtask {}.", sinkSubtask);
        activeSinkWriters.add(sinkSubtask);
    }

    /**
     * Record flushed sink subtasks after receiving FlushSuccessEvent.
     *
     * @param tableId the subtask in SchemaOperator and table that the FlushEvent is about
     * @param sinkSubtask the sink subtask succeed flushing
     */
    public void flushSuccess(TableId tableId, int sinkSubtask) throws InterruptedException {
        flushedSinkWriters.add(sinkSubtask);
        if (flushedSinkWriters.equals(activeSinkWriters)) {
            LOG.info(
                    "All sink subtask have flushed for table {}. Start to apply schema change.",
                    tableId.toString());
            PendingSchemaChange waitFlushSuccess = pendingSchemaChanges.get(0);
            schemaChangeThreadPool.submit(
                    () -> applySchemaChange(tableId, waitFlushSuccess.derivedSchemaChangeEvents));
            Thread.sleep(1000);
            if (schemaChangeException != null) {
                throw new RuntimeException("failed to apply schema change.", schemaChangeException);
            }
            if (isSchemaChangeApplying) {
                waitFlushSuccess
                        .getResponseFuture()
                        .complete(wrap(new SchemaChangeProcessingResponse()));
            } else {
                waitFlushSuccess.getResponseFuture().complete(wrap(new ReleaseUpstreamResponse()));
            }
        }
    }

    private void startNextSchemaChangeRequest() {
        pendingSchemaChanges.remove(0);
        flushedSinkWriters.clear();
        while (!pendingSchemaChanges.isEmpty()) {
            PendingSchemaChange pendingSchemaChange = pendingSchemaChanges.get(0);
            SchemaChangeRequest request = pendingSchemaChange.changeRequest;
            if (request.getSchemaChangeEvent() instanceof CreateTableEvent
                    && schemaManager.schemaExists(request.getTableId())) {
                pendingSchemaChange
                        .getResponseFuture()
                        .complete(wrap(new SchemaChangeResponse(Collections.emptyList())));
                pendingSchemaChanges.remove(0);
            } else {
                schemaManager.applySchemaChange(request.getSchemaChangeEvent());
                List<SchemaChangeEvent> derivedSchemaChangeEvents =
                        schemaDerivation.applySchemaChange(request.getSchemaChangeEvent());
                pendingSchemaChange
                        .getResponseFuture()
                        .complete(wrap(new SchemaChangeResponse(derivedSchemaChangeEvents)));
                if (!derivedSchemaChangeEvents.isEmpty()) {
                    pendingSchemaChange.derivedSchemaChangeEvents = derivedSchemaChangeEvents;
                    pendingSchemaChange.startToWaitForReleaseRequest();
                    break;
                }
            }
        }
    }

    public CompletableFuture<CoordinationResponse> refreshPendingLists() {
        pendingSchemaChanges.clear();
        flushedSinkWriters.clear();
        return CompletableFuture.completedFuture(wrap(new RefreshPendingListsResponse()));
    }

    public CompletableFuture<CoordinationResponse> getSchemaChangeResult() {
        if (schemaChangeException != null) {
            throw new RuntimeException("failed to apply schema change.", schemaChangeException);
        }
        if (isSchemaChangeApplying) {
            return CompletableFuture.supplyAsync(() -> wrap(new SchemaChangeProcessingResponse()));
        } else {
            return CompletableFuture.supplyAsync(() -> wrap(new ReleaseUpstreamResponse()));
        }
    }

    @Override
    public void close() throws IOException {
        if (schemaChangeThreadPool != null) {
            schemaChangeThreadPool.shutdown();
        }
    }

    private static class PendingSchemaChange {
        private final SchemaChangeRequest changeRequest;
        private List<SchemaChangeEvent> derivedSchemaChangeEvents;
        private CompletableFuture<CoordinationResponse> responseFuture;
        private RequestStatus status;

        public PendingSchemaChange(
                SchemaChangeRequest changeRequest,
                CompletableFuture<CoordinationResponse> responseFuture) {
            this.changeRequest = changeRequest;
            this.responseFuture = responseFuture;
            this.status = RequestStatus.PENDING;
        }

        public SchemaChangeRequest getChangeRequest() {
            return changeRequest;
        }

        public CompletableFuture<CoordinationResponse> getResponseFuture() {
            return responseFuture;
        }

        public RequestStatus getStatus() {
            return status;
        }

        public void startToWaitForReleaseRequest() {
            if (!responseFuture.isDone()) {
                throw new IllegalStateException(
                        "Cannot start to wait for flush success before the SchemaChangeRequest is done.");
            }
            this.responseFuture = new CompletableFuture<>();
            this.status = RequestStatus.WAIT_RELEASE_REQUEST;
        }

        public void receiveReleaseRequest() {
            this.status = RECEIVED_RELEASE_REQUEST;
        }
    }

    enum RequestStatus {
        PENDING,
        WAIT_RELEASE_REQUEST,
        RECEIVED_RELEASE_REQUEST
    }
}
