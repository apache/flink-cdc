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

package com.ververica.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.FlinkException;

import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.runtime.operators.schema.SchemaOperator;
import com.ververica.cdc.runtime.operators.schema.event.FlushSuccessEvent;
import com.ververica.cdc.runtime.operators.schema.event.GetSchemaRequest;
import com.ververica.cdc.runtime.operators.schema.event.GetSchemaResponse;
import com.ververica.cdc.runtime.operators.schema.event.ReleaseUpstreamRequest;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import com.ververica.cdc.runtime.operators.schema.event.SinkWriterRegisterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.ververica.cdc.runtime.operators.schema.event.CoordinationResponseUtils.wrap;

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

    /**
     * Tracks the subtask failed reason to throw a more meaningful exception in {@link
     * #subtaskReset}.
     */
    private final Map<Integer, Throwable> failedReasons;

    /** Metadata applier for applying schema changes to external system. */
    private final MetadataApplier metadataApplier;

    /** The request handler that handle all requests and events. */
    private SchemaRegistryRequestHandler requestHandler;

    /** Schema manager for tracking schemas of all tables. */
    private SchemaManager schemaManager = new SchemaManager();

    public SchemaRegistry(
            String operatorName,
            OperatorCoordinator.Context context,
            MetadataApplier metadataApplier) {
        this.context = context;
        this.operatorName = operatorName;
        this.failedReasons = new HashMap<>();
        this.metadataApplier = metadataApplier;
        schemaManager = new SchemaManager();
        requestHandler = new SchemaRegistryRequestHandler(metadataApplier, schemaManager);
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting SchemaRegistry for {}.", operatorName);
        this.failedReasons.clear();
        LOG.info("Started SchemaRegistry for {}.", operatorName);
    }

    @Override
    public void close() throws Exception {
        LOG.info("SchemaRegistry for {} closed.", operatorName);
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
            throws Exception {
        if (event instanceof FlushSuccessEvent) {
            FlushSuccessEvent flushSuccessEvent = (FlushSuccessEvent) event;
            LOG.info(
                    "Sink subtask {} succeed flushing for table {}.",
                    flushSuccessEvent.getSubtask(),
                    flushSuccessEvent.getTableId().toString());
            requestHandler.flushSuccess(
                    flushSuccessEvent.getTableId(), flushSuccessEvent.getSubtask());
        } else if (event instanceof SinkWriterRegisterEvent) {
            requestHandler.registerSinkWriter(((SinkWriterRegisterEvent) event).getSubtask());
        } else {
            throw new FlinkException("Unrecognized Operator Event: " + event);
        }
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {
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
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        if (request instanceof SchemaChangeRequest) {
            SchemaChangeRequest schemaChangeRequest = (SchemaChangeRequest) request;
            return requestHandler.handleSchemaChangeRequest(schemaChangeRequest);
        } else if (request instanceof ReleaseUpstreamRequest) {
            return requestHandler.handleReleaseUpstreamRequest();
        } else if (request instanceof GetSchemaRequest) {
            return CompletableFuture.completedFuture(
                    wrap(handleGetSchemaRequest(((GetSchemaRequest) request))));
        } else {
            throw new IllegalArgumentException("Unrecognized CoordinationRequest type: " + request);
        }
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
            int length = in.readInt();
            byte[] serializedSchemaManager = new byte[length];
            in.readFully(serializedSchemaManager);
            schemaManager =
                    SchemaManager.SERIALIZER.deserialize(
                            schemaManagerSerializerVersion, serializedSchemaManager);
            requestHandler = new SchemaRegistryRequestHandler(metadataApplier, schemaManager);
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

    private GetSchemaResponse handleGetSchemaRequest(GetSchemaRequest getSchemaRequest) {
        LOG.info("Handling schema request: {}", getSchemaRequest);
        int schemaVersion = getSchemaRequest.getSchemaVersion();
        TableId tableId = getSchemaRequest.getTableId();
        if (schemaVersion == GetSchemaRequest.LATEST_SCHEMA_VERSION) {
            return new GetSchemaResponse(schemaManager.getLatestSchema(tableId).orElse(null));
        } else {
            try {
                return new GetSchemaResponse(schemaManager.getSchema(tableId, schemaVersion));
            } catch (IllegalArgumentException iae) {
                LOG.warn(
                        "Some client is requesting an non-existed schema for table {} with version {}",
                        tableId,
                        schemaVersion);
                return new GetSchemaResponse(null);
            }
        }
    }
}
