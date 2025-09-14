/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.maxcompute.coordinator;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.maxcompute.common.Constant;
import org.apache.flink.cdc.connectors.maxcompute.common.SessionIdentifier;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.CreateSessionRequest;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.CreateSessionResponse;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.WaitForFlushSuccessRequest;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;
import org.apache.flink.cdc.connectors.maxcompute.utils.TypeConvertUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.SerializedValue;

import com.aliyun.odps.PartitionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Processes a {@link DataChangeEvent}, extracting data and encapsulating it into a {@link
 * SessionIdentifier}, and then sends a {@link CreateSessionRequest} to the {@link
 * SessionManageCoordinator} to create a writing session. Subsequently, it incorporates the
 * SessionId into the metadata of the {@link DataChangeEvent} for downstream processing.
 */
public class SessionManageOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, OperatorEventHandler, BoundedOneInput {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SessionManageOperator.class);

    /** TODO: a tricky way to get an Operator from sink. */
    public static SessionManageOperator instance;

    private final MaxComputeOptions options;
    private final OperatorID schemaOperatorUid;

    private transient TaskOperatorEventGateway taskOperatorEventGateway;
    private transient Map<SessionIdentifier, String> sessionCache;
    private transient Map<TableId, Schema> schemaMaps;
    private transient Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps;
    private transient SchemaEvolutionClient schemaEvolutionClient;

    private transient Future<CoordinationResponse> snapshotFlushSuccess;
    private transient int indexOfThisSubtask;
    /**
     * trigger endOfInput is ahead of prepareSnapshotPreBarrier, so we need this flag to handle when
     * endOfInput, send WaitForSuccessRequest in advance.
     */
    private transient boolean endOfInput;

    public SessionManageOperator(MaxComputeOptions options, OperatorID schemaOperatorUid) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.options = options;
        this.schemaOperatorUid = schemaOperatorUid;
    }

    @Override
    public void open() throws Exception {
        this.sessionCache = new HashMap<>();
        this.schemaMaps = new HashMap<>();
        this.fieldGetterMaps = new HashMap<>();
        SessionManageOperator.instance = this;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        schemaEvolutionClient =
                new SchemaEvolutionClient(
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway(),
                        schemaOperatorUid);
        indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        if (element.getValue() instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) element.getValue();
            TableId tableId = dataChangeEvent.tableId();
            // because of this operator is between SchemaOperator and DataSinkWriterOperator, no
            // schema will fill when CreateTableEvent is loss.
            if (!schemaMaps.containsKey(tableId)) {
                emitLatestSchema(tableId);
            }
            String partitionName =
                    extractPartition(
                            dataChangeEvent.op() == OperationType.DELETE
                                    ? dataChangeEvent.before()
                                    : dataChangeEvent.after(),
                            tableId);
            SessionIdentifier sessionIdentifier =
                    SessionIdentifier.of(
                            options.getProject(),
                            MaxComputeUtils.getSchema(options, tableId),
                            tableId.getTableName(),
                            partitionName);
            if (!sessionCache.containsKey(sessionIdentifier)) {
                CreateSessionResponse response =
                        (CreateSessionResponse)
                                sendRequestToOperator(new CreateSessionRequest(sessionIdentifier));
                sessionCache.put(sessionIdentifier, response.getSessionId());
            }
            dataChangeEvent
                    .meta()
                    .put(Constant.TUNNEL_SESSION_ID, sessionCache.get(sessionIdentifier));
            dataChangeEvent.meta().put(Constant.MAXCOMPUTE_PARTITION_NAME, partitionName);
            output.collect(new StreamRecord<>(dataChangeEvent));
        } else if (element.getValue() instanceof FlushEvent) {
            LOG.info(
                    "operator {} handle FlushEvent begin, wait for sink writers flush success",
                    indexOfThisSubtask);
            sessionCache.clear();
            Future<CoordinationResponse> waitForSuccess =
                    submitRequestToOperator(new WaitForFlushSuccessRequest(indexOfThisSubtask));
            output.collect(element);
            // wait for sink writers flush success
            waitForSuccess.get();
            LOG.info(
                    "operator {} handle FlushEvent end, all sink writers flush success",
                    indexOfThisSubtask);
        } else if (element.getValue() instanceof CreateTableEvent) {
            TableId tableId = ((CreateTableEvent) element.getValue()).tableId();
            Schema schema = ((CreateTableEvent) element.getValue()).getSchema();
            schemaMaps.put(tableId, schema);
            fieldGetterMaps.put(tableId, TypeConvertUtils.createFieldGetters(schema));
            output.collect(element);
        } else if (element.getValue() instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) element.getValue();
            TableId tableId = schemaChangeEvent.tableId();
            Schema newSchema =
                    SchemaUtils.applySchemaChangeEvent(schemaMaps.get(tableId), schemaChangeEvent);
            schemaMaps.put(tableId, newSchema);
            fieldGetterMaps.put(tableId, TypeConvertUtils.createFieldGetters(newSchema));
            output.collect(element);
        } else {
            output.collect(element);
            LOG.warn("unknown element {}", element.getValue());
        }
    }

    private void emitLatestSchema(TableId tableId) throws Exception {
        Optional<Schema> schema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
        if (schema.isPresent()) {
            Schema latestSchema = schema.get();
            schemaMaps.put(tableId, latestSchema);
            fieldGetterMaps.put(tableId, TypeConvertUtils.createFieldGetters(latestSchema));
            output.collect(new StreamRecord<>(new CreateTableEvent(tableId, latestSchema)));
        } else {
            throw new RuntimeException(
                    "Could not find schema message from SchemaRegistry for " + tableId);
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        if (endOfInput) {
            return;
        }
        LOG.info(
                "operator {} prepare snapshot, wait for sink writers flush success",
                indexOfThisSubtask);
        // wait for sink writers flush success
        waitLastSnapshotFlushSuccess();
        snapshotFlushSuccess =
                submitRequestToOperator(
                        new WaitForFlushSuccessRequest(
                                getRuntimeContext().getIndexOfThisSubtask()));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        sessionCache.clear();
        waitLastSnapshotFlushSuccess();
        LOG.info("operator {} snapshot end, all sink writers flush success", indexOfThisSubtask);
    }

    @Override
    public void endInput() throws Exception {
        this.endOfInput = true;
        LOG.info(
                "operator {} end of input, wait for sink writers flush success",
                indexOfThisSubtask);
        waitLastSnapshotFlushSuccess();
        snapshotFlushSuccess =
                submitRequestToOperator(
                        new WaitForFlushSuccessRequest(
                                getRuntimeContext().getIndexOfThisSubtask()));
    }

    private void waitLastSnapshotFlushSuccess() throws Exception {
        if (snapshotFlushSuccess != null) {
            snapshotFlushSuccess.get();
            snapshotFlushSuccess = null;
        }
    }

    /** partition column is always after data column. */
    private String extractPartition(RecordData recordData, TableId tableId) {
        Schema schema = schemaMaps.get(tableId);
        int partitionKeyCount = schema.partitionKeys().size();
        if (partitionKeyCount == 0) {
            return null;
        }
        int columnCount = schema.getColumnCount();
        List<RecordData.FieldGetter> fieldGetters = fieldGetterMaps.get(tableId);

        PartitionSpec partitionSpec = new PartitionSpec();
        for (int i = 0; i < partitionKeyCount; i++) {
            RecordData.FieldGetter fieldGetter =
                    fieldGetters.get(columnCount - partitionKeyCount - 1 + i);
            Object value = fieldGetter.getFieldOrNull(recordData);
            partitionSpec.set(schema.partitionKeys().get(i), Objects.toString(value));
        }
        return partitionSpec.toString(true, true);
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        // handle event
    }

    /** call from CreateSessionCoordinatedOperatorFactory. */
    public void setTaskOperatorEventGateway(TaskOperatorEventGateway taskOperatorEventGateway) {
        this.taskOperatorEventGateway = taskOperatorEventGateway;
    }

    public CoordinationResponse sendRequestToOperator(CoordinationRequest request)
            throws IOException, ExecutionException, InterruptedException {
        CompletableFuture<CoordinationResponse> responseFuture =
                taskOperatorEventGateway.sendRequestToCoordinator(
                        getOperatorID(), new SerializedValue<>(request));
        return CoordinationResponseUtils.unwrap(responseFuture.get());
    }

    public Future<CoordinationResponse> submitRequestToOperator(CoordinationRequest request)
            throws IOException {
        return taskOperatorEventGateway.sendRequestToCoordinator(
                getOperatorID(), new SerializedValue<>(request));
    }
}
