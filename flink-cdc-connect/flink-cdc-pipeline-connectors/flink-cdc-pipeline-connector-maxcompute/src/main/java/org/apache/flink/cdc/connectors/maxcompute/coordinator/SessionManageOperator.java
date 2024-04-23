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
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.FlushSuccessRequest;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.TypeConvertUtils;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.SerializedValue;

import com.aliyun.odps.PartitionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Processes a {@link DataChangeEvent}, extracting data and encapsulating it into a {@link
 * SessionIdentifier}, and then sends a {@link CreateSessionRequest} to the {@link
 * SessionManageCoordinator} to create a writing session. Subsequently, it incorporates the
 * SessionId into the metadata of the {@link DataChangeEvent} for downstream processing.
 */
public class SessionManageOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, OperatorEventHandler {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SessionManageOperator.class);

    /** a tricky way to get an Operator from sink. */
    public static SessionManageOperator instance;

    private final MaxComputeOptions options;

    private transient TaskOperatorEventGateway taskOperatorEventGateway;
    private transient Map<SessionIdentifier, String> sessionCache;
    private transient Map<TableId, Schema> schemaMaps;
    private transient Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps;

    /** use for sync with sink writer. * */
    private transient boolean isFlushSuccess;

    private transient Lock lock;
    private transient Condition condition;

    public SessionManageOperator(MaxComputeOptions options) {
        this.options = options;
    }

    @Override
    public void open() throws Exception {
        this.sessionCache = new HashMap<>();
        this.schemaMaps = new HashMap<>();
        this.fieldGetterMaps = new HashMap<>();
        SessionManageOperator.instance = this;

        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        if (element.getValue() instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) element.getValue();
            TableId tableId = dataChangeEvent.tableId();
            String partitionName =
                    extractPartition(
                            dataChangeEvent.op() == OperationType.DELETE
                                    ? dataChangeEvent.before()
                                    : dataChangeEvent.after(),
                            tableId);
            SessionIdentifier sessionIdentifier =
                    SessionIdentifier.of(
                            options.getProject(),
                            tableId.getNamespace(),
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
            LOG.info("handle {} begin, wait for sink writers flush success", element.getValue());
            isFlushSuccess = false;
            sessionCache.clear();
            output.collect(element);
            // wait for sink writers flush success
            blockUntilSessionCommitted();
            LOG.info("handle flush event {} end", element.getValue());
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
            LOG.warn("unknown element", element.getValue());
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

    public void blockUntilSessionCommitted()
            throws IOException, ExecutionException, InterruptedException {
        sendRequestToOperator(new FlushSuccessRequest(false));
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        // handle event
    }

    /** call from CreateSessionCoordinatedOperatorFactory. */
    public void setTaskOperatorEventGateway(TaskOperatorEventGateway taskOperatorEventGateway) {
        this.taskOperatorEventGateway = taskOperatorEventGateway;
    }

    public int getOperatorIndex() {
        return getRuntimeContext().getIndexOfThisSubtask();
    }

    public CoordinationResponse sendRequestToOperator(CoordinationRequest request)
            throws IOException, ExecutionException, InterruptedException {
        return taskOperatorEventGateway
                .sendRequestToCoordinator(getOperatorID(), new SerializedValue<>(request))
                .get();
    }
}
