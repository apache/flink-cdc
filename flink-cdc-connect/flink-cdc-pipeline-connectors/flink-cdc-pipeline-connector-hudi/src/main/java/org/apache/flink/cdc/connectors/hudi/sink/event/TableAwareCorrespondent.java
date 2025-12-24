/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.hudi.sink.event;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.hudi.sink.coordinator.MultiTableStreamWriteOperatorCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.SerializedValue;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.utils.CoordinationResponseSerDe;

/**
 * A correspondent between a write task and the multi-table coordinator. This class is responsible
 * for sending table-aware requests to the {@link MultiTableStreamWriteOperatorCoordinator}.
 */
public class TableAwareCorrespondent extends Correspondent {
    private final OperatorID operatorID;
    private final TaskOperatorEventGateway gateway;
    private final TableId tableId;

    private TableAwareCorrespondent(
            OperatorID operatorID, TaskOperatorEventGateway gateway, TableId tableId) {
        this.operatorID = operatorID;
        this.gateway = gateway;
        this.tableId = tableId;
    }

    /**
     * Creates a coordinator correspondent.
     *
     * @param correspondent The original correspondent
     * @param tableId The table ID
     * @return an instance of {@code TableAwareCorrespondent}.
     */
    public static TableAwareCorrespondent getInstance(
            Correspondent correspondent, TableId tableId) {
        return new TableAwareCorrespondent(
                correspondent.getOperatorID(), correspondent.getGateway(), tableId);
    }

    /**
     * Sends a request to the coordinator to fetch the instant time for a specific table.
     *
     * @param checkpointId The current checkpoint ID.
     * @return The instant time string allocated by the coordinator.
     */
    @Override
    public String requestInstantTime(long checkpointId) {
        try {
            MultiTableInstantTimeRequest request =
                    new MultiTableInstantTimeRequest(checkpointId, tableId);
            Correspondent.InstantTimeResponse response =
                    CoordinationResponseSerDe.unwrap(
                            this.gateway
                                    .sendRequestToCoordinator(
                                            this.operatorID, new SerializedValue<>(request))
                                    .get());
            return response.getInstant();
        } catch (Exception e) {
            throw new HoodieException(
                    "Error requesting the instant time from the coordinator for table " + tableId,
                    e);
        }
    }

    /**
     * A custom coordination request that includes the TableId to request an instant for a specific
     * table.
     */
    public static class MultiTableInstantTimeRequest implements CoordinationRequest {
        private static final long serialVersionUID = 1L;
        private final long checkpointId;
        private final TableId tableId;

        public MultiTableInstantTimeRequest(long checkpointId, TableId tableId) {
            this.checkpointId = checkpointId;
            this.tableId = tableId;
        }

        public long getCheckpointId() {
            return checkpointId;
        }

        public TableId getTableId() {
            return tableId;
        }
    }

    /**
     * Send a request to coordinator to create a hudi table.
     *
     * @param createTableEvent The creating table event.
     * @return Whether the table is created successfully.
     */
    public boolean requestCreatingTable(CreateTableEvent createTableEvent) {
        try {
            CreateTableRequest request = new CreateTableRequest(createTableEvent);
            SchemaChangeResponse response =
                    CoordinationResponseSerDe.unwrap(
                            this.gateway
                                    .sendRequestToCoordinator(
                                            this.operatorID, new SerializedValue<>(request))
                                    .get());
            return response.isSuccess();
        } catch (Exception e) {
            throw new HoodieException(
                    "Error requesting the instant time from the coordinator for table " + tableId,
                    e);
        }
    }

    /**
     * Send a request to coordinator to apply the schema change.
     *
     * @param tableId the id of table
     * @param newSchema the new table schema
     * @return Whether the schema change is applied successfully.
     */
    public boolean requestSchemaChange(TableId tableId, Schema newSchema) {
        try {
            SchemaChangeRequest request = new SchemaChangeRequest(tableId, newSchema);
            SchemaChangeResponse response =
                    CoordinationResponseSerDe.unwrap(
                            this.gateway
                                    .sendRequestToCoordinator(
                                            this.operatorID, new SerializedValue<>(request))
                                    .get());
            return response.isSuccess();
        } catch (Exception e) {
            throw new HoodieException(
                    "Error requesting the instant time from the coordinator for table " + tableId,
                    e);
        }
    }

    /**
     * A CoordinationRequest that encapsulates a {@link CreateTableEvent}.
     *
     * <p>This request is sent from the {@code MultiTableEventStreamWriteFunction} to the {@code
     * MultiTableStreamWriteOperatorCoordinator} to signal that a new table has been discovered in
     * the CDC stream. The coordinator uses this event to initialize all necessary resources for the
     * new table, such as its dedicated write client and event buffers, before any data is written.
     */
    public static class CreateTableRequest extends SchemaChangeRequest {
        private static final long serialVersionUID = 1L;

        public CreateTableRequest(CreateTableEvent createTableEvent) {
            super(createTableEvent.tableId(), createTableEvent.getSchema());
        }
    }

    /**
     * A CoordinationRequest that represents a request to change table schema.
     *
     * <p>This request is sent from the {@code MultiTableEventStreamWriteFunction} to the {@code
     * MultiTableStreamWriteOperatorCoordinator} to signal that a schema change has been discovered
     * in the CDC stream.
     */
    public static class SchemaChangeRequest implements CoordinationRequest {
        private static final long serialVersionUID = 1L;

        private final TableId tableId;
        private final Schema schema;

        public SchemaChangeRequest(TableId tableId, Schema schema) {
            this.tableId = tableId;
            this.schema = schema;
        }

        public TableId getTableId() {
            return tableId;
        }

        public Schema getSchema() {
            return schema;
        }

        @Override
        public String toString() {
            return "SchemaChangeRequest{" + "tableId=" + tableId + ", schema=" + schema + '}';
        }
    }

    /**
     * Response for a {@link CreateTableRequest} or {@link SchemaChangeRequest}. This response is
     * sent from writer coordinator to indicate whether the schema change is applied successfully.
     */
    public static class SchemaChangeResponse implements CoordinationResponse {
        private static final long serialVersionUID = 1L;
        private final TableId tableId;
        private final boolean success;

        private SchemaChangeResponse(TableId tableId, boolean success) {
            this.tableId = tableId;
            this.success = success;
        }

        public boolean isSuccess() {
            return success;
        }

        public TableId getTableId() {
            return tableId;
        }

        public static SchemaChangeResponse of(TableId tableId, boolean success) {
            return new SchemaChangeResponse(tableId, success);
        }
    }
}
