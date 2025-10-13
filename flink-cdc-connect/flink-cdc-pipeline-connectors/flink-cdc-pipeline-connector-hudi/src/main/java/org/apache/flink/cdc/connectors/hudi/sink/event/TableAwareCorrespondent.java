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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.hudi.sink.coordinator.MultiTableStreamWriteOperatorCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
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
            MultiTableStreamWriteOperatorCoordinator.MultiTableInstantTimeRequest request =
                    new MultiTableStreamWriteOperatorCoordinator.MultiTableInstantTimeRequest(
                            checkpointId, tableId);

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
}
