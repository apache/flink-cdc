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

package org.apache.flink.cdc.runtime.testutils.schema;

import org.apache.flink.cdc.runtime.operators.schema.common.SchemaRegistry;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.CompletableFuture;

/**
 * A {@link TaskOperatorEventGateway} for testing that deliver all gateway events and requests to
 * {@link SchemaRegistry}.
 */
public class TestingSchemaRegistryGateway implements TaskOperatorEventGateway, AutoCloseable {
    public static final OperatorID SCHEMA_OPERATOR_ID = new OperatorID(15213L, 15513L);
    private final SchemaRegistry schemaRegistry;

    public TestingSchemaRegistryGateway(SchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    public void open() throws Exception {
        schemaRegistry.start();
    }

    @Override
    public void close() throws Exception {
        schemaRegistry.close();
    }

    @Override
    public void sendOperatorEventToCoordinator(
            OperatorID operator, SerializedValue<OperatorEvent> event) {
        try {
            schemaRegistry.handleEventFromOperator(
                    0, 0, event.deserializeValue(Thread.currentThread().getContextClassLoader()));
        } catch (Exception e) {
            throw new RuntimeException("Unable to deliver OperatorEvent to SchemaRegistry", e);
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operator, SerializedValue<CoordinationRequest> request) {
        try {
            return schemaRegistry.handleCoordinationRequest(
                    request.deserializeValue(Thread.currentThread().getContextClassLoader()));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to deliver CoordinationRequest to SchemaRegistry", e);
        }
    }
}
