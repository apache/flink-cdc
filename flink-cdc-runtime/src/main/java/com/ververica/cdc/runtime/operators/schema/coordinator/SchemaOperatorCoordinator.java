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

import com.ververica.cdc.runtime.operators.schema.SchemaOperator;
import com.ververica.cdc.runtime.operators.schema.event.FlushSuccessEvent;
import com.ververica.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * The implementation of the {@link OperatorCoordinator} for the {@link SchemaOperator}.
 *
 * <p>The <code>SchemaOperatorCoordinator</code> provides an event loop style thread model to
 * interact with the Flink runtime. The coordinator ensures that all the state manipulations are
 * made by its event loop thread.
 *
 * <p>This coordinator is responsible for:
 *
 * <ul>
 *   <li>Apply schema changes when receiving the {@link SchemaChangeRequest} from {@link
 *       SchemaOperator}
 *   <li>Notify {@link SchemaOperator} to continue to push data for the table after receiving {@link
 *       FlushSuccessEvent} from its registered sink writer
 * </ul>
 */
public class SchemaOperatorCoordinator implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaOperatorCoordinator.class);

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest coordinationRequest) {
        return null;
    }

    @Override
    public void start() throws Exception {}

    @Override
    public void close() throws Exception {}

    @Override
    public void handleEventFromOperator(int i, int i1, OperatorEvent operatorEvent)
            throws Exception {}

    @Override
    public void checkpointCoordinator(long l, CompletableFuture<byte[]> completableFuture)
            throws Exception {}

    @Override
    public void notifyCheckpointComplete(long l) {}

    @Override
    public void resetToCheckpoint(long l, @Nullable byte[] bytes) throws Exception {}

    @Override
    public void subtaskReset(int i, long l) {}

    @Override
    public void executionAttemptFailed(int i, int i1, @Nullable Throwable throwable) {}

    @Override
    public void executionAttemptReady(int i, int i1, SubtaskGateway subtaskGateway) {}
}
