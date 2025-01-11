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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator.Provider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/** The {@link AbstractStreamOperatorFactory} for {@link SessionManageOperator}. */
public class SessionManageCoordinatedOperatorFactory extends AbstractStreamOperatorFactory<Event>
        implements CoordinatedOperatorFactory<Event>, OneInputStreamOperatorFactory<Event, Event> {
    private static final long serialVersionUID = 1L;
    private final MaxComputeOptions options;
    private final MaxComputeWriteOptions writeOptions;
    private final String schemaOperatorUid;

    public SessionManageCoordinatedOperatorFactory(
            MaxComputeOptions options,
            MaxComputeWriteOptions writeOptions,
            String schemaOperatorUid) {
        this.options = options;
        this.writeOptions = writeOptions;
        this.schemaOperatorUid = schemaOperatorUid;
    }

    @Override
    public <T extends StreamOperator<Event>> T createStreamOperator(
            StreamOperatorParameters<Event> parameters) {
        OperatorIDGenerator schemaOperatorIdGenerator = new OperatorIDGenerator(schemaOperatorUid);
        SessionManageOperator operator =
                new SessionManageOperator(options, schemaOperatorIdGenerator.generate());
        TaskOperatorEventGateway taskOperatorEventGateway =
                parameters
                        .getContainingTask()
                        .getEnvironment()
                        .getOperatorCoordinatorEventGateway();
        operator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        operator.setTaskOperatorEventGateway(taskOperatorEventGateway);
        parameters
                .getOperatorEventDispatcher()
                .registerEventHandler(operator.getOperatorID(), operator);
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return SessionManageOperator.class;
    }

    @Override
    public Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new SessionManageCoordinator.SessionManageCoordinatorProvider(
                operatorName, operatorID, options, writeOptions);
    }
}
