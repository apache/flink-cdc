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

package org.apache.flink.cdc.connectors.hudi.sink.operator;

import org.apache.flink.cdc.connectors.hudi.sink.coordinator.MultiTableStreamWriteOperatorCoordinator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;

import org.apache.hudi.sink.common.AbstractWriteOperator;
import org.apache.hudi.sink.event.Correspondent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom WriteOperatorFactory that creates our extended multi-table StreamWriteOperatorCoordinator
 * instead of Hudi's original single-table coordinator.
 *
 * <p>This factory ensures that multi-table CDC scenarios use the proper coordinator with:
 *
 * <ul>
 *   <li>Per-table client management
 *   <li>Per-table instant tracking
 *   <li>Dynamic table registration
 *   <li>Table-aware event routing
 * </ul>
 *
 * @param <I> The input type for the write operator
 */
public class MultiTableWriteOperatorFactory<I> extends SimpleUdfStreamOperatorFactory<RowData>
        implements CoordinatedOperatorFactory<RowData>, OneInputStreamOperatorFactory<I, RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableWriteOperatorFactory.class);
    private static final long serialVersionUID = 1L;

    private final Configuration conf;
    private final AbstractWriteOperator<I> writeOperator;

    public MultiTableWriteOperatorFactory(
            Configuration conf, AbstractWriteOperator<I> writeOperator) {
        super(writeOperator);
        this.conf = conf;
        this.writeOperator = writeOperator;
    }

    public static <IN> MultiTableWriteOperatorFactory<IN> instance(
            Configuration conf, AbstractWriteOperator<IN> writeOperator) {
        return new MultiTableWriteOperatorFactory<>(conf, writeOperator);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        LOG.info("Creating multi-table write operator with extended coordinator support");

        // necessary setting for the operator.
        super.createStreamOperator(parameters);

        final OperatorID operatorID = parameters.getStreamConfig().getOperatorID();
        final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();

        this.writeOperator.setCorrespondent(
                Correspondent.getInstance(
                        operatorID,
                        parameters
                                .getContainingTask()
                                .getEnvironment()
                                .getOperatorCoordinatorEventGateway()));
        this.writeOperator.setOperatorEventGateway(
                eventDispatcher.getOperatorEventGateway(operatorID));
        eventDispatcher.registerEventHandler(operatorID, writeOperator);
        return (T) writeOperator;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        LOG.info(
                "Creating multi-table StreamWriteOperatorCoordinator provider for operator: {}",
                operatorName);
        return new MultiTableStreamWriteOperatorCoordinator.Provider(operatorID, conf);
    }
}
