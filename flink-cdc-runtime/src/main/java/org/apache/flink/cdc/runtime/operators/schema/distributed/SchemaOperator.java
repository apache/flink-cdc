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

package org.apache.flink.cdc.runtime.operators.schema.distributed;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaDerivator;
import org.apache.flink.cdc.runtime.operators.schema.common.TableIdRouter;
import org.apache.flink.cdc.runtime.operators.schema.common.metrics.SchemaOperatorMetrics;
import org.apache.flink.cdc.runtime.operators.schema.distributed.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.distributed.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava31.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** This operator merges upstream inferred schema into a centralized Schema Registry. */
public class SchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<PartitioningEvent, Event>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaOperator.class);

    // Final fields that are set upon construction
    private final Duration rpcTimeOut;
    private final String timezone;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final List<RouteRule> routingRules;

    public SchemaOperator(
            List<RouteRule> routingRules,
            Duration rpcTimeOut,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        this.routingRules = routingRules;
        this.rpcTimeOut = rpcTimeOut;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.timezone = timezone;
    }

    // Transient fields that are set when operator is running
    private transient TaskOperatorEventGateway toCoordinator;
    private transient int subTaskId;

    // Trace schema by both TableId and source partition
    private transient volatile Table<TableId, Integer, Schema> upstreamSchemaTable;
    private transient volatile Map<TableId, Schema> evolvedSchemaMap;
    private transient TableIdRouter tableIdRouter;
    private transient SchemaDerivator derivator;
    private transient SchemaOperatorMetrics schemaOperatorMetrics;

    @Override
    public void open() throws Exception {
        super.open();
        subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        upstreamSchemaTable = HashBasedTable.create();
        evolvedSchemaMap = new HashMap<>();
        tableIdRouter = new TableIdRouter(routingRules);
        derivator = new SchemaDerivator();
        this.schemaOperatorMetrics =
                new SchemaOperatorMetrics(
                        getRuntimeContext().getMetricGroup(), schemaChangeBehavior);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        this.toCoordinator = containingTask.getEnvironment().getOperatorCoordinatorEventGateway();
    }

    @Override
    public void processElement(StreamRecord<PartitioningEvent> streamRecord) throws Exception {
        // Unpack partitioned events
        PartitioningEvent partitioningEvent = streamRecord.getValue();
        Event event = partitioningEvent.getPayload();
        int sourcePartition = partitioningEvent.getSourcePartition();

        if (event instanceof SchemaChangeEvent) {
            schemaOperatorMetrics.increaseSchemaChangeEvents(1);

            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();

            // First, update upstream schema map unconditionally and it will never fail
            Schema beforeSchema = upstreamSchemaTable.get(tableId, sourcePartition);
            Schema afterSchema =
                    SchemaUtils.applySchemaChangeEvent(beforeSchema, schemaChangeEvent);
            upstreamSchemaTable.put(tableId, sourcePartition, afterSchema);

            // Check if we need to send a schema change request to the coordinator
            if (!(schemaChangeEvent instanceof CreateTableEvent)) {
                if (schemaChangeBehavior == SchemaChangeBehavior.IGNORE) {
                    LOG.info("{}> Schema change event {} has been ignored.", subTaskId, event);
                    schemaOperatorMetrics.increaseIgnoredSchemaChangeEvents(1);
                    return;
                } else if (schemaChangeBehavior == SchemaChangeBehavior.EXCEPTION) {
                    throw new SchemaEvolveException(
                            schemaChangeEvent,
                            "Unexpected schema change events occurred in EXCEPTION mode. Job will fail now.");
                }
            }

            // Then, notify this information to the coordinator
            requestSchemaChange(
                    tableId,
                    new SchemaChangeRequest(sourcePartition, subTaskId, schemaChangeEvent));
            schemaOperatorMetrics.increaseFinishedSchemaChangeEvents(1);
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            TableId tableId = dataChangeEvent.tableId();

            // First, we obtain the upstream schema corresponding to this data change event
            Schema upstreamSchema =
                    upstreamSchemaTable.get(dataChangeEvent.tableId(), sourcePartition);

            // Then, for each routing terminus, coerce data records to the expected schema
            for (TableId sinkTableId : tableIdRouter.route(tableId)) {
                Schema evolvedSchema = evolvedSchemaMap.get(sinkTableId);

                DataChangeEvent coercedDataRecord =
                        derivator
                                .coerceDataRecord(
                                        timezone,
                                        DataChangeEvent.route(dataChangeEvent, sinkTableId),
                                        upstreamSchema,
                                        evolvedSchema)
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        String.format(
                                                                "Unable to coerce data record from %s (schema: %s) to %s (schema: %s)",
                                                                tableId,
                                                                upstreamSchema,
                                                                sinkTableId,
                                                                evolvedSchema)));
                output.collect(new StreamRecord<>(coercedDataRecord));
            }
        } else {
            throw new IllegalStateException(
                    subTaskId + "> SchemaOperator received an unexpected event: " + event);
        }
    }

    private void requestSchemaChange(
            TableId sourceTableId, SchemaChangeRequest schemaChangeRequest) {
        LOG.info("{}> Sent FlushEvent to downstream...", subTaskId);
        output.collect(
                new StreamRecord<>(
                        new FlushEvent(
                                subTaskId,
                                tableIdRouter.route(sourceTableId),
                                schemaChangeRequest.getSchemaChangeEvent().getType())));

        LOG.info("{}> Sending evolve request...", subTaskId);
        SchemaChangeResponse response = sendRequestToCoordinator(schemaChangeRequest);

        LOG.info("{}> Evolve request response: {}", subTaskId, response);

        // Update local evolved schema cache
        evolvedSchemaMap.putAll(response.getEvolvedSchemas());

        // And emit schema change events to downstream
        response.getEvolvedSchemaChangeEvents()
                .forEach(evt -> output.collect(new StreamRecord<>(evt)));
        LOG.info(
                "{}> Successfully updated evolved schema cache. Current state: {}",
                subTaskId,
                evolvedSchemaMap);
    }

    private <REQUEST extends CoordinationRequest, RESPONSE extends CoordinationResponse>
            RESPONSE sendRequestToCoordinator(REQUEST request) {
        try {
            CompletableFuture<CoordinationResponse> responseFuture =
                    toCoordinator.sendRequestToCoordinator(
                            getOperatorID(), new SerializedValue<>(request));
            return CoordinationResponseUtils.unwrap(
                    responseFuture.get(rpcTimeOut.toMillis(), TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to send request to coordinator: " + request.toString(), e);
        }
    }
}
