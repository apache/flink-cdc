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

package org.apache.flink.cdc.runtime.operators.schema.regular;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaDerivator;
import org.apache.flink.cdc.runtime.operators.schema.common.TableIdRouter;
import org.apache.flink.cdc.runtime.operators.schema.common.metrics.SchemaOperatorMetrics;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeResponse;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;

/**
 * The operator will evolve schemas in {@link
 * org.apache.flink.cdc.runtime.operators.schema.regular.SchemaCoordinator} for incoming {@link
 * SchemaChangeEvent}s and block the stream for tables before their schema changes finish.
 */
@Internal
public class SchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaOperator.class);

    // Final fields that are set in constructor
    private final String timezone;
    private final Duration rpcTimeout;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final List<RouteRule> routingRules;

    // Transient fields that are set during open()
    private transient int subTaskId;
    private transient TaskOperatorEventGateway toCoordinator;
    private transient SchemaOperatorMetrics schemaOperatorMetrics;
    private transient volatile Map<TableId, Schema> originalSchemaMap;
    private transient volatile Map<TableId, Schema> evolvedSchemaMap;
    private transient TableIdRouter router;
    private transient SchemaDerivator derivator;

    @VisibleForTesting
    public SchemaOperator(List<RouteRule> routingRules) {
        this(routingRules, DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT);
    }

    @VisibleForTesting
    public SchemaOperator(List<RouteRule> routingRules, Duration rpcTimeOut) {
        this(routingRules, rpcTimeOut, SchemaChangeBehavior.EVOLVE);
    }

    @VisibleForTesting
    public SchemaOperator(
            List<RouteRule> routingRules,
            Duration rpcTimeOut,
            SchemaChangeBehavior schemaChangeBehavior) {
        this(routingRules, rpcTimeOut, schemaChangeBehavior, "UTC");
    }

    public SchemaOperator(
            List<RouteRule> routingRules,
            Duration rpcTimeOut,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeout = rpcTimeOut;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.timezone = timezone;
        this.routingRules = routingRules;
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
    public void open() throws Exception {
        super.open();
        this.schemaOperatorMetrics =
                new SchemaOperatorMetrics(
                        getRuntimeContext().getMetricGroup(), schemaChangeBehavior);
        this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.originalSchemaMap = new HashMap<>();
        this.evolvedSchemaMap = new HashMap<>();
        this.router = new TableIdRouter(routingRules);
        this.derivator = new SchemaDerivator();
    }

    /**
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     */
    @Override
    public void processElement(StreamRecord<Event> streamRecord) throws Exception {
        Event event = streamRecord.getValue();
        if (event instanceof SchemaChangeEvent) {
            handleSchemaChangeEvent((SchemaChangeEvent) event);
        } else if (event instanceof DataChangeEvent) {
            handleDataChangeEvent((DataChangeEvent) event);
        } else {
            throw new RuntimeException("Unknown event type in Stream record: " + event);
        }
    }

    private void handleSchemaChangeEvent(SchemaChangeEvent originalEvent) throws Exception {
        // First, update original schema map unconditionally and it will never fail
        TableId tableId = originalEvent.tableId();
        originalSchemaMap.compute(
                tableId,
                (tId, schema) -> SchemaUtils.applySchemaChangeEvent(schema, originalEvent));
        schemaOperatorMetrics.increaseSchemaChangeEvents(1);

        // First, send FlushEvent or it might be blocked later
        List<TableId> sinkTables = router.route(tableId);
        LOG.info("{}> Sending the FlushEvent.", subTaskId);
        output.collect(
                new StreamRecord<>(new FlushEvent(subTaskId, sinkTables, originalEvent.getType())));

        LOG.info("{}> Going to request schema change...", subTaskId);

        // Then, queue to request schema change to SchemaCoordinator.
        SchemaChangeResponse response = requestSchemaChange(tableId, originalEvent);

        LOG.info(
                "{}> Finished schema change events: {}",
                subTaskId,
                response.getAppliedSchemaChangeEvents());
        LOG.info("{}> Refreshed evolved schemas: {}", subTaskId, response.getEvolvedSchemas());

        // After this request got successfully applied to DBMS, we can...
        List<SchemaChangeEvent> finishedSchemaChangeEvents =
                response.getAppliedSchemaChangeEvents();

        // Update local evolved schema map's cache
        evolvedSchemaMap.putAll(response.getEvolvedSchemas());

        // and emit the finished event to downstream
        for (SchemaChangeEvent finishedEvent : finishedSchemaChangeEvents) {
            output.collect(new StreamRecord<>(finishedEvent));
        }

        schemaOperatorMetrics.increaseFinishedSchemaChangeEvents(finishedSchemaChangeEvents.size());
    }

    private void handleDataChangeEvent(DataChangeEvent dataChangeEvent) {
        TableId tableId = dataChangeEvent.tableId();

        // First, we obtain the original schema corresponding to this data change event
        Schema originalSchema = originalSchemaMap.get(dataChangeEvent.tableId());

        // Then, for each routing terminus, coerce data records to the expected schema
        for (TableId sinkTableId : router.route(tableId)) {
            Schema evolvedSchema = evolvedSchemaMap.get(sinkTableId);
            DataChangeEvent coercedDataRecord =
                    derivator
                            .coerceDataRecord(
                                    timezone,
                                    DataChangeEvent.route(dataChangeEvent, sinkTableId),
                                    originalSchema,
                                    evolvedSchema)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    String.format(
                                                            "Unable to coerce data record from %s (schema: %s) to %s (schema: %s)",
                                                            tableId,
                                                            originalSchema,
                                                            sinkTableId,
                                                            evolvedSchema)));
            output.collect(new StreamRecord<>(coercedDataRecord));
        }
    }

    private SchemaChangeResponse requestSchemaChange(
            TableId tableId, SchemaChangeEvent schemaChangeEvent) {
        return sendRequestToCoordinator(
                new SchemaChangeRequest(tableId, schemaChangeEvent, subTaskId));
    }

    private <REQUEST extends CoordinationRequest, RESPONSE extends CoordinationResponse>
            RESPONSE sendRequestToCoordinator(REQUEST request) {
        try {
            CompletableFuture<CoordinationResponse> responseFuture =
                    toCoordinator.sendRequestToCoordinator(
                            getOperatorID(), new SerializedValue<>(request));
            return CoordinationResponseUtils.unwrap(
                    responseFuture.get(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to send request to coordinator: " + request.toString(), e);
        }
    }

    @VisibleForTesting
    public void registerInitialSchema(TableId tableId, Schema schema) {
        originalSchemaMap.put(tableId, schema);
        evolvedSchemaMap.put(tableId, schema);
    }
}
