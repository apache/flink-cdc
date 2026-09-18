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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.ExistingTableSchemaExpansionMode;
import org.apache.flink.cdc.common.pipeline.RouteMode;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.route.TableIdRouter;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.ExistingTableSchemaExpansionSupport;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.AbstractStreamOperatorAdapter;
import org.apache.flink.cdc.runtime.operators.schema.common.ExistingTableSchemaExpander;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaDerivator;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** The operator will apply create table event and router mapper in batch mode. */
@Internal
public class BatchSchemaOperator extends AbstractStreamOperatorAdapter<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BatchSchemaOperator.class);

    // Final fields that are set in constructor
    private final String timezone;
    private final List<RouteRule> routingRules;
    private final RouteMode routeMode;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final ExistingTableSchemaExpansionMode existingTableSchemaExpansionMode;

    // Transient fields that are set during open()
    private transient volatile Map<TableId, Schema> originalSchemaMap;
    private transient volatile Map<TableId, Schema> evolvedSchemaMap;
    private transient TableIdRouter router;
    private transient SchemaDerivator derivator;
    private transient ExistingTableSchemaExpander existingTableSchemaExpander;
    protected transient SchemaManager schemaManager;
    protected MetadataApplier metadataApplier;
    private boolean alreadyMergedCreateTableTables = false;

    public BatchSchemaOperator(
            List<RouteRule> routingRules,
            RouteMode routeMode,
            MetadataApplier metadataApplier,
            String timezone) {
        this(
                routingRules,
                routeMode,
                metadataApplier,
                SchemaChangeBehavior.IGNORE,
                ExistingTableSchemaExpansionMode.OFF,
                timezone);
    }

    public BatchSchemaOperator(
            List<RouteRule> routingRules,
            RouteMode routeMode,
            MetadataApplier metadataApplier,
            SchemaChangeBehavior schemaChangeBehavior,
            ExistingTableSchemaExpansionMode existingTableSchemaExpansionMode,
            String timezone) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.timezone = timezone;
        this.routingRules = routingRules;
        this.routeMode = routeMode;
        this.metadataApplier = metadataApplier;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.existingTableSchemaExpansionMode = existingTableSchemaExpansionMode;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.originalSchemaMap = new HashMap<>();
        this.evolvedSchemaMap = new HashMap<>();
        this.router = new TableIdRouter(routingRules, routeMode);
        this.derivator = new SchemaDerivator();
        this.schemaManager = new SchemaManager(SchemaChangeBehavior.IGNORE);
        // TRY_EXPAND/EXPAND never run under IGNORE/EXCEPTION, so skip their initialization there.
        // CHECK guards the initial table state and initializes regardless of the behavior.
        if (existingTableSchemaExpansionMode != ExistingTableSchemaExpansionMode.OFF
                && (existingTableSchemaExpansionMode == ExistingTableSchemaExpansionMode.CHECK
                        || (schemaChangeBehavior != SchemaChangeBehavior.IGNORE
                                && schemaChangeBehavior != SchemaChangeBehavior.EXCEPTION))) {
            try {
                Optional<ExistingTableSchemaExpansionSupport> supportOptional =
                        metadataApplier.getExistingTableSchemaExpansionSupport();
                if (supportOptional.isPresent()) {
                    this.existingTableSchemaExpander =
                            new ExistingTableSchemaExpander(
                                    metadataApplier,
                                    supportOptional.get(),
                                    schemaChangeBehavior,
                                    existingTableSchemaExpansionMode);
                } else {
                    String message =
                            String.format(
                                    "Existing target table schema expansion is enabled with mode %s, but MetadataApplier %s does not support it.",
                                    existingTableSchemaExpansionMode,
                                    metadataApplier.getClass().getName());
                    if (existingTableSchemaExpansionMode
                            == ExistingTableSchemaExpansionMode.TRY_EXPAND) {
                        LOG.warn("{}. The sink's original schema handling will be used.", message);
                    } else {
                        throw new FlinkRuntimeException(message);
                    }
                }
            } catch (Exception e) {
                String message =
                        String.format(
                                "Failed to initialize existing target table schema expansion with mode %s.",
                                existingTableSchemaExpansionMode);
                if (existingTableSchemaExpansionMode
                        == ExistingTableSchemaExpansionMode.TRY_EXPAND) {
                    LOG.warn("{}. The sink's original schema handling will be used.", message, e);
                } else {
                    throw new FlinkRuntimeException(message, e);
                }
            }
        }
    }

    /**
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     */
    @Override
    public void processElement(StreamRecord<Event> streamRecord) throws Exception {
        Event event = streamRecord.getValue();
        // Only catch create table event and data change event in batch mode
        if (event instanceof CreateTableEvent) {
            handleCreateTableEvent((CreateTableEvent) event);
        } else if (event instanceof DataChangeEvent) {
            if (!alreadyMergedCreateTableTables) {
                handleFirstDataChangeEvent();
                alreadyMergedCreateTableTables = true;
            }
            handleDataChangeEvent((DataChangeEvent) event);
        } else {
            throw new RuntimeException("Unknown event type in Batch record: " + event);
        }
    }

    private void handleCreateTableEvent(CreateTableEvent originalEvent) throws Exception {
        originalSchemaMap.put(originalEvent.tableId(), originalEvent.getSchema());
    }

    private void handleFirstDataChangeEvent() {
        List<CreateTableEvent> originalCreateTableEvents =
                originalSchemaMap.entrySet().stream()
                        .map(entry -> new CreateTableEvent(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());
        List<CreateTableEvent> deducedCreateTableEvents =
                SchemaDerivator.deduceMergedCreateTableEvent(router, originalCreateTableEvents);
        deducedCreateTableEvents.forEach(
                createTableEvent -> {
                    evolvedSchemaMap.put(createTableEvent.tableId(), createTableEvent.getSchema());
                    applyAndUpdateEvolvedSchemaChange(createTableEvent);
                    output.collect(new StreamRecord<>(createTableEvent));
                });
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

    private boolean applyAndUpdateEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        boolean shouldApplyOriginalCreateTable = true;
        if (existingTableSchemaExpander != null && schemaChangeEvent instanceof CreateTableEvent) {
            shouldApplyOriginalCreateTable =
                    existingTableSchemaExpander.expand((CreateTableEvent) schemaChangeEvent);
        }
        try {
            if (shouldApplyOriginalCreateTable) {
                metadataApplier.applySchemaChange(schemaChangeEvent);
            }
            schemaManager.applyEvolvedSchemaChange(schemaChangeEvent);
            LOG.info(
                    "Successfully applied schema change event {} to external system.",
                    schemaChangeEvent);
            return true;
        } catch (Throwable t) {
            throw t;
        }
    }
}
