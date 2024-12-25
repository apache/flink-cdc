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
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaDerivator;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaManager;
import org.apache.flink.cdc.runtime.operators.schema.common.TableIdRouter;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The operator will apply create table event and router mapper in batch mode. */
@Internal
public class SchemaBatchOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaBatchOperator.class);

    // Final fields that are set in constructor
    private final String timezone;
    private final List<RouteRule> routingRules;

    // Transient fields that are set during open()
    private transient volatile Map<TableId, Schema> originalSchemaMap;
    private transient volatile Map<TableId, Schema> evolvedSchemaMap;
    private transient TableIdRouter router;
    private transient SchemaDerivator derivator;
    protected transient SchemaManager schemaManager;
    protected MetadataApplier metadataApplier;

    public SchemaBatchOperator(
            List<RouteRule> routingRules, MetadataApplier metadataApplier, String timezone) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.timezone = timezone;
        this.routingRules = routingRules;
        this.metadataApplier = metadataApplier;
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
        this.router = new TableIdRouter(routingRules);
        this.derivator = new SchemaDerivator();
        this.schemaManager = new SchemaManager(SchemaChangeBehavior.IGNORE);
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
            handleDataChangeEvent((DataChangeEvent) event);
        } else {
            throw new RuntimeException("Unknown event type in Batch record: " + event);
        }
    }

    private void handleCreateTableEvent(CreateTableEvent originalEvent) throws Exception {
        TableId originalTableId = originalEvent.tableId();
        originalSchemaMap.put(originalTableId, originalEvent.getSchema());
        Schema currentUpstreamSchema =
                schemaManager.getLatestOriginalSchema(originalTableId).orElse(null);

        List<SchemaChangeEvent> deducedSchemaChangeEvents = new ArrayList<>();

        // For redundant schema change events (possibly coming from duplicate emitted
        // CreateTableEvents in snapshot stage), we just skip them.
        if (!SchemaUtils.isSchemaChangeEventRedundant(currentUpstreamSchema, originalEvent)) {
            schemaManager.applyOriginalSchemaChange(originalEvent);
            deducedSchemaChangeEvents.addAll(deduceEvolvedSchemaChanges(originalEvent));
        } else {
            LOG.info(
                    "Schema change event {} is redundant for current schema {}, just skip it.",
                    originalEvent,
                    currentUpstreamSchema);
        }

        // Update local evolved schema map's cache
        deducedSchemaChangeEvents.forEach(
                schemaChangeEvent -> {
                    if (schemaChangeEvent instanceof CreateTableEvent) {
                        evolvedSchemaMap.put(
                                schemaChangeEvent.tableId(),
                                ((CreateTableEvent) schemaChangeEvent).getSchema());
                        output.collect(new StreamRecord<>(schemaChangeEvent));
                    }
                });

        applySchemaChange(originalEvent, deducedSchemaChangeEvents);
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

    private List<SchemaChangeEvent> deduceEvolvedSchemaChanges(SchemaChangeEvent event) {
        LOG.info("Step 1 - Start deducing evolved schema change for {}", event);

        TableId originalTableId = event.tableId();
        List<SchemaChangeEvent> deducedSchemaChangeEvents = new ArrayList<>();
        Set<TableId> originalTables = schemaManager.getAllOriginalTables();

        // First, grab all affected evolved tables.
        Set<TableId> affectedEvolvedTables =
                SchemaDerivator.getAffectedEvolvedTables(
                        router, Collections.singleton(originalTableId));
        LOG.info("Step 2 - Affected downstream tables are: {}", affectedEvolvedTables);

        // For each affected table, we need to...
        for (TableId evolvedTableId : affectedEvolvedTables) {
            Schema currentEvolvedSchema =
                    schemaManager.getLatestEvolvedSchema(evolvedTableId).orElse(null);
            LOG.info(
                    "Step 3.1 - For to-be-evolved table {} with schema {}...",
                    evolvedTableId,
                    currentEvolvedSchema);

            // ... reversely look up this affected sink table's upstream dependency
            Set<TableId> upstreamDependencies =
                    SchemaDerivator.reverseLookupDependingUpstreamTables(
                            router, evolvedTableId, originalTables);
            Preconditions.checkArgument(
                    !upstreamDependencies.isEmpty(),
                    "An affected sink table's upstream dependency cannot be empty.");
            LOG.info("Step 3.2 - upstream dependency tables are: {}", upstreamDependencies);

            List<SchemaChangeEvent> rawSchemaChangeEvents = new ArrayList<>();
            if (upstreamDependencies.size() == 1) {
                // If it's a one-by-one routing rule, we can simply forward it to downstream sink.
                SchemaChangeEvent rawEvent = event.copy(evolvedTableId);
                rawSchemaChangeEvents.add(rawEvent);
                LOG.info(
                        "Step 3.3 - It's an one-by-one routing and could be forwarded as {}.",
                        rawEvent);
            } else {
                Set<Schema> toBeMergedSchemas =
                        SchemaDerivator.reverseLookupDependingUpstreamSchemas(
                                router, evolvedTableId, schemaManager);
                LOG.info("Step 3.3 - Upstream dependency schemas are: {}.", toBeMergedSchemas);

                // We're in a table routing mode now, so we need to infer a widest schema for all
                // upstream tables.
                Schema mergedSchema = currentEvolvedSchema;
                for (Schema toBeMergedSchema : toBeMergedSchemas) {
                    mergedSchema =
                            SchemaMergingUtils.getLeastCommonSchema(mergedSchema, toBeMergedSchema);
                }
                LOG.info("Step 3.4 - Deduced widest schema is: {}.", mergedSchema);

                // Detect what schema changes we need to apply to get expected sink table.
                List<SchemaChangeEvent> rawEvents =
                        SchemaMergingUtils.getSchemaDifference(
                                evolvedTableId, currentEvolvedSchema, mergedSchema);
                LOG.info(
                        "Step 3.5 - It's an many-to-one routing and causes schema changes: {}.",
                        rawEvents);

                rawSchemaChangeEvents.addAll(rawEvents);
            }

            // Finally, we normalize schema change events, including rewriting events by current
            // schema change behavior configuration, dropping explicitly excluded schema change
            // event types.
            List<SchemaChangeEvent> normalizedEvents =
                    SchemaDerivator.normalizeSchemaChangeEvents(
                            currentEvolvedSchema,
                            rawSchemaChangeEvents,
                            SchemaChangeBehavior.IGNORE,
                            metadataApplier);
            LOG.info(
                    "Step 4 - After being normalized with {} behavior, final schema change events are: {}",
                    SchemaChangeBehavior.IGNORE,
                    normalizedEvents);

            deducedSchemaChangeEvents.addAll(normalizedEvents);
        }

        return deducedSchemaChangeEvents;
    }

    /** Applies the schema change to the external system. */
    private void applySchemaChange(
            SchemaChangeEvent originalEvent, List<SchemaChangeEvent> deducedSchemaChangeEvents) {

        // Tries to apply it to external system
        List<SchemaChangeEvent> appliedSchemaChangeEvents = new ArrayList<>();
        for (SchemaChangeEvent event : deducedSchemaChangeEvents) {
            if (applyAndUpdateEvolvedSchemaChange(event)) {
                appliedSchemaChangeEvents.add(event);
            }
        }

        Map<TableId, Schema> refreshedEvolvedSchemas = new HashMap<>();

        // We need to retrieve all possibly modified evolved schemas and refresh SchemaOperator's
        // local cache since it might have been altered by another SchemaOperator instance.
        // SchemaChangeEvents doesn't need to be emitted to downstream (since it might be broadcast
        // from other SchemaOperators) though.
        for (TableId tableId : router.route(originalEvent.tableId())) {
            refreshedEvolvedSchemas.put(
                    tableId, schemaManager.getLatestEvolvedSchema(tableId).orElse(null));
        }
    }

    private boolean applyAndUpdateEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        try {
            metadataApplier.applySchemaChange(schemaChangeEvent);
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
