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

package org.apache.flink.cdc.runtime.operators.schema.common;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventWithPreSchema;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A utility class to derive how evolved schemas should change. */
public class SchemaDerivator {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaDerivator.class);

    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);
    private final LoadingCache<Schema, List<RecordData.FieldGetter>> upstreamRecordGetterCache;
    private final LoadingCache<Schema, BinaryRecordDataGenerator> evolvedRecordWriterCache;

    public SchemaDerivator() {
        upstreamRecordGetterCache =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<Schema, List<RecordData.FieldGetter>>() {
                                    @Override
                                    public @Nonnull List<RecordData.FieldGetter> load(
                                            @Nonnull Schema schema) {
                                        return SchemaUtils.createFieldGetters(schema);
                                    }
                                });

        evolvedRecordWriterCache =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<Schema, BinaryRecordDataGenerator>() {
                                    @Override
                                    public @Nonnull BinaryRecordDataGenerator load(
                                            @Nonnull Schema schema) {
                                        return new BinaryRecordDataGenerator(
                                                schema.getColumnDataTypes()
                                                        .toArray(new DataType[0]));
                                    }
                                });
    }

    /** Get affected evolved table IDs based on changed upstream tables. */
    public static Set<TableId> getAffectedEvolvedTables(
            final TableIdRouter tableIdRouter, final Set<TableId> changedUpstreamTables) {
        return changedUpstreamTables.stream()
                .flatMap(cut -> tableIdRouter.route(cut).stream())
                .collect(Collectors.toSet());
    }

    /** For an evolved table ID, reverse lookup all upstream tables that it depends on. */
    public static Set<TableId> reverseLookupDependingUpstreamTables(
            final TableIdRouter tableIdRouter,
            final TableId evolvedTableId,
            final Set<TableId> upstreamSchemaTables) {
        return upstreamSchemaTables.stream()
                .filter(kut -> tableIdRouter.route(kut).contains(evolvedTableId))
                .collect(Collectors.toSet());
    }

    /** For an evolved table ID, reverse lookup all upstream tables that it depends on. */
    public static Set<TableId> reverseLookupDependingUpstreamTables(
            final TableIdRouter tableIdRouter,
            final TableId evolvedTableId,
            final Table<TableId, Integer, Schema> upstreamSchemaTable) {
        return upstreamSchemaTable.rowKeySet().stream()
                .filter(kut -> tableIdRouter.route(kut).contains(evolvedTableId))
                .collect(Collectors.toSet());
    }

    /** For an evolved table ID, reverse lookup all upstream schemas that needs to be fit in. */
    public static Set<Schema> reverseLookupDependingUpstreamSchemas(
            final TableIdRouter tableIdRouter,
            final TableId evolvedTableId,
            final SchemaManager schemaManager) {
        return reverseLookupDependingUpstreamTables(
                        tableIdRouter, evolvedTableId, schemaManager.getAllOriginalTables())
                .stream()
                .map(utid -> schemaManager.getLatestOriginalSchema(utid).get())
                .collect(Collectors.toSet());
    }

    /** For an evolved table ID, reverse lookup all upstream schemas that needs to be fit in. */
    public static Set<Schema> reverseLookupDependingUpstreamSchemas(
            final TableIdRouter tableIdRouter,
            final TableId evolvedTableId,
            final Table<TableId, Integer, Schema> upstreamSchemaTable) {
        return reverseLookupDependingUpstreamTables(
                        tableIdRouter, evolvedTableId, upstreamSchemaTable)
                .stream()
                .flatMap(utid -> upstreamSchemaTable.row(utid).values().stream())
                .collect(Collectors.toSet());
    }

    /**
     * Rewrite {@link SchemaChangeEvent}s by current {@link SchemaChangeBehavior} and include /
     * exclude them by fine-grained schema change event configurations.
     */
    public static List<SchemaChangeEvent> normalizeSchemaChangeEvents(
            Schema oldSchema,
            List<SchemaChangeEvent> schemaChangeEvents,
            SchemaChangeBehavior schemaChangeBehavior,
            MetadataApplier metadataApplier) {
        List<SchemaChangeEvent> rewrittenSchemaChangeEvents =
                rewriteSchemaChangeEvents(oldSchema, schemaChangeEvents, schemaChangeBehavior);
        rewrittenSchemaChangeEvents.forEach(
                evt -> {
                    if (evt instanceof SchemaChangeEventWithPreSchema) {
                        SchemaChangeEventWithPreSchema eventNeedsPreSchema =
                                (SchemaChangeEventWithPreSchema) evt;
                        if (!eventNeedsPreSchema.hasPreSchema()) {
                            eventNeedsPreSchema.fillPreSchema(oldSchema);
                        }
                    }
                });

        List<SchemaChangeEvent> finalSchemaChangeEvents = new ArrayList<>();
        for (SchemaChangeEvent schemaChangeEvent : rewrittenSchemaChangeEvents) {
            if (metadataApplier.acceptsSchemaEvolutionType(schemaChangeEvent.getType())) {
                finalSchemaChangeEvents.add(schemaChangeEvent);
            } else {
                LOG.info("Ignored schema change {}.", schemaChangeEvent);
            }
        }
        return finalSchemaChangeEvents;
    }

    private static List<SchemaChangeEvent> rewriteSchemaChangeEvents(
            Schema oldSchema,
            List<SchemaChangeEvent> schemaChangeEvents,
            SchemaChangeBehavior schemaChangeBehavior) {
        switch (schemaChangeBehavior) {
            case EVOLVE:
            case TRY_EVOLVE:
            case EXCEPTION:
                return schemaChangeEvents;
            case LENIENT:
                return schemaChangeEvents.stream()
                        .flatMap(evt -> lenientizeSchemaChangeEvent(oldSchema, evt))
                        .collect(Collectors.toList());
            case IGNORE:
                return schemaChangeEvents.stream()
                        .filter(e -> e instanceof CreateTableEvent)
                        .collect(Collectors.toList());
            default:
                throw new IllegalArgumentException(
                        "Unexpected schema change behavior: " + schemaChangeBehavior);
        }
    }

    private static Stream<SchemaChangeEvent> lenientizeSchemaChangeEvent(
            Schema oldSchema, SchemaChangeEvent schemaChangeEvent) {
        TableId tableId = schemaChangeEvent.tableId();
        switch (schemaChangeEvent.getType()) {
            case ADD_COLUMN:
                return lenientizeAddColumnEvent((AddColumnEvent) schemaChangeEvent, tableId);
            case DROP_COLUMN:
                return lenientizeDropColumnEvent(
                        oldSchema, (DropColumnEvent) schemaChangeEvent, tableId);
            case RENAME_COLUMN:
                return lenientizeRenameColumnEvent(
                        oldSchema, (RenameColumnEvent) schemaChangeEvent, tableId);
            default:
                return Stream.of(schemaChangeEvent);
        }
    }

    private static Stream<SchemaChangeEvent> lenientizeRenameColumnEvent(
            Schema oldSchema, RenameColumnEvent schemaChangeEvent, TableId tableId) {
        List<AddColumnEvent.ColumnWithPosition> appendColumns = new ArrayList<>();
        Map<String, DataType> convertNullableColumns = new HashMap<>();
        schemaChangeEvent
                .getNameMapping()
                .forEach(
                        (oldColName, newColName) -> {
                            Column column =
                                    oldSchema
                                            .getColumn(oldColName)
                                            .orElseThrow(
                                                    () ->
                                                            new IllegalArgumentException(
                                                                    "Non-existed column "
                                                                            + oldColName
                                                                            + " in evolved schema."));
                            if (!column.getType().isNullable()) {
                                // It's a not-nullable column, we need to cast it to
                                // nullable first
                                convertNullableColumns.put(oldColName, column.getType().nullable());
                            }
                            appendColumns.add(
                                    new AddColumnEvent.ColumnWithPosition(
                                            column.copy(newColName)
                                                    .copy(column.getType().nullable())));
                        });

        List<SchemaChangeEvent> events = new ArrayList<>();
        events.add(new AddColumnEvent(tableId, appendColumns));
        if (!convertNullableColumns.isEmpty()) {
            events.add(new AlterColumnTypeEvent(tableId, convertNullableColumns));
        }
        return events.stream();
    }

    private static Stream<SchemaChangeEvent> lenientizeDropColumnEvent(
            Schema oldSchema, DropColumnEvent schemaChangeEvent, TableId tableId) {
        Map<String, DataType> convertNullableColumns =
                schemaChangeEvent.getDroppedColumnNames().stream()
                        .map(oldSchema::getColumn)
                        .flatMap(e -> e.map(Stream::of).orElse(Stream.empty()))
                        .filter(col -> !col.getType().isNullable())
                        .collect(
                                Collectors.toMap(
                                        Column::getName, column -> column.getType().nullable()));

        if (convertNullableColumns.isEmpty()) {
            return Stream.empty();
        } else {
            return Stream.of(new AlterColumnTypeEvent(tableId, convertNullableColumns));
        }
    }

    private static Stream<SchemaChangeEvent> lenientizeAddColumnEvent(
            AddColumnEvent schemaChangeEvent, TableId tableId) {
        return Stream.of(
                new AddColumnEvent(
                        tableId,
                        schemaChangeEvent.getAddedColumns().stream()
                                .map(
                                        col ->
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                col.getAddColumn().getName(),
                                                                col.getAddColumn()
                                                                        .getType()
                                                                        .nullable(),
                                                                col.getAddColumn().getComment(),
                                                                col.getAddColumn()
                                                                        .getDefaultValueExpression())))
                                .collect(Collectors.toList())));
    }

    /** Coerce a {@link DataChangeEvent} from upstream to expected downstream schema. */
    public Optional<DataChangeEvent> coerceDataRecord(
            String timezone,
            DataChangeEvent dataChangeEvent,
            Schema upstreamSchema,
            @Nullable Schema evolvedSchema) {
        if (evolvedSchema == null) {
            // Sink does not recognize this tableId, might have been dropped, just ignore it.
            return Optional.empty();
        }

        if (upstreamSchema.equals(evolvedSchema)) {
            // If there's no schema difference, just return the original event.
            return Optional.of(dataChangeEvent);
        }

        List<RecordData.FieldGetter> upstreamSchemaReader =
                upstreamRecordGetterCache.getUnchecked(upstreamSchema);
        BinaryRecordDataGenerator evolvedSchemaWriter =
                evolvedRecordWriterCache.getUnchecked(evolvedSchema);

        // Coerce binary data records
        if (dataChangeEvent.before() != null) {
            List<Object> upstreamFields =
                    SchemaUtils.restoreOriginalData(dataChangeEvent.before(), upstreamSchemaReader);
            Object[] coercedRow =
                    SchemaMergingUtils.coerceRow(
                            timezone, evolvedSchema, upstreamSchema, upstreamFields);

            dataChangeEvent =
                    DataChangeEvent.projectBefore(
                            dataChangeEvent, evolvedSchemaWriter.generate(coercedRow));
        }

        if (dataChangeEvent.after() != null) {
            List<Object> upstreamFields =
                    SchemaUtils.restoreOriginalData(dataChangeEvent.after(), upstreamSchemaReader);
            Object[] coercedRow =
                    SchemaMergingUtils.coerceRow(
                            timezone, evolvedSchema, upstreamSchema, upstreamFields);

            dataChangeEvent =
                    DataChangeEvent.projectAfter(
                            dataChangeEvent, evolvedSchemaWriter.generate(coercedRow));
        }

        return Optional.of(dataChangeEvent);
    }

    /** Deduce merged CreateTableEvent. */
    public static List<CreateTableEvent> deduceMergedCreateTableEvent(
            TableIdRouter router, List<CreateTableEvent> createTableEvents) {
        Set<TableId> originalTables =
                createTableEvents.stream()
                        .map(CreateTableEvent::tableId)
                        .collect(Collectors.toSet());

        List<Set<TableId>> sourceTablesByRouteRule =
                router.groupSourceTablesByRouteRule(originalTables);
        Map<TableId, Schema> sourceTableIdToSchemaMap =
                createTableEvents.stream()
                        .collect(
                                Collectors.toMap(
                                        CreateTableEvent::tableId, CreateTableEvent::getSchema));
        Map<TableId, Schema> sinkTableIdToSchemaMap = new HashMap<>();
        Set<TableId> routedTables = new HashSet<>();
        for (Set<TableId> sourceTables : sourceTablesByRouteRule) {
            List<Schema> toBeMergedSchemas = new ArrayList<>();
            for (TableId tableId : sourceTables) {
                toBeMergedSchemas.add(sourceTableIdToSchemaMap.get(tableId));
                routedTables.add(tableId);
            }
            if (toBeMergedSchemas.isEmpty()) {
                continue;
            }
            Schema mergedSchema = SchemaMergingUtils.getCommonSchema(toBeMergedSchemas);

            for (TableId tableId : sourceTables) {
                List<TableId> sinkTableIds = router.route(tableId);
                for (TableId sinkTableId : sinkTableIds) {
                    sinkTableIdToSchemaMap.put(sinkTableId, mergedSchema);
                }
            }
        }
        for (TableId tableId : originalTables) {
            if (!sinkTableIdToSchemaMap.containsKey(tableId) && !routedTables.contains(tableId)) {
                sinkTableIdToSchemaMap.put(tableId, sourceTableIdToSchemaMap.get(tableId));
            }
        }
        return sinkTableIdToSchemaMap.entrySet().stream()
                .map(entry -> new CreateTableEvent(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }
}
