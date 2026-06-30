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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.AlterTableCommentEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.doris.utils.DorisSchemaUtils;
import org.apache.flink.cdc.connectors.doris.utils.DorisTypeUtils;
import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.catalog.doris.DorisSchemaFactory;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Field;
import org.apache.doris.flink.sink.schema.AddColumnPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Locale.ROOT;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.TRUNCATE_TABLE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.CHARSET_ENCODING;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SCHEMA_CHANGE_COLUMN_DEFAULT_VALUE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SCHEMA_CHANGE_COLUMN_NULL_ENABLE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_BUCKETS;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_EXTRA_SCHEMA;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PROPERTIES_PREFIX;

/** Supports {@link DorisDataSink} to schema evolution. */
public class DorisMetadataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(DorisMetadataApplier.class);
    private static final String DORIS_STRING_TYPE = "STRING";
    private static final String DORIS_STRING_KEY_TYPE = "VARCHAR(65533)";
    private static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    private static final long DORIS_SCHEMA_REFRESH_MAX_WAIT_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final long DORIS_SCHEMA_REFRESH_RETRY_MILLIS = 500L;

    private static final DorisSchemaFetcher DEFAULT_DORIS_SCHEMA_FETCHER =
            (dorisOptions, tableId) ->
                    RestService.getSchema(
                            dorisOptions, tableId.getSchemaName(), tableId.getTableName(), LOG);

    @FunctionalInterface
    interface DorisSchemaFetcher extends Serializable {
        org.apache.doris.flink.rest.models.Schema fetch(DorisOptions dorisOptions, TableId tableId);
    }

    private DorisOptions dorisOptions;
    private DorisSchemaChangeManager schemaChangeManager;
    private Configuration config;
    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;
    private Map<String, Integer> tableBucketsMap;
    private Map<TableId, Schema> schemaCache;
    private Map<TableId, List<String>> dorisColumnOrderCache;
    private final DorisSchemaFetcher dorisSchemaFetcher;
    private final DorisTableExistenceChecker tableExistenceChecker;

    public DorisMetadataApplier(DorisOptions dorisOptions, Configuration config) {
        this(
                dorisOptions,
                config,
                new DorisSchemaChangeManager(dorisOptions, config.get(CHARSET_ENCODING)),
                DEFAULT_DORIS_SCHEMA_FETCHER,
                DorisTableExistenceChecker.HTTP);
    }

    DorisMetadataApplier(
            DorisOptions dorisOptions,
            Configuration config,
            DorisSchemaChangeManager schemaChangeManager,
            DorisSchemaFetcher dorisSchemaFetcher) {
        this(
                dorisOptions,
                config,
                schemaChangeManager,
                dorisSchemaFetcher,
                (options, tableId) -> DorisTableExistenceChecker.Existence.TABLE_EXISTS);
    }

    DorisMetadataApplier(
            DorisOptions dorisOptions,
            Configuration config,
            DorisSchemaChangeManager schemaChangeManager,
            DorisSchemaFetcher dorisSchemaFetcher,
            DorisTableExistenceChecker tableExistenceChecker) {
        this.dorisOptions = dorisOptions;
        this.schemaChangeManager = schemaChangeManager;
        this.config = config;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
        this.tableBucketsMap = parseTableBuckets(config);
        this.schemaCache = new HashMap<>();
        this.dorisColumnOrderCache = new HashMap<>();
        this.dorisSchemaFetcher = dorisSchemaFetcher;
        this.tableExistenceChecker = tableExistenceChecker;
    }

    @VisibleForTesting
    static Map<String, Integer> parseTableBuckets(Configuration config) {
        Map<String, Integer> bucketsMap = new LinkedHashMap<>();
        config.getOptional(TABLE_BUCKETS)
                .ifPresent(
                        value -> {
                            String[] tableBucketsArray = value.split(",");
                            for (String tableBucket : tableBucketsArray) {
                                String[] parts = tableBucket.split(":");
                                bucketsMap.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                            }
                        });
        return bucketsMap;
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Sets.newHashSet(
                ADD_COLUMN,
                ALTER_COLUMN_TYPE,
                DROP_COLUMN,
                DROP_TABLE,
                RENAME_COLUMN,
                TRUNCATE_TABLE);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        SchemaChangeEventVisitor.<Void, SchemaEvolveException>visit(
                event,
                addColumnEvent -> {
                    applyAddColumnEvent(addColumnEvent);
                    return null;
                },
                alterColumnTypeEvent -> {
                    applyAlterColumnTypeEvent(alterColumnTypeEvent);
                    return null;
                },
                createTableEvent -> {
                    applyCreateTableEvent(createTableEvent);
                    return null;
                },
                dropColumnEvent -> {
                    applyDropColumnEvent(dropColumnEvent);
                    return null;
                },
                dropTableEvent -> {
                    applyDropTableEvent(dropTableEvent);
                    return null;
                },
                renameColumnEvent -> {
                    applyRenameColumnEvent(renameColumnEvent);
                    return null;
                },
                truncateTableEvent -> {
                    applyTruncateTableEvent(truncateTableEvent);
                    return null;
                },
                alterTableCommentEvent -> {
                    applyAlterTableCommentEvent(alterTableCommentEvent);
                    return null;
                });
    }

    private void applyCreateTableEvent(CreateTableEvent event) throws SchemaEvolveException {
        try {
            Schema schema = event.getSchema();
            TableId tableId = event.tableId();
            org.apache.doris.flink.rest.models.Schema existingDorisSchema =
                    fetchExistingDorisSchema(tableId);
            if (existingDorisSchema == null) {
                ensureSchemaChangeSucceeded(
                        schemaChangeManager.createTable(buildTableSchema(tableId, schema)),
                        "create table",
                        tableId);
                updateDorisOrderCache(tableId, schema.getColumnNames());
            } else {
                updateDorisOrderCache(
                        tableId, reconcileAndGetColumnOrder(tableId, schema, existingDorisSchema));
            }
            schemaCache.put(tableId, schema);
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private TableSchema buildTableSchema(TableId tableId, Schema schema) {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setTable(tableId.getTableName());
        tableSchema.setDatabase(tableId.getSchemaName());
        tableSchema.setModel(
                CollectionUtils.isEmpty(schema.primaryKeys())
                        ? DataModel.DUPLICATE
                        : DataModel.UNIQUE);
        Tuple2<String, String> partitionInfo =
                DorisSchemaUtils.getPartitionInfo(config, schema, tableId);
        List<String> keys = dorisKeyColumns(schema);
        tableSchema.setFields(
                buildFields(schema, partitionInfo == null ? null : partitionInfo.f0, keys));
        tableSchema.setKeys(keys);
        tableSchema.setDistributeKeys(keys);
        tableSchema.setTableComment(schema.comment());
        config.getOptional(TABLE_CREATE_EXTRA_SCHEMA)
                .ifPresent(
                        extraSchema ->
                                tableSchema.setExtraSchemaElements(
                                        DorisSchemaFactory.parseExtraSchemaElements(
                                                extraSchema, schema.getColumnNames())));

        Map<String, String> tableProperties =
                DorisDataSinkOptions.getPropertiesByPrefix(config, TABLE_CREATE_PROPERTIES_PREFIX);
        tableSchema.setProperties(tableProperties);
        tableSchema.setTableBuckets(
                DorisSchemaFactory.parseTableSchemaBuckets(
                        tableBucketsMap, tableId.getTableName()));

        if (partitionInfo != null) {
            LOG.info("Partition info of {} is: {}.", tableId.identifier(), partitionInfo);
            tableSchema.setPartitionInfo(partitionInfo);
        }
        return tableSchema;
    }

    private org.apache.doris.flink.rest.models.Schema fetchExistingDorisSchema(TableId tableId) {
        DorisTableExistenceChecker.Existence existence =
                tableExistenceChecker.check(dorisOptions, tableId);
        if (existence == DorisTableExistenceChecker.Existence.DATABASE_ABSENT
                || existence == DorisTableExistenceChecker.Existence.TABLE_ABSENT) {
            LOG.info(
                    "Doris table {} is absent before create-table handling "
                            + "(existence={}). Will try CREATE TABLE first.",
                    tableId.identifier(),
                    existence);
            return null;
        }

        try {
            org.apache.doris.flink.rest.models.Schema existingSchema =
                    dorisSchemaFetcher.fetch(dorisOptions, tableId);
            if (existingSchema == null) {
                throw new IllegalStateException(
                        "Doris table "
                                + tableId.identifier()
                                + " exists but schema lookup returned null");
            }
            return existingSchema;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to resolve existing Doris schema for {} before create-table handling. "
                            + "Aborting instead of assuming the table is absent.",
                    tableId.identifier(),
                    e);
            throw new IllegalStateException(
                    "Failed to resolve existing Doris schema for " + tableId.identifier(), e);
        }
    }

    private List<String> reconcileAndGetColumnOrder(
            TableId tableId,
            Schema desiredSchema,
            org.apache.doris.flink.rest.models.Schema existingDorisSchema)
            throws Exception {
        if (existingDorisSchema.getProperties() == null) {
            throw new IllegalStateException(
                    "Failed to resolve Doris physical schema properties for " + tableId);
        }

        logExistingColumnTypeDifferences(tableId, desiredSchema, existingDorisSchema);

        Schema currentSchema =
                buildCurrentSchemaFromExistingTable(desiredSchema, existingDorisSchema);
        List<String> currentColumnNames = extractDorisNames(existingDorisSchema);
        List<AddColumnEvent.ColumnWithPosition> missingColumns =
                resolveMissingColumns(tableId, desiredSchema, existingDorisSchema, currentSchema);

        if (missingColumns.isEmpty()) {
            LOG.info(
                    "Doris table {} already exists and covers all columns in the incoming CreateTableEvent.",
                    tableId.identifier());
            return currentColumnNames;
        }

        LOG.info(
                "Doris table {} already exists but is missing columns {} from the incoming CreateTableEvent. "
                        + "Applying ADD COLUMN reconciliation.",
                tableId.identifier(),
                missingColumns);

        for (AddColumnEvent.ColumnWithPosition missingColumn : missingColumns) {
            Column column = missingColumn.getAddColumn();
            AddColumnPlacement placement =
                    resolveAddColumnPosition(
                            tableId, missingColumn, currentSchema, currentColumnNames);
            ensureSchemaChangeSucceeded(
                    schemaChangeManager.addColumn(
                            tableId.getSchemaName(),
                            tableId.getTableName(),
                            buildAddFieldSchema(column),
                            placement.dorisPosition),
                    "add missing column " + column.getName(),
                    tableId);
            currentColumnNames =
                    tryRefreshOrderAfterAdd(
                            tableId, column.getName(), "add missing column " + column.getName());
            currentSchema =
                    SchemaUtils.applySchemaChangeEvent(
                            currentSchema,
                            new AddColumnEvent(tableId, Collections.singletonList(missingColumn)));
        }
        return currentColumnNames;
    }

    private void logExistingColumnTypeDifferences(
            TableId tableId,
            Schema desiredSchema,
            org.apache.doris.flink.rest.models.Schema existingDorisSchema) {
        Map<String, Field> existingFieldsByLowerCase = new LinkedHashMap<>();
        for (Field field : existingDorisSchema.getProperties()) {
            existingFieldsByLowerCase.putIfAbsent(field.getName().toLowerCase(ROOT), field);
        }

        for (Column desiredColumn : desiredSchema.getColumns()) {
            Field existingField =
                    existingFieldsByLowerCase.get(desiredColumn.getName().toLowerCase(ROOT));
            if (existingField == null) {
                continue;
            }

            DorisExistingColumnTypeAnalyzer.ExistingColumnTypeAssessment assessment =
                    DorisExistingColumnTypeAnalyzer.assess(desiredColumn, existingField);
            String desiredType = assessment.desiredType;
            String existingType = assessment.existingType;

            if (assessment.typeDefinitionDrift) {
                LOG.warn(
                        "Doris existing-table type drift detected. table={}, column={}, "
                                + "physicalType={}, desiredType={}, typeDefinitionDrift=true. "
                                + "Existing-table reconcile will not alter existing columns and "
                                + "will only add missing columns.",
                        tableId.identifier(),
                        existingField.getName(),
                        existingType,
                        desiredType);
            }
            if (assessment.lowConfidencePhysicalMetadata) {
                LOG.warn(
                        "Doris existing-table schema metadata has low confidence. table={}, "
                                + "column={}, physicalType={}, lowConfidencePhysicalMetadata=true. "
                                + "Doris FE did not expose complete type parameters. Verify SHOW "
                                + "CREATE TABLE before trusting runtime writes.",
                        tableId.identifier(),
                        existingField.getName(),
                        existingType);
            }
            if (assessment.capacityRisk) {
                LOG.warn(
                        "Doris existing-table column has capacity risk. table={}, column={}, "
                                + "physicalType={}, desiredType={}, capacityRisk=true. Job startup "
                                + "will continue, but data may be truncated or rejected during "
                                + "Stream Load.",
                        tableId.identifier(),
                        existingField.getName(),
                        existingType,
                        desiredType);
            }
            if (assessment.familyMismatch) {
                LOG.warn(
                        "Doris existing-table column has family mismatch. table={}, column={}, "
                                + "physicalType={}, desiredType={}, familyMismatch=true. Job "
                                + "startup will continue, but Stream Load may fail at runtime.",
                        tableId.identifier(),
                        existingField.getName(),
                        existingType,
                        desiredType);
            }
        }
    }

    private Schema buildCurrentSchemaFromExistingTable(
            Schema desiredSchema, org.apache.doris.flink.rest.models.Schema existingDorisSchema) {
        Map<String, String> existingColumnsByLowerCase = new LinkedHashMap<>();
        for (Field field : existingDorisSchema.getProperties()) {
            existingColumnsByLowerCase.putIfAbsent(
                    field.getName().toLowerCase(ROOT), field.getName());
        }

        List<Column> currentColumns = new ArrayList<>();
        for (Column desiredColumn : desiredSchema.getColumns()) {
            String physicalName =
                    existingColumnsByLowerCase.get(desiredColumn.getName().toLowerCase(ROOT));
            if (physicalName != null) {
                currentColumns.add(desiredColumn.copy(physicalName));
            }
        }

        List<String> currentPrimaryKeys =
                desiredSchema.primaryKeys().stream()
                        .map(key -> existingColumnsByLowerCase.get(key.toLowerCase(ROOT)))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

        List<String> currentPartitionKeys =
                desiredSchema.partitionKeys().stream()
                        .map(key -> existingColumnsByLowerCase.get(key.toLowerCase(ROOT)))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(currentColumns)
                .primaryKey(currentPrimaryKeys)
                .partitionKey(currentPartitionKeys)
                .options(desiredSchema.options())
                .comment(desiredSchema.comment())
                .build();
    }

    private List<AddColumnEvent.ColumnWithPosition> resolveMissingColumns(
            TableId tableId,
            Schema desiredSchema,
            org.apache.doris.flink.rest.models.Schema existingDorisSchema,
            Schema currentSchema) {
        Set<String> existingPhysicalColumns =
                existingDorisSchema.getProperties().stream()
                        .map(field -> field.getName().toLowerCase(ROOT))
                        .collect(Collectors.toSet());

        List<AddColumnEvent.ColumnWithPosition> missingColumns = new ArrayList<>();
        List<Column> desiredColumns = desiredSchema.getColumns();
        for (int i = 0; i < desiredColumns.size(); i++) {
            Column desiredColumn = desiredColumns.get(i);
            String normalizedDesiredName = desiredColumn.getName().toLowerCase(ROOT);
            if (existingPhysicalColumns.contains(normalizedDesiredName)) {
                continue;
            }

            AddColumnEvent.ColumnWithPosition position =
                    resolveCreateTableReconcilePosition(
                            desiredColumn, desiredColumns, i, currentSchema);
            missingColumns.add(position);
            currentSchema =
                    SchemaUtils.applySchemaChangeEvent(
                            currentSchema,
                            new AddColumnEvent(tableId, Collections.singletonList(position)));
            existingPhysicalColumns.add(normalizedDesiredName);
        }
        return missingColumns;
    }

    private AddColumnEvent.ColumnWithPosition resolveCreateTableReconcilePosition(
            Column desiredColumn,
            List<Column> desiredColumns,
            int targetIndex,
            Schema currentSchema) {
        Set<String> currentColumnNamesLowerCase =
                currentSchema.getColumnNames().stream()
                        .map(name -> name.toLowerCase(ROOT))
                        .collect(Collectors.toSet());

        for (int i = targetIndex - 1; i >= 0; i--) {
            Column previousColumn = desiredColumns.get(i);
            if (currentColumnNamesLowerCase.contains(previousColumn.getName().toLowerCase(ROOT))) {
                String currentColumnName =
                        DorisSchemaUtils.getColumnCaseInsensitive(
                                        currentSchema, previousColumn.getName())
                                .map(Column::getName)
                                .orElse(previousColumn.getName());
                return AddColumnEvent.after(desiredColumn, currentColumnName);
            }
        }

        for (int i = targetIndex + 1; i < desiredColumns.size(); i++) {
            Column nextColumn = desiredColumns.get(i);
            if (currentColumnNamesLowerCase.contains(nextColumn.getName().toLowerCase(ROOT))) {
                String currentColumnName =
                        DorisSchemaUtils.getColumnCaseInsensitive(
                                        currentSchema, nextColumn.getName())
                                .map(Column::getName)
                                .orElse(nextColumn.getName());
                return AddColumnEvent.before(desiredColumn, currentColumnName);
            }
        }

        return AddColumnEvent.last(desiredColumn);
    }

    private Map<String, FieldSchema> buildFields(
            Schema schema, String autoPartitionColumn, List<String> keyColumns) {
        // Guaranteed the order of column
        Map<String, FieldSchema> fieldSchemaMap = new LinkedHashMap<>();
        List<String> columnNameList = schema.getColumnNames();
        for (String columnName : columnNameList) {
            Column column = schema.getColumn(columnName).get();
            fieldSchemaMap.put(
                    column.getName(),
                    new FieldSchema(
                            column.getName(),
                            resolveColumnType(
                                    column, autoPartitionColumn, isKeyColumn(column, keyColumns)),
                            resolveColumnDefaultValue(column),
                            column.getComment()));
        }
        return fieldSchemaMap;
    }

    private boolean isKeyColumn(Column column, List<String> keyColumns) {
        return keyColumns.stream().anyMatch(key -> key.equalsIgnoreCase(column.getName()));
    }

    private List<String> dorisKeyColumns(Schema schema) {
        if (!CollectionUtil.isNullOrEmpty(schema.primaryKeys())) {
            return schema.primaryKeys();
        }
        if (!CollectionUtil.isNullOrEmpty(schema.getColumnNames())) {
            return Collections.singletonList(schema.getColumnNames().get(0));
        }
        return new ArrayList<>();
    }

    private void applyAddColumnEvent(AddColumnEvent event) throws SchemaEvolveException {
        try {
            TableId tableId = event.tableId();
            Schema currentSchema = schemaCache.get(tableId);
            List<String> currentColumnNames = columnOrderForAdd(tableId, currentSchema, event);
            List<AddColumnEvent.ColumnWithPosition> addedColumns = event.getAddedColumns();
            for (AddColumnEvent.ColumnWithPosition col : addedColumns) {
                Column column = col.getAddColumn();
                AddColumnPlacement placement =
                        resolveAddColumnPosition(tableId, col, currentSchema, currentColumnNames);
                ensureSchemaChangeSucceeded(
                        schemaChangeManager.addColumn(
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                buildAddFieldSchema(column),
                                placement.dorisPosition),
                        "add column " + column.getName(),
                        tableId);
                currentColumnNames =
                        tryRefreshOrderAfterAdd(
                                tableId, column.getName(), "add column " + column.getName());
                if (currentSchema != null) {
                    currentSchema =
                            tryApplySchemaChangeToCache(
                                    tableId,
                                    currentSchema,
                                    new AddColumnEvent(tableId, Collections.singletonList(col)),
                                    "add column " + column.getName());
                }
            }
            updateSchemaCache(tableId, currentSchema, "add column event");
        } catch (Exception e) {
            throw new SchemaEvolveException(event, "fail to apply add column event", e);
        }
    }

    private FieldSchema buildAddFieldSchema(Column column) {
        return new FieldSchema(
                column.getName(),
                resolveColumnType(column, null, false),
                resolveColumnDefaultValue(column),
                column.getComment());
    }

    private AddColumnPlacement resolveAddColumnPosition(
            TableId tableId,
            AddColumnEvent.ColumnWithPosition columnWithPosition,
            Schema currentSchema,
            List<String> currentColumnNames) {
        switch (columnWithPosition.getPosition()) {
            case FIRST:
                return firstValuePosition(currentSchema, currentColumnNames);
            case AFTER:
                return resolveAfterPosition(
                        tableId, columnWithPosition.getExistedColumnName(), currentColumnNames);
            case BEFORE:
                return resolveBeforePosition(
                        columnWithPosition.getExistedColumnName(),
                        currentSchema,
                        currentColumnNames);
            case LAST:
            default:
                return AddColumnPlacement.last();
        }
    }

    private AddColumnPlacement firstValuePosition(
            Schema currentSchema, List<String> currentColumnNames) {
        if (currentSchema == null || CollectionUtil.isNullOrEmpty(currentColumnNames)) {
            return AddColumnPlacement.first();
        }

        List<String> keyColumns = dorisKeyColumns(currentSchema);
        int lastKeyIndex = -1;
        for (String keyColumn : keyColumns) {
            int keyIndex = findColumnIndex(currentColumnNames, keyColumn);
            if (keyIndex > lastKeyIndex) {
                lastKeyIndex = keyIndex;
            }
        }
        if (lastKeyIndex < 0) {
            return AddColumnPlacement.first();
        }
        return AddColumnPlacement.after(currentColumnNames.get(lastKeyIndex));
    }

    private List<String> columnOrderForAdd(
            TableId tableId, Schema currentSchema, AddColumnEvent event) {
        List<String> cachedDorisColumnNames = dorisColumnOrderCache.get(tableId);
        if (cachedDorisColumnNames != null) {
            return new ArrayList<>(cachedDorisColumnNames);
        }
        if (needsColumnOrderFetch(event)) {
            try {
                return refreshDorisOrder(
                        tableId,
                        DorisPhysicalSchemaExpectation.any(),
                        "resolve add-column position");
            } catch (RuntimeException e) {
                LOG.warn(
                        "Failed to resolve Doris physical schema before translating ADD COLUMN "
                                + "position for {}. Falling back to current CDC schema order; "
                                + "CSV serialization will still resolve Doris physical schema before writes.",
                        tableId.identifier(),
                        e);
            }
        }

        return currentSchema == null ? null : new ArrayList<>(currentSchema.getColumnNames());
    }

    private List<Field> fetchDorisFieldsOnce(TableId tableId) {
        try {
            org.apache.doris.flink.rest.models.Schema dorisSchema =
                    dorisSchemaFetcher.fetch(dorisOptions, tableId);
            if (dorisSchema == null || dorisSchema.getProperties() == null) {
                throw new IllegalStateException(
                        "Doris physical schema lookup returned no column properties for "
                                + tableId.identifier());
            }
            return new ArrayList<>(dorisSchema.getProperties());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to resolve Doris physical schema of "
                            + tableId.identifier()
                            + " for Doris physical column-name resolution.",
                    e);
        }
    }

    private boolean needsColumnOrderFetch(AddColumnEvent event) {
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            switch (columnWithPosition.getPosition()) {
                case FIRST:
                case BEFORE:
                case AFTER:
                    return true;
                case LAST:
                default:
                    break;
            }
        }
        return false;
    }

    private AddColumnPlacement resolveAfterPosition(
            TableId tableId, String referenceColumn, List<String> currentColumnNames) {
        if (referenceColumn == null) {
            return AddColumnPlacement.last();
        }
        if (currentColumnNames == null) {
            // The physical column-order cache was invalidated (e.g. a prior Doris FE refresh did
            // not catch up within the budget). Resolve the physical name of the reference column
            // best-effort so the ADD COLUMN AFTER position targets the real Doris column even when
            // the CDC logical name differs in case. Fall back to the raw CDC name if the physical
            // name still cannot be resolved, preserving the previous behavior.
            try {
                return AddColumnPlacement.after(resolveDorisColumnName(tableId, referenceColumn));
            } catch (RuntimeException e) {
                LOG.warn(
                        "Could not resolve Doris physical name for reference column {} of {} before "
                                + "translating ADD COLUMN AFTER position; falling back to the CDC "
                                + "column name.",
                        referenceColumn,
                        tableId.identifier(),
                        e);
                return AddColumnPlacement.after(referenceColumn);
            }
        }
        int referenceIndex = findColumnIndex(currentColumnNames, referenceColumn);
        if (referenceIndex < 0) {
            return AddColumnPlacement.after(referenceColumn);
        }
        return AddColumnPlacement.after(currentColumnNames.get(referenceIndex));
    }

    private AddColumnPlacement resolveBeforePosition(
            String referenceColumn, Schema currentSchema, List<String> currentColumnNames) {
        if (referenceColumn == null) {
            throw new IllegalStateException(
                    "Cannot translate CDC BEFORE column position to Doris ADD COLUMN position "
                            + "without reference column.");
        }
        if (currentColumnNames == null) {
            LOG.warn(
                    "Cannot translate CDC BEFORE column position for reference column {} "
                            + "without current Doris physical column order. Falling back to ADD COLUMN LAST.",
                    referenceColumn);
            return AddColumnPlacement.last();
        }

        int referenceIndex = findColumnIndex(currentColumnNames, referenceColumn);
        if (referenceIndex < 0) {
            throw new IllegalStateException(
                    "Cannot find reference column "
                            + referenceColumn
                            + " while translating CDC BEFORE column position to Doris ADD COLUMN position.");
        }
        if (referenceIndex == 0 || isDorisKeyColumn(currentSchema, referenceColumn)) {
            return firstValuePosition(currentSchema, currentColumnNames);
        }
        return AddColumnPlacement.after(currentColumnNames.get(referenceIndex - 1));
    }

    private boolean isDorisKeyColumn(Schema currentSchema, String columnName) {
        return currentSchema != null
                && dorisKeyColumns(currentSchema).stream()
                        .anyMatch(key -> key.equalsIgnoreCase(columnName));
    }

    private static final class AddColumnPlacement {
        private final AddColumnPosition dorisPosition;

        private AddColumnPlacement(AddColumnPosition dorisPosition) {
            this.dorisPosition = dorisPosition;
        }

        private static AddColumnPlacement first() {
            return new AddColumnPlacement(AddColumnPosition.first());
        }

        private static AddColumnPlacement after(String referenceColumn) {
            return new AddColumnPlacement(AddColumnPosition.after(referenceColumn));
        }

        private static AddColumnPlacement last() {
            return new AddColumnPlacement(AddColumnPosition.last());
        }
    }

    private static List<String> resolveDroppedColumnNames(
            Schema currentSchema, DropColumnEvent event) {
        if (currentSchema == null) {
            return event.getDroppedColumnNames();
        }
        return event.getDroppedColumnNames().stream()
                .map(columnName -> SchemaUtils.resolveExistingColumnName(currentSchema, columnName))
                .collect(Collectors.toList());
    }

    private static Map<String, String> resolveRenameColumnNameMapping(
            Schema currentSchema, RenameColumnEvent event) {
        if (currentSchema == null) {
            return event.getNameMapping();
        }
        return SchemaUtils.resolveExistingColumnNameMap(currentSchema, event.getNameMapping());
    }

    private int findColumnIndex(List<String> columnNames, String columnName) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equals(columnName)) {
                return i;
            }
        }
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    private void applyDropColumnEvent(DropColumnEvent event) throws SchemaEvolveException {
        try {
            TableId tableId = event.tableId();
            Schema currentSchema = schemaCache.get(tableId);
            List<String> droppedColumns = resolveDroppedColumnNames(currentSchema, event);
            List<String> physicalDroppedColumns =
                    resolveDorisColumnNamesForDrop(tableId, droppedColumns);
            for (String col : physicalDroppedColumns) {
                ensureSchemaChangeSucceeded(
                        schemaChangeManager.dropColumn(
                                tableId.getSchemaName(), tableId.getTableName(), col),
                        "drop column " + col,
                        tableId);
            }
            refreshDorisOrder(
                    tableId,
                    DorisPhysicalSchemaExpectation.excludesAll(droppedColumns),
                    "drop column event");
            updateCacheAfterSchemaChange(tableId, event, "drop column event");
        } catch (Exception e) {
            throw new SchemaEvolveException(event, "fail to apply drop column event", e);
        }
    }

    private void applyRenameColumnEvent(RenameColumnEvent event) throws SchemaEvolveException {
        try {
            TableId tableId = event.tableId();
            Schema currentSchema = schemaCache.get(tableId);
            Map<String, String> nameMapping = resolveRenameColumnNameMapping(currentSchema, event);
            Map<String, String> physicalNameMapping = new LinkedHashMap<>();
            nameMapping.forEach(
                    (oldColumnName, newColumnName) -> {
                        String oldPhysicalColumnName =
                                resolveDorisColumnName(tableId, oldColumnName);
                        if (isDorisNoOpRename(oldPhysicalColumnName, newColumnName)) {
                            LOG.info(
                                    "Skip Doris rename for {}.{} from {} to {} because Doris"
                                            + " treats these names as the same physical column.",
                                    tableId.getSchemaName(),
                                    tableId.getTableName(),
                                    oldPhysicalColumnName,
                                    newColumnName);
                        } else {
                            physicalNameMapping.put(oldPhysicalColumnName, newColumnName);
                        }
                    });
            for (Map.Entry<String, String> entry : physicalNameMapping.entrySet()) {
                ensureSchemaChangeSucceeded(
                        schemaChangeManager.renameColumn(
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                entry.getKey(),
                                entry.getValue()),
                        "rename column " + entry.getKey() + " to " + entry.getValue(),
                        tableId);
            }
            if (!physicalNameMapping.isEmpty()) {
                refreshDorisOrder(
                        tableId,
                        DorisPhysicalSchemaExpectation.containsAll(
                                new ArrayList<>(physicalNameMapping.values())),
                        "rename column event");
            }
            updateCacheAfterSchemaChange(tableId, event, "rename column event");
        } catch (Exception e) {
            throw new SchemaEvolveException(event, "fail to apply rename column event", e);
        }
    }

    private static boolean isDorisNoOpRename(String oldColumnName, String newColumnName) {
        return oldColumnName != null
                && newColumnName != null
                && oldColumnName.equalsIgnoreCase(newColumnName);
    }

    private void applyAlterColumnTypeEvent(AlterColumnTypeEvent event)
            throws SchemaEvolveException {
        try {
            TableId tableId = event.tableId();
            Schema currentSchema = schemaCache.get(tableId);
            Map<String, DataType> typeMapping =
                    currentSchema == null
                            ? event.getTypeMapping()
                            : SchemaUtils.resolveExistingColumnNameMap(
                                    currentSchema, event.getTypeMapping());
            Map<String, String> comments =
                    currentSchema == null
                            ? event.getComments()
                            : SchemaUtils.resolveExistingColumnNameMap(
                                    currentSchema, event.getComments());
            Map<String, DataType> effectiveTypeMapping =
                    resolveEffectiveTypeMapping(currentSchema, typeMapping);
            Map<String, DataType> oldTypeMapping =
                    currentSchema == null
                            ? event.getOldTypeMapping()
                            : SchemaUtils.resolveExistingColumnNameMap(
                                    currentSchema, event.getOldTypeMapping());
            Map<String, DataType> effectiveOldTypeMapping =
                    oldTypeMapping.entrySet().stream()
                            .filter(entry -> effectiveTypeMapping.containsKey(entry.getKey()))
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            Map.Entry::getValue,
                                            (left, right) -> right,
                                            LinkedHashMap::new));
            List<String> modifiedPhysicalColumnNames = new ArrayList<>();
            for (String columnName : effectiveTypeMapping.keySet()) {
                DataType columnType = effectiveTypeMapping.get(columnName);
                boolean applyCommentWithType =
                        comments.containsKey(columnName)
                                && canApplyCommentWithTypeChange(comments.get(columnName));
                String physicalColumnName = resolveDorisColumnName(tableId, columnName);
                modifiedPhysicalColumnNames.add(physicalColumnName);
                FieldSchema fieldSchema =
                        new FieldSchema(
                                physicalColumnName,
                                resolveColumnType(columnName, columnType, null, false),
                                getCurrentColumnDefaultValue(currentSchema, columnName),
                                applyCommentWithType
                                        ? comments.get(columnName)
                                        : getCurrentColumnComment(currentSchema, columnName));
                ensureSchemaChangeSucceeded(
                        schemaChangeManager.modifyColumnDataType(
                                tableId.getSchemaName(), tableId.getTableName(), fieldSchema),
                        "alter column type " + columnName,
                        tableId);
            }
            if (!effectiveTypeMapping.isEmpty()) {
                tryRefreshDorisOrder(
                        tableId,
                        DorisPhysicalSchemaExpectation.containsAll(modifiedPhysicalColumnNames),
                        "alter column type event");
            }
            for (String columnName : comments.keySet()) {
                if (effectiveTypeMapping.containsKey(columnName)
                        && canApplyCommentWithTypeChange(comments.get(columnName))) {
                    continue;
                }
                String comment = comments.get(columnName);
                if (!isColumnCommentChanged(currentSchema, columnName, comment)) {
                    continue;
                }
                ensureSchemaChangeSucceeded(
                        schemaChangeManager.modifyColumnComment(
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                resolveDorisColumnName(tableId, columnName),
                                comment == null ? "" : comment),
                        "alter column comment " + columnName,
                        tableId);
            }
            updateCacheAfterSchemaChange(
                    tableId,
                    new AlterColumnTypeEvent(
                            tableId, effectiveTypeMapping, effectiveOldTypeMapping, comments),
                    "alter column type event");
        } catch (Exception e) {
            throw new SchemaEvolveException(event, "fail to apply alter column type event", e);
        }
    }

    private static boolean canApplyCommentWithTypeChange(String comment) {
        return comment != null && !comment.trim().isEmpty();
    }

    private static Map<String, DataType> resolveEffectiveTypeMapping(
            Schema currentSchema, Map<String, DataType> typeMapping) {
        if (currentSchema == null) {
            return typeMapping;
        }
        Map<String, DataType> effectiveTypeMapping = new LinkedHashMap<>();
        typeMapping.forEach(
                (columnName, newType) -> {
                    Column currentColumn = currentSchema.getColumn(columnName).orElse(null);
                    if (currentColumn == null
                            || !Objects.equals(currentColumn.getType(), newType)) {
                        effectiveTypeMapping.put(columnName, newType);
                    }
                });
        return effectiveTypeMapping;
    }

    private static boolean isColumnCommentChanged(
            Schema currentSchema, String columnName, String comment) {
        return currentSchema == null
                || currentSchema
                        .getColumn(columnName)
                        .map(column -> !Objects.equals(column.getComment(), comment))
                        .orElse(true);
    }

    private static String getCurrentColumnComment(Schema currentSchema, String columnName) {
        if (currentSchema == null) {
            return null;
        }
        return currentSchema.getColumn(columnName).map(Column::getComment).orElse(null);
    }

    private String getCurrentColumnDefaultValue(Schema currentSchema, String columnName) {
        if (currentSchema == null) {
            return null;
        }
        return currentSchema
                .getColumn(columnName)
                .map(this::resolveColumnDefaultValue)
                .orElse(null);
    }

    private void applyTruncateTableEvent(TruncateTableEvent truncateTableEvent)
            throws SchemaEvolveException {
        TableId tableId = truncateTableEvent.tableId();
        try {
            ensureSchemaChangeSucceeded(
                    schemaChangeManager.truncateTable(
                            tableId.getSchemaName(), tableId.getTableName()),
                    "truncate table",
                    tableId);
        } catch (Exception e) {
            throw new SchemaEvolveException(truncateTableEvent, "fail to truncate table", e);
        }
    }

    private void applyDropTableEvent(DropTableEvent dropTableEvent) throws SchemaEvolveException {
        TableId tableId = dropTableEvent.tableId();
        try {
            ensureSchemaChangeSucceeded(
                    schemaChangeManager.dropTable(tableId.getSchemaName(), tableId.getTableName()),
                    "drop table",
                    tableId);
            schemaCache.remove(tableId);
            dorisColumnOrderCache.remove(tableId);
        } catch (Exception e) {
            throw new SchemaEvolveException(dropTableEvent, "fail to drop table", e);
        }
    }

    private Schema tryApplySchemaChangeToCache(
            TableId tableId, Schema schema, SchemaChangeEvent event, String operation) {
        try {
            return SchemaUtils.applySchemaChangeEvent(schema, event);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to update local schema cache for {} after {}. "
                            + "Doris schema change has been applied, invalidate local cache.",
                    tableId,
                    operation,
                    e);
            return null;
        }
    }

    private void updateSchemaCache(TableId tableId, Schema schema, String operation) {
        if (schema == null) {
            LOG.warn(
                    "Local schema cache of {} is unavailable after {}. "
                            + "Doris schema change has been applied, skip local cache update.",
                    tableId,
                    operation);
            schemaCache.remove(tableId);
            return;
        }
        schemaCache.put(tableId, schema);
    }

    private List<String> extractDorisNames(org.apache.doris.flink.rest.models.Schema dorisSchema) {
        return extractDorisNames(dorisSchema.getProperties());
    }

    private List<String> extractDorisNames(List<Field> fields) {
        List<String> columnNames = new ArrayList<>();
        for (Field field : fields) {
            if (!DORIS_DELETE_SIGN.equals(field.getName())) {
                columnNames.add(field.getName());
            }
        }
        return columnNames;
    }

    private void updateDorisOrderCache(TableId tableId, List<String> columnNames) {
        if (columnNames == null) {
            dorisColumnOrderCache.remove(tableId);
            return;
        }
        dorisColumnOrderCache.put(tableId, new ArrayList<>(columnNames));
    }

    private String resolveDorisColumnName(TableId tableId, String columnName) {
        List<String> columnNames = dorisColumnOrderCache.get(tableId);
        if (columnNames == null || findColumnIndex(columnNames, columnName) < 0) {
            columnNames =
                    refreshDorisOrder(
                            tableId,
                            DorisPhysicalSchemaExpectation.contains(columnName),
                            "resolve Doris physical column name " + columnName);
        }
        int columnIndex = findColumnIndex(columnNames, columnName);
        if (columnIndex < 0) {
            throw new IllegalStateException(
                    "Failed to resolve Doris physical column name for "
                            + tableId.identifier()
                            + "."
                            + columnName);
        }
        return columnNames.get(columnIndex);
    }

    private List<String> resolveDorisColumnNamesForDrop(
            TableId tableId, List<String> droppedColumns) {
        List<String> columnNames = dorisColumnOrderCache.get(tableId);
        if (columnNames == null) {
            columnNames =
                    refreshDorisOrder(
                            tableId,
                            DorisPhysicalSchemaExpectation.any(),
                            "resolve Doris physical column names for drop");
        }

        List<String> physicalDroppedColumns = new ArrayList<>();
        for (String droppedColumn : droppedColumns) {
            int columnIndex = findColumnIndex(columnNames, droppedColumn);
            if (columnIndex >= 0) {
                physicalDroppedColumns.add(columnNames.get(columnIndex));
            } else {
                LOG.info(
                        "Skip Doris DROP COLUMN for {}.{} because the column is already absent.",
                        tableId.identifier(),
                        droppedColumn);
            }
        }
        return physicalDroppedColumns;
    }

    private List<String> tryRefreshOrderAfterAdd(
            TableId tableId, String addedColumnName, String operation) {
        return tryRefreshDorisOrder(
                tableId, DorisPhysicalSchemaExpectation.contains(addedColumnName), operation);
    }

    private List<String> tryRefreshDorisOrder(
            TableId tableId, DorisPhysicalSchemaExpectation expectation, String operation) {
        try {
            return refreshDorisOrder(tableId, expectation, operation);
        } catch (RuntimeException e) {
            updateDorisOrderCache(tableId, null);
            LOG.warn(
                    "Could not confirm Doris physical schema after {} on {} within the metadata "
                            + "refresh budget. Invalidated the physical column-order cache; later "
                            + "schema changes and sink serialization will resolve Doris physical schema again.",
                    operation,
                    tableId.identifier(),
                    e);
            return null;
        }
    }

    private List<String> refreshDorisOrder(
            TableId tableId, DorisPhysicalSchemaExpectation expectation, String operation) {
        long waitStartNanos = System.nanoTime();
        long deadlineNanos =
                waitStartNanos + TimeUnit.MILLISECONDS.toNanos(dorisRefreshMaxWaitMillis());
        RuntimeException lastFailure = null;
        boolean waitingLogged = false;

        while (true) {
            try {
                List<Field> fields = fetchDorisFieldsOnce(tableId);
                List<String> columnNames = extractDorisNames(fields);
                if (expectation == null || expectation.matches(fields, this)) {
                    updateDorisOrderCache(tableId, columnNames);
                    return new ArrayList<>(columnNames);
                }
                lastFailure =
                        new IllegalStateException(
                                "Doris physical schema of "
                                        + tableId.identifier()
                                        + " does not satisfy "
                                        + expectation.description
                                        + ". Current columns: "
                                        + columnNames);
            } catch (RuntimeException e) {
                lastFailure = e;
            }

            long nowNanos = System.nanoTime();
            if (nowNanos >= deadlineNanos) {
                updateDorisOrderCache(tableId, null);
                throw new IllegalStateException(
                        "Failed to refresh Doris physical column-order cache for "
                                + tableId.identifier()
                                + " after "
                                + operation
                                + ". Unknown Doris physical schema cannot be used safely for "
                                + "following schema changes or CSV writes.",
                        lastFailure);
            }
            if (!waitingLogged) {
                waitingLogged = true;
                LOG.info(
                        "Waiting for Doris physical schema to catch up after {} on {}. expectation={}",
                        operation,
                        tableId.identifier(),
                        expectation == null ? "any schema" : expectation.description);
            }
            sleepBeforeDorisRetry();
        }
    }

    protected void sleepBeforeDorisRetry() {
        try {
            Thread.sleep(dorisRefreshRetryMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for Doris physical schema refresh.", e);
        }
    }

    protected long dorisRefreshMaxWaitMillis() {
        return DORIS_SCHEMA_REFRESH_MAX_WAIT_MILLIS;
    }

    protected long dorisRefreshRetryMillis() {
        return DORIS_SCHEMA_REFRESH_RETRY_MILLIS;
    }

    private void updateCacheAfterSchemaChange(
            TableId tableId, SchemaChangeEvent event, String operation) {
        Schema currentSchema = schemaCache.get(tableId);
        if (currentSchema == null) {
            updateSchemaCache(tableId, null, operation);
            return;
        }
        updateSchemaCache(
                tableId,
                tryApplySchemaChangeToCache(tableId, currentSchema, event, operation),
                operation);
    }

    private static final class DorisPhysicalSchemaExpectation {
        private final List<String> includedColumns;
        private final List<String> excludedColumns;
        private final String description;

        private DorisPhysicalSchemaExpectation(
                List<String> includedColumns, List<String> excludedColumns, String description) {
            this.includedColumns = includedColumns;
            this.excludedColumns = excludedColumns;
            this.description = description;
        }

        private static DorisPhysicalSchemaExpectation any() {
            return new DorisPhysicalSchemaExpectation(
                    Collections.emptyList(), Collections.emptyList(), "any physical schema");
        }

        private static DorisPhysicalSchemaExpectation contains(String columnName) {
            return containsAll(Collections.singletonList(columnName));
        }

        private static DorisPhysicalSchemaExpectation containsAll(List<String> columnNames) {
            return new DorisPhysicalSchemaExpectation(
                    new ArrayList<>(columnNames),
                    Collections.emptyList(),
                    "contains columns " + columnNames);
        }

        private static DorisPhysicalSchemaExpectation excludesAll(List<String> columnNames) {
            return new DorisPhysicalSchemaExpectation(
                    Collections.emptyList(),
                    new ArrayList<>(columnNames),
                    "excludes columns " + columnNames);
        }

        private boolean matches(List<Field> fields, DorisMetadataApplier applier) {
            List<String> columnNames = applier.extractDorisNames(fields);
            for (String includedColumn : includedColumns) {
                if (applier.findColumnIndex(columnNames, includedColumn) < 0) {
                    return false;
                }
            }
            for (String excludedColumn : excludedColumns) {
                if (applier.findColumnIndex(columnNames, excludedColumn) >= 0) {
                    return false;
                }
            }
            return true;
        }
    }

    @VisibleForTesting
    Schema getCachedSchema(TableId tableId) {
        return schemaCache.get(tableId);
    }

    @VisibleForTesting
    List<String> getCachedDorisColumnOrder(TableId tableId) {
        List<String> columnNames = dorisColumnOrderCache.get(tableId);
        return columnNames == null ? null : new ArrayList<>(columnNames);
    }

    private void ensureSchemaChangeSucceeded(boolean succeeded, String operation, TableId tableId) {
        if (!succeeded) {
            throw new IllegalStateException(
                    String.format(
                            "Doris schema change returned false for %s on %s. "
                                    + "Will not update local schema cache.",
                            operation, tableId.identifier()));
        }
    }

    private String resolveColumnType(Column column, String autoPartitionColumn, boolean keyColumn) {
        return resolveColumnType(
                column.getName(), column.getType(), autoPartitionColumn, keyColumn);
    }

    private String resolveColumnType(
            String columnName, DataType dataType, String autoPartitionColumn, boolean keyColumn) {
        String type = DorisTypeUtils.toDorisTypeString(dataType);
        if (shouldAppendNotNull(columnName, dataType, autoPartitionColumn)) {
            if (keyColumn && DORIS_STRING_TYPE.equals(type)) {
                type = DORIS_STRING_KEY_TYPE;
            }
            return type + " NOT NULL";
        }
        return type;
    }

    private boolean shouldAppendNotNull(
            String columnName, DataType dataType, String autoPartitionColumn) {
        return config.get(SCHEMA_CHANGE_COLUMN_NULL_ENABLE)
                && !dataType.isNullable()
                && (autoPartitionColumn == null
                        || !columnName.equalsIgnoreCase(autoPartitionColumn));
    }

    private String resolveColumnDefaultValue(Column column) {
        if (!config.get(SCHEMA_CHANGE_COLUMN_DEFAULT_VALUE)) {
            return null;
        }
        return convertInvalidTimestampDefaultValue(
                column.getDefaultValueExpression(), column.getType());
    }

    private String convertInvalidTimestampDefaultValue(String defaultValue, DataType dataType) {
        if (defaultValue == null) {
            return null;
        }

        if (dataType instanceof LocalZonedTimestampType
                || dataType instanceof TimestampType
                || dataType instanceof ZonedTimestampType) {

            if (DorisSchemaUtils.INVALID_OR_MISSING_DATATIME.equals(defaultValue)) {
                return DorisSchemaUtils.DEFAULT_DATETIME;
            }
        }

        return defaultValue;
    }

    private void applyAlterTableCommentEvent(AlterTableCommentEvent alterTableCommentEvent)
            throws SchemaEvolveException {
        TableId tableId = alterTableCommentEvent.tableId();
        try {
            ensureSchemaChangeSucceeded(
                    schemaChangeManager.alterTableComment(
                            tableId.getSchemaName(),
                            tableId.getTableName(),
                            alterTableCommentEvent.getComment()),
                    "alter table comment",
                    tableId);
        } catch (Exception e) {
            throw new SchemaEvolveException(
                    alterTableCommentEvent, "fail to alter table comment", e);
        }
    }
}
