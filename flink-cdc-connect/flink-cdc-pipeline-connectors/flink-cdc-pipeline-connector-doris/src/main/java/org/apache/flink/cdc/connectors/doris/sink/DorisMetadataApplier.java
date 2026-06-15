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
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.TRUNCATE_TABLE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.CHARSET_ENCODING;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_BUCKETS;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_EXTRA_SCHEMA;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PROPERTIES_PREFIX;

/** Supports {@link DorisDataSink} to schema evolution. */
public class DorisMetadataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(DorisMetadataApplier.class);
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
    private final DorisSchemaFetcher dorisSchemaFetcher;

    public DorisMetadataApplier(DorisOptions dorisOptions, Configuration config) {
        this(
                dorisOptions,
                config,
                new DorisSchemaChangeManager(dorisOptions, config.get(CHARSET_ENCODING)),
                DEFAULT_DORIS_SCHEMA_FETCHER);
    }

    DorisMetadataApplier(
            DorisOptions dorisOptions,
            Configuration config,
            DorisSchemaChangeManager schemaChangeManager,
            DorisSchemaFetcher dorisSchemaFetcher) {
        this.dorisOptions = dorisOptions;
        this.schemaChangeManager = schemaChangeManager;
        this.config = config;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
        this.tableBucketsMap = parseTableBuckets(config);
        this.schemaCache = new HashMap<>();
        this.dorisSchemaFetcher = dorisSchemaFetcher;
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
            } else {
                reconcileExistingTable(tableId, schema, existingDorisSchema);
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
        tableSchema.setFields(buildFields(schema));
        tableSchema.setKeys(buildKeys(schema));
        tableSchema.setDistributeKeys(buildDistributeKeys(schema));
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

        Tuple2<String, String> partitionInfo =
                DorisSchemaUtils.getPartitionInfo(config, schema, tableId);
        if (partitionInfo != null) {
            LOG.info("Partition info of {} is: {}.", tableId.identifier(), partitionInfo);
            tableSchema.setPartitionInfo(partitionInfo);
        }
        return tableSchema;
    }

    private org.apache.doris.flink.rest.models.Schema fetchExistingDorisSchema(TableId tableId) {
        try {
            return dorisSchemaFetcher.fetch(dorisOptions, tableId);
        } catch (Exception e) {
            if (isLikelyMissingTableSchema(e)) {
                LOG.info(
                        "Doris schema for {} is absent before create-table handling. "
                                + "Will treat it as a new table and try CREATE TABLE first.",
                        tableId.identifier(),
                        e);
                return null;
            }
            LOG.warn(
                    "Failed to resolve existing Doris schema for {} before create-table handling. "
                            + "Aborting instead of assuming the table is absent.",
                    tableId.identifier(),
                    e);
            throw new IllegalStateException(
                    "Failed to resolve existing Doris schema for " + tableId.identifier(), e);
        }
    }

    private void reconcileExistingTable(
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
        List<AddColumnEvent.ColumnWithPosition> missingColumns =
                resolveMissingColumns(tableId, desiredSchema, existingDorisSchema, currentSchema);

        if (missingColumns.isEmpty()) {
            LOG.info(
                    "Doris table {} already exists and covers all columns in the incoming CreateTableEvent.",
                    tableId.identifier());
            return;
        }

        LOG.info(
                "Doris table {} already exists but is missing columns {} from the incoming CreateTableEvent. "
                        + "Applying ADD COLUMN reconciliation.",
                tableId.identifier(),
                missingColumns);

        for (AddColumnEvent.ColumnWithPosition missingColumn : missingColumns) {
            Column column = missingColumn.getAddColumn();
            ensureSchemaChangeSucceeded(
                    schemaChangeManager.addColumn(
                            tableId.getSchemaName(),
                            tableId.getTableName(),
                            buildAddFieldSchema(column),
                            toDorisAddColumnPosition(missingColumn, currentSchema)),
                    "add missing column " + column.getName(),
                    tableId);
            currentSchema =
                    SchemaUtils.applySchemaChangeEvent(
                            currentSchema,
                            new AddColumnEvent(tableId, Collections.singletonList(missingColumn)));
        }
    }

    private void logExistingColumnTypeDifferences(
            TableId tableId,
            Schema desiredSchema,
            org.apache.doris.flink.rest.models.Schema existingDorisSchema) {
        Map<String, Field> existingFieldsByLowerCase = new LinkedHashMap<>();
        for (Field field : existingDorisSchema.getProperties()) {
            existingFieldsByLowerCase.putIfAbsent(field.getName().toLowerCase(Locale.ROOT), field);
        }

        for (Column desiredColumn : desiredSchema.getColumns()) {
            Field existingField =
                    existingFieldsByLowerCase.get(desiredColumn.getName().toLowerCase(Locale.ROOT));
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
                    field.getName().toLowerCase(java.util.Locale.ROOT), field.getName());
        }

        List<Column> currentColumns = new ArrayList<>();
        for (Column desiredColumn : desiredSchema.getColumns()) {
            String physicalName =
                    existingColumnsByLowerCase.get(
                            desiredColumn.getName().toLowerCase(java.util.Locale.ROOT));
            if (physicalName != null) {
                currentColumns.add(desiredColumn.copy(physicalName));
            }
        }

        List<String> currentPrimaryKeys =
                desiredSchema.primaryKeys().stream()
                        .map(
                                key ->
                                        existingColumnsByLowerCase.get(
                                                key.toLowerCase(java.util.Locale.ROOT)))
                        .filter(java.util.Objects::nonNull)
                        .collect(java.util.stream.Collectors.toList());

        List<String> currentPartitionKeys =
                desiredSchema.partitionKeys().stream()
                        .map(
                                key ->
                                        existingColumnsByLowerCase.get(
                                                key.toLowerCase(java.util.Locale.ROOT)))
                        .filter(java.util.Objects::nonNull)
                        .collect(java.util.stream.Collectors.toList());

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
                        .map(field -> field.getName().toLowerCase(java.util.Locale.ROOT))
                        .collect(java.util.stream.Collectors.toSet());

        List<AddColumnEvent.ColumnWithPosition> missingColumns = new ArrayList<>();
        List<Column> desiredColumns = desiredSchema.getColumns();
        for (int i = 0; i < desiredColumns.size(); i++) {
            Column desiredColumn = desiredColumns.get(i);
            String normalizedDesiredName =
                    desiredColumn.getName().toLowerCase(java.util.Locale.ROOT);
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
                        .map(name -> name.toLowerCase(java.util.Locale.ROOT))
                        .collect(java.util.stream.Collectors.toSet());

        for (int i = targetIndex - 1; i >= 0; i--) {
            Column previousColumn = desiredColumns.get(i);
            if (currentColumnNamesLowerCase.contains(
                    previousColumn.getName().toLowerCase(java.util.Locale.ROOT))) {
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
            if (currentColumnNamesLowerCase.contains(
                    nextColumn.getName().toLowerCase(java.util.Locale.ROOT))) {
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

    private Map<String, FieldSchema> buildFields(Schema schema) {
        // Guaranteed the order of column
        Map<String, FieldSchema> fieldSchemaMap = new LinkedHashMap<>();
        List<String> columnNameList = schema.getColumnNames();
        for (String columnName : columnNameList) {
            Column column = schema.getColumn(columnName).get();
            fieldSchemaMap.put(
                    column.getName(),
                    new FieldSchema(
                            column.getName(),
                            DorisTypeUtils.toDorisTypeString(column.getType()),
                            convertInvalidTimestampDefaultValue(
                                    column.getDefaultValueExpression(), column.getType()),
                            column.getComment()));
        }
        return fieldSchemaMap;
    }

    private List<String> buildKeys(Schema schema) {
        return buildDistributeKeys(schema);
    }

    private List<String> buildDistributeKeys(Schema schema) {
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
            List<AddColumnEvent.ColumnWithPosition> addedColumns = event.getAddedColumns();
            for (AddColumnEvent.ColumnWithPosition col : addedColumns) {
                Column column = col.getAddColumn();
                ensureSchemaChangeSucceeded(
                        schemaChangeManager.addColumn(
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                buildAddFieldSchema(column),
                                toDorisAddColumnPosition(col, currentSchema)),
                        "add column " + column.getName(),
                        tableId);
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
                DorisTypeUtils.toDorisTypeString(column.getType()),
                convertInvalidTimestampDefaultValue(
                        column.getDefaultValueExpression(), column.getType()),
                column.getComment());
    }

    private AddColumnPosition toDorisAddColumnPosition(
            AddColumnEvent.ColumnWithPosition columnWithPosition, Schema currentSchema) {
        switch (columnWithPosition.getPosition()) {
            case FIRST:
                return AddColumnPosition.first();
            case AFTER:
                return columnWithPosition.getExistedColumnName() == null
                        ? AddColumnPosition.last()
                        : AddColumnPosition.after(columnWithPosition.getExistedColumnName());
            case BEFORE:
                return translateBeforePosition(
                        columnWithPosition.getExistedColumnName(), currentSchema);
            case LAST:
            default:
                return AddColumnPosition.last();
        }
    }

    private AddColumnPosition translateBeforePosition(
            String referenceColumn, Schema currentSchema) {
        if (referenceColumn == null || currentSchema == null) {
            LOG.warn(
                    "Cannot translate CDC BEFORE column position to Doris ADD COLUMN position. "
                            + "referenceColumn={}, hasSchemaCache={}. Fallback to LAST.",
                    referenceColumn,
                    currentSchema != null);
            return AddColumnPosition.last();
        }

        List<String> columnNames = currentSchema.getColumnNames();
        int referenceIndex = findColumnIndex(columnNames, referenceColumn);
        if (referenceIndex < 0) {
            LOG.warn(
                    "Cannot find reference column {} in local schema cache while translating CDC "
                            + "BEFORE position to Doris ADD COLUMN position. Fallback to LAST.",
                    referenceColumn);
            return AddColumnPosition.last();
        }
        if (referenceIndex == 0) {
            return AddColumnPosition.first();
        }
        return AddColumnPosition.after(columnNames.get(referenceIndex - 1));
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
            List<String> droppedColumns = event.getDroppedColumnNames();
            for (String col : droppedColumns) {
                ensureSchemaChangeSucceeded(
                        schemaChangeManager.dropColumn(
                                tableId.getSchemaName(), tableId.getTableName(), col),
                        "drop column " + col,
                        tableId);
            }
            schemaCache.put(
                    tableId, SchemaUtils.applySchemaChangeEvent(getSchemaOrThrow(tableId), event));
        } catch (Exception e) {
            throw new SchemaEvolveException(event, "fail to apply drop column event", e);
        }
    }

    private void applyRenameColumnEvent(RenameColumnEvent event) throws SchemaEvolveException {
        try {
            TableId tableId = event.tableId();
            Map<String, String> nameMapping = event.getNameMapping();
            for (Map.Entry<String, String> entry : nameMapping.entrySet()) {
                ensureSchemaChangeSucceeded(
                        schemaChangeManager.renameColumn(
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                entry.getKey(),
                                entry.getValue()),
                        "rename column " + entry.getKey() + " to " + entry.getValue(),
                        tableId);
            }
            schemaCache.put(
                    tableId, SchemaUtils.applySchemaChangeEvent(getSchemaOrThrow(tableId), event));
        } catch (Exception e) {
            throw new SchemaEvolveException(event, "fail to apply rename column event", e);
        }
    }

    private void applyAlterColumnTypeEvent(AlterColumnTypeEvent event)
            throws SchemaEvolveException {
        try {
            TableId tableId = event.tableId();
            Map<String, DataType> typeMapping = event.getTypeMapping();
            Map<String, String> comments = event.getComments();

            for (Map.Entry<String, DataType> entry : typeMapping.entrySet()) {
                ensureSchemaChangeSucceeded(
                        schemaChangeManager.modifyColumnDataType(
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                new FieldSchema(
                                        entry.getKey(),
                                        DorisTypeUtils.toDorisTypeString(entry.getValue()),
                                        comments.get(entry.getKey()))),
                        "alter column type " + entry.getKey(),
                        tableId);
            }
            schemaCache.put(
                    tableId, SchemaUtils.applySchemaChangeEvent(getSchemaOrThrow(tableId), event));
        } catch (Exception e) {
            throw new SchemaEvolveException(event, "fail to apply alter column type event", e);
        }
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

    private Schema getSchemaOrThrow(TableId tableId) {
        Schema schema = schemaCache.get(tableId);
        if (schema == null) {
            throw new IllegalStateException(
                    "Schema cache of " + tableId + " is not initialized before schema evolution.");
        }
        return schema;
    }

    @VisibleForTesting
    Schema getCachedSchema(TableId tableId) {
        return schemaCache.get(tableId);
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

    private boolean isLikelyMissingTableSchema(Exception e) {
        StringBuilder message = new StringBuilder();
        Throwable current = e;
        while (current != null) {
            if (current.getMessage() != null) {
                if (!message.isEmpty()) {
                    message.append(" | ");
                }
                message.append(current.getMessage());
            }
            current = current.getCause();
        }
        String normalizedMessage = message.toString().toLowerCase(Locale.ROOT);
        return normalizedMessage.contains("status: 404")
                || normalizedMessage.contains("reason: not found")
                || normalizedMessage.contains("unknown table")
                || normalizedMessage.contains("table does not exist")
                || normalizedMessage.contains("table not exist")
                || normalizedMessage.contains("table not found");
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
