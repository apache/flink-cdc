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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.ExistingTableSchemaExpansionSupport;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Performs best-effort safe schema expansion for an existing target table. */
@Internal
public class ExistingTableSchemaExpander {

    private static final Logger LOG = LoggerFactory.getLogger(ExistingTableSchemaExpander.class);

    private final MetadataApplier metadataApplier;
    private final ExistingTableSchemaExpansionSupport expansionSupport;
    private final SchemaChangeBehavior schemaChangeBehavior;

    public ExistingTableSchemaExpander(
            MetadataApplier metadataApplier,
            ExistingTableSchemaExpansionSupport expansionSupport,
            SchemaChangeBehavior schemaChangeBehavior) {
        this.metadataApplier = metadataApplier;
        this.expansionSupport = expansionSupport;
        this.schemaChangeBehavior = schemaChangeBehavior;
    }

    /** Tries safe expansions without imposing new compatibility failures. */
    public void expand(CreateTableEvent createTableEvent) {
        try {
            expandInternal(createTableEvent);
        } catch (Exception e) {
            LOG.warn(
                    "Unexpected error while expanding target table {}. Delegating schema handling to the sink.",
                    createTableEvent.tableId(),
                    e);
        }
    }

    private void expandInternal(CreateTableEvent createTableEvent) throws Exception {
        if (schemaChangeBehavior == SchemaChangeBehavior.IGNORE
                || schemaChangeBehavior == SchemaChangeBehavior.EXCEPTION) {
            return;
        }
        boolean supportsAddColumn = supportsSchemaEvolutionType(SchemaChangeEventType.ADD_COLUMN);
        boolean supportsAlterColumnType =
                supportsSchemaEvolutionType(SchemaChangeEventType.ALTER_COLUMN_TYPE);
        if (!supportsAddColumn && !supportsAlterColumnType) {
            return;
        }

        Optional<Schema> targetSchema = queryTargetSchema(createTableEvent.tableId());
        if (!targetSchema.isPresent()) {
            return;
        }

        Schema pipelineSchema = createTableEvent.getSchema();
        Schema currentTargetSchema = targetSchema.get();
        boolean columnNameCaseSensitive = expansionSupport.isColumnNameCaseSensitive();
        ColumnIndex targetColumns = indexColumns(currentTargetSchema, columnNameCaseSensitive);
        Set<String> ambiguousColumnNames =
                new HashSet<>(
                        indexColumns(pipelineSchema, columnNameCaseSensitive)
                                .getAmbiguousColumnNames());
        ambiguousColumnNames.addAll(targetColumns.getAmbiguousColumnNames());
        Set<String> keyColumns =
                getKeyColumns(pipelineSchema, currentTargetSchema, columnNameCaseSensitive);

        List<Column> columnsToAdd = new ArrayList<>();
        Map<String, DataType> columnsToWiden = new LinkedHashMap<>();

        for (Column pipelineColumn : pipelineSchema.getColumns()) {
            String columnName = pipelineColumn.getName();
            String comparisonName = normalizeColumnName(columnName, columnNameCaseSensitive);
            if (ambiguousColumnNames.contains(comparisonName)) {
                LOG.info(
                        "Column name {} in target table {} is ambiguous under the target system's case-sensitivity rule. Delegating this difference to the sink.",
                        columnName,
                        createTableEvent.tableId());
                continue;
            }

            Column targetColumn = targetColumns.get(columnName);
            if (targetColumn == null) {
                if (pipelineColumn.isPhysical() && !keyColumns.contains(comparisonName)) {
                    columnsToAdd.add(pipelineColumn.copy(pipelineColumn.getType().nullable()));
                } else {
                    LOG.info(
                            "Target table {} is missing special column {}. Delegating this difference to the sink.",
                            createTableEvent.tableId(),
                            columnName);
                }
                continue;
            }

            Optional<DataType> normalizedPipelineTypeOptional =
                    normalizeType(
                            createTableEvent.tableId(),
                            columnName,
                            pipelineColumn.getType(),
                            currentTargetSchema);
            if (!normalizedPipelineTypeOptional.isPresent()) {
                continue;
            }
            DataType normalizedPipelineType = normalizedPipelineTypeOptional.get().nullable();
            DataType targetType = targetColumn.getType().nullable();

            if (pipelineColumn.getType().isNullable() && !targetColumn.getType().isNullable()) {
                LOG.info(
                        "Target column {}.{} is NOT NULL while the pipeline column is nullable. Delegating this difference to the sink.",
                        createTableEvent.tableId(),
                        columnName);
            }

            if (canContain(targetType, normalizedPipelineType)) {
                continue;
            }
            if (keyColumns.contains(comparisonName)) {
                LOG.info(
                        "Target key column {}.{} cannot contain pipeline type {}. Delegating this difference to the sink.",
                        createTableEvent.tableId(),
                        columnName,
                        normalizedPipelineType);
                continue;
            }

            Optional<DataType> widenedType = getSafeWidenedType(targetType, normalizedPipelineType);
            if (!widenedType.isPresent()) {
                LOG.info(
                        "Target column {}.{} with type {} cannot safely contain pipeline type {}. Delegating this difference to the sink.",
                        createTableEvent.tableId(),
                        columnName,
                        targetType,
                        normalizedPipelineType);
                continue;
            }

            Optional<DataType> normalizedWidenedTypeOptional =
                    normalizeType(
                            createTableEvent.tableId(),
                            columnName,
                            widenedType.get(),
                            currentTargetSchema);
            if (!normalizedWidenedTypeOptional.isPresent()) {
                continue;
            }
            DataType normalizedWidenedType = normalizedWidenedTypeOptional.get().nullable();
            if (!canContain(normalizedWidenedType, targetType)
                    || !canContain(normalizedWidenedType, normalizedPipelineType)) {
                LOG.info(
                        "Target system normalizes proposed type {} for {}.{} to {}, which is not a safe widening. Delegating this difference to the sink.",
                        widenedType.get(),
                        createTableEvent.tableId(),
                        columnName,
                        normalizedWidenedType);
                continue;
            }

            String targetColumnName = targetColumn.getName();
            columnsToWiden.put(
                    targetColumnName, widenedType.get().copy(targetColumn.getType().isNullable()));
        }

        if (!columnsToAdd.isEmpty()) {
            if (supportsAddColumn) {
                AddColumnEvent addColumnEvent =
                        new AddColumnEvent(
                                createTableEvent.tableId(),
                                columnsToAdd.stream()
                                        .map(AddColumnEvent.ColumnWithPosition::new)
                                        .collect(Collectors.toList()));
                applySchemaChange(addColumnEvent, columnsToAdd);
            } else {
                LOG.info(
                        "Target table {} is missing columns {}, but ADD_COLUMN is not enabled or supported. Delegating this difference to the sink.",
                        createTableEvent.tableId(),
                        getColumnNames(columnsToAdd));
            }
        }

        if (!columnsToWiden.isEmpty()) {
            if (supportsAlterColumnType) {
                AlterColumnTypeEvent alterColumnTypeEvent =
                        new AlterColumnTypeEvent(
                                createTableEvent.tableId(),
                                columnsToWiden,
                                columnsToWiden.keySet().stream()
                                        .collect(
                                                Collectors.toMap(
                                                        columnName -> columnName,
                                                        columnName ->
                                                                targetColumns
                                                                        .get(columnName)
                                                                        .getType())));
                applySchemaChange(alterColumnTypeEvent, columnsToWiden);
            } else {
                LOG.info(
                        "Target table {} has narrow columns {}, but ALTER_COLUMN_TYPE is not enabled or supported. Delegating this difference to the sink.",
                        createTableEvent.tableId(),
                        columnsToWiden.keySet());
            }
        }
    }

    private Optional<Schema> queryTargetSchema(TableId tableId) throws Exception {
        try {
            return expansionSupport.getExistingTableSchema(tableId);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to query schema of target table {}. Delegating schema handling to the sink.",
                    tableId,
                    e);
            throw e;
        }
    }

    private Optional<DataType> normalizeType(
            TableId tableId,
            String columnName,
            DataType pipelineType,
            Schema existingTargetSchema) {
        try {
            DataType normalizedType =
                    expansionSupport.normalizeToTargetDataType(
                            tableId, columnName, pipelineType, existingTargetSchema);
            if (normalizedType == null) {
                LOG.warn(
                        "Target schema expansion support returned a null normalized type for {}.{}. Delegating this column to the sink.",
                        tableId,
                        columnName);
                return Optional.empty();
            }
            return Optional.of(normalizedType);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to normalize type {} for {}.{}. Delegating this column to the sink.",
                    pipelineType,
                    tableId,
                    columnName,
                    e);
            return Optional.empty();
        }
    }

    private void applySchemaChange(SchemaChangeEvent event, Object changes) {
        try {
            LOG.info(
                    "Attempting to apply schema change event derived for existing table expansion: {}",
                    event);
            metadataApplier.applySchemaChange(event);
            LOG.info(
                    "The schema change call for existing table expansion completed without an exception: {}",
                    event);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to apply expansion change {} to target table {}. Delegating schema handling to the sink.",
                    changes,
                    event.tableId(),
                    e);
        }
    }

    private boolean supportsSchemaEvolutionType(SchemaChangeEventType eventType) {
        try {
            return metadataApplier.acceptsSchemaEvolutionType(eventType)
                    && metadataApplier.getSupportedSchemaEvolutionTypes().contains(eventType);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to determine whether {} is enabled and supported. Delegating schema handling to the sink.",
                    eventType,
                    e);
            return false;
        }
    }

    private static ColumnIndex indexColumns(Schema schema, boolean caseSensitive) {
        Map<String, Column> columns = new HashMap<>();
        Set<String> ambiguousColumnNames = new HashSet<>();
        for (Column column : schema.getColumns()) {
            String comparisonName = normalizeColumnName(column.getName(), caseSensitive);
            if (ambiguousColumnNames.contains(comparisonName)) {
                continue;
            }
            if (columns.putIfAbsent(comparisonName, column) != null) {
                columns.remove(comparisonName);
                ambiguousColumnNames.add(comparisonName);
            }
        }
        return new ColumnIndex(columns, ambiguousColumnNames, caseSensitive);
    }

    private static Set<String> getKeyColumns(
            Schema pipelineSchema, Schema targetSchema, boolean caseSensitive) {
        Set<String> keyColumns = new HashSet<>();
        pipelineSchema.primaryKeys().stream()
                .map(columnName -> normalizeColumnName(columnName, caseSensitive))
                .forEach(keyColumns::add);
        pipelineSchema.partitionKeys().stream()
                .map(columnName -> normalizeColumnName(columnName, caseSensitive))
                .forEach(keyColumns::add);
        targetSchema.primaryKeys().stream()
                .map(columnName -> normalizeColumnName(columnName, caseSensitive))
                .forEach(keyColumns::add);
        targetSchema.partitionKeys().stream()
                .map(columnName -> normalizeColumnName(columnName, caseSensitive))
                .forEach(keyColumns::add);
        return keyColumns;
    }

    private static String normalizeColumnName(String columnName, boolean caseSensitive) {
        return caseSensitive ? columnName : columnName.toLowerCase(Locale.ROOT);
    }

    private static List<String> getColumnNames(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    private static boolean canContain(DataType targetType, DataType sourceType) {
        targetType = targetType.notNull();
        sourceType = sourceType.notNull();
        if (targetType.equals(sourceType)) {
            return true;
        }
        if (targetType.is(DataTypeFamily.INTEGER_NUMERIC)
                && sourceType.is(DataTypeFamily.INTEGER_NUMERIC)) {
            return integerRank(targetType) >= integerRank(sourceType);
        }
        if (targetType.is(DataTypeFamily.APPROXIMATE_NUMERIC)
                && sourceType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
            return approximateNumericRank(targetType) >= approximateNumericRank(sourceType);
        }
        if (targetType instanceof DecimalType && sourceType instanceof DecimalType) {
            DecimalType targetDecimal = (DecimalType) targetType;
            DecimalType sourceDecimal = (DecimalType) sourceType;
            return targetDecimal.getScale() >= sourceDecimal.getScale()
                    && targetDecimal.getPrecision() - targetDecimal.getScale()
                            >= sourceDecimal.getPrecision() - sourceDecimal.getScale();
        }
        if (targetType.is(DataTypeFamily.CHARACTER_STRING)
                && sourceType.is(DataTypeFamily.CHARACTER_STRING)) {
            return (targetType instanceof VarCharType
                            && getCharacterLength(targetType) >= getCharacterLength(sourceType))
                    || (targetType instanceof CharType
                            && sourceType instanceof CharType
                            && getCharacterLength(targetType) >= getCharacterLength(sourceType));
        }
        if (targetType.is(DataTypeFamily.BINARY_STRING)
                && sourceType.is(DataTypeFamily.BINARY_STRING)) {
            return (targetType instanceof VarBinaryType
                            && getBinaryLength(targetType) >= getBinaryLength(sourceType))
                    || (targetType instanceof BinaryType
                            && sourceType instanceof BinaryType
                            && getBinaryLength(targetType) >= getBinaryLength(sourceType));
        }
        return isTemporalWithPrecision(targetType)
                && targetType.getClass().equals(sourceType.getClass())
                && getTemporalPrecision(targetType) >= getTemporalPrecision(sourceType);
    }

    private static Optional<DataType> getSafeWidenedType(DataType targetType, DataType sourceType) {
        boolean nullable = targetType.isNullable();
        targetType = targetType.notNull();
        sourceType = sourceType.notNull();

        if (targetType.is(DataTypeFamily.INTEGER_NUMERIC)
                && sourceType.is(DataTypeFamily.INTEGER_NUMERIC)) {
            return Optional.of(
                    (integerRank(targetType) >= integerRank(sourceType) ? targetType : sourceType)
                            .copy(nullable));
        }
        if (targetType.is(DataTypeFamily.APPROXIMATE_NUMERIC)
                && sourceType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
            return Optional.of(
                    (approximateNumericRank(targetType) >= approximateNumericRank(sourceType)
                                    ? targetType
                                    : sourceType)
                            .copy(nullable));
        }
        if (targetType instanceof DecimalType && sourceType instanceof DecimalType) {
            DecimalType targetDecimal = (DecimalType) targetType;
            DecimalType sourceDecimal = (DecimalType) sourceType;
            int scale = Math.max(targetDecimal.getScale(), sourceDecimal.getScale());
            int integerDigits =
                    Math.max(
                            targetDecimal.getPrecision() - targetDecimal.getScale(),
                            sourceDecimal.getPrecision() - sourceDecimal.getScale());
            int precision = integerDigits + scale;
            if (precision <= DecimalType.MAX_PRECISION) {
                return Optional.of(new DecimalType(nullable, precision, scale));
            }
            return Optional.empty();
        }
        if (targetType.is(DataTypeFamily.CHARACTER_STRING)
                && sourceType.is(DataTypeFamily.CHARACTER_STRING)) {
            int length = Math.max(getCharacterLength(targetType), getCharacterLength(sourceType));
            if (targetType instanceof VarCharType || sourceType instanceof VarCharType) {
                return Optional.of(new VarCharType(nullable, length));
            }
            return Optional.of(new CharType(nullable, length));
        }
        if (targetType.is(DataTypeFamily.BINARY_STRING)
                && sourceType.is(DataTypeFamily.BINARY_STRING)) {
            int length = Math.max(getBinaryLength(targetType), getBinaryLength(sourceType));
            if (targetType instanceof VarBinaryType || sourceType instanceof VarBinaryType) {
                return Optional.of(new VarBinaryType(nullable, length));
            }
            return Optional.of(new BinaryType(nullable, length));
        }
        if (targetType.getClass().equals(sourceType.getClass())) {
            int precision =
                    Math.max(getTemporalPrecision(targetType), getTemporalPrecision(sourceType));
            if (targetType instanceof TimeType) {
                return Optional.of(new TimeType(nullable, precision));
            }
            if (targetType instanceof TimestampType) {
                return Optional.of(new TimestampType(nullable, precision));
            }
            if (targetType instanceof LocalZonedTimestampType) {
                return Optional.of(new LocalZonedTimestampType(nullable, precision));
            }
            if (targetType instanceof ZonedTimestampType) {
                return Optional.of(new ZonedTimestampType(nullable, precision));
            }
        }
        return Optional.empty();
    }

    private static int integerRank(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case TINYINT:
                return 0;
            case SMALLINT:
                return 1;
            case INTEGER:
                return 2;
            case BIGINT:
                return 3;
            default:
                throw new IllegalArgumentException("Not an integer type: " + type);
        }
    }

    private static int approximateNumericRank(DataType type) {
        switch (type.getTypeRoot()) {
            case FLOAT:
                return 0;
            case DOUBLE:
                return 1;
            default:
                throw new IllegalArgumentException("Not an approximate numeric type: " + type);
        }
    }

    private static int getCharacterLength(DataType type) {
        if (type instanceof CharType) {
            return ((CharType) type).getLength();
        }
        return ((VarCharType) type).getLength();
    }

    private static int getBinaryLength(DataType type) {
        if (type instanceof BinaryType) {
            return ((BinaryType) type).getLength();
        }
        return ((VarBinaryType) type).getLength();
    }

    private static int getTemporalPrecision(DataType type) {
        if (type instanceof TimeType) {
            return ((TimeType) type).getPrecision();
        }
        if (type instanceof TimestampType) {
            return ((TimestampType) type).getPrecision();
        }
        if (type instanceof LocalZonedTimestampType) {
            return ((LocalZonedTimestampType) type).getPrecision();
        }
        if (type instanceof ZonedTimestampType) {
            return ((ZonedTimestampType) type).getPrecision();
        }
        return -1;
    }

    private static boolean isTemporalWithPrecision(DataType type) {
        return type instanceof TimeType
                || type instanceof TimestampType
                || type instanceof LocalZonedTimestampType
                || type instanceof ZonedTimestampType;
    }

    private static class ColumnIndex {
        private final Map<String, Column> columns;
        private final Set<String> ambiguousColumnNames;
        private final boolean caseSensitive;

        private ColumnIndex(
                Map<String, Column> columns,
                Set<String> ambiguousColumnNames,
                boolean caseSensitive) {
            this.columns = columns;
            this.ambiguousColumnNames = ambiguousColumnNames;
            this.caseSensitive = caseSensitive;
        }

        private Column get(String columnName) {
            return columns.get(normalizeColumnName(columnName, caseSensitive));
        }

        private Set<String> getAmbiguousColumnNames() {
            return ambiguousColumnNames;
        }
    }
}
