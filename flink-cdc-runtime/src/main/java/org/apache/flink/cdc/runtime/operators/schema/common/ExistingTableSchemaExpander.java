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
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.pipeline.ExistingTableSchemaExpansionMode;
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
import org.apache.flink.util.FlinkRuntimeException;

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

/**
 * Handles the initial {@link CreateTableEvent} for an existing target table according to the
 * configured {@link ExistingTableSchemaExpansionMode}.
 */
@Internal
public class ExistingTableSchemaExpander {

    private static final Logger LOG = LoggerFactory.getLogger(ExistingTableSchemaExpander.class);

    private final MetadataApplier metadataApplier;
    private final ExistingTableSchemaExpansionSupport expansionSupport;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final ExistingTableSchemaExpansionMode mode;

    public ExistingTableSchemaExpander(
            MetadataApplier metadataApplier,
            ExistingTableSchemaExpansionSupport expansionSupport,
            SchemaChangeBehavior schemaChangeBehavior) {
        this(
                metadataApplier,
                expansionSupport,
                schemaChangeBehavior,
                ExistingTableSchemaExpansionMode.TRY_EXPAND);
    }

    public ExistingTableSchemaExpander(
            MetadataApplier metadataApplier,
            ExistingTableSchemaExpansionSupport expansionSupport,
            SchemaChangeBehavior schemaChangeBehavior,
            ExistingTableSchemaExpansionMode mode) {
        this.metadataApplier = metadataApplier;
        this.expansionSupport = expansionSupport;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.mode = mode;
    }

    /**
     * Handles the initial {@link CreateTableEvent} for an existing target table.
     *
     * @return whether the caller should proceed to apply the original {@link CreateTableEvent} to
     *     the sink. {@code CHECK} mode returns {@code false} after a successful check so that no
     *     external DDL is issued by the pipeline.
     */
    public boolean expand(CreateTableEvent createTableEvent) {
        if (mode == ExistingTableSchemaExpansionMode.CHECK) {
            // CHECK guards the initial table state and runs regardless of schema.change.behavior.
            checkCompatibility(createTableEvent);
            return false;
        }
        if (schemaChangeBehavior == SchemaChangeBehavior.IGNORE
                || schemaChangeBehavior == SchemaChangeBehavior.EXCEPTION) {
            // Keep the original rule: TRY_EXPAND/EXPAND skip framework-side handling here.
            return true;
        }
        switch (mode) {
            case TRY_EXPAND:
                tryExpand(createTableEvent);
                return true;
            case EXPAND:
                expandStrictly(createTableEvent);
                return true;
            case OFF:
            default:
                return true;
        }
    }

    private void checkCompatibility(CreateTableEvent createTableEvent) {
        Optional<Schema> targetSchema = queryTargetSchema(createTableEvent.tableId());
        if (!targetSchema.isPresent()) {
            throw new SchemaEvolveException(
                    createTableEvent,
                    String.format(
                            "Existing target table %s does not exist. CHECK mode never creates tables; create the target table externally first.",
                            createTableEvent.tableId()));
        }
        ExpansionPlan plan = analyze(createTableEvent, targetSchema.get());
        // CHECK never issues DDL, so every difference - including missing columns and narrow
        // column types that EXPAND could repair - makes the target table unable to contain the
        // upstream schema.
        if (!plan.incompatibilities.isEmpty()
                || !plan.columnsToAdd.isEmpty()
                || !plan.columnsToWiden.isEmpty()) {
            throw incompatibleException(createTableEvent, plan);
        }
        LOG.info(
                "Existing target table {} passed the schema compatibility check.",
                createTableEvent.tableId());
    }

    private void tryExpand(CreateTableEvent createTableEvent) {
        try {
            if (!supportsAnyExpansionDdl()) {
                LOG.info(
                        "Neither ADD_COLUMN nor ALTER_COLUMN_TYPE is enabled or supported for target table {}. Delegating schema handling to the sink.",
                        createTableEvent.tableId());
                return;
            }
            Optional<Schema> targetSchema = queryTargetSchema(createTableEvent.tableId());
            if (!targetSchema.isPresent()) {
                LOG.info(
                        "Target table {} does not exist. Delegating table creation to the sink.",
                        createTableEvent.tableId());
                return;
            }
            ExpansionPlan plan = analyze(createTableEvent, targetSchema.get());
            for (String incompatibility : plan.incompatibilities) {
                LOG.warn(
                        "Target table {} has an unsupported difference: {}. Delegating it to the sink.",
                        createTableEvent.tableId(),
                        incompatibility);
            }
            applyPlan(createTableEvent, plan);
            verifyExpansion(createTableEvent, plan, false);
        } catch (Exception e) {
            LOG.warn(
                    "Best-effort schema expansion failed for existing target table {}. Delegating schema handling to the sink.",
                    createTableEvent.tableId(),
                    e);
        }
    }

    private void expandStrictly(CreateTableEvent createTableEvent) {
        Optional<Schema> targetSchema = queryTargetSchema(createTableEvent.tableId());
        if (!targetSchema.isPresent()) {
            LOG.info(
                    "Target table {} does not exist. Delegating table creation to the sink.",
                    createTableEvent.tableId());
            return;
        }
        ExpansionPlan plan = analyze(createTableEvent, targetSchema.get());
        if (!plan.incompatibilities.isEmpty()) {
            throw incompatibleException(createTableEvent, plan);
        }
        // A fully compatible target table needs no DDL, so a missing DDL capability is only an
        // error when differences actually require repair; applyPlan enforces that per event type.
        applyPlan(createTableEvent, plan);
        verifyExpansion(createTableEvent, plan, true);
    }

    private boolean supportsAnyExpansionDdl() {
        return supportsSchemaEvolutionType(SchemaChangeEventType.ADD_COLUMN)
                || supportsSchemaEvolutionType(SchemaChangeEventType.ALTER_COLUMN_TYPE);
    }

    private SchemaEvolveException incompatibleException(
            CreateTableEvent createTableEvent, ExpansionPlan plan) {
        StringBuilder differences = new StringBuilder();
        for (String incompatibility : plan.incompatibilities) {
            differences.append("\n - ").append(incompatibility);
        }
        for (Column columnToAdd : plan.columnsToAdd) {
            differences
                    .append("\n - target table is missing column \"")
                    .append(columnToAdd.getName())
                    .append("\"");
        }
        for (Map.Entry<String, DataType> columnToWiden : plan.columnsToWiden.entrySet()) {
            differences
                    .append("\n - target column \"")
                    .append(columnToWiden.getKey())
                    .append("\" is narrower than pipeline type ")
                    .append(columnToWiden.getValue());
        }
        String message =
                String.format(
                        "Existing target table %s cannot contain the pipeline schema:%s",
                        createTableEvent.tableId(), differences);
        String repairSuggestions = renderRepairSuggestions(createTableEvent, plan);
        if (!repairSuggestions.isEmpty()) {
            message +=
                    String.format(
                            "\nSuggested repair statements (adjust to the target system's DDL dialect):%s",
                            repairSuggestions);
        }
        return new SchemaEvolveException(createTableEvent, message);
    }

    /**
     * Renders lightweight, review-oriented ALTER TABLE suggestions for the safely repairable
     * differences. Differences that cannot be fixed safely never get a suggested statement.
     */
    private String renderRepairSuggestions(CreateTableEvent createTableEvent, ExpansionPlan plan) {
        StringBuilder suggestions = new StringBuilder();
        for (Column columnToAdd : plan.columnsToAdd) {
            suggestions
                    .append("\n - ALTER TABLE ")
                    .append(createTableEvent.tableId())
                    .append(" ADD COLUMN ")
                    .append(columnToAdd.getName())
                    .append(" ")
                    .append(columnToAdd.getType())
                    .append(";");
        }
        for (Map.Entry<String, DataType> columnToWiden : plan.columnsToWiden.entrySet()) {
            suggestions
                    .append("\n - ALTER TABLE ")
                    .append(createTableEvent.tableId())
                    .append(" ALTER COLUMN ")
                    .append(columnToWiden.getKey())
                    .append(" TYPE ")
                    .append(columnToWiden.getValue())
                    .append(";");
        }
        return suggestions.toString();
    }

    /**
     * Analyzes the pipeline schema against the current target schema and derives the safe DDL plan.
     * Differences that cannot be fixed by safe DDL are collected in {@link
     * ExpansionPlan#incompatibilities}.
     */
    private ExpansionPlan analyze(CreateTableEvent createTableEvent, Schema currentTargetSchema) {
        ExpansionPlan plan = new ExpansionPlan();
        Schema pipelineSchema = createTableEvent.getSchema();
        boolean columnNameCaseSensitive = expansionSupport.isColumnNameCaseSensitive();
        ColumnIndex targetColumns = indexColumns(currentTargetSchema, columnNameCaseSensitive);
        plan.targetColumns = targetColumns;
        Set<String> ambiguousColumnNames =
                new HashSet<>(
                        indexColumns(pipelineSchema, columnNameCaseSensitive)
                                .getAmbiguousColumnNames());
        ambiguousColumnNames.addAll(targetColumns.getAmbiguousColumnNames());
        Set<String> keyColumns =
                getKeyColumns(pipelineSchema, currentTargetSchema, columnNameCaseSensitive);

        for (Column pipelineColumn : pipelineSchema.getColumns()) {
            String columnName = pipelineColumn.getName();
            String comparisonName = normalizeColumnName(columnName, columnNameCaseSensitive);
            if (ambiguousColumnNames.contains(comparisonName)) {
                plan.incompatibilities.add(
                        String.format(
                                "column \"%s\" is ambiguous under the target system's case-sensitivity rule",
                                columnName));
                continue;
            }

            Column targetColumn = targetColumns.get(columnName);
            if (targetColumn == null) {
                if (pipelineColumn.isPhysical() && !keyColumns.contains(comparisonName)) {
                    plan.columnsToAdd.add(pipelineColumn.copy(pipelineColumn.getType().nullable()));
                } else {
                    plan.incompatibilities.add(
                            String.format(
                                    "target table is missing the non-addable column \"%s\"",
                                    columnName));
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
                plan.incompatibilities.add(
                        String.format(
                                "pipeline type %s of column \"%s\" cannot be normalized to the target type system",
                                pipelineColumn.getType(), columnName));
                continue;
            }
            DataType normalizedPipelineType = normalizedPipelineTypeOptional.get().nullable();
            DataType targetType = targetColumn.getType().nullable();

            if (pipelineColumn.getType().isNullable() && !targetColumn.getType().isNullable()) {
                plan.incompatibilities.add(
                        String.format(
                                "column \"%s\" is nullable in the pipeline but NOT NULL in the target table",
                                columnName));
                continue;
            }

            if (canContain(targetType, normalizedPipelineType)) {
                continue;
            }
            if (keyColumns.contains(comparisonName)) {
                plan.incompatibilities.add(
                        String.format(
                                "key column \"%s\" with target type %s cannot contain pipeline type %s",
                                columnName, targetType, normalizedPipelineType));
                continue;
            }

            Optional<DataType> widenedType = getSafeWidenedType(targetType, normalizedPipelineType);
            if (!widenedType.isPresent()) {
                plan.incompatibilities.add(
                        String.format(
                                "column \"%s\" with target type %s cannot safely contain pipeline type %s",
                                columnName, targetType, normalizedPipelineType));
                continue;
            }

            Optional<DataType> normalizedWidenedTypeOptional =
                    normalizeType(
                            createTableEvent.tableId(),
                            columnName,
                            widenedType.get(),
                            currentTargetSchema);
            if (!normalizedWidenedTypeOptional.isPresent()) {
                plan.incompatibilities.add(
                        String.format(
                                "proposed widened type %s for column \"%s\" cannot be normalized to the target type system",
                                widenedType.get(), columnName));
                continue;
            }
            DataType normalizedWidenedType = normalizedWidenedTypeOptional.get().nullable();
            if (!canContain(normalizedWidenedType, targetType)
                    || !canContain(normalizedWidenedType, normalizedPipelineType)) {
                plan.incompatibilities.add(
                        String.format(
                                "target system normalizes proposed widened type %s for column \"%s\" to %s, which is not a safe widening",
                                widenedType.get(), columnName, normalizedWidenedType));
                continue;
            }

            plan.columnsToWiden.put(
                    targetColumn.getName(),
                    widenedType.get().copy(targetColumn.getType().isNullable()));
        }
        return plan;
    }

    private void applyPlan(CreateTableEvent createTableEvent, ExpansionPlan plan) {
        if (!plan.columnsToAdd.isEmpty()) {
            if (!supportsSchemaEvolutionType(SchemaChangeEventType.ADD_COLUMN)) {
                throw new SchemaEvolveException(
                        createTableEvent,
                        String.format(
                                "Target table %s is missing columns %s, but ADD_COLUMN is not enabled or supported by the sink.",
                                createTableEvent.tableId(), getColumnNames(plan.columnsToAdd)));
            }
            AddColumnEvent addColumnEvent =
                    new AddColumnEvent(
                            createTableEvent.tableId(),
                            plan.columnsToAdd.stream()
                                    .map(AddColumnEvent.ColumnWithPosition::new)
                                    .collect(Collectors.toList()));
            applySchemaChange(addColumnEvent, plan.columnsToAdd);
        }
        if (!plan.columnsToWiden.isEmpty()) {
            if (!supportsSchemaEvolutionType(SchemaChangeEventType.ALTER_COLUMN_TYPE)) {
                throw new SchemaEvolveException(
                        createTableEvent,
                        String.format(
                                "Target table %s has narrow columns %s, but ALTER_COLUMN_TYPE is not enabled or supported by the sink.",
                                createTableEvent.tableId(), plan.columnsToWiden.keySet()));
            }
            AlterColumnTypeEvent alterColumnTypeEvent =
                    new AlterColumnTypeEvent(
                            createTableEvent.tableId(),
                            plan.columnsToWiden,
                            plan.columnsToWiden.keySet().stream()
                                    .collect(
                                            Collectors.toMap(
                                                    columnName -> columnName,
                                                    columnName ->
                                                            plan.targetColumns
                                                                    .get(columnName)
                                                                    .getType())));
            applySchemaChange(alterColumnTypeEvent, plan.columnsToWiden);
        }
    }

    /** Re-reads the target schema after applying derived DDL to detect no-op or failed DDL. */
    private void verifyExpansion(
            CreateTableEvent createTableEvent, ExpansionPlan plan, boolean strict) {
        if (plan.columnsToAdd.isEmpty() && plan.columnsToWiden.isEmpty()) {
            return;
        }
        Optional<Schema> updatedTargetSchema = queryTargetSchema(createTableEvent.tableId());
        if (!updatedTargetSchema.isPresent()) {
            throw new SchemaEvolveException(
                    createTableEvent,
                    String.format(
                            "Failed to read back target table %s after expansion.",
                            createTableEvent.tableId()));
        }
        ExpansionPlan remaining = analyze(createTableEvent, updatedTargetSchema.get());
        if (!remaining.incompatibilities.isEmpty()
                || !remaining.columnsToAdd.isEmpty()
                || !remaining.columnsToWiden.isEmpty()) {
            String message =
                    String.format(
                            "Target table %s still has unresolved differences after expansion: %s",
                            createTableEvent.tableId(), remaining.incompatibilities);
            if (strict) {
                throw new SchemaEvolveException(createTableEvent, message);
            }
            LOG.warn("{}. Sink data may lose those columns.", message);
        }
    }

    private Optional<Schema> queryTargetSchema(TableId tableId) {
        try {
            return expansionSupport.getExistingTableSchema(tableId);
        } catch (Exception e) {
            // Propagate so the job fails over and retries, instead of proceeding to apply the
            // original CreateTableEvent and silently dropping columns.
            throw new FlinkRuntimeException(
                    "Failed to query schema of existing target table " + tableId, e);
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
        // Failures propagate to the caller: TRY_EXPAND catches and delegates to the sink, while
        // CHECK/EXPAND fail the job.
        LOG.info(
                "Attempting to apply schema change event derived for existing table expansion: {} ({})",
                event,
                changes);
        metadataApplier.applySchemaChange(event);
        LOG.info(
                "The schema change call for existing table expansion completed without an exception: {}",
                event);
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

    /** Result of analyzing a pipeline schema against an existing target schema. */
    private static final class ExpansionPlan {
        private final List<Column> columnsToAdd = new ArrayList<>();
        private final Map<String, DataType> columnsToWiden = new LinkedHashMap<>();
        private final List<String> incompatibilities = new ArrayList<>();
        private ColumnIndex targetColumns;
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
