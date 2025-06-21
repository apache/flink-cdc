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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utils for {@link Schema} to perform the ability of evolution. */
@PublicEvolving
public class SchemaUtils {

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Schema} to get Object from
     * RecordData.
     */
    @CheckReturnValue
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema) {
        return createFieldGetters(schema.getColumns());
    }

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Column} to get Object from
     * RecordData.
     */
    @CheckReturnValue
    public static List<RecordData.FieldGetter> createFieldGetters(List<Column> columns) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            fieldGetters.add(RecordData.createFieldGetter(columns.get(i).getType(), i));
        }
        return fieldGetters;
    }

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Column} to get Object from
     * RecordData.
     */
    @CheckReturnValue
    public static List<RecordData.FieldGetter> createFieldGetters(DataType[] dataTypes) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(dataTypes.length);
        for (int i = 0; i < dataTypes.length; i++) {
            fieldGetters.add(RecordData.createFieldGetter(dataTypes[i], i));
        }
        return fieldGetters;
    }

    /** Restore original data fields from RecordData structure. */
    @CheckReturnValue
    public static List<Object> restoreOriginalData(
            @Nullable RecordData recordData, List<RecordData.FieldGetter> fieldGetters) {
        if (recordData == null) {
            return null;
        }
        List<Object> actualFields = new ArrayList<>();
        for (RecordData.FieldGetter fieldGetter : fieldGetters) {
            actualFields.add(fieldGetter.getFieldOrNull(recordData));
        }
        return actualFields;
    }

    /** apply SchemaChangeEvent to the old schema and return the schema after changing. */
    @CheckReturnValue
    public static Schema applySchemaChangeEvent(Schema schema, SchemaChangeEvent event) {
        return SchemaChangeEventVisitor.visit(
                event,
                addColumnEvent -> applyAddColumnEvent(addColumnEvent, schema),
                alterColumnTypeEvent -> applyAlterColumnTypeEvent(alterColumnTypeEvent, schema),
                createTableEvent -> createTableEvent.getSchema(),
                dropColumnEvent -> applyDropColumnEvent(dropColumnEvent, schema),
                dropTableEvent -> schema,
                renameColumnEvent -> applyRenameColumnEvent(renameColumnEvent, schema),
                truncateTableEvent -> schema);
    }

    private static Schema applyAddColumnEvent(AddColumnEvent event, Schema oldSchema) {
        LinkedList<Column> columns = new LinkedList<>(oldSchema.getColumns());
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            switch (columnWithPosition.getPosition()) {
                case FIRST:
                    {
                        columns.addFirst(columnWithPosition.getAddColumn());
                        break;
                    }
                case LAST:
                    {
                        columns.addLast(columnWithPosition.getAddColumn());
                        break;
                    }
                case BEFORE:
                    {
                        Preconditions.checkNotNull(
                                columnWithPosition.getExistedColumnName(),
                                "existedColumnName could not be null in BEFORE type AddColumnEvent");
                        List<String> columnNames =
                                columns.stream().map(Column::getName).collect(Collectors.toList());
                        int index = columnNames.indexOf(columnWithPosition.getExistedColumnName());
                        if (index < 0) {
                            throw new IllegalArgumentException(
                                    columnWithPosition.getExistedColumnName()
                                            + " of AddColumnEvent is not existed");
                        }
                        columns.add(index, columnWithPosition.getAddColumn());
                        break;
                    }
                case AFTER:
                    {
                        Preconditions.checkNotNull(
                                columnWithPosition.getExistedColumnName(),
                                "existedColumnName could not be null in AFTER type AddColumnEvent");
                        List<String> columnNames =
                                columns.stream().map(Column::getName).collect(Collectors.toList());
                        int index = columnNames.indexOf(columnWithPosition.getExistedColumnName());
                        if (index < 0) {
                            throw new IllegalArgumentException(
                                    columnWithPosition.getExistedColumnName()
                                            + " of AddColumnEvent is not existed");
                        }
                        columns.add(index + 1, columnWithPosition.getAddColumn());
                        break;
                    }
            }
        }
        return oldSchema.copy(columns);
    }

    private static Schema applyDropColumnEvent(DropColumnEvent event, Schema oldSchema) {
        List<Column> columns =
                oldSchema.getColumns().stream()
                        .filter(
                                (column ->
                                        !event.getDroppedColumnNames().contains(column.getName())))
                        .collect(Collectors.toList());
        return oldSchema.copy(columns);
    }

    private static Schema applyRenameColumnEvent(RenameColumnEvent event, Schema oldSchema) {
        List<Column> columns = new ArrayList<>();
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getNameMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getNameMapping().get(column.getName())));
                            } else {
                                columns.add(column);
                            }
                        });
        return oldSchema.copy(columns);
    }

    private static Schema applyAlterColumnTypeEvent(AlterColumnTypeEvent event, Schema oldSchema) {
        List<Column> columns = new ArrayList<>();
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getTypeMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getTypeMapping().get(column.getName())));
                            } else {
                                columns.add(column);
                            }
                        });
        return oldSchema.copy(columns);
    }

    /**
     * This function determines if the given schema change event {@code event} should be sent to
     * downstream based on if the given transform rule has asterisk, and what columns are
     * referenced.
     *
     * <p>For example, if {@code hasAsterisk} is false, then all {@code AddColumnEvent} and {@code
     * DropColumnEvent} should be ignored since asterisk-less transform should not emit schema
     * change events that change number of downstream columns.
     *
     * <p>Also, {@code referencedColumns} will be used to determine if the schema change event
     * affects any referenced columns, since if a column has been projected out of downstream, its
     * corresponding schema change events should not be emitted, either.
     *
     * <p>For the case when {@code hasAsterisk} is true, things will be cleaner since we don't have
     * to filter out any schema change events. All we need to do is to change {@code
     * AddColumnEvent}'s inserting position, and replacing `FIRST` / `LAST` with column-relative
     * position indicators. This is necessary since extra calculated columns might be added, and
     * `FIRST` / `LAST` position might differ.
     */
    @CheckReturnValue
    public static Optional<SchemaChangeEvent> transformSchemaChangeEvent(
            boolean hasAsterisk, List<String> referencedColumns, SchemaChangeEvent event) {
        Optional<SchemaChangeEvent> evolvedSchemaChangeEvent = Optional.empty();
        if (event instanceof AddColumnEvent) {
            // Send add column events to downstream iff there's an asterisk
            if (hasAsterisk) {
                List<AddColumnEvent.ColumnWithPosition> addedColumns =
                        ((AddColumnEvent) event)
                                .getAddedColumns().stream()
                                        .map(
                                                e -> {
                                                    if (AddColumnEvent.ColumnPosition.LAST.equals(
                                                            e.getPosition())) {
                                                        return new AddColumnEvent
                                                                .ColumnWithPosition(
                                                                e.getAddColumn(),
                                                                AddColumnEvent.ColumnPosition.AFTER,
                                                                referencedColumns.get(
                                                                        referencedColumns.size()
                                                                                - 1));
                                                    } else if (AddColumnEvent.ColumnPosition.FIRST
                                                            .equals(e.getPosition())) {
                                                        return new AddColumnEvent
                                                                .ColumnWithPosition(
                                                                e.getAddColumn(),
                                                                AddColumnEvent.ColumnPosition
                                                                        .BEFORE,
                                                                referencedColumns.get(0));
                                                    } else {
                                                        return e;
                                                    }
                                                })
                                        .collect(Collectors.toList());
                evolvedSchemaChangeEvent =
                        Optional.of(new AddColumnEvent(event.tableId(), addedColumns));
            }
        } else if (event instanceof AlterColumnTypeEvent) {
            AlterColumnTypeEvent alterColumnTypeEvent = (AlterColumnTypeEvent) event;
            if (hasAsterisk) {
                // In wildcard mode, all alter column type events should be sent to downstream
                evolvedSchemaChangeEvent = Optional.of(event);
            } else {
                // Or, we need to filter out those referenced columns and reconstruct
                // SchemaChangeEvents
                Map<String, DataType> newDataTypeMap =
                        alterColumnTypeEvent.getTypeMapping().entrySet().stream()
                                .filter(e -> referencedColumns.contains(e.getKey()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (!newDataTypeMap.isEmpty()) {
                    evolvedSchemaChangeEvent =
                            Optional.of(
                                    new AlterColumnTypeEvent(
                                            alterColumnTypeEvent.tableId(), newDataTypeMap));
                }
            }
        } else if (event instanceof RenameColumnEvent) {
            if (hasAsterisk) {
                evolvedSchemaChangeEvent = Optional.of(event);
            }
        } else if (event instanceof DropColumnEvent) {
            if (hasAsterisk) {
                evolvedSchemaChangeEvent = Optional.of(event);
            }
        } else {
            evolvedSchemaChangeEvent = Optional.of(event);
        }
        return evolvedSchemaChangeEvent;
    }

    /**
     * This function checks if the given schema change event has been applied already. If so, it
     * will be ignored to avoid sending duplicate evolved schema change events to sink metadata
     * applier.
     */
    public static boolean isSchemaChangeEventRedundant(
            @Nullable Schema currentSchema, SchemaChangeEvent event) {
        Optional<Schema> latestSchema = Optional.ofNullable(currentSchema);
        return Boolean.TRUE.equals(
                SchemaChangeEventVisitor.visit(
                        event,
                        addColumnEvent -> {
                            // It has not been applied if schema does not even exist
                            if (!latestSchema.isPresent()) {
                                return false;
                            }
                            List<Column> existedColumns = latestSchema.get().getColumns();

                            // It has been applied only if all columns are present in existedColumns
                            for (AddColumnEvent.ColumnWithPosition column :
                                    addColumnEvent.getAddedColumns()) {
                                if (!existedColumns.contains(column.getAddColumn())) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        alterColumnTypeEvent -> {
                            // It has not been applied if schema does not even exist
                            if (!latestSchema.isPresent()) {
                                return false;
                            }
                            Schema schema = latestSchema.get();

                            // It has been applied only if all column types are set as expected
                            for (Map.Entry<String, DataType> entry :
                                    alterColumnTypeEvent.getTypeMapping().entrySet()) {
                                if (!schema.getColumn(entry.getKey()).isPresent()
                                        || !schema.getColumn(entry.getKey())
                                                .get()
                                                .getType()
                                                .equals(entry.getValue())) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        createTableEvent -> {
                            // It has been applied if such table already exists
                            return latestSchema.isPresent();
                        },
                        dropColumnEvent -> {
                            // It has not been applied if schema does not even exist
                            if (!latestSchema.isPresent()) {
                                return false;
                            }
                            List<String> existedColumnNames = latestSchema.get().getColumnNames();

                            // It has been applied only if corresponding column types do not exist
                            return dropColumnEvent.getDroppedColumnNames().stream()
                                    .noneMatch(existedColumnNames::contains);
                        },
                        dropTableEvent -> {
                            // It has been applied if such table does not exist
                            return !latestSchema.isPresent();
                        },
                        renameColumnEvent -> {
                            // It has been applied if such table already exists
                            if (!latestSchema.isPresent()) {
                                return false;
                            }
                            List<String> existedColumnNames = latestSchema.get().getColumnNames();

                            // It has been applied only if all previous names do not exist, and all
                            // new names already exist
                            for (Map.Entry<String, String> entry :
                                    renameColumnEvent.getNameMapping().entrySet()) {
                                if (existedColumnNames.contains(entry.getKey())
                                        || !existedColumnNames.contains(entry.getValue())) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        truncateTableEvent -> {
                            // We have no way to ensure if a TruncateTableEvent has been applied
                            // before. Just assume it's not.
                            return false;
                        }));
    }

    @CheckReturnValue
    public static Schema ensurePkNonNull(Schema schema) {
        Set<String> pkColumns = new HashSet<>(schema.primaryKeys());
        List<Column> columns =
                schema.getColumns().stream()
                        .map(
                                col ->
                                        pkColumns.contains(col.getName())
                                                ? col.copy(col.getType().notNull())
                                                : col)
                        .collect(Collectors.toList());
        return schema.copy(columns);
    }

    // Schema merging related utility methods have been moved to SchemaMergingUtils class.
    // The following methods have been deprecated and should not be used.

    /**
     * Merge compatible schemas.
     *
     * @deprecated Use {@code getCommonSchema} in {@link SchemaMergingUtils} instead.
     */
    @Deprecated
    public static Schema inferWiderSchema(List<Schema> schemas) {
        if (schemas.isEmpty()) {
            return null;
        } else if (schemas.size() == 1) {
            return schemas.get(0);
        } else {
            Schema outputSchema = null;
            for (Schema schema : schemas) {
                outputSchema = inferWiderSchema(outputSchema, schema);
            }
            return outputSchema;
        }
    }

    /**
     * Try to combine two schemas with potential incompatible type.
     *
     * @deprecated Use {@code getLeastCommonSchema} in {@link SchemaMergingUtils} instead.
     */
    @Deprecated
    @VisibleForTesting
    public static Schema inferWiderSchema(@Nullable Schema lSchema, Schema rSchema) {
        if (lSchema == null) {
            return rSchema;
        }
        if (lSchema.getColumnCount() != rSchema.getColumnCount()) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge schema %s and %s with different column counts.",
                            lSchema, rSchema));
        }
        if (!lSchema.primaryKeys().equals(rSchema.primaryKeys())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge schema %s and %s with different primary keys.",
                            lSchema, rSchema));
        }
        if (!lSchema.partitionKeys().equals(rSchema.partitionKeys())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge schema %s and %s with different partition keys.",
                            lSchema, rSchema));
        }
        if (!lSchema.options().equals(rSchema.options())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge schema %s and %s with different options.",
                            lSchema, rSchema));
        }
        if (!Objects.equals(lSchema.comment(), rSchema.comment())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge schema %s and %s with different comments.",
                            lSchema, rSchema));
        }

        List<Column> leftColumns = lSchema.getColumns();
        List<Column> rightColumns = rSchema.getColumns();

        List<Column> mergedColumns =
                IntStream.range(0, lSchema.getColumnCount())
                        .mapToObj(i -> inferWiderColumn(leftColumns.get(i), rightColumns.get(i)))
                        .collect(Collectors.toList());

        return lSchema.copy(mergedColumns);
    }

    /**
     * Try to combine two columns with potential incompatible type.
     *
     * @deprecated Use {@code getLeastCommonType} in {@link SchemaMergingUtils} instead.
     */
    @Deprecated
    @VisibleForTesting
    public static Column inferWiderColumn(Column lColumn, Column rColumn) {
        if (!Objects.equals(lColumn.getName(), rColumn.getName())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge column %s and %s with different name.",
                            lColumn, rColumn));
        }
        if (!Objects.equals(lColumn.getComment(), rColumn.getComment())) {
            throw new IllegalStateException(
                    String.format(
                            "Unable to merge column %s and %s with different comments.",
                            lColumn, rColumn));
        }
        return lColumn.copy(inferWiderType(lColumn.getType(), rColumn.getType()));
    }

    /**
     * Try to combine given data types to a compatible wider data type.
     *
     * @deprecated Use {@code getLeastCommonType} in {@link SchemaMergingUtils} instead.
     */
    @Deprecated
    @VisibleForTesting
    public static DataType inferWiderType(DataType lType, DataType rType) {
        // Ignore nullability during data type merge
        boolean nullable = lType.isNullable() || rType.isNullable();
        lType = lType.notNull();
        rType = rType.notNull();

        DataType mergedType;
        if (lType.equals(rType)) {
            // identical type
            mergedType = rType;
        } else if (lType instanceof TimestampType && rType instanceof TimestampType) {
            return DataTypes.TIMESTAMP(
                    Math.max(
                            ((TimestampType) lType).getPrecision(),
                            ((TimestampType) rType).getPrecision()));
        } else if (lType instanceof ZonedTimestampType && rType instanceof ZonedTimestampType) {
            return DataTypes.TIMESTAMP_TZ(
                    Math.max(
                            ((ZonedTimestampType) lType).getPrecision(),
                            ((ZonedTimestampType) rType).getPrecision()));
        } else if (lType instanceof LocalZonedTimestampType
                && rType instanceof LocalZonedTimestampType) {
            return DataTypes.TIMESTAMP_LTZ(
                    Math.max(
                            ((LocalZonedTimestampType) lType).getPrecision(),
                            ((LocalZonedTimestampType) rType).getPrecision()));
        } else if (lType.is(DataTypeFamily.TIMESTAMP) && rType.is(DataTypeFamily.TIMESTAMP)) {
            return DataTypes.TIMESTAMP(TimestampType.MAX_PRECISION);
        } else if (lType.is(DataTypeFamily.INTEGER_NUMERIC)
                && rType.is(DataTypeFamily.INTEGER_NUMERIC)) {
            mergedType = DataTypes.BIGINT();
        } else if (lType.is(DataTypeFamily.CHARACTER_STRING)
                && rType.is(DataTypeFamily.CHARACTER_STRING)) {
            mergedType = DataTypes.STRING();
        } else if (lType.is(DataTypeFamily.APPROXIMATE_NUMERIC)
                && rType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
            mergedType = DataTypes.DOUBLE();
        } else if (lType instanceof DecimalType && rType instanceof DecimalType) {
            // Merge two decimal types
            DecimalType lhsDecimal = (DecimalType) lType;
            DecimalType rhsDecimal = (DecimalType) rType;
            int resultIntDigits =
                    Math.max(
                            lhsDecimal.getPrecision() - lhsDecimal.getScale(),
                            rhsDecimal.getPrecision() - rhsDecimal.getScale());
            int resultScale = Math.max(lhsDecimal.getScale(), rhsDecimal.getScale());
            Preconditions.checkArgument(
                    resultIntDigits + resultScale <= DecimalType.MAX_PRECISION,
                    String.format(
                            "Failed to merge %s and %s type into DECIMAL. %d precision digits required, %d available",
                            lType,
                            rType,
                            resultIntDigits + resultScale,
                            DecimalType.MAX_PRECISION));
            mergedType = DataTypes.DECIMAL(resultIntDigits + resultScale, resultScale);
        } else if (lType instanceof DecimalType && rType.is(DataTypeFamily.EXACT_NUMERIC)) {
            // Merge decimal and int
            mergedType = mergeExactNumericsIntoDecimal((DecimalType) lType, rType);
        } else if (rType instanceof DecimalType && lType.is(DataTypeFamily.EXACT_NUMERIC)) {
            // Merge decimal and int
            mergedType = mergeExactNumericsIntoDecimal((DecimalType) rType, lType);
        } else {
            throw new IllegalStateException(
                    String.format("Incompatible types: \"%s\" and \"%s\"", lType, rType));
        }

        if (nullable) {
            return mergedType.nullable();
        } else {
            return mergedType.notNull();
        }
    }

    private static DataType mergeExactNumericsIntoDecimal(
            DecimalType decimalType, DataType otherType) {
        int resultPrecision =
                Math.max(
                        decimalType.getPrecision(),
                        decimalType.getScale() + getNumericPrecision(otherType));
        Preconditions.checkArgument(
                resultPrecision <= DecimalType.MAX_PRECISION,
                String.format(
                        "Failed to merge %s and %s type into DECIMAL. %d precision digits required, %d available",
                        decimalType, otherType, resultPrecision, DecimalType.MAX_PRECISION));
        return DataTypes.DECIMAL(resultPrecision, decimalType.getScale());
    }

    @Deprecated
    @VisibleForTesting
    public static int getNumericPrecision(DataType dataType) {
        if (dataType.is(DataTypeFamily.EXACT_NUMERIC)) {
            if (dataType.is(DataTypeRoot.TINYINT)) {
                return 3;
            } else if (dataType.is(DataTypeRoot.SMALLINT)) {
                return 5;
            } else if (dataType.is(DataTypeRoot.INTEGER)) {
                return 10;
            } else if (dataType.is(DataTypeRoot.BIGINT)) {
                return 19;
            } else if (dataType.is(DataTypeRoot.DECIMAL)) {
                return ((DecimalType) dataType).getPrecision();
            }
        }

        throw new IllegalArgumentException(
                "Failed to get precision of non-exact decimal type " + dataType);
    }
}
