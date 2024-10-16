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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utils for {@link Schema} to perform the ability of evolution. */
@PublicEvolving
public class SchemaUtils {

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Schema} to get Object from
     * RecordData.
     */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema) {
        return createFieldGetters(schema.getColumns());
    }

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Column} to get Object from
     * RecordData.
     */
    public static List<RecordData.FieldGetter> createFieldGetters(List<Column> columns) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            fieldGetters.add(RecordData.createFieldGetter(columns.get(i).getType(), i));
        }
        return fieldGetters;
    }

    /** Restore original data fields from RecordData structure. */
    public static List<Object> restoreOriginalData(
            @Nullable RecordData recordData, List<RecordData.FieldGetter> fieldGetters) {
        if (recordData == null) {
            return Collections.emptyList();
        }
        List<Object> actualFields = new ArrayList<>();
        for (RecordData.FieldGetter fieldGetter : fieldGetters) {
            actualFields.add(fieldGetter.getFieldOrNull(recordData));
        }
        return actualFields;
    }

    /** Merge compatible upstream schemas. */
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

    /** Try to combine two schemas with potential incompatible type. */
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

    /** Try to combine two columns with potential incompatible type. */
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

    /** Try to combine given data types to a compatible wider data type. */
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
            mergedType = DataTypes.DECIMAL(resultIntDigits + resultScale, resultScale);
        } else if (lType instanceof DecimalType && rType.is(DataTypeFamily.EXACT_NUMERIC)) {
            // Merge decimal and int
            DecimalType lhsDecimal = (DecimalType) lType;
            mergedType =
                    DataTypes.DECIMAL(
                            Math.max(
                                    lhsDecimal.getPrecision(),
                                    lhsDecimal.getScale() + getNumericPrecision(rType)),
                            lhsDecimal.getScale());
        } else if (rType instanceof DecimalType && lType.is(DataTypeFamily.EXACT_NUMERIC)) {
            // Merge decimal and int
            DecimalType rhsDecimal = (DecimalType) rType;
            mergedType =
                    DataTypes.DECIMAL(
                            Math.max(
                                    rhsDecimal.getPrecision(),
                                    rhsDecimal.getScale() + getNumericPrecision(lType)),
                            rhsDecimal.getScale());
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

    /** apply SchemaChangeEvent to the old schema and return the schema after changing. */
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
}
