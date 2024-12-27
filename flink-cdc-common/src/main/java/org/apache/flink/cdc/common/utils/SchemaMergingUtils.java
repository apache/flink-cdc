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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.Streams;
import org.apache.flink.shaded.guava31.com.google.common.io.BaseEncoding;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Utils for merging {@link Schema}s and {@link DataType}s. Prefer using this over {@link
 * SchemaUtils} to get consistent schema merging behaviors.
 */
@PublicEvolving
public class SchemaMergingUtils {
    /**
     * Checking if given {@code upcomingSchema} could be fit into currently known {@code
     * currentSchema}. Current schema could be null (as the cold opening state, and in this case it
     * always returns {@code false}) but the upcoming schema should never be null. <br>
     * This method only checks columns' type compatibility, but ignores metadata fields like
     * primaryKeys, partitionKeys, options.
     */
    public static boolean isSchemaCompatible(
            @Nullable Schema currentSchema, Schema upcomingSchema) {
        if (currentSchema == null) {
            return false;
        }
        Map<String, DataType> currentColumnTypes =
                currentSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Column::getType));
        List<Column> upcomingColumns = upcomingSchema.getColumns();

        for (Column upcomingColumn : upcomingColumns) {
            String columnName = upcomingColumn.getName();
            DataType upcomingColumnType = upcomingColumn.getType();
            DataType currentColumnType = currentColumnTypes.get(columnName);

            if (!isDataTypeCompatible(currentColumnType, upcomingColumnType)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Try to merge {@code upcomingSchema} into {@code currentSchema} by performing lenient schema
     * changes. Returns a wider schema that could both of them.
     */
    public static Schema getLeastCommonSchema(
            @Nullable Schema currentSchema, Schema upcomingSchema) {
        // No current schema record, we need to create it first.
        if (currentSchema == null) {
            return upcomingSchema;
        }

        // Current schema is compatible with upcoming ones, just return it and perform no schema
        // evolution.
        if (isSchemaCompatible(currentSchema, upcomingSchema)) {
            return currentSchema;
        }

        Map<String, DataType> newTypeMapping = new HashMap<>();

        Map<String, Column> currentColumns =
                currentSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, col -> col));
        List<Column> upcomingColumns = upcomingSchema.getColumns();

        List<Column> appendedColumns = new ArrayList<>();

        for (Column upcomingColumn : upcomingColumns) {
            String columnName = upcomingColumn.getName();
            DataType upcomingColumnType = upcomingColumn.getType();
            if (currentColumns.containsKey(columnName)) {
                Column currentColumn = currentColumns.get(columnName);
                DataType currentColumnType = currentColumn.getType();
                DataType leastCommonType =
                        getLeastCommonType(currentColumnType, upcomingColumnType);
                if (!Objects.equals(leastCommonType, currentColumnType)) {
                    newTypeMapping.put(columnName, leastCommonType);
                }
            } else {
                appendedColumns.add(upcomingColumn);
            }
        }

        List<Column> commonColumns = new ArrayList<>();
        for (Column column : currentSchema.getColumns()) {
            if (newTypeMapping.containsKey(column.getName())) {
                commonColumns.add(column.copy(newTypeMapping.get(column.getName())));
            } else {
                commonColumns.add(column);
            }
        }

        commonColumns.addAll(appendedColumns);
        return currentSchema.copy(commonColumns);
    }

    /** Merge compatible schemas. */
    public static Schema getCommonSchema(List<Schema> schemas) {
        if (schemas.isEmpty()) {
            return null;
        } else if (schemas.size() == 1) {
            return schemas.get(0);
        } else {
            Schema outputSchema = null;
            for (Schema schema : schemas) {
                outputSchema = getLeastCommonSchema(outputSchema, schema);
            }
            return outputSchema;
        }
    }

    /**
     * Generating what schema change events we need to do by converting compatible {@code
     * beforeSchema} to {@code afterSchema}.
     */
    public static List<SchemaChangeEvent> getSchemaDifference(
            TableId tableId, @Nullable Schema beforeSchema, Schema afterSchema) {
        if (beforeSchema == null) {
            return Collections.singletonList(new CreateTableEvent(tableId, afterSchema));
        }

        Map<String, Column> beforeColumns =
                beforeSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, col -> col));

        Map<String, DataType> oldTypeMapping = new HashMap<>();
        Map<String, DataType> newTypeMapping = new HashMap<>();
        List<AddColumnEvent.ColumnWithPosition> appendedColumns = new ArrayList<>();

        String afterWhichColumnPosition = null;
        for (Column afterColumn : afterSchema.getColumns()) {
            String columnName = afterColumn.getName();
            DataType afterType = afterColumn.getType();
            if (beforeColumns.containsKey(columnName)) {
                DataType beforeType = beforeColumns.get(columnName).getType();
                if (!Objects.equals(beforeType, afterType)) {
                    oldTypeMapping.put(columnName, beforeType);
                    newTypeMapping.put(columnName, afterType);
                }
            } else {
                if (afterWhichColumnPosition == null) {
                    appendedColumns.add(
                            new AddColumnEvent.ColumnWithPosition(
                                    afterColumn, AddColumnEvent.ColumnPosition.FIRST, null));
                } else {
                    appendedColumns.add(
                            new AddColumnEvent.ColumnWithPosition(
                                    afterColumn,
                                    AddColumnEvent.ColumnPosition.AFTER,
                                    afterWhichColumnPosition));
                }
            }
            afterWhichColumnPosition = afterColumn.getName();
        }

        List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>();
        if (!appendedColumns.isEmpty()) {
            schemaChangeEvents.add(new AddColumnEvent(tableId, appendedColumns));
        }

        if (!newTypeMapping.isEmpty()) {
            schemaChangeEvents.add(
                    new AlterColumnTypeEvent(tableId, newTypeMapping, oldTypeMapping));
        }

        return schemaChangeEvents;
    }

    /**
     * Coercing {@code upcomingRow} with {@code upcomingTypes} schema into {@code currentTypes}
     * schema. Invoking this method implicitly assumes that {@code isSchemaCompatible(currentSchema,
     * upcomingSchema)} returns true. Otherwise, some upstream records might be lost.
     */
    public static Object[] coerceRow(
            String timezone,
            Schema currentSchema,
            Schema upcomingSchema,
            List<Object> upcomingRow) {
        return coerceRow(timezone, currentSchema, upcomingSchema, upcomingRow, true);
    }

    /**
     * Coercing {@code upcomingRow} with {@code upcomingTypes} schema into {@code currentTypes}
     * schema. Invoking this method implicitly assumes that {@code isSchemaCompatible(currentSchema,
     * upcomingSchema)} returns true. Otherwise, some upstream records might be lost.
     */
    public static Object[] coerceRow(
            String timezone,
            Schema currentSchema,
            Schema upcomingSchema,
            List<Object> upcomingRow,
            boolean toleranceMode) {
        List<Column> currentColumns = currentSchema.getColumns();
        Map<String, DataType> upcomingColumnTypes =
                upcomingSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Column::getType));
        Map<String, Object> upcomingColumnObjects =
                Streams.zip(
                                upcomingSchema.getColumnNames().stream(),
                                upcomingRow.stream(),
                                Tuple2::of)
                        .filter(t -> t.f1 != null)
                        .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
        Object[] coercedRow = new Object[currentSchema.getColumnCount()];

        for (int i = 0; i < currentSchema.getColumnCount(); i++) {
            Column currentColumn = currentColumns.get(i);
            String columnName = currentColumn.getName();
            if (upcomingColumnTypes.containsKey(columnName)) {

                DataType upcomingType = upcomingColumnTypes.get(columnName);
                DataType currentType = currentColumn.getType();

                if (Objects.equals(upcomingType, currentType)) {
                    coercedRow[i] = upcomingColumnObjects.get(columnName);
                } else {
                    try {
                        coercedRow[i] =
                                coerceObject(
                                        timezone,
                                        upcomingColumnObjects.get(columnName),
                                        upcomingColumnTypes.get(columnName),
                                        currentColumn.getType());
                    } catch (IllegalArgumentException e) {
                        if (!toleranceMode) {
                            throw e;
                        }
                    }
                }
            } else {
                coercedRow[i] = null;
            }
        }
        return coercedRow;
    }

    @VisibleForTesting
    static boolean isDataTypeCompatible(@Nullable DataType currentType, DataType upcomingType) {
        // If two types are identical, they're compatible of course.
        if (Objects.equals(currentType, upcomingType)) {
            return true;
        }

        // Or, if an upcoming column does not exist in current schema, it can't be compatible.
        if (currentType == null) {
            return false;
        }

        // Or, check if upcomingType is presented in the type merging tree.
        return TYPE_MERGING_TREE.get(upcomingType.getClass()).contains(currentType);
    }

    @VisibleForTesting
    static DataType getLeastCommonType(DataType currentType, DataType targetType) {
        // Ignore nullability during data type merge, and restore it later
        boolean nullable = currentType.isNullable() || targetType.isNullable();
        currentType = currentType.notNull();
        targetType = targetType.notNull();

        if (Objects.equals(currentType, targetType)) {
            return currentType.copy(nullable);
        }

        // For TIMESTAMP and EXACT_NUMERIC types, we have fine-grained type merging logic.
        if (currentType.is(DataTypeFamily.TIMESTAMP) && targetType.is(DataTypeFamily.TIMESTAMP)) {
            return mergeTimestampType(currentType, targetType).copy(nullable);
        }

        if (currentType instanceof DecimalType || targetType instanceof DecimalType) {
            return mergeDecimalType(currentType, targetType).copy(nullable);
        }

        List<DataType> currentTypeTree = TYPE_MERGING_TREE.get(currentType.getClass());
        List<DataType> targetTypeTree = TYPE_MERGING_TREE.get(targetType.getClass());

        for (DataType type : currentTypeTree) {
            if (targetTypeTree.contains(type)) {
                return type.copy(nullable);
            }
        }

        // The most universal type and our final resort: STRING.
        return DataTypes.STRING().copy(nullable);
    }

    @VisibleForTesting
    static DataType mergeTimestampType(DataType lType, DataType rType) {
        // TIMESTAMP (0) -> TIMESTAMP_LTZ (1) -> TIMESTAMP_TZ (2)
        int leftTypeLevel;
        int leftPrecision;
        int rightTypeLevel;
        int rightPrecision;

        if (lType instanceof TimestampType) {
            leftTypeLevel = 0;
            leftPrecision = ((TimestampType) lType).getPrecision();
        } else if (lType instanceof LocalZonedTimestampType) {
            leftTypeLevel = 1;
            leftPrecision = ((LocalZonedTimestampType) lType).getPrecision();
        } else if (lType instanceof ZonedTimestampType) {
            leftTypeLevel = 2;
            leftPrecision = ((ZonedTimestampType) lType).getPrecision();
        } else {
            throw new IllegalArgumentException("Unknown TIMESTAMP type: " + lType);
        }

        if (rType instanceof TimestampType) {
            rightTypeLevel = 0;
            rightPrecision = ((TimestampType) rType).getPrecision();
        } else if (rType instanceof LocalZonedTimestampType) {
            rightTypeLevel = 1;
            rightPrecision = ((LocalZonedTimestampType) rType).getPrecision();
        } else if (rType instanceof ZonedTimestampType) {
            rightTypeLevel = 2;
            rightPrecision = ((ZonedTimestampType) rType).getPrecision();
        } else {
            throw new IllegalArgumentException("Unknown TIMESTAMP type: " + lType);
        }

        int precision = Math.max(leftPrecision, rightPrecision);

        switch (Math.max(leftTypeLevel, rightTypeLevel)) {
            case 0:
                return DataTypes.TIMESTAMP(precision);
            case 1:
                return DataTypes.TIMESTAMP_LTZ(precision);
            case 2:
                return DataTypes.TIMESTAMP_TZ(precision);
            default:
                throw new IllegalArgumentException("Unreachable");
        }
    }

    @VisibleForTesting
    static DataType mergeDecimalType(DataType lType, DataType rType) {
        if (lType instanceof DecimalType && rType instanceof DecimalType) {
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
            return DataTypes.DECIMAL(resultIntDigits + resultScale, resultScale);
        } else if (lType instanceof DecimalType && rType.is(DataTypeFamily.EXACT_NUMERIC)) {
            // Merge decimal and int
            return mergeExactNumericsIntoDecimal((DecimalType) lType, rType);
        } else if (rType instanceof DecimalType && lType.is(DataTypeFamily.EXACT_NUMERIC)) {
            // Merge decimal and int
            return mergeExactNumericsIntoDecimal((DecimalType) rType, lType);
        } else {
            return DataTypes.STRING();
        }
    }

    private static DataType mergeExactNumericsIntoDecimal(
            DecimalType decimalType, DataType otherType) {
        int resultPrecision =
                Math.max(
                        decimalType.getPrecision(),
                        decimalType.getScale() + getNumericPrecision(otherType));
        if (resultPrecision <= DecimalType.MAX_PRECISION) {
            return DataTypes.DECIMAL(resultPrecision, decimalType.getScale());
        } else {
            return DataTypes.STRING();
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

    @VisibleForTesting
    static Object coerceObject(
            String timezone,
            Object originalField,
            DataType originalType,
            DataType destinationType) {
        if (originalField == null) {
            return null;
        }

        if (destinationType instanceof BooleanType) {
            return Boolean.valueOf(originalField.toString());
        }

        if (destinationType instanceof TinyIntType) {
            return coerceToByte(originalField);
        }

        if (destinationType instanceof SmallIntType) {
            return coerceToShort(originalField);
        }

        if (destinationType instanceof IntType) {
            return coerceToInt(originalField);
        }

        if (destinationType instanceof BigIntType) {
            return coerceToLong(originalField);
        }

        if (destinationType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) destinationType;
            return coerceToDecimal(
                    originalField, decimalType.getPrecision(), decimalType.getScale());
        }

        if (destinationType instanceof FloatType) {
            return coerceToFloat(originalField);
        }

        if (destinationType instanceof DoubleType) {
            return coerceToDouble(originalField);
        }

        if (destinationType instanceof CharType) {
            return coerceToString(originalField, originalType);
        }

        if (destinationType instanceof VarCharType) {
            return coerceToString(originalField, originalType);
        }

        if (destinationType instanceof BinaryType) {
            return coerceToBytes(originalField);
        }

        if (destinationType instanceof VarBinaryType) {
            return coerceToBytes(originalField);
        }

        if (destinationType instanceof DateType) {
            try {
                return coerceToLong(originalField);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format("Cannot fit \"%s\" into a DATE column.", originalField));
            }
        }

        if (destinationType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                && originalType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            // For now, TimestampData / ZonedTimestampData / LocalZonedTimestampData has no
            // difference in its internal representation, so there's no need to do any precision
            // conversion.
            return originalField;
        }

        if (destinationType.is(DataTypeRoot.TIMESTAMP_WITH_TIME_ZONE)
                && originalType.is(DataTypeRoot.TIMESTAMP_WITH_TIME_ZONE)) {
            return originalField;
        }

        if (destinationType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                && originalType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return originalField;
        }

        if (destinationType instanceof TimestampType) {
            return coerceToTimestamp(originalField, timezone);
        }

        if (destinationType instanceof LocalZonedTimestampType) {
            return coerceToLocalZonedTimestamp(originalField, timezone);
        }

        if (destinationType instanceof ZonedTimestampType) {
            return coerceToZonedTimestamp(originalField, timezone);
        }

        throw new IllegalArgumentException(
                String.format(
                        "Column type \"%s\" doesn't support type coercion to \"%s\"",
                        originalType, destinationType));
    }

    private static Object coerceToString(Object originalField, DataType originalType) {
        if (originalField == null) {
            return BinaryStringData.fromString("null");
        }

        if (originalType instanceof DateType) {
            long epochOfDay = coerceToLong(originalField);
            return BinaryStringData.fromString(LocalDate.ofEpochDay(epochOfDay).toString());
        }

        if (originalField instanceof StringData) {
            return originalField;
        }

        if (originalField instanceof byte[]) {
            return BinaryStringData.fromString(hexlify((byte[]) originalField));
        }

        return BinaryStringData.fromString(originalField.toString());
    }

    private static Object coerceToBytes(Object originalField) {
        if (originalField instanceof byte[]) {
            return originalField;
        } else {
            return originalField.toString().getBytes();
        }
    }

    private static byte coerceToByte(Object o) {
        if (o instanceof Byte) {
            return (Byte) o;
        } else {
            throw new IllegalArgumentException(
                    String.format("Cannot fit type \"%s\" into a TINYINT column. ", o.getClass()));
        }
    }

    private static short coerceToShort(Object o) {
        if (o instanceof Byte) {
            return ((Byte) o).shortValue();
        } else if (o instanceof Short) {
            return (Short) o;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot fit type \"%s\" into a SMALLINT column. "
                                    + "Currently only TINYINT can be accepted by a SMALLINT column",
                            o.getClass()));
        }
    }

    private static int coerceToInt(Object o) {
        if (o instanceof Byte) {
            return ((Byte) o).intValue();
        } else if (o instanceof Short) {
            return ((Short) o).intValue();
        } else if (o instanceof Integer) {
            return (Integer) o;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot fit type \"%s\" into a INT column. "
                                    + "Currently only TINYINT / SMALLINT can be accepted by a INT column",
                            o.getClass()));
        }
    }

    private static long coerceToLong(Object o) {
        if (o instanceof Byte) {
            return ((Byte) o).longValue();
        } else if (o instanceof Short) {
            return ((Short) o).longValue();
        } else if (o instanceof Integer) {
            return ((Integer) o).longValue();
        } else if (o instanceof Long) {
            return (long) o;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot fit type \"%s\" into a BIGINT column. "
                                    + "Currently only TINYINT / SMALLINT / INT can be accepted by a BIGINT column",
                            o.getClass()));
        }
    }

    private static DecimalData coerceToDecimal(Object o, int precision, int scale) {
        BigDecimal decimalValue;
        if (o instanceof Byte) {
            decimalValue = BigDecimal.valueOf(((Byte) o).longValue(), 0);
        } else if (o instanceof Short) {
            decimalValue = BigDecimal.valueOf(((Short) o).longValue(), 0);
        } else if (o instanceof Integer) {
            decimalValue = BigDecimal.valueOf(((Integer) o).longValue(), 0);
        } else if (o instanceof Long) {
            decimalValue = BigDecimal.valueOf((Long) o, 0);
        } else if (o instanceof DecimalData) {
            decimalValue = ((DecimalData) o).toBigDecimal();
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot fit type \"%s\" into a DECIMAL column. "
                                    + "Currently only TINYINT / SMALLINT / INT / BIGINT / DECIMAL can be accepted by a DECIMAL column",
                            o.getClass()));
        }
        return decimalValue != null
                ? DecimalData.fromBigDecimal(decimalValue, precision, scale)
                : null;
    }

    private static float coerceToFloat(Object o) {
        if (o instanceof Byte) {
            return ((Byte) o).floatValue();
        } else if (o instanceof Short) {
            return ((Short) o).floatValue();
        } else if (o instanceof Integer) {
            return ((Integer) o).floatValue();
        } else if (o instanceof Long) {
            return ((Long) o).floatValue();
        } else if (o instanceof DecimalData) {
            return ((DecimalData) o).toBigDecimal().floatValue();
        } else if (o instanceof Float) {
            return (Float) o;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot fit type \"%s\" into a FLOAT column. "
                                    + "Currently only TINYINT / SMALLINT / INT / BIGINT / DECIMAL can be accepted by a FLOAT column",
                            o.getClass()));
        }
    }

    private static double coerceToDouble(Object o) {
        if (o instanceof Byte) {
            return ((Byte) o).doubleValue();
        } else if (o instanceof Short) {
            return ((Short) o).doubleValue();
        } else if (o instanceof Integer) {
            return ((Integer) o).doubleValue();
        } else if (o instanceof Long) {
            return ((Long) o).doubleValue();
        } else if (o instanceof DecimalData) {
            return ((DecimalData) o).toBigDecimal().doubleValue();
        } else if (o instanceof Float) {
            return ((Float) o).doubleValue();
        } else if (o instanceof Double) {
            return (Double) o;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot fit type \"%s\" into a DOUBLE column. "
                                    + "Currently only TINYINT / SMALLINT / INT / BIGINT / DECIMAL / FLOAT can be accepted by a DOUBLE column",
                            o.getClass()));
        }
    }

    private static TimestampData coerceToTimestamp(Object object, String timezone) {
        if (object == null) {
            return null;
        }
        if (object instanceof Long) {
            return TimestampData.fromLocalDateTime(
                    LocalDate.ofEpochDay((long) object).atStartOfDay());
        } else if (object instanceof LocalZonedTimestampData) {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.ofInstant(
                            ((LocalZonedTimestampData) object).toInstant(), ZoneId.of(timezone)));
        } else if (object instanceof ZonedTimestampData) {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.ofInstant(
                            ((ZonedTimestampData) object).toInstant(), ZoneId.of(timezone)));
        } else if (object instanceof TimestampData) {
            return (TimestampData) object;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unable to implicitly coerce object `%s` as a TIMESTAMP.", object));
        }
    }

    private static LocalZonedTimestampData coerceToLocalZonedTimestamp(
            Object object, String timezone) {
        if (object == null) {
            return null;
        }

        TimestampData timestampData = coerceToTimestamp(object, timezone);
        return LocalZonedTimestampData.fromEpochMillis(
                timestampData.getMillisecond(), timestampData.getNanoOfMillisecond());
    }

    private static ZonedTimestampData coerceToZonedTimestamp(Object object, String timezone) {
        if (object == null) {
            return null;
        }

        TimestampData timestampData = coerceToTimestamp(object, timezone);
        return ZonedTimestampData.fromZonedDateTime(
                ZonedDateTime.ofInstant(
                        timestampData.toLocalDateTime().toInstant(ZoneOffset.UTC),
                        ZoneId.of(timezone)));
    }

    private static String hexlify(byte[] bytes) {
        return BaseEncoding.base64().encode(bytes);
    }

    private static final Map<Class<? extends DataType>, List<DataType>> TYPE_MERGING_TREE =
            getTypeMergingTree();

    private static Map<Class<? extends DataType>, List<DataType>> getTypeMergingTree() {
        DataType stringType = DataTypes.STRING();
        DataType doubleType = DataTypes.DOUBLE();
        DataType floatType = DataTypes.FLOAT();
        DataType decimalType =
                DataTypes.DECIMAL(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE);
        DataType bigIntType = DataTypes.BIGINT();
        DataType intType = DataTypes.INT();
        DataType smallIntType = DataTypes.SMALLINT();
        DataType tinyIntType = DataTypes.TINYINT();
        DataType timestampTzType = DataTypes.TIMESTAMP_TZ(ZonedTimestampType.MAX_PRECISION);
        DataType timestampLtzType = DataTypes.TIMESTAMP_LTZ(LocalZonedTimestampType.MAX_PRECISION);
        DataType timestampType = DataTypes.TIMESTAMP(TimestampType.MAX_PRECISION);
        DataType dateType = DataTypes.DATE();

        Map<Class<? extends DataType>, List<DataType>> mergingTree = new HashMap<>();

        // Simple data types
        mergingTree.put(VarCharType.class, ImmutableList.of(stringType));
        mergingTree.put(CharType.class, ImmutableList.of(stringType));
        mergingTree.put(BooleanType.class, ImmutableList.of(stringType));
        mergingTree.put(BinaryType.class, ImmutableList.of(stringType));
        mergingTree.put(VarBinaryType.class, ImmutableList.of(stringType));
        mergingTree.put(DoubleType.class, ImmutableList.of(doubleType, stringType));
        mergingTree.put(FloatType.class, ImmutableList.of(floatType, doubleType, stringType));
        mergingTree.put(DecimalType.class, ImmutableList.of(stringType));
        mergingTree.put(
                BigIntType.class,
                ImmutableList.of(bigIntType, decimalType, doubleType, stringType));
        mergingTree.put(
                IntType.class,
                ImmutableList.of(intType, bigIntType, decimalType, doubleType, stringType));
        mergingTree.put(
                SmallIntType.class,
                ImmutableList.of(
                        smallIntType,
                        intType,
                        bigIntType,
                        decimalType,
                        floatType,
                        doubleType,
                        stringType));
        mergingTree.put(
                TinyIntType.class,
                ImmutableList.of(
                        tinyIntType,
                        smallIntType,
                        intType,
                        bigIntType,
                        decimalType,
                        floatType,
                        doubleType,
                        stringType));

        // Timestamp series
        mergingTree.put(ZonedTimestampType.class, ImmutableList.of(timestampTzType, stringType));
        mergingTree.put(
                LocalZonedTimestampType.class,
                ImmutableList.of(timestampLtzType, timestampTzType, stringType));
        mergingTree.put(
                TimestampType.class,
                ImmutableList.of(timestampType, timestampLtzType, timestampTzType, stringType));
        mergingTree.put(
                DateType.class,
                ImmutableList.of(
                        dateType, timestampType, timestampLtzType, timestampTzType, stringType));
        mergingTree.put(TimeType.class, ImmutableList.of(stringType));

        // Complex types
        mergingTree.put(RowType.class, ImmutableList.of(stringType));
        mergingTree.put(ArrayType.class, ImmutableList.of(stringType));
        mergingTree.put(MapType.class, ImmutableList.of(stringType));
        return mergingTree;
    }
}
