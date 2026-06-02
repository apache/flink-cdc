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

package org.apache.flink.cdc.connectors.pgvector.utils;

import org.apache.flink.cdc.common.converter.JavaObjectConverter;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.flink.cdc.common.types.DataTypeRoot.ARRAY;

/** Record conversion helpers for JDBC writes. */
public class PgVectorRecordUtils {

    private PgVectorRecordUtils() {}

    public static void bindRecord(
            Connection connection,
            PreparedStatement statement,
            PgVectorTableInfo tableInfo,
            Schema schema,
            RecordData record,
            Map<String, PgVectorColumnSpec> vectorColumns)
            throws SQLException {
        List<RecordData.FieldGetter> fieldGetters = SchemaUtils.createFieldGetters(schema);
        int parameterIndex = 1;
        List<Column> columns = schema.getColumns();
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            Column column = columns.get(columnIndex);
            if (!column.isPhysical()) {
                continue;
            }
            Object field = fieldGetters.get(columnIndex).getFieldOrNull(record);
            bindField(
                    connection,
                    statement,
                    parameterIndex++,
                    tableInfo,
                    column.getName(),
                    column.getType(),
                    field,
                    vectorColumns);
        }
    }

    public static void bindPrimaryKeys(
            Connection connection,
            PreparedStatement statement,
            PgVectorTableInfo tableInfo,
            Schema schema,
            RecordData record,
            Map<String, PgVectorColumnSpec> vectorColumns)
            throws SQLException {
        List<RecordData.FieldGetter> fieldGetters = SchemaUtils.createFieldGetters(schema);
        int parameterIndex = 1;
        for (String primaryKey : schema.primaryKeys()) {
            int columnIndex = schema.getColumnNames().indexOf(primaryKey);
            if (columnIndex < 0) {
                throw new IllegalStateException(
                        "Primary key column " + primaryKey + " is absent from schema.");
            }
            Column column = schema.getColumns().get(columnIndex);
            Object field = fieldGetters.get(columnIndex).getFieldOrNull(record);
            bindField(
                    connection,
                    statement,
                    parameterIndex++,
                    tableInfo,
                    column.getName(),
                    column.getType(),
                    field,
                    vectorColumns);
        }
    }

    public static Object convertField(
            Connection connection,
            PgVectorTableInfo tableInfo,
            String columnName,
            DataType dataType,
            Object value,
            Map<String, PgVectorColumnSpec> vectorColumns)
            throws SQLException {
        if (value == null) {
            return null;
        }
        PgVectorColumnSpec vectorSpec =
                PgVectorTypeUtils.findVectorColumnSpec(tableInfo, columnName, vectorColumns)
                        .orElse(null);
        if (vectorSpec != null) {
            return toPgVectorObject(value, dataType, vectorSpec);
        }
        Object javaValue = JavaObjectConverter.convertToJava(value, dataType);
        if (dataType.getTypeRoot() == ARRAY) {
            return toSqlArray(connection, (ArrayType) dataType, javaValue);
        }
        if (javaValue instanceof LocalDate) {
            return Date.valueOf((LocalDate) javaValue);
        }
        if (javaValue instanceof LocalTime) {
            return Time.valueOf((LocalTime) javaValue);
        }
        if (javaValue instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) javaValue);
        }
        if (javaValue instanceof Instant) {
            return OffsetDateTime.ofInstant((Instant) javaValue, ZoneOffset.UTC);
        }
        if (javaValue instanceof ZonedDateTime) {
            return ((ZonedDateTime) javaValue).toOffsetDateTime();
        }
        if (javaValue instanceof Map) {
            return javaValue.toString();
        }
        return javaValue;
    }

    public static boolean primaryKeysEqual(Schema schema, RecordData left, RecordData right) {
        List<RecordData.FieldGetter> fieldGetters = SchemaUtils.createFieldGetters(schema);
        for (String primaryKey : schema.primaryKeys()) {
            int columnIndex = schema.getColumnNames().indexOf(primaryKey);
            if (columnIndex < 0) {
                throw new IllegalStateException(
                        "Primary key column " + primaryKey + " is absent from schema.");
            }
            Object leftValue = fieldGetters.get(columnIndex).getFieldOrNull(left);
            Object rightValue = fieldGetters.get(columnIndex).getFieldOrNull(right);
            if (!fieldValuesEqual(leftValue, rightValue)) {
                return false;
            }
        }
        return true;
    }

    private static boolean fieldValuesEqual(Object left, Object right) {
        if (left instanceof byte[] && right instanceof byte[]) {
            return Arrays.equals((byte[]) left, (byte[]) right);
        }
        return Objects.equals(left, right);
    }

    private static void bindField(
            Connection connection,
            PreparedStatement statement,
            int parameterIndex,
            PgVectorTableInfo tableInfo,
            String columnName,
            DataType dataType,
            Object value,
            Map<String, PgVectorColumnSpec> vectorColumns)
            throws SQLException {
        Object convertedValue =
                convertField(connection, tableInfo, columnName, dataType, value, vectorColumns);
        if (convertedValue == null) {
            statement.setNull(parameterIndex, Types.NULL);
        } else if (convertedValue instanceof java.sql.Array) {
            statement.setArray(parameterIndex, (java.sql.Array) convertedValue);
        } else if (convertedValue instanceof byte[]) {
            statement.setBytes(parameterIndex, (byte[]) convertedValue);
        } else {
            statement.setObject(parameterIndex, convertedValue);
        }
    }

    private static java.sql.Array toSqlArray(
            Connection connection, ArrayType arrayType, Object value) throws SQLException {
        List<?> list = toJavaList(value);
        DataType elementType = arrayType.getElementType();
        Object[] elements = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            elements[i] = convertArrayElement(list.get(i), elementType);
        }
        return connection.createArrayOf(
                PgVectorTypeUtils.toJdbcArrayTypeName(elementType), elements);
    }

    private static List<?> toJavaList(Object value) {
        if (value instanceof List) {
            return (List<?>) value;
        }
        throw new IllegalArgumentException(
                "Array column value must be a list but was " + value.getClass().getName());
    }

    private static Object convertArrayElement(Object value, DataType elementType)
            throws SQLException {
        if (value == null) {
            return null;
        }
        if (elementType.getTypeRoot() == ARRAY) {
            return value.toString();
        }
        Object javaValue = value;
        if (!(value instanceof LocalDate)
                && !(value instanceof LocalTime)
                && !(value instanceof LocalDateTime)
                && !(value instanceof Instant)
                && !(value instanceof ZonedDateTime)) {
            javaValue = JavaObjectConverter.convertToJava(value, elementType);
        }
        if (javaValue instanceof LocalDate) {
            return Date.valueOf((LocalDate) javaValue);
        }
        if (javaValue instanceof LocalTime) {
            return Time.valueOf((LocalTime) javaValue);
        }
        if (javaValue instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) javaValue);
        }
        if (javaValue instanceof Instant) {
            return OffsetDateTime.ofInstant((Instant) javaValue, ZoneOffset.UTC);
        }
        if (javaValue instanceof ZonedDateTime) {
            return ((ZonedDateTime) javaValue).toOffsetDateTime();
        }
        if (javaValue instanceof Map || javaValue instanceof List) {
            return javaValue.toString();
        }
        return javaValue;
    }

    private static PGobject toPgVectorObject(
            Object value, DataType dataType, PgVectorColumnSpec vectorSpec) throws SQLException {
        Object javaValue = JavaObjectConverter.convertToJava(value, dataType);
        PGobject pgObject = new PGobject();
        pgObject.setType(vectorSpec.getType().name().toLowerCase());
        pgObject.setValue(toVectorLiteral(javaValue, vectorSpec));
        return pgObject;
    }

    public static String toVectorLiteral(Object value, PgVectorColumnSpec vectorSpec) {
        switch (vectorSpec.getType()) {
            case BIT:
                return toBitLiteral(value, vectorSpec.getDimension());
            case SPARSEVEC:
                return toSparseVectorLiteral(value, vectorSpec.getDimension());
            case VECTOR:
            case HALFVEC:
                return toDenseVectorLiteral(value, vectorSpec.getDimension());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported pgvector type: " + vectorSpec.getType());
        }
    }

    private static String toDenseVectorLiteral(Object value, int dimension) {
        List<Double> values = toDoubleList(value);
        if (values.size() != dimension) {
            throw new IllegalArgumentException(
                    "Dense vector dimension mismatch. Expected "
                            + dimension
                            + " but was "
                            + values.size()
                            + ".");
        }
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            double item = values.get(i);
            if (!Double.isFinite(item)) {
                throw new IllegalArgumentException("Vector values must be finite numbers.");
            }
            builder.append(Double.toString(item));
        }
        return builder.append(']').toString();
    }

    private static String toSparseVectorLiteral(Object value, int dimension) {
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) value;
            TreeMap<Integer, Double> sortedEntries = new TreeMap<>();
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                int index = Integer.parseInt(entry.getKey().toString());
                if (sortedEntries.containsKey(index)) {
                    throw new IllegalArgumentException(
                            "Sparse vector index " + index + " is duplicated.");
                }
                double item = Double.parseDouble(entry.getValue().toString());
                if (index < 1 || index > dimension) {
                    throw new IllegalArgumentException(
                            "Sparse vector index " + index + " is out of dimension " + dimension);
                }
                if (!Double.isFinite(item)) {
                    throw new IllegalArgumentException("Sparse vector values must be finite.");
                }
                sortedEntries.put(index, item);
            }
            StringBuilder builder = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<Integer, Double> entry : sortedEntries.entrySet()) {
                if (!first) {
                    builder.append(',');
                }
                builder.append(entry.getKey()).append(':').append(entry.getValue());
                first = false;
            }
            return builder.append("}/").append(dimension).toString();
        }
        List<Double> values = toDoubleList(value);
        if (values.size() != dimension) {
            throw new IllegalArgumentException(
                    "Sparse vector dimension mismatch. Expected "
                            + dimension
                            + " but was "
                            + values.size()
                            + ".");
        }
        StringBuilder builder = new StringBuilder("{");
        boolean first = true;
        for (int i = 0; i < values.size(); i++) {
            double item = values.get(i);
            if (!Double.isFinite(item)) {
                throw new IllegalArgumentException("Sparse vector values must be finite.");
            }
            if (item == 0.0d) {
                continue;
            }
            if (!first) {
                builder.append(',');
            }
            builder.append(i + 1).append(':').append(item);
            first = false;
        }
        return builder.append("}/").append(dimension).toString();
    }

    private static String toBitLiteral(Object value, int dimension) {
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            StringBuilder builder = new StringBuilder(bytes.length * 8);
            for (byte b : bytes) {
                for (int i = 7; i >= 0; i--) {
                    builder.append(((b >> i) & 1) == 1 ? '1' : '0');
                }
            }
            String bits = builder.toString();
            if (bits.length() != dimension) {
                throw new IllegalArgumentException(
                        "Bit vector dimension mismatch. Expected "
                                + dimension
                                + " but was "
                                + bits.length()
                                + ".");
            }
            return bits;
        }
        String bits = value.toString().replaceAll("\\s+", "");
        if (bits.length() != dimension) {
            throw new IllegalArgumentException(
                    "Bit vector dimension mismatch. Expected "
                            + dimension
                            + " but was "
                            + bits.length()
                            + ".");
        }
        for (int i = 0; i < bits.length(); i++) {
            char c = bits.charAt(i);
            if (c != '0' && c != '1') {
                throw new IllegalArgumentException("Bit vector must contain only 0 or 1.");
            }
        }
        return bits;
    }

    private static List<Double> toDoubleList(Object value) {
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            List<Double> values = new ArrayList<>(list.size());
            for (Object item : list) {
                values.add(Double.parseDouble(item.toString()));
            }
            return values;
        }
        String literal = value.toString().trim();
        if (literal.startsWith("[") && literal.endsWith("]")) {
            literal = literal.substring(1, literal.length() - 1);
        }
        List<Double> values = new ArrayList<>();
        if (literal.trim().isEmpty()) {
            return values;
        }
        for (String item : literal.split(",")) {
            values.add(Double.parseDouble(item.trim()));
        }
        return values;
    }
}
