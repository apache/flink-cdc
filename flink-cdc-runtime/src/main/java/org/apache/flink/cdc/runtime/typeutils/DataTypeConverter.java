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

package org.apache.flink.cdc.runtime.typeutils;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** A data type converter. */
public class DataTypeConverter {

    static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
    static final long NANOSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    static final long NANOSECONDS_PER_DAY = TimeUnit.DAYS.toNanos(1);

    public static RowType toRowType(List<Column> columnList) {
        DataType[] dataTypes = columnList.stream().map(Column::getType).toArray(DataType[]::new);
        String[] columnNames = columnList.stream().map(Column::getName).toArray(String[]::new);
        return RowType.of(dataTypes, columnNames);
    }

    public static Class<?> convertOriginalClass(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return Boolean.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case DATE:
                return DateData.class;
            case TIME_WITHOUT_TIME_ZONE:
                return TimeData.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.class;
            case TIMESTAMP_WITH_TIME_ZONE:
                return ZonedTimestampData.class;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalZonedTimestampData.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case CHAR:
            case VARCHAR:
                return String.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case DECIMAL:
                return DecimalData.class;
            case ROW:
                return Object.class;
            case ARRAY:
                return ArrayData.class;
            case MAP:
                return MapData.class;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    public static RelDataType convertCalciteRelDataType(
            RelDataTypeFactory typeFactory, List<Column> columns) {
        RelDataTypeFactory.Builder fieldInfoBuilder = typeFactory.builder();
        for (Column column : columns) {
            switch (column.getType().getTypeRoot()) {
                case BOOLEAN:
                    BooleanType booleanType = (BooleanType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.BOOLEAN)
                            .nullable(booleanType.isNullable());
                    break;
                case TINYINT:
                    TinyIntType tinyIntType = (TinyIntType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.TINYINT)
                            .nullable(tinyIntType.isNullable());
                    break;
                case SMALLINT:
                    SmallIntType smallIntType = (SmallIntType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.SMALLINT)
                            .nullable(smallIntType.isNullable());
                    break;
                case INTEGER:
                    IntType intType = (IntType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.INTEGER)
                            .nullable(intType.isNullable());
                    break;
                case BIGINT:
                    BigIntType bigIntType = (BigIntType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.BIGINT)
                            .nullable(bigIntType.isNullable());
                    break;
                case DATE:
                    DateType dataType = (DateType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.DATE)
                            .nullable(dataType.isNullable());
                    break;
                case TIME_WITHOUT_TIME_ZONE:
                    TimeType timeType = (TimeType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.TIME, timeType.getPrecision())
                            .nullable(timeType.isNullable());
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    TimestampType timestampType = (TimestampType) column.getType();
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    SqlTypeName.TIMESTAMP,
                                    timestampType.getPrecision())
                            .nullable(timestampType.isNullable());
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    ZonedTimestampType zonedTimestampType = (ZonedTimestampType) column.getType();
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    SqlTypeName.TIMESTAMP,
                                    zonedTimestampType.getPrecision())
                            .nullable(zonedTimestampType.isNullable());
                    break;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    LocalZonedTimestampType localZonedTimestampType =
                            (LocalZonedTimestampType) column.getType();
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                                    localZonedTimestampType.getPrecision())
                            .nullable(localZonedTimestampType.isNullable());
                    break;
                case FLOAT:
                    FloatType floatType = (FloatType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.FLOAT)
                            .nullable(floatType.isNullable());
                    break;
                case DOUBLE:
                    DoubleType doubleType = (DoubleType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.DOUBLE)
                            .nullable(doubleType.isNullable());
                    break;
                case CHAR:
                    CharType charType = (CharType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.CHAR, charType.getLength())
                            .nullable(charType.isNullable());
                    break;
                case VARCHAR:
                    VarCharType varCharType = (VarCharType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.VARCHAR, varCharType.getLength())
                            .nullable(varCharType.isNullable());
                    break;
                case BINARY:
                    BinaryType binaryType = (BinaryType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.BINARY, binaryType.getLength())
                            .nullable(binaryType.isNullable());
                    break;
                case VARBINARY:
                    VarBinaryType varBinaryType = (VarBinaryType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.VARBINARY, varBinaryType.getLength())
                            .nullable(varBinaryType.isNullable());
                    break;
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) column.getType();
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    SqlTypeName.DECIMAL,
                                    decimalType.getPrecision(),
                                    decimalType.getScale())
                            .nullable(decimalType.isNullable());
                    break;
                case ROW:
                    List<RelDataType> dataTypes =
                            ((RowType) column.getType())
                                    .getFieldTypes().stream()
                                            .map((type) -> convertCalciteType(typeFactory, type))
                                            .collect(Collectors.toList());
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    typeFactory.createStructType(
                                            dataTypes,
                                            ((RowType) column.getType()).getFieldNames()))
                            .nullable(true);
                    break;
                case ARRAY:
                    DataType elementType = ((ArrayType) column.getType()).getElementType();
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    typeFactory.createArrayType(
                                            convertCalciteType(typeFactory, elementType), -1))
                            .nullable(true);
                    break;
                case MAP:
                    RelDataType keyType =
                            convertCalciteType(
                                    typeFactory, ((MapType) column.getType()).getKeyType());
                    RelDataType valueType =
                            convertCalciteType(
                                    typeFactory, ((MapType) column.getType()).getValueType());
                    fieldInfoBuilder
                            .add(column.getName(), typeFactory.createMapType(keyType, valueType))
                            .nullable(true);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported type: " + column.getType());
            }
        }
        return fieldInfoBuilder.build();
    }

    public static RelDataType convertCalciteType(
            RelDataTypeFactory typeFactory, DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            case TINYINT:
                return typeFactory.createSqlType(SqlTypeName.TINYINT);
            case SMALLINT:
                return typeFactory.createSqlType(SqlTypeName.SMALLINT);
            case INTEGER:
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case BIGINT:
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            case DATE:
                return typeFactory.createSqlType(SqlTypeName.DATE);
            case TIME_WITHOUT_TIME_ZONE:
                TimeType timeType = (TimeType) dataType;
                return typeFactory.createSqlType(SqlTypeName.TIME, timeType.getPrecision());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) dataType;
                return typeFactory.createSqlType(
                        SqlTypeName.TIMESTAMP, timestampType.getPrecision());
            case TIMESTAMP_WITH_TIME_ZONE:
                // TODO: Bump Calcite to support its TIMESTAMP_TZ type via #FLINK-37123
                throw new UnsupportedOperationException("Unsupported type: TIMESTAMP_TZ");
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) dataType;
                return typeFactory.createSqlType(
                        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                        localZonedTimestampType.getPrecision());
            case FLOAT:
                return typeFactory.createSqlType(SqlTypeName.FLOAT);
            case DOUBLE:
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            case CHAR:
                CharType charType = (CharType) dataType;
                return typeFactory.createSqlType(SqlTypeName.CHAR, charType.getLength());
            case VARCHAR:
                VarCharType varCharType = (VarCharType) dataType;
                return typeFactory.createSqlType(SqlTypeName.VARCHAR, varCharType.getLength());
            case BINARY:
                BinaryType binaryType = (BinaryType) dataType;
                return typeFactory.createSqlType(SqlTypeName.BINARY, binaryType.getLength());
            case VARBINARY:
                VarBinaryType varBinaryType = (VarBinaryType) dataType;
                return typeFactory.createSqlType(SqlTypeName.VARBINARY, varBinaryType.getLength());
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return typeFactory.createSqlType(
                        SqlTypeName.DECIMAL, decimalType.getPrecision(), decimalType.getScale());
            case ROW:
                List<RelDataType> dataTypes =
                        ((RowType) dataType)
                                .getFieldTypes().stream()
                                        .map((type) -> convertCalciteType(typeFactory, type))
                                        .collect(Collectors.toList());
                return typeFactory.createStructType(
                        dataTypes, ((RowType) dataType).getFieldNames());
            case ARRAY:
                DataType elementType = ((ArrayType) dataType).getElementType();
                return typeFactory.createArrayType(
                        convertCalciteType(typeFactory, elementType), -1);
            case MAP:
                RelDataType keyType =
                        convertCalciteType(typeFactory, ((MapType) dataType).getKeyType());
                RelDataType valueType =
                        convertCalciteType(typeFactory, ((MapType) dataType).getValueType());
                return typeFactory.createMapType(keyType, valueType);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    public static DataType convertCalciteRelDataTypeToDataType(RelDataType relDataType) {
        switch (relDataType.getSqlTypeName()) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                return DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INTEGER:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case DATE:
                return DataTypes.DATE();
            case TIME:
                return DataTypes.TIME(relDataType.getPrecision());
            case TIMESTAMP:
                return DataTypes.TIMESTAMP(relDataType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DataTypes.TIMESTAMP_LTZ(relDataType.getPrecision());
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case CHAR:
            case VARCHAR:
                return DataTypes.STRING();
            case BINARY:
                return DataTypes.BINARY(relDataType.getPrecision());
            case VARBINARY:
                return DataTypes.VARBINARY(relDataType.getPrecision());
            case DECIMAL:
                return DataTypes.DECIMAL(relDataType.getPrecision(), relDataType.getScale());
            case ARRAY:
                RelDataType componentType = relDataType.getComponentType();
                return DataTypes.ARRAY(convertCalciteRelDataTypeToDataType(componentType));
            case MAP:
                RelDataType keyType = relDataType.getKeyType();
                RelDataType valueType = relDataType.getValueType();
                return DataTypes.MAP(
                        convertCalciteRelDataTypeToDataType(keyType),
                        convertCalciteRelDataTypeToDataType(valueType));
            case ROW:
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type: " + relDataType.getSqlTypeName());
        }
    }

    public static Object convert(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return convertToBoolean(value);
            case TINYINT:
                return convertToByte(value);
            case SMALLINT:
                return convertToShort(value);
            case INTEGER:
                return convertToInt(value);
            case BIGINT:
                return convertToLong(value);
            case DATE:
                return convertToDate(value);
            case TIME_WITHOUT_TIME_ZONE:
                return convertToTime(value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return convertToTimestamp(value);
            case TIMESTAMP_WITH_TIME_ZONE:
                return convertToZonedTimestampData(value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertToLocalTimeZoneTimestamp(value);
            case FLOAT:
                return convertToFloat(value);
            case DOUBLE:
                return convertToDouble(value);
            case CHAR:
            case VARCHAR:
                return convertToString(value);
            case BINARY:
            case VARBINARY:
                return convertToBinary(value);
            case DECIMAL:
                return convertToDecimal(value);
            case ROW:
                return value;
            case ARRAY:
                return convertToArray(value, (ArrayType) dataType);
            case MAP:
                return convertToMap(value, (MapType) dataType);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    public static Object convertToOriginal(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return convertToBoolean(value);
            case TINYINT:
                return convertToByte(value);
            case SMALLINT:
                return convertToShort(value);
            case INTEGER:
                return convertToInt(value);
            case BIGINT:
                return convertToLong(value);
            case DATE:
                return convertToDate(value);
            case TIME_WITHOUT_TIME_ZONE:
                return convertToTime(value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return convertToTimestamp(value);
            case TIMESTAMP_WITH_TIME_ZONE:
                return convertToZonedTimestampData(value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertToLocalTimeZoneTimestamp(value);
            case FLOAT:
                return convertToFloat(value);
            case DOUBLE:
                return convertToDouble(value);
            case CHAR:
            case VARCHAR:
                return convertToStringOriginal(value);
            case BINARY:
            case VARBINARY:
                return convertToBinary(value);
            case DECIMAL:
                return convertToDecimal(value);
            case ROW:
                return value;
            case ARRAY:
                return convertToArrayOriginal(value, (ArrayType) dataType);
            case MAP:
                return convertToMapOriginal(value, (MapType) dataType);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static Object convertToBoolean(Object obj) {
        if (obj instanceof Boolean) {
            return obj;
        } else if (obj instanceof Byte) {
            return (byte) obj == 1;
        } else if (obj instanceof Short) {
            return (short) obj == 1;
        } else {
            return Boolean.parseBoolean(obj.toString());
        }
    }

    private static Object convertToByte(Object obj) {
        return Byte.parseByte(obj.toString());
    }

    private static Object convertToShort(Object obj) {
        return Short.parseShort(obj.toString());
    }

    private static Object convertToInt(Object obj) {
        if (obj instanceof Integer) {
            return obj;
        } else if (obj instanceof Long) {
            return ((Long) obj).intValue();
        } else {
            return Integer.parseInt(obj.toString());
        }
    }

    private static Object convertToLong(Object obj) {
        if (obj instanceof Integer) {
            return ((Integer) obj).longValue();
        } else if (obj instanceof Long) {
            return obj;
        } else {
            return Long.parseLong(obj.toString());
        }
    }

    private static Object convertToFloat(Object obj) {
        if (obj instanceof Float) {
            return obj;
        } else if (obj instanceof Double) {
            return ((Double) obj).floatValue();
        } else {
            return Float.parseFloat(obj.toString());
        }
    }

    private static Object convertToDouble(Object obj) {
        if (obj instanceof Float) {
            return ((Float) obj).doubleValue();
        } else if (obj instanceof Double) {
            return obj;
        } else {
            return Double.parseDouble(obj.toString());
        }
    }

    private static DateData convertToDate(Object obj) {
        if (obj instanceof DateData) {
            return (DateData) obj;
        }
        return DateData.fromLocalDate(toLocalDate(obj));
    }

    private static LocalDate toLocalDate(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof LocalDate) {
            return (LocalDate) obj;
        }
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj).toLocalDate();
        }
        if (obj instanceof java.sql.Date) {
            return ((java.sql.Date) obj).toLocalDate();
        }
        if (obj instanceof java.sql.Time) {
            throw new IllegalArgumentException(
                    "Unable to convert to LocalDate from a java.sql.Time value '" + obj + "'");
        }
        if (obj instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) obj;
            return LocalDate.of(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
        }
        if (obj instanceof Long) {
            // Assume the value is the epoch day number
            return LocalDate.ofEpochDay((Long) obj);
        }
        if (obj instanceof Integer) {
            // Assume the value is the epoch day number
            return LocalDate.ofEpochDay((Integer) obj);
        }
        throw new IllegalArgumentException(
                "Unable to convert to LocalDate from unexpected value '"
                        + obj
                        + "' of type "
                        + obj.getClass().getName());
    }

    private static TimeData convertToTime(Object obj) {
        if (obj instanceof TimeData) {
            return (TimeData) obj;
        }
        return TimeData.fromLocalTime(toLocalTime(obj));
    }

    private static Object convertToArray(Object obj, ArrayType arrayType) {
        if (obj instanceof ArrayData) {
            return obj;
        }
        if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            GenericArrayData arrayData = new GenericArrayData(list.toArray());
            return arrayData;
        }
        if (obj.getClass().isArray()) {
            return new GenericArrayData((Object[]) obj);
        }
        throw new IllegalArgumentException("Unable to convert to ArrayData: " + obj);
    }

    private static Object convertToArrayOriginal(Object obj, ArrayType arrayType) {
        if (obj instanceof ArrayData) {
            ArrayData arrayData = (ArrayData) obj;
            Object[] result = new Object[arrayData.size()];
            for (int i = 0; i < arrayData.size(); i++) {
                result[i] = getArrayElement(arrayData, i, arrayType.getElementType());
            }
            return result;
        }
        return obj;
    }

    private static Object getArrayElement(ArrayData arrayData, int pos, DataType elementType) {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                return arrayData.getBoolean(pos);
            case TINYINT:
                return arrayData.getByte(pos);
            case SMALLINT:
                return arrayData.getShort(pos);
            case INTEGER:
                return arrayData.getInt(pos);
            case BIGINT:
                return arrayData.getLong(pos);
            case FLOAT:
                return arrayData.getFloat(pos);
            case DOUBLE:
                return arrayData.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return arrayData.getString(pos);
            case DECIMAL:
                return arrayData.getDecimal(
                        pos,
                        ((DecimalType) elementType).getPrecision(),
                        ((DecimalType) elementType).getScale());
            case DATE:
                return arrayData.getInt(pos);
            case TIME_WITHOUT_TIME_ZONE:
                return arrayData.getInt(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return arrayData.getTimestamp(pos, ((TimestampType) elementType).getPrecision());
            case ARRAY:
                return convertToArrayOriginal(arrayData.getArray(pos), (ArrayType) elementType);
            case MAP:
                return convertToMapOriginal(arrayData.getMap(pos), (MapType) elementType);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported array element type: " + elementType);
        }
    }

    private static Object convertToMap(Object obj, MapType mapType) {
        if (obj instanceof MapData) {
            return obj;
        }
        if (obj instanceof Map) {
            Map<?, ?> javaMap = (Map<?, ?>) obj;
            GenericMapData mapData = new GenericMapData(javaMap);
            return mapData;
        }
        throw new IllegalArgumentException("Unable to convert to MapData: " + obj);
    }

    private static Object convertToMapOriginal(Object obj, MapType mapType) {
        if (obj instanceof MapData) {
            MapData mapData = (MapData) obj;
            Map<Object, Object> result = new HashMap<>();
            ArrayData keyArray = mapData.keyArray();
            ArrayData valueArray = mapData.valueArray();
            for (int i = 0; i < mapData.size(); i++) {
                Object key = getArrayElement(keyArray, i, mapType.getKeyType());
                Object value = getArrayElement(valueArray, i, mapType.getValueType());
                result.put(key, value);
            }
            return result;
        }
        return obj;
    }

    private static LocalTime toLocalTime(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof LocalTime) {
            return (LocalTime) obj;
        }
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj).toLocalTime();
        }
        if (obj instanceof java.sql.Date) {
            throw new IllegalArgumentException(
                    "Unable to convert to LocalDate from a java.sql.Date value '" + obj + "'");
        }
        if (obj instanceof java.sql.Time) {
            java.sql.Time time = (java.sql.Time) obj;
            long millis = (int) (time.getTime() % MILLISECONDS_PER_SECOND);
            int nanosOfSecond = (int) (millis * NANOSECONDS_PER_MILLISECOND);
            return LocalTime.of(
                    time.getHours(), time.getMinutes(), time.getSeconds(), nanosOfSecond);
        }
        if (obj instanceof java.sql.Timestamp) {
            java.sql.Timestamp timestamp = (java.sql.Timestamp) obj;
            return LocalTime.of(
                    timestamp.getHours(),
                    timestamp.getMinutes(),
                    timestamp.getSeconds(),
                    timestamp.getNanos());
        }
        if (obj instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) obj;
            long millis = (int) (date.getTime() % MILLISECONDS_PER_SECOND);
            int nanosOfSecond = (int) (millis * NANOSECONDS_PER_MILLISECOND);
            return LocalTime.of(
                    date.getHours(), date.getMinutes(), date.getSeconds(), nanosOfSecond);
        }
        if (obj instanceof Duration) {
            Long value = ((Duration) obj).toNanos();
            if (value >= 0 && value <= NANOSECONDS_PER_DAY) {
                return LocalTime.ofNanoOfDay(value);
            } else {
                throw new IllegalArgumentException(
                        "Time values must use number of milliseconds greater than 0 and less than 86400000000000");
            }
        }
        throw new IllegalArgumentException(
                "Unable to convert to LocalTime from unexpected value '"
                        + obj
                        + "' of type "
                        + obj.getClass().getName());
    }

    private static Object convertToTimestamp(Object obj) {
        if (obj instanceof Long) {
            return TimestampData.fromMillis((Long) obj);
        } else if (obj instanceof Timestamp) {
            return TimestampData.fromTimestamp((Timestamp) obj);
        } else if (obj instanceof TimestampData) {
            return obj;
        }
        throw new IllegalArgumentException(
                "Unable to convert to TIMESTAMP from unexpected value '"
                        + obj
                        + "' of type "
                        + obj.getClass().getName());
    }

    private static Object convertToZonedTimestampData(Object obj) {
        if (obj instanceof ZonedTimestampData) {
            return obj;
        }
        throw new IllegalArgumentException(
                "Unable to convert to TIMESTAMP_TZ from unexpected value '"
                        + obj
                        + "' of type "
                        + obj.getClass().getName());
    }

    private static Object convertToLocalTimeZoneTimestamp(Object obj) {
        if (obj instanceof String) {
            String str = (String) obj;
            // TIMESTAMP_LTZ type is encoded in string type
            Instant instant = Instant.parse(str);
            return LocalZonedTimestampData.fromInstant(instant);
        } else if (obj instanceof Long) {
            return LocalZonedTimestampData.fromEpochMillis((Long) obj);
        } else if (obj instanceof LocalZonedTimestampData) {
            return obj;
        }
        throw new IllegalArgumentException(
                "Unable to convert to TIMESTAMP_LTZ from unexpected value '"
                        + obj
                        + "' of type "
                        + obj.getClass().getName());
    }

    private static Object convertToString(Object obj) {
        return BinaryStringData.fromString(obj.toString());
    }

    private static Object convertToStringOriginal(Object obj) {
        return String.valueOf(obj);
    }

    private static Object convertToBinary(Object obj) {
        if (obj instanceof byte[]) {
            return obj;
        } else if (obj instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) obj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported BYTES value type: " + obj.getClass().getSimpleName());
        }
    }

    // convert to DecimalData
    private static Object convertToDecimal(Object obj) {
        if (obj instanceof BigDecimal) {
            BigDecimal bigDecimalValue = (BigDecimal) obj;
            return DecimalData.fromBigDecimal(
                    bigDecimalValue, bigDecimalValue.precision(), bigDecimalValue.scale());
        } else if (obj instanceof DecimalData) {
            return obj;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Decimal value type: " + obj.getClass().getSimpleName());
        }
    }
}
