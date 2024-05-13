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

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.VarBinaryType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
                return Integer.class;
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.class;
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
                return BigDecimal.class;
            case ROW:
                return Object.class;
            case ARRAY:
            case MAP:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    public static SqlTypeName convertCalciteType(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return SqlTypeName.BOOLEAN;
            case TINYINT:
                return SqlTypeName.TINYINT;
            case SMALLINT:
                return SqlTypeName.SMALLINT;
            case INTEGER:
                return SqlTypeName.INTEGER;
            case BIGINT:
                return SqlTypeName.BIGINT;
            case DATE:
                return SqlTypeName.DATE;
            case TIME_WITHOUT_TIME_ZONE:
                return SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return SqlTypeName.TIMESTAMP;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            case FLOAT:
                return SqlTypeName.FLOAT;
            case DOUBLE:
                return SqlTypeName.DOUBLE;
            case CHAR:
                return SqlTypeName.CHAR;
            case VARCHAR:
                return SqlTypeName.VARCHAR;
            case BINARY:
                return SqlTypeName.BINARY;
            case VARBINARY:
                return SqlTypeName.VARBINARY;
            case DECIMAL:
                return SqlTypeName.DECIMAL;
            case ROW:
                return SqlTypeName.ROW;
            case ARRAY:
            case MAP:
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
            case TIME_WITH_LOCAL_TIME_ZONE:
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
                return DataTypes.BINARY(BinaryType.MAX_LENGTH);
            case VARBINARY:
                return DataTypes.VARBINARY(VarBinaryType.MAX_LENGTH);
            case DECIMAL:
                return DataTypes.DECIMAL(relDataType.getPrecision(), relDataType.getScale());
            case ROW:
            case ARRAY:
            case MAP:
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
            case MAP:
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
                return convertToDecimalOriginal(value);
            case ROW:
                return value;
            case ARRAY:
            case MAP:
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

    private static Object convertToDate(Object obj) {
        return (int) toLocalDate(obj).toEpochDay();
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

    private static Object convertToTime(Object obj) {
        if (obj instanceof Integer) {
            return obj;
        }
        // get number of milliseconds of the day
        return toLocalTime(obj).toSecondOfDay() * 1000;
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

    private static Object convertToDecimalOriginal(Object obj) {
        if (obj instanceof BigDecimal) {
            return obj;
        } else if (obj instanceof DecimalData) {
            DecimalData decimalData = (DecimalData) obj;
            return decimalData.toBigDecimal();
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Decimal value type: " + obj.getClass().getSimpleName());
        }
    }
}
