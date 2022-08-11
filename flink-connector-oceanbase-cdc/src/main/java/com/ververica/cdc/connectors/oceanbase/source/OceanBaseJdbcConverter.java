/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oceanbase.source;

import com.oceanbase.oms.logmessage.ByteString;
import com.oceanbase.oms.logmessage.DataMessage;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.util.NumberConversions;
import org.apache.kafka.connect.data.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.Arrays;

/** Utils to convert jdbc type and value of a field. */
public class OceanBaseJdbcConverter {

    public static ValueConverterProvider valueConverterProvider(ZoneOffset zoneOffset) {
        return new JdbcValueConverters(
                JdbcValueConverters.DecimalMode.STRING,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                zoneOffset,
                null,
                JdbcValueConverters.BigIntUnsignedMode.PRECISE,
                CommonConnectorConfig.BinaryHandlingMode.BYTES);
    }

    public static Object getField(int jdbcType, Object value) {
        if (value == null) {
            return null;
        }
        jdbcType = getType(jdbcType, null);
        switch (jdbcType) {
            case Types.BIT:
                if (value instanceof Boolean) {
                    return new byte[] {NumberConversions.getByte((Boolean) value)};
                }
                return value;
            case Types.INTEGER:
                if (value instanceof Boolean) {
                    return NumberConversions.getInteger((Boolean) value);
                }
                if (value instanceof Date) {
                    return ((Date) value).getYear() + 1900;
                }
                return value;
            case Types.FLOAT:
                Float f = (Float) value;
                return f.doubleValue();
            case Types.DECIMAL:
                if (value instanceof BigInteger) {
                    return value.toString();
                }
                BigDecimal decimal = (BigDecimal) value;
                return decimal.toString();
            case Types.DATE:
                Date date = (Date) value;
                return io.debezium.time.Date.toEpochDay(date, null);
            case Types.TIME:
                Time time = (Time) value;
                return io.debezium.time.MicroTime.toMicroOfDay(time, true);
            case Types.TIMESTAMP:
                Timestamp timestamp = (Timestamp) value;
                return io.debezium.time.MicroTimestamp.toEpochMicros(timestamp, null);
            default:
                return value;
        }
    }

    public static Object getField(
            Schema.Type schemaType, DataMessage.Record.Field.Type fieldType, ByteString value) {
        if (value == null) {
            return null;
        }
        int jdbcType = getType(fieldType);
        switch (jdbcType) {
            case Types.NULL:
                return null;
            case Types.INTEGER:
                if (schemaType.equals(Schema.Type.INT64)) {
                    return Long.parseLong(value.toString());
                }
                return Integer.parseInt(value.toString());
            case Types.BIGINT:
                if (schemaType.equals(Schema.Type.STRING)) {
                    return value.toString();
                }
                return Long.parseLong(value.toString());
            case Types.DOUBLE:
                return Double.parseDouble(value.toString());
            case Types.DATE:
                Date date = Date.valueOf(value.toString());
                return io.debezium.time.Date.toEpochDay(date, null);
            case Types.TIME:
                Time time = Time.valueOf(value.toString());
                return io.debezium.time.MicroTime.toMicroOfDay(time, true);
            case Types.TIMESTAMP:
                Timestamp timestamp = Timestamp.valueOf(value.toString());
                return io.debezium.time.MicroTimestamp.toEpochMicros(timestamp, null);
            case Types.BIT:
                long v = Long.parseLong(value.toString());
                byte[] bytes = ByteBuffer.allocate(8).putLong(v).array();
                int i = 0;
                while (bytes[i] == 0 && i < Long.BYTES - 1) {
                    i++;
                }
                return Arrays.copyOfRange(bytes, i, Long.BYTES);
            case Types.BINARY:
                return ByteBuffer.wrap(value.toString().getBytes(StandardCharsets.UTF_8));
            default:
                return value.toString(StandardCharsets.UTF_8.toString());
        }
    }

    private static boolean isBoolean(int jdbcType, String typeName) {
        return jdbcType == Types.BOOLEAN || (jdbcType == Types.BIT && "TINYINT".equals(typeName));
    }

    public static int getType(int jdbcType, String typeName) {
        // treat boolean as tinyint type
        if (isBoolean(jdbcType, typeName)) {
            jdbcType = Types.TINYINT;
        }
        // treat year as int type
        if ("YEAR".equals(typeName)) {
            jdbcType = Types.INTEGER;
        }

        // upcasting
        if ("INT UNSIGNED".equals(typeName)) {
            jdbcType = Types.BIGINT;
        }
        if ("BIGINT UNSIGNED".equals(typeName)) {
            jdbcType = Types.DECIMAL;
        }

        // widening conversion according to com.mysql.jdbc.ResultSetImpl#getObject
        switch (jdbcType) {
            case Types.TINYINT:
            case Types.SMALLINT:
                return Types.INTEGER;
            case Types.REAL:
                return Types.FLOAT;
            default:
                return jdbcType;
        }
    }

    public static int getType(DataMessage.Record.Field.Type fieldType) {
        switch (fieldType) {
            case NULL:
                return Types.NULL;
            case INT8:
            case INT16:
            case INT24:
            case INT32:
            case YEAR:
                return Types.INTEGER;
            case INT64:
                return Types.BIGINT;
            case FLOAT:
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.DECIMAL;
            case ENUM:
            case SET:
            case STRING:
            case JSON:
                return Types.CHAR;
            case TIMESTAMP:
            case DATETIME:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_NANO:
                return Types.TIMESTAMP;
            case DATE:
                return Types.DATE;
            case TIME:
                return Types.TIME;
            case BIT:
                return Types.BIT;
            case BLOB:
            case BINARY:
                return Types.BINARY;
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
            case GEOMETRY:
            case RAW:
                // it's weird to get wrong type from TEXT column, temporarily treat it as a string
            case UNKOWN:
            default:
                return Types.VARCHAR;
        }
    }
}
