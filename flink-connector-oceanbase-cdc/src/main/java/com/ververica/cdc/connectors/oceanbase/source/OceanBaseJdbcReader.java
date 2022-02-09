/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneOffset;

/** Utils to get jdbc type and value of a field. */
public class OceanBaseJdbcReader {

    public static ValueConverterProvider valueConverterProvider() {
        return new JdbcValueConverters(
                JdbcValueConverters.DecimalMode.STRING,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                ZoneOffset.UTC,
                null,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                CommonConnectorConfig.BinaryHandlingMode.BYTES);
    }

    public static Object getField(int jdbcType, Object value) {
        if (value == null) {
            return null;
        }
        jdbcType = getType(jdbcType);
        switch (jdbcType) {
            case Types.DECIMAL:
                BigDecimal decimal = (BigDecimal) value;
                return decimal.toString();
            case Types.DATE:
                if (value instanceof Short) {
                    return ((Short) value).intValue();
                }
                Date date = (Date) value;
                return io.debezium.time.Date.toEpochDay(date, null);
            case Types.TIME:
                Time time = (Time) value;
                return io.debezium.time.MicroTime.toMicroOfDay(time, true);
            case Types.TIMESTAMP:
                Timestamp timestamp = (Timestamp) value;
                return io.debezium.time.MicroTimestamp.toEpochMicros(timestamp, null);
            case Types.FLOAT:
                Float f = (Float) value;
                return f.doubleValue();
            default:
                return value;
        }
    }

    public static Object getField(DataMessage.Record.Field.Type fieldType, ByteString value) {
        if (value == null) {
            return null;
        }
        int jdbcType = getType(fieldType);
        switch (jdbcType) {
            case Types.NULL:
                return null;
            case Types.INTEGER:
                return Integer.parseInt(value.toString());
            case Types.BIGINT:
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
            case Types.BINARY:
                return value.getBytes();
            default:
                return value.toString();
        }
    }

    public static int getType(int jdbcType) {
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
            case BLOB:
            case BINARY:
                return Types.BINARY;
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
            case GEOMETRY:
            case RAW:
            case UNKOWN:
            default:
                throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
    }
}
