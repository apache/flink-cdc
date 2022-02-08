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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/** Utils to get jdbc type and value of a field. */
public class OceanBaseJdbcReader {

    public static Object getField(int jdbcType, Object value) {
        if (value == null) {
            return null;
        }
        switch (jdbcType) {
            case Types.DECIMAL:
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
                return io.debezium.time.Timestamp.toEpochMillis(timestamp, null);
            case Types.REAL:
                Float real = (Float) value;
                return real.doubleValue();
            default:
                return value;
        }
    }

    public static Object getField(DataMessage.Record.Field.Type fieldType, ByteString value) {
        if (value == null) {
            return null;
        }
        switch (fieldType) {
            case NULL:
                return null;
            case BIT:
            case INT8:
            case INT16:
            case INT24:
            case INT32:
            case YEAR:
                return Integer.parseInt(value.toString());
            case INT64:
                return Long.parseLong(value.toString());
            case FLOAT:
            case DOUBLE:
                return Double.parseDouble(value.toString());
            case DECIMAL:
            case ENUM:
            case SET:
            case STRING:
            case JSON:
                return value.toString();
            case DATE:
                return io.debezium.time.Date.toEpochDay(Date.valueOf(value.toString()), null);
            case TIME:
                return io.debezium.time.MicroTime.toMicroOfDay(
                        Time.valueOf(value.toString()), true);
            case DATETIME:
            case TIMESTAMP:
            case TIMESTAMP_NANO:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return io.debezium.time.Timestamp.toEpochMillis(
                        Timestamp.valueOf(value.toString()), null);
            case BLOB:
            case BINARY:
                return value.getBytes();
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
            case GEOMETRY:
            case RAW:
            case UNKOWN:
            default:
                throw new IllegalArgumentException("Unsupported field type " + fieldType);
        }
    }

    public static int getType(DataMessage.Record.Field.Type fieldType) {
        switch (fieldType) {
            case NULL:
                return Types.NULL;
            case INT8:
                return Types.TINYINT;
            case INT16:
                return Types.SMALLINT;
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
                return Types.LONGVARBINARY;
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
