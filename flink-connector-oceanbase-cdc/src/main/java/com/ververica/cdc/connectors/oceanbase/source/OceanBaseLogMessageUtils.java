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

import com.oceanbase.oms.logmessage.DataMessage;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/** Utils to deal the LogMessage. */
public class OceanBaseLogMessageUtils {

    public static String getDatabase(String rawName, String tenant) {
        return rawName.replace(tenant + ".", "");
    }

    public static int getJdbcType(DataMessage.Record.Field field) {
        switch (field.getType()) {
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
                return Types.FLOAT;
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
                throw new UnsupportedOperationException("Unsupported type: " + field.getType());
        }
    }

    public static Object getObject(DataMessage.Record.Field field) {
        switch (field.getType()) {
            case NULL:
                return null;
            case BIT:
            case INT8:
            case INT16:
            case INT24:
            case INT32:
            case YEAR:
                return Integer.parseInt(field.getValue().toString());
            case INT64:
                return Long.parseLong(field.getValue().toString());
            case FLOAT:
                return Float.parseFloat(field.getValue().toString());
            case DOUBLE:
                return Double.parseDouble(field.getValue().toString());
            case DECIMAL:
            case ENUM:
            case SET:
            case STRING:
            case JSON:
                return field.getValue().toString();
            case DATETIME:
            case TIMESTAMP:
            case TIMESTAMP_NANO:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.valueOf(field.getValue().toString());
            case DATE:
                return Date.valueOf(field.getValue().toString());
            case TIME:
                return Time.valueOf(field.getValue().toString());
            case BLOB:
            case BINARY:
                return field.getValue().getBytes();
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
            case GEOMETRY:
            case RAW:
            case UNKOWN:
            default:
                throw new IllegalArgumentException("Unsupported type " + field.getType());
        }
    }
}
