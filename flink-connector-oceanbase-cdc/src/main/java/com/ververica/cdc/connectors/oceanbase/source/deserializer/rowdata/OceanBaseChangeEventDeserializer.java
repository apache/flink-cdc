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

package com.ververica.cdc.connectors.oceanbase.source.deserializer.rowdata;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import com.ververica.cdc.connectors.oceanbase.source.deserializer.OceanBaseChangeEventDeserializerSchema;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Deserialization schema from OceanBase commit log data to Flink Table/SQL internal data structure
 * {@link RowData}.
 */
public class OceanBaseChangeEventDeserializer
        implements OceanBaseChangeEventDeserializerSchema<RowData> {

    private static final long serialVersionUID = -368417339580799352L;

    public OceanBaseChangeEventDeserializer() {}

    @Override
    public List<RowData> deserialize(LogMessage message) {
        List<RowData> result = new ArrayList<>();
        switch (message.getOpt()) {
            case INSERT:
                result.add(
                        GenericRowData.ofKind(
                                RowKind.INSERT,
                                message.getFieldList().stream().map(this::convertField).toArray()));
                break;
            case UPDATE:
                result.add(
                        GenericRowData.ofKind(
                                RowKind.UPDATE_BEFORE,
                                message.getFieldList().stream()
                                        .filter(DataMessage.Record.Field::isPrev)
                                        .map(this::convertField)
                                        .toArray()));
                result.add(
                        GenericRowData.ofKind(
                                RowKind.UPDATE_AFTER,
                                message.getFieldList().stream()
                                        .filter(field -> !field.isPrev())
                                        .map(this::convertField)
                                        .toArray()));
                break;
            case DELETE:
                result.add(
                        GenericRowData.ofKind(
                                RowKind.DELETE,
                                message.getFieldList().stream().map(this::convertField).toArray()));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + message.getOpt());
        }
        return result;
    }

    private Object convertField(DataMessage.Record.Field field) {
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
            case DECIMAL:
                BigDecimal bigDecimal = new BigDecimal(field.getValue().toString());
                return DecimalData.fromBigDecimal(
                        bigDecimal, bigDecimal.precision(), bigDecimal.scale());
            case FLOAT:
                return Float.parseFloat(field.getValue().toString());
            case DOUBLE:
                return Double.parseDouble(field.getValue().toString());
            case ENUM:
            case SET:
            case STRING:
            case JSON:
                return StringData.fromString(field.getValue().toString());
            case DATETIME:
            case TIMESTAMP:
            case TIMESTAMP_NANO:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromTimestamp(Timestamp.valueOf(field.getValue().toString()));
            case DATE:
                return (int) Date.valueOf(field.getValue().toString()).toLocalDate().toEpochDay();
            case TIME:
                return (int)
                        (Time.valueOf(field.getValue().toString()).toLocalTime().toNanoOfDay()
                                / 1_000_000L);
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
