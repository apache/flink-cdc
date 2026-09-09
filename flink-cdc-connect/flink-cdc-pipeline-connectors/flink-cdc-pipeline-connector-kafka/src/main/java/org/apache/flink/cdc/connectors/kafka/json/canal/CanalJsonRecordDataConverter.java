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

package org.apache.flink.cdc.connectors.kafka.json.canal;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DecimalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Base64;

/** Converts a Canal JSON row into CDC {@link RecordData}. */
class CanalJsonRecordDataConverter {

    private static final DateTimeFormatter CANAL_TIMESTAMP =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .optionalEnd()
                    .toFormatter();

    RecordData convertRecord(JsonNode row, Schema targetSchema) {
        if (row == null || row.isNull()) {
            return null;
        }
        GenericRecordData result = new GenericRecordData(targetSchema.getColumnCount());
        for (int i = 0; i < targetSchema.getColumnCount(); i++) {
            Column column = targetSchema.getColumns().get(i);
            result.setField(i, convertValue(row.get(column.getName()), column.getType()));
        }
        return result;
    }

    RecordData convertUpdateBefore(JsonNode after, JsonNode old, Schema targetSchema) {
        if (after == null || after.isNull()) {
            return null;
        }
        GenericRecordData result = new GenericRecordData(targetSchema.getColumnCount());
        for (int i = 0; i < targetSchema.getColumnCount(); i++) {
            Column column = targetSchema.getColumns().get(i);
            JsonNode node = after.get(column.getName());
            if (old != null && old.has(column.getName())) {
                node = old.get(column.getName());
            }
            result.setField(i, convertValue(node, column.getType()));
        }
        return result;
    }

    private Object convertValue(JsonNode node, DataType targetType) {
        if (node == null || node.isNull()) {
            return null;
        }
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                return (byte) Integer.parseInt(node.asText());
            case SMALLINT:
                return (short) Integer.parseInt(node.asText());
            case INTEGER:
                return node.isNumber() ? node.asInt() : Integer.parseInt(node.asText());
            case BIGINT:
                return node.isNumber() ? node.asLong() : Long.parseLong(node.asText());
            case FLOAT:
                return node.isNumber() ? (float) node.asDouble() : Float.parseFloat(node.asText());
            case DOUBLE:
                return node.isNumber() ? node.asDouble() : Double.parseDouble(node.asText());
            case BOOLEAN:
                if (node.isBoolean()) {
                    return node.asBoolean();
                }
                String booleanText = node.asText();
                return "1".equals(booleanText) || Boolean.parseBoolean(booleanText);
            case CHAR:
            case VARCHAR:
                return BinaryStringData.fromString(node.asText());
            case BINARY:
            case VARBINARY:
                return decodeBinary(node);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) targetType;
                return DecimalData.fromBigDecimal(
                        new BigDecimal(node.asText()),
                        decimalType.getPrecision(),
                        decimalType.getScale());
            case DATE:
                return DateData.fromLocalDate(LocalDate.parse(node.asText()));
            case TIME_WITHOUT_TIME_ZONE:
                return TimeData.fromLocalTime(LocalTime.parse(node.asText()));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromLocalDateTime(parseTimestamp(node.asText()));
            default:
                throw new IllegalArgumentException(
                        "Unsupported target type " + targetType.asSummaryString() + ".");
        }
    }

    private byte[] decodeBinary(JsonNode node) {
        if (node.isBinary()) {
            try {
                return node.binaryValue();
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot decode Canal binary value.", e);
            }
        }
        try {
            return Base64.getDecoder().decode(node.asText().getBytes(StandardCharsets.UTF_8));
        } catch (IllegalArgumentException ignored) {
            return node.asText().getBytes(StandardCharsets.UTF_8);
        }
    }

    private LocalDateTime parseTimestamp(String value) {
        if (value.indexOf('T') >= 0) {
            return LocalDateTime.parse(value);
        }
        return LocalDateTime.parse(value, CANAL_TIMESTAMP);
    }
}
