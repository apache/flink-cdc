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

package org.apache.flink.cdc.connectors.kafka.json.debezium;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;

/** Converts a Debezium JSON row into CDC {@link RecordData}. */
class DebeziumJsonRecordDataConverter {

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

    private Object convertValue(JsonNode node, DataType targetType) {
        if (node == null || node.isNull()) {
            return null;
        }
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                return (byte) node.asInt();
            case SMALLINT:
                return (short) node.asInt();
            case INTEGER:
                return node.asInt();
            case BIGINT:
                return node.asLong();
            case FLOAT:
                return (float) node.asDouble();
            case DOUBLE:
                return node.asDouble();
            case BOOLEAN:
                return node.asBoolean();
            case CHAR:
            case VARCHAR:
                return BinaryStringData.fromString(node.asText());
            case BINARY:
            case VARBINARY:
                return node.isBinary()
                        ? binaryValue(node)
                        : Base64.getDecoder()
                                .decode(node.asText().getBytes(StandardCharsets.UTF_8));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) targetType;
                return DecimalData.fromBigDecimal(
                        decimalValue(node, decimalType.getScale()),
                        decimalType.getPrecision(),
                        decimalType.getScale());
            case DATE:
                return node.isIntegralNumber()
                        ? DateData.fromEpochDay(node.asInt())
                        : DateData.fromIsoLocalDateString(node.asText());
            case TIME_WITHOUT_TIME_ZONE:
                return node.isIntegralNumber()
                        ? TimeData.fromNanoOfDay(
                                normalizeTimeToNanos(
                                        node.asLong(),
                                        DataTypes.getPrecision(targetType).orElse(3)))
                        : TimeData.fromIsoLocalTimeString(node.asText());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return node.isIntegralNumber()
                        ? TimestampData.fromLocalDateTime(
                                LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(
                                                normalizeTimestampToMillis(
                                                        node.asLong(),
                                                        DataTypes.getPrecision(targetType)
                                                                .orElse(3))),
                                        ZoneOffset.UTC))
                        : TimestampData.fromLocalDateTime(LocalDateTime.parse(node.asText()));
            case TIMESTAMP_WITH_TIME_ZONE:
                return ZonedTimestampData.fromZonedDateTime(ZonedDateTime.parse(node.asText()));
            default:
                throw new IllegalArgumentException(
                        "Unsupported target type " + targetType.asSummaryString() + ".");
        }
    }

    private byte[] binaryValue(JsonNode node) {
        try {
            return node.binaryValue();
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot decode Debezium binary value.", e);
        }
    }

    private BigDecimal decimalValue(JsonNode node, int scale) {
        if (node.isNumber()) {
            return node.decimalValue();
        }
        try {
            return new BigDecimal(node.asText());
        } catch (NumberFormatException ignored) {
            byte[] unscaled = Base64.getDecoder().decode(node.asText());
            return new BigDecimal(new BigInteger(unscaled), scale);
        }
    }

    private long normalizeTimeToNanos(long value, int precision) {
        if (precision <= 3) {
            return value * 1_000_000L;
        }
        if (precision <= 6) {
            return value * 1_000L;
        }
        return value;
    }

    private long normalizeTimestampToMillis(long value, int precision) {
        if (precision > 6) {
            return value / 1_000_000L;
        }
        if (precision > 3) {
            return value / 1_000L;
        }
        return value;
    }
}
