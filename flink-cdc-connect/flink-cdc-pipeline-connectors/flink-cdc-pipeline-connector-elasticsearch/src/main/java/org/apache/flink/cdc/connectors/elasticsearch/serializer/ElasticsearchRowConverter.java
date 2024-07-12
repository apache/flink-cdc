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

package org.apache.flink.cdc.connectors.elasticsearch.serializer;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;

import java.time.*;
import java.time.format.DateTimeFormatter;

/** Converter class for serializing row data to Elasticsearch compatible formats. */
public class ElasticsearchRowConverter {

    // Date and time formatters for various temporal types
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    /**
     * Creates a nullable external converter for the given column type and time zone.
     *
     * @param columnType The type of the column to convert.
     * @param zoneId The time zone to use for temporal conversions.
     * @return A SerializationConverter that can handle null values.
     */
    public static SerializationConverter createNullableExternalConverter(
            ColumnType columnType, java.time.ZoneId zoneId) {
        return wrapIntoNullableExternalConverter(createExternalConverter(columnType, zoneId));
    }

    /**
     * Wraps a SerializationConverter to handle null values.
     *
     * @param serializationConverter The original SerializationConverter.
     * @return A SerializationConverter that returns null for null input.
     */
    static SerializationConverter wrapIntoNullableExternalConverter(
            SerializationConverter serializationConverter) {
        return (pos, data) -> {
            if (data == null || data.isNullAt(pos)) {
                return null;
            } else {
                return serializationConverter.serialize(pos, data);
            }
        };
    }

    /**
     * Creates an external converter for the given column type and time zone.
     *
     * @param columnType The type of the column to convert.
     * @param zoneId The time zone to use for temporal conversions.
     * @return A SerializationConverter for the specified column type.
     */
    static SerializationConverter createExternalConverter(
            ColumnType columnType, java.time.ZoneId zoneId) {
        switch (columnType) {
                // Basic types
            case BOOLEAN:
                return (pos, data) -> data.getBoolean(pos);
            case INTEGER:
                return (pos, data) -> data.getInt(pos);
            case DOUBLE:
                return (pos, data) -> data.getDouble(pos);
            case VARCHAR:
            case CHAR:
                return (pos, data) -> data.getString(pos).toString();
            case FLOAT:
                return (pos, data) -> data.getFloat(pos);
            case BIGINT:
                return (pos, data) -> data.getLong(pos);
            case TINYINT:
                return (pos, data) -> data.getByte(pos);
            case SMALLINT:
                return (pos, data) -> data.getShort(pos);
            case BINARY:
            case VARBINARY:
                return (pos, data) -> data.getBinary(pos);

                // Decimal type
            case DECIMAL:
                return (pos, data) -> {
                    DecimalData decimalData = data.getDecimal(pos, 17, 2);
                    return decimalData != null ? decimalData.toBigDecimal().toString() : null;
                };

                // Date and time types
            case DATE:
                return (pos, data) -> {
                    int days = data.getInt(pos);
                    LocalDate date = LocalDate.ofEpochDay(days);
                    return date.format(DATE_FORMATTER);
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (pos, data) -> {
                    int milliseconds = data.getInt(pos);
                    LocalTime time = LocalTime.ofNanoOfDay(milliseconds * 1_000_000L);
                    return time.format(TIME_FORMATTER);
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (pos, data) -> {
                    long milliseconds = data.getTimestamp(pos, 6).getMillisecond();
                    LocalDateTime dateTime =
                            LocalDateTime.ofEpochSecond(
                                    milliseconds / 1000,
                                    (int) (milliseconds % 1000) * 1_000_000,
                                    java.time.ZoneOffset.UTC);
                    return dateTime.format(DATE_TIME_FORMATTER);
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (pos, data) -> {
                    try {
                        if (data.isNullAt(pos)) {
                            return null;
                        }

                        // Debug logging
                        System.out.println(
                                "Processing TIMESTAMP_WITH_LOCAL_TIME_ZONE at position: " + pos);
                        System.out.println("Data type: " + data.getClass().getName());

                        // Attempt to retrieve timestamp data
                        long milliseconds;
                        int nanos = 0;

                        try {
                            // Try using getTimestamp method
                            TimestampData timestampData = data.getTimestamp(pos, 6);
                            milliseconds = timestampData.getMillisecond();
                            nanos = timestampData.getNanoOfMillisecond();
                        } catch (Exception e) {
                            // Fallback to getLong if getTimestamp fails
                            milliseconds = data.getLong(pos);
                        }

                        // Create Instant object
                        Instant instant =
                                Instant.ofEpochSecond(
                                        milliseconds / 1000,
                                        (milliseconds % 1000) * 1_000_000L + nanos);

                        // Format timestamp using UTC timezone
                        return DateTimeFormatter.ISO_INSTANT.format(instant);

                    } catch (Exception e) {
                        return "ERROR_PROCESSING_TIMESTAMP";
                    }
                };
            case TIMESTAMP_WITH_TIME_ZONE:
                return (pos, data) -> {
                    long milliseconds = data.getTimestamp(pos, 6).getMillisecond();
                    LocalDateTime dateTime =
                            LocalDateTime.ofEpochSecond(
                                    milliseconds / 1000,
                                    (int) (milliseconds % 1000) * 1_000_000,
                                    zoneId.getRules().getOffset(java.time.Instant.now()));
                    return dateTime.atZone(zoneId).format(DATE_TIME_FORMATTER);
                };

                // Complex types
            case ARRAY:
                return (pos, data) -> data.getArray(pos);
            case MAP:
                return (pos, data) -> data.getMap(pos);
            case ROW:
                return (pos, data) -> {
                    RecordData rowData = data.getRow(pos, 5); // Assuming 5 fields, adjust as needed
                    return rowData != null ? rowData.toString() : null;
                };
            default:
                throw new IllegalArgumentException("Unsupported column type: " + columnType);
        }
    }

    /** Interface for serialization converters. */
    public interface SerializationConverter {
        /**
         * Serializes a value from the given position in the RecordData.
         *
         * @param pos The position of the value in the RecordData.
         * @param data The RecordData containing the value to serialize.
         * @return The serialized object.
         */
        Object serialize(int pos, RecordData data);
    }
}
