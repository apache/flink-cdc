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

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.elasticsearch6.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converter class for serializing row data to Elasticsearch compatible formats. */
public class ElasticsearchRowConverter {
    private static final ObjectMapper objectMapper = new ObjectMapper();

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
            DataType columnType, java.time.ZoneId zoneId) {
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
    static ElasticsearchRowConverter.SerializationConverter createExternalConverter(
            DataType columnType, ZoneId zoneId) {
        switch (columnType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (pos, data) -> data.getString(pos).toString();
            case BOOLEAN:
                return (pos, data) -> data.getBoolean(pos);
            case BINARY:
            case VARBINARY:
                return (pos, data) -> data.getBinary(pos);
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) columnType).getPrecision();
                final int decimalScale = ((DecimalType) columnType).getScale();
                return (pos, data) ->
                        data.getDecimal(pos, decimalPrecision, decimalScale)
                                .toBigDecimal()
                                .toString();
            case TINYINT:
                return (pos, data) -> data.getByte(pos);
            case SMALLINT:
                return (pos, data) -> data.getShort(pos);
            case INTEGER:
                return (pos, data) -> data.getInt(pos);
            case BIGINT:
                return (pos, data) -> data.getLong(pos);
            case FLOAT:
                return (pos, data) -> data.getFloat(pos);
            case DOUBLE:
                return (pos, data) -> data.getDouble(pos);
            case DATE:
                return (pos, data) -> data.getDate(pos).toLocalDate().format(DATE_FORMATTER);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (pos, data) ->
                        data.getTimestamp(pos, DataTypeChecks.getPrecision(columnType))
                                .toLocalDateTime()
                                .format(DATE_TIME_FORMATTER);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (pos, data) ->
                        data.getTimestamp(pos, DataTypeChecks.getPrecision(columnType))
                                .toLocalDateTime()
                                .atZone(zoneId)
                                .format(DATE_TIME_FORMATTER);
            case TIMESTAMP_WITH_TIME_ZONE:
                final int zonedP = ((ZonedTimestampType) columnType).getPrecision();
                return (pos, data) ->
                        data.getTimestamp(pos, zonedP)
                                .toTimestamp()
                                .toInstant()
                                .atZone(zoneId)
                                .format(DATE_TIME_FORMATTER);
            case ARRAY:
                return (pos, data) -> convertArrayData(data.getArray(pos), columnType);
            case MAP:
                return (pos, data) ->
                        writeValueAsString(convertMapData(data.getMap(pos), columnType));
            case ROW:
                return (pos, data) ->
                        writeValueAsString(convertRowData(data, pos, columnType, zoneId));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + columnType);
        }
    }

    private static List<Object> convertArrayData(ArrayData array, DataType type) {
        if (array instanceof GenericArrayData) {
            return Arrays.asList(((GenericArrayData) array).toObjectArray());
        }
        throw new UnsupportedOperationException("Unsupported array data: " + array.getClass());
    }

    private static Object convertMapData(MapData map, DataType type) {
        Map<Object, Object> result = new HashMap<>();
        if (map instanceof GenericMapData) {
            GenericMapData gMap = (GenericMapData) map;
            for (Object key : ((GenericArrayData) gMap.keyArray()).toObjectArray()) {
                result.put(key, gMap.get(key));
            }
            return result;
        }
        throw new UnsupportedOperationException("Unsupported map data: " + map.getClass());
    }

    private static Object convertRowData(
            RecordData val, int index, DataType type, ZoneId pipelineZoneId) {
        RowType rowType = (RowType) type;
        Map<String, Object> value = new HashMap<>();
        RecordData row = val.getRow(index, rowType.getFieldCount());

        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField rowField = fields.get(i);
            SerializationConverter converter =
                    createNullableExternalConverter(rowField.getType(), pipelineZoneId);
            Object valTmp = converter.serialize(i, row);
            value.put(rowField.getName(), valTmp.toString());
        }
        return value;
    }

    private static String writeValueAsString(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
