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

package org.apache.flink.cdc.connectors.dws.utils;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/** Utility helpers for converting Flink internal values into GaussDB DWS compatible objects. */
public final class DwsUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private DwsUtils() {}

    public static RecordData.FieldGetter createFieldGetter(
            DataType fieldType, int fieldPos, ZoneId zoneId) {
        final RecordData.FieldGetter fieldGetter;
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                fieldGetter = record -> record.getBoolean(fieldPos);
                break;
            case TINYINT:
                fieldGetter = record -> record.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = record -> record.getShort(fieldPos);
                break;
            case INTEGER:
                fieldGetter = record -> record.getInt(fieldPos);
                break;
            case BIGINT:
                fieldGetter = record -> record.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = record -> record.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = record -> record.getDouble(fieldPos);
                break;
            case DECIMAL:
                int decimalPrecision = getPrecision(fieldType);
                int decimalScale = getScale(fieldType);
                fieldGetter =
                        record ->
                                record.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                        .toBigDecimal();
                break;
            case CHAR:
            case VARCHAR:
                fieldGetter = record -> record.getString(fieldPos).toString();
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = record -> record.getBinary(fieldPos);
                break;
            case DATE:
                fieldGetter =
                        record ->
                                LocalDate.ofEpochDay(record.getInt(fieldPos))
                                        .format(DATE_FORMATTER);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        record ->
                                record.getTimestamp(fieldPos, getPrecision(fieldType))
                                        .toLocalDateTime()
                                        .format(DATETIME_FORMATTER);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        record ->
                                ZonedDateTime.ofInstant(
                                                record.getLocalZonedTimestampData(
                                                                fieldPos, getPrecision(fieldType))
                                                        .toInstant(),
                                                zoneId)
                                        .toLocalDateTime()
                                        .format(DATETIME_FORMATTER);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                int zonedPrecision = ((ZonedTimestampType) fieldType).getPrecision();
                fieldGetter = record -> record.getTimestamp(fieldPos, zonedPrecision).toTimestamp();
                break;
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter =
                        record -> LocalTime.ofNanoOfDay(record.getLong(fieldPos) * 1_000_000L);
                break;
            case ARRAY:
                fieldGetter = record -> convertArrayData(record.getArray(fieldPos));
                break;
            case MAP:
                fieldGetter = record -> writeValueAsString(convertMapData(record.getMap(fieldPos)));
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type " + fieldType.getTypeRoot());
        }

        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> row.isNullAt(fieldPos) ? null : fieldGetter.getFieldOrNull(row);
    }

    private static List<Object> convertArrayData(ArrayData arrayData) {
        if (arrayData instanceof GenericArrayData) {
            return Arrays.asList(((GenericArrayData) arrayData).toObjectArray());
        }
        throw new UnsupportedOperationException("Unsupported array data: " + arrayData.getClass());
    }

    private static Object convertMapData(MapData mapData) {
        if (!(mapData instanceof GenericMapData)) {
            throw new UnsupportedOperationException("Unsupported map data: " + mapData.getClass());
        }

        GenericMapData genericMapData = (GenericMapData) mapData;
        Map<Object, Object> result = new HashMap<>();
        for (Object key : ((GenericArrayData) genericMapData.keyArray()).toObjectArray()) {
            result.put(key, genericMapData.get(key));
        }
        return result;
    }

    private static String writeValueAsString(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize value to JSON.", e);
        }
    }
}
