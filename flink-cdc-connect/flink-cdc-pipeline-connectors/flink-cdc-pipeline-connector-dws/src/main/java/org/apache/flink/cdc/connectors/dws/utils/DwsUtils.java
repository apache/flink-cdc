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
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldCount;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldNames;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldTypes;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getNestedTypes;
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
                        record -> record.getDate(fieldPos).toLocalDate().format(DATE_FORMATTER);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        record ->
                                record.getTimestamp(fieldPos, getPrecision(fieldType))
                                        .toTimestamp();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        record ->
                                Timestamp.valueOf(
                                        ZonedDateTime.ofInstant(
                                                        record.getLocalZonedTimestampData(
                                                                        fieldPos,
                                                                        getPrecision(fieldType))
                                                                .toInstant(),
                                                        zoneId)
                                                .toLocalDateTime());
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                int zonedPrecision = ((ZonedTimestampType) fieldType).getPrecision();
                fieldGetter =
                        record -> record.getZonedTimestamp(fieldPos, zonedPrecision).toTimestamp();
                break;
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = record -> record.getTime(fieldPos).toLocalTime();
                break;
            case ARRAY:
                DataType elementType = getNestedTypes(fieldType).get(0);
                fieldGetter =
                        record ->
                                writeValueAsString(
                                        convertArrayData(
                                                record.getArray(fieldPos), elementType, zoneId));
                break;
            case MAP:
                DataType keyType = getNestedTypes(fieldType).get(0);
                DataType valueType = getNestedTypes(fieldType).get(1);
                fieldGetter =
                        record ->
                                writeValueAsString(
                                        convertMapData(
                                                record.getMap(fieldPos),
                                                keyType,
                                                valueType,
                                                zoneId));
                break;
            case ROW:
                List<String> fieldNames = getFieldNames(fieldType);
                List<DataType> fieldTypes = getFieldTypes(fieldType);
                int fieldCount = getFieldCount(fieldType);
                fieldGetter =
                        record ->
                                writeValueAsString(
                                        convertRowData(
                                                record.getRow(fieldPos, fieldCount),
                                                fieldNames,
                                                fieldTypes,
                                                zoneId));
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

    private static List<Object> convertArrayData(
            ArrayData arrayData, DataType elementType, ZoneId zoneId) {
        ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        List<Object> result = new ArrayList<>(arrayData.size());
        for (int i = 0; i < arrayData.size(); i++) {
            Object value = elementGetter.getElementOrNull(arrayData, i);
            result.add(convertInternalValue(value, elementType, zoneId));
        }
        return result;
    }

    private static Object convertMapData(
            MapData mapData, DataType keyType, DataType valueType, ZoneId zoneId) {
        ArrayData keyArray = mapData.keyArray();
        ArrayData valueArray = mapData.valueArray();
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        Map<Object, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < mapData.size(); i++) {
            Object key =
                    convertInternalValue(keyGetter.getElementOrNull(keyArray, i), keyType, zoneId);
            Object value =
                    convertInternalValue(
                            valueGetter.getElementOrNull(valueArray, i), valueType, zoneId);
            result.put(key, value);
        }
        return result;
    }

    private static Object convertRowData(
            RecordData rowData, List<String> fieldNames, List<DataType> fieldTypes, ZoneId zoneId) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            RecordData.FieldGetter fieldGetter = RecordData.createFieldGetter(fieldTypes.get(i), i);
            result.put(
                    fieldNames.get(i),
                    convertInternalValue(
                            fieldGetter.getFieldOrNull(rowData), fieldTypes.get(i), zoneId));
        }
        return result;
    }

    private static Object convertInternalValue(Object value, DataType dataType, ZoneId zoneId) {
        if (value == null) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return value instanceof StringData ? ((StringData) value).toString() : value;
            case DATE:
                if (value instanceof DateData) {
                    return ((DateData) value).toLocalDate().format(DATE_FORMATTER);
                }
                return LocalDate.ofEpochDay(((Number) value).longValue()).format(DATE_FORMATTER);
            case TIME_WITHOUT_TIME_ZONE:
                if (value instanceof TimeData) {
                    return ((TimeData) value).toLocalTime().toString();
                }
                return LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampData) value).toLocalDateTime().format(DATETIME_FORMATTER);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ZonedDateTime.ofInstant(
                                ((LocalZonedTimestampData) value).toInstant(), zoneId)
                        .toLocalDateTime()
                        .format(DATETIME_FORMATTER);
            case TIMESTAMP_WITH_TIME_ZONE:
                return ((ZonedTimestampData) value).toString();
            case ARRAY:
                return convertArrayData((ArrayData) value, getNestedTypes(dataType).get(0), zoneId);
            case MAP:
                return convertMapData(
                        (MapData) value,
                        getNestedTypes(dataType).get(0),
                        getNestedTypes(dataType).get(1),
                        zoneId);
            case ROW:
                return convertRowData(
                        (RecordData) value,
                        getFieldNames(dataType),
                        getFieldTypes(dataType),
                        zoneId);
            default:
                return value;
        }
    }

    private static String writeValueAsString(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize value to JSON.", e);
        }
    }
}
