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

package org.apache.flink.cdc.connectors.doris.sink;

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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** converter {@link RecordData} type object to doris field. */
public class DorisRowConverter implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Runtime converter to convert {@link RecordData} type object to doris field. */
    @FunctionalInterface
    interface SerializationConverter extends Serializable {
        Object serialize(int index, RecordData field);
    }

    static SerializationConverter createNullableExternalConverter(
            DataType type, ZoneId pipelineZoneId) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type, pipelineZoneId));
    }

    static SerializationConverter wrapIntoNullableExternalConverter(
            SerializationConverter serializationConverter) {
        return (index, val) -> {
            if (val == null || val.isNullAt(index)) {
                return null;
            } else {
                return serializationConverter.serialize(index, val);
            }
        };
    }

    static SerializationConverter createExternalConverter(DataType type, ZoneId pipelineZoneId) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (index, val) -> val.getString(index).toString();
            case BOOLEAN:
                return (index, val) -> val.getBoolean(index);
            case BINARY:
            case VARBINARY:
                return (index, val) -> val.getBinary(index);
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (index, val) ->
                        val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal();
            case TINYINT:
                return (index, val) -> val.getByte(index);
            case SMALLINT:
                return (index, val) -> val.getShort(index);
            case INTEGER:
                return (index, val) -> val.getInt(index);
            case BIGINT:
                return (index, val) -> val.getLong(index);
            case FLOAT:
                return (index, val) -> val.getFloat(index);
            case DOUBLE:
                return (index, val) -> val.getDouble(index);
            case DATE:
                return (index, val) ->
                        val.getDate(index)
                                .toLocalDate()
                                .format(DorisEventSerializer.DATE_FORMATTER);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (index, val) ->
                        val.getTimestamp(index, DataTypeChecks.getPrecision(type))
                                .toLocalDateTime()
                                .format(DorisEventSerializer.DATE_TIME_FORMATTER);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (index, val) ->
                        ZonedDateTime.ofInstant(
                                        val.getLocalZonedTimestampData(
                                                        index, DataTypeChecks.getPrecision(type))
                                                .toInstant(),
                                        pipelineZoneId)
                                .toLocalDateTime()
                                .format(DorisEventSerializer.DATE_TIME_FORMATTER);
            case TIMESTAMP_WITH_TIME_ZONE:
                final int zonedP = ((ZonedTimestampType) type).getPrecision();
                return (index, val) -> val.getTimestamp(index, zonedP).toTimestamp();
            case TIME_WITHOUT_TIME_ZONE:
                return (index, val) -> val.getTime(index).toLocalTime();
            case ARRAY:
                return (index, val) -> convertArrayData(val.getArray(index), type);
            case MAP:
                return (index, val) -> writeValueAsString(convertMapData(val.getMap(index), type));
            case ROW:
                return (index, val) ->
                        writeValueAsString(convertRowData(val, index, type, pipelineZoneId));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
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
}
