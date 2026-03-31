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

package org.apache.flink.cdc.connectors.kafka.json.utils;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/** Utility class for converting CDC {@link RecordData} to Flink SQL {@link RowData}. */
public class RecordDataConverter {

    /** Create {@link RecordData.FieldGetter}s from given CDC {@link Schema}. */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema, ZoneId zoneId) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            fieldGetters.add(createFieldGetter(schema.getColumns().get(i).getType(), i, zoneId));
        }
        return fieldGetters;
    }

    /**
     * Converts a CDC {@link RecordData} to Flink SQL {@link RowData}.
     *
     * @param recordData the CDC record data to convert
     * @param rowType the row type schema defining the structure
     * @param zoneId the time zone for timestamp conversions
     * @return the converted Flink SQL RowData, or null if input is null
     */
    private static RowData convertRowData(RecordData recordData, RowType rowType, ZoneId zoneId) {
        if (recordData == null) {
            return null;
        }

        List<DataField> fields = rowType.getFields();
        GenericRowData rowData = new GenericRowData(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            DataType fieldType = field.getType();

            Object value = convertField(recordData, i, fieldType, zoneId);
            rowData.setField(i, value);
        }

        return rowData;
    }

    /**
     * Converts a single field from CDC {@link RecordData} to Flink SQL format.
     *
     * <p>This method handles various data types including primitives, temporal types, and complex
     * types (arrays, maps, rows).
     *
     * @param recordData the CDC record data containing the field
     * @param pos the position of the field in the record
     * @param fieldType the data type of the field
     * @param zoneId the time zone for timestamp conversions
     * @return the converted field value in Flink SQL format, or null if the field is null
     * @throws IllegalArgumentException if the field type is not supported
     */
    private static Object convertField(
            RecordData recordData, int pos, DataType fieldType, ZoneId zoneId) {
        if (recordData.isNullAt(pos)) {
            return null;
        }

        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryStringData.fromString(recordData.getString(pos).toString());
            case BOOLEAN:
                return recordData.getBoolean(pos);
            case BINARY:
            case VARBINARY:
                return recordData.getBinary(pos);
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                return DecimalData.fromBigDecimal(
                        recordData.getDecimal(pos, decimalPrecision, decimalScale).toBigDecimal(),
                        decimalPrecision,
                        decimalScale);
            case TINYINT:
                return recordData.getByte(pos);
            case SMALLINT:
                return recordData.getShort(pos);
            case INTEGER:
                return recordData.getInt(pos);
            case DATE:
                return recordData.getDate(pos).toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return recordData.getTime(pos).toMillisOfDay();
            case BIGINT:
                return recordData.getLong(pos);
            case FLOAT:
                return recordData.getFloat(pos);
            case DOUBLE:
                return recordData.getDouble(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromTimestamp(
                        recordData.getTimestamp(pos, getPrecision(fieldType)).toTimestamp());
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromInstant(
                        recordData
                                .getLocalZonedTimestampData(
                                        pos, DataTypeChecks.getPrecision(fieldType))
                                .toInstant());
            case ARRAY:
                ArrayData arrayData = recordData.getArray(pos);
                return convertArrayData(arrayData, (ArrayType) fieldType, zoneId);
            case MAP:
                MapData mapData = recordData.getMap(pos);
                return convertMapData(mapData, (MapType) fieldType, zoneId);
            case ROW:
                RecordData nestedRecordData =
                        recordData.getRow(pos, DataTypeChecks.getFieldCount(fieldType));
                return convertRowData(nestedRecordData, (RowType) fieldType, zoneId);
            default:
                throw new IllegalArgumentException(
                        "Unsupported field type: " + fieldType.getTypeRoot());
        }
    }

    /**
     * Converts a CDC {@link ArrayData} to Flink SQL {@link org.apache.flink.table.data.ArrayData}.
     *
     * <p>This method recursively converts each element in the array according to the element type.
     *
     * @param arrayData the CDC array data to convert
     * @param arrayType the array type schema defining the element type
     * @param zoneId the time zone for timestamp conversions
     * @return the converted Flink SQL ArrayData, or null if input is null
     */
    private static org.apache.flink.table.data.ArrayData convertArrayData(
            ArrayData arrayData, ArrayType arrayType, ZoneId zoneId) {
        if (arrayData == null) {
            return null;
        }

        DataType elementType = arrayType.getElementType();
        int size = arrayData.size();
        Object[] result = new Object[size];

        for (int i = 0; i < size; i++) {
            result[i] = convertElement(arrayData, i, elementType, zoneId);
        }

        return new org.apache.flink.table.data.GenericArrayData(result);
    }

    /**
     * Converts a CDC {@link MapData} to Flink SQL {@link org.apache.flink.table.data.MapData}.
     *
     * <p>This method converts both keys and values in the map according to their respective types.
     *
     * @param mapData the CDC map data to convert
     * @param mapType the map type schema defining key and value types
     * @param zoneId the time zone for timestamp conversions
     * @return the converted Flink SQL MapData, or null if input is null
     */
    private static org.apache.flink.table.data.MapData convertMapData(
            MapData mapData, MapType mapType, ZoneId zoneId) {
        if (mapData == null) {
            return null;
        }

        ArrayData keyArray = mapData.keyArray();
        ArrayData valueArray = mapData.valueArray();

        int size = keyArray.size();
        Map<Object, Object> result = new HashMap<>();

        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();

        for (int i = 0; i < size; i++) {
            Object key = convertElement(keyArray, i, keyType, zoneId);
            Object value = convertElement(valueArray, i, valueType, zoneId);
            result.put(key, value);
        }

        return new org.apache.flink.table.data.GenericMapData(result);
    }

    /**
     * Converts a single element from CDC {@link ArrayData} to Flink SQL format.
     *
     * <p>This method is similar to {@link #convertField} but operates on array elements.
     *
     * @param arrayData the CDC array data containing the element
     * @param pos the position of the element in the array
     * @param elementType the data type of the element
     * @param zoneId the time zone for timestamp conversions
     * @return the converted element value in Flink SQL format, or null if the element is null
     * @throws IllegalArgumentException if the element type is not supported
     */
    private static Object convertElement(
            ArrayData arrayData, int pos, DataType elementType, ZoneId zoneId) {
        if (arrayData.isNullAt(pos)) {
            return null;
        }

        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryStringData.fromString(arrayData.getString(pos).toString());
            case BOOLEAN:
                return arrayData.getBoolean(pos);
            case BINARY:
            case VARBINARY:
                return arrayData.getBinary(pos);
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                final int decimalScale = getScale(elementType);
                return DecimalData.fromBigDecimal(
                        arrayData
                                .getDecimal(pos, getPrecision(elementType), getScale(elementType))
                                .toBigDecimal(),
                        decimalPrecision,
                        decimalScale);
            case TINYINT:
                return arrayData.getByte(pos);
            case SMALLINT:
                return arrayData.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return arrayData.getInt(pos);
            case BIGINT:
                return arrayData.getLong(pos);
            case FLOAT:
                return arrayData.getFloat(pos);
            case DOUBLE:
                return arrayData.getDouble(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromTimestamp(
                        arrayData.getTimestamp(pos, getPrecision(elementType)).toTimestamp());
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromInstant(
                        arrayData
                                .getLocalZonedTimestamp(pos, getPrecision(elementType))
                                .toInstant());
            case ARRAY:
                ArrayData nestedArrayData = arrayData.getArray(pos);
                return convertArrayData(nestedArrayData, (ArrayType) elementType, zoneId);
            case MAP:
                MapData mapData = arrayData.getMap(pos);
                return convertMapData(mapData, (MapType) elementType, zoneId);
            case ROW:
                RecordData recordData =
                        arrayData.getRecord(pos, DataTypeChecks.getFieldCount(elementType));
                return convertRowData(recordData, (RowType) elementType, zoneId);
            default:
                throw new IllegalArgumentException(
                        "Unsupported element type: " + elementType.getTypeRoot());
        }
    }

    /**
     * Creates a {@link RecordData.FieldGetter} for extracting and converting a specific field.
     *
     * <p>This method generates a lambda function that extracts a field at the given position and
     * converts it to Flink SQL format.
     *
     * @param fieldType the data type of the field
     * @param fieldPos the position of the field in the record
     * @param zoneId the time zone for timestamp conversions
     * @return a FieldGetter that extracts and converts the field value
     * @throws IllegalArgumentException if the field type is not supported
     */
    private static RecordData.FieldGetter createFieldGetter(
            DataType fieldType, int fieldPos, ZoneId zoneId) {
        final RecordData.FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter =
                        record ->
                                BinaryStringData.fromString(record.getString(fieldPos).toString());
                break;
            case BOOLEAN:
                fieldGetter = record -> record.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = record -> record.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter =
                        record ->
                                DecimalData.fromBigDecimal(
                                        record.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                                .toBigDecimal(),
                                        decimalPrecision,
                                        decimalScale);
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
            case DATE:
                fieldGetter = record -> (int) record.getDate(fieldPos).toEpochDay();
                break;
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = record -> (int) record.getTime(fieldPos).toMillisOfDay();
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
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        record ->
                                TimestampData.fromTimestamp(
                                        record.getTimestamp(fieldPos, getPrecision(fieldType))
                                                .toTimestamp());
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        record ->
                                TimestampData.fromInstant(
                                        record.getLocalZonedTimestampData(
                                                        fieldPos,
                                                        DataTypeChecks.getPrecision(fieldType))
                                                .toInstant());
                break;
            case ARRAY:
                fieldGetter =
                        record ->
                                convertArrayData(
                                        record.getArray(fieldPos), (ArrayType) fieldType, zoneId);
                break;
            case MAP:
                fieldGetter =
                        record ->
                                convertMapData(
                                        record.getMap(fieldPos), (MapType) fieldType, zoneId);
                break;
            case ROW:
                fieldGetter =
                        record ->
                                convertRowData(
                                        record.getRow(
                                                fieldPos, ((RowType) fieldType).getFieldCount()),
                                        (RowType) fieldType,
                                        zoneId);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported field type: " + fieldType.getTypeRoot());
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }
}
