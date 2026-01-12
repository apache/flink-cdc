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

/** Utility class for converting CDC RecordData to Flink SQL RowData. */
public class RecordDataConverter {

    /**
     * Convert CDC RecordData to Flink Table RowData.
     *
     * @param recordData CDC record data
     * @param rowType row type information
     * @param zoneId time zone for temporal type conversion
     * @return Flink Table RowData
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
     * Convert a field from CDC RecordData to Flink Table format.
     *
     * @param recordData CDC record data
     * @param pos field position
     * @param fieldType field data type
     * @param zoneId time zone for temporal type conversion
     * @return converted field value
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
     * Convert CDC ArrayData to Flink Table ArrayData.
     *
     * @param arrayData CDC array data
     * @param arrayType array type information
     * @param zoneId time zone for temporal type conversion
     * @return Flink Table ArrayData
     */
    public static org.apache.flink.table.data.ArrayData convertArrayData(
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
     * Convert CDC MapData to Flink Table MapData.
     *
     * @param mapData CDC map data
     * @param mapType map type information
     * @param zoneId time zone for temporal type conversion
     * @return Flink Table MapData
     */
    public static org.apache.flink.table.data.MapData convertMapData(
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
     * Convert an element from CDC ArrayData to Flink Table format.
     *
     * @param arrayData CDC array data
     * @param pos element position
     * @param elementType element data type
     * @param zoneId time zone for temporal type conversion
     * @return converted element value
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
     * Create field getters for converting CDC RecordData to Flink Table RowData.
     *
     * @param schema CDC schema
     * @param zoneId time zone for temporal type conversion
     * @return list of field getters
     */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema, ZoneId zoneId) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            fieldGetters.add(createFieldGetter(schema.getColumns().get(i).getType(), i, zoneId));
        }
        return fieldGetters;
    }

    /**
     * Create a field getter for a specific field.
     *
     * @param fieldType field data type
     * @param fieldPos field position
     * @param zoneId time zone for temporal type conversion
     * @return field getter
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
                        "don't support type of " + fieldType.getTypeRoot());
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
