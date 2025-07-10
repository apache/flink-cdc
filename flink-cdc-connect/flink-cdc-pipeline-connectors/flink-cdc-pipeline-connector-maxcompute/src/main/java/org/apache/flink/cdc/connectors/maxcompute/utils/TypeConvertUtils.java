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

package org.apache.flink.cdc.connectors.maxcompute.utils;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldCount;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/**
 * Data type mapping table This table shows the mapping relationship from Flink types to MaxCompute
 * types and the corresponding Java type representation.
 *
 * <pre>
 * | Flink Type                        | MaxCompute Type| Flink Java Type     | MaxCompute Java Type |
 * |-----------------------------------|----------------|---------------------|----------------------|
 * | CHAR/VARCHAR/STRING               | STRING         | StringData          | String               |
 * | BOOLEAN                           | BOOLEAN        | Boolean             | Boolean              |
 * | BINARY/VARBINARY                  | BINARY         | byte[]              | odps.data.Binary     |
 * | DECIMAL                           | DECIMAL        | DecimalData         | BigDecimal           |
 * | TINYINT                           | TINYINT        | Byte                | Byte                 |
 * | SMALLINT                          | SMALLINT       | Short               | Short                |
 * | INTEGER                           | INTEGER        | Integer             | Integer              |
 * | BIGINT                            | BIGINT         | Long                | Long                 |
 * | FLOAT                             | FLOAT          | Float               | Float                |
 * | DOUBLE                            | DOUBLE         | Double              | Double               |
 * | TIME_WITHOUT_TIME_ZONE            | STRING         | Integer             | String               |
 * | DATE                              | DATE           | Integer             | LocalDate            |
 * | TIMESTAMP_WITHOUT_TIME_ZONE       | TIMESTAMP_NTZ  | TimestampData       | LocalDateTime        |
 * | TIMESTAMP_WITH_LOCAL_TIME_ZONE    | TIMESTAMP      | LocalZonedTimestampData | Instant          |
 * | TIMESTAMP_WITH_TIME_ZONE          | TIMESTAMP      | ZonedTimestampData  | Instant              |
 * | ARRAY                             | ARRAY          | ArrayData           | ArrayList            |
 * | MAP                               | MAP            | MapData             | HashMap              |
 * | ROW                               | STRUCT         | RowData             | odps.data.SimpleStruct|
 * </pre>
 */
public class TypeConvertUtils {

    /** this method ignore the message of primary key in flinkSchema. */
    public static TableSchema toMaxCompute(Schema flinkSchema) {
        Preconditions.checkNotNull(flinkSchema, "flink Schema");
        TableSchema tableSchema = new TableSchema();
        Set<String> partitionKeys = new HashSet<>(flinkSchema.partitionKeys());
        List<org.apache.flink.cdc.common.schema.Column> columns = flinkSchema.getColumns();
        Set<String> pkSet = new HashSet<>(flinkSchema.primaryKeys());

        for (int i = 0; i < flinkSchema.getColumnCount(); i++) {
            org.apache.flink.cdc.common.schema.Column flinkColumn = columns.get(i);
            Column odpsColumn = toMaxCompute(flinkColumn, pkSet.contains(flinkColumn.getName()));
            if (partitionKeys.contains(flinkColumn.getName())) {
                tableSchema.addPartitionColumn(odpsColumn);
            } else {
                tableSchema.addColumn(odpsColumn);
            }
        }
        return tableSchema;
    }

    public static Column toMaxCompute(
            org.apache.flink.cdc.common.schema.Column flinkColumn, boolean notNull) {
        Preconditions.checkNotNull(flinkColumn, "flink Schema Column");
        DataType type = flinkColumn.getType();
        Column.ColumnBuilder columnBuilder =
                Column.newBuilder(flinkColumn.getName(), toMaxCompute(type));
        if (!type.isNullable() || notNull) {
            columnBuilder.notNull();
        }
        return columnBuilder.build();
    }

    public static TypeInfo toMaxCompute(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case TIME_WITHOUT_TIME_ZONE:
                return TypeInfoFactory.STRING;
            case BOOLEAN:
                return TypeInfoFactory.BOOLEAN;
            case BINARY:
            case VARBINARY:
                return TypeInfoFactory.BINARY;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return TypeInfoFactory.getDecimalTypeInfo(
                        decimalType.getPrecision(), decimalType.getScale());
            case TINYINT:
                return TypeInfoFactory.TINYINT;
            case SMALLINT:
                return TypeInfoFactory.SMALLINT;
            case INTEGER:
                return TypeInfoFactory.INT;
            case BIGINT:
                return TypeInfoFactory.BIGINT;
            case FLOAT:
                return TypeInfoFactory.FLOAT;
            case DOUBLE:
                return TypeInfoFactory.DOUBLE;
            case DATE:
                return TypeInfoFactory.DATE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TypeInfoFactory.TIMESTAMP_NTZ;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TypeInfoFactory.TIMESTAMP;
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                return TypeInfoFactory.getArrayTypeInfo(toMaxCompute(arrayType.getElementType()));
            case MAP:
                MapType mapType = (MapType) type;
                return TypeInfoFactory.getMapTypeInfo(
                        toMaxCompute(mapType.getKeyType()), toMaxCompute(mapType.getValueType()));
            case ROW:
                RowType rowType = (RowType) type;
                return TypeInfoFactory.getStructTypeInfo(
                        rowType.getFieldNames(),
                        rowType.getFieldTypes().stream()
                                .map(TypeConvertUtils::toMaxCompute)
                                .collect(Collectors.toList()));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    public static void toMaxComputeRecord(Schema flinkSchema, RecordData from, ArrayRecord to) {
        Preconditions.checkNotNull(from, "flink record data");
        Preconditions.checkNotNull(to, "maxcompute arrayRecord");
        int partitionKeyCount = flinkSchema.partitionKeys().size();

        List<RecordData.FieldGetter> fieldGetters = createFieldGetters(flinkSchema);

        if (to.getColumnCount() != (fieldGetters.size() - partitionKeyCount)) {
            throw new IllegalArgumentException(
                    "record data count not match, odps {"
                            + Arrays.stream(to.getColumns())
                                    .map(c -> c.getName() + " " + c.getTypeInfo().getTypeName())
                                    .collect(Collectors.joining(", "))
                            + "} count "
                            + to.getColumnCount()
                            + "vs flink {"
                            + flinkSchema
                            + "} count "
                            + (fieldGetters.size() - partitionKeyCount));
        }
        for (int i = 0; i < (fieldGetters.size() - partitionKeyCount); i++) {
            Object value = fieldGetters.get(i).getFieldOrNull(from);
            to.set(i, value);
        }
    }

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Schema} to get Object from
     * RecordData.
     *
     * <p>This method is a modified version of {@link SchemaUtils#createFieldGetters(Schema)}, which
     * return MaxCompute Java type
     */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(schema.getColumns().size());
        for (int i = 0; i < schema.getColumns().size(); i++) {
            fieldGetters.add(createFieldGetter(schema.getColumns().get(i).getType(), i));
        }
        return fieldGetters;
    }

    /**
     * Creates an accessor for getting elements in an internal RecordData structure at the given
     * position.
     *
     * <p>This method is a modified version of {@link RecordData#createFieldGetter(DataType, int)},
     * which return MaxCompute Java type
     *
     * @param fieldType the element type of the RecordData
     * @param fieldPos the element position of the RecordData
     */
    public static RecordData.FieldGetter createFieldGetter(DataType fieldType, int fieldPos) {
        final RecordData.FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = record -> record.getString(fieldPos).toString();
                break;
            case BOOLEAN:
                fieldGetter = record -> record.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = record -> new Binary(record.getBinary(fieldPos));
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter =
                        record ->
                                record.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                        .toBigDecimal();
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
                fieldGetter = record -> record.getDate(fieldPos).toLocalDate();
                break;
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = record -> record.getTime(fieldPos).toString();
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
                                record.getTimestamp(fieldPos, getPrecision(fieldType))
                                        .toLocalDateTime();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        record ->
                                record.getLocalZonedTimestampData(fieldPos, getPrecision(fieldType))
                                        .toInstant();
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                fieldGetter =
                        record ->
                                record.getZonedTimestamp(fieldPos, getPrecision(fieldType))
                                        .toInstant();
                break;
            case ARRAY:
                fieldGetter =
                        record -> {
                            ArrayData array = record.getArray(fieldPos);
                            DataType elementType = ((ArrayType) fieldType).getElementType();
                            ArrayData.ElementGetter elementGetter =
                                    createElementGetter(elementType);
                            List<Object> listData = new ArrayList<>();
                            for (int j = 0; j < array.size(); j++) {
                                listData.add(elementGetter.getElementOrNull(array, j));
                            }
                            return listData;
                        };
                break;
            case MAP:
                fieldGetter =
                        record -> {
                            MapData map = record.getMap(fieldPos);
                            ArrayData keyArrayData = map.keyArray();
                            ArrayData valueArrayData = map.valueArray();
                            DataType keyType = ((MapType) fieldType).getKeyType();
                            DataType valueType = ((MapType) fieldType).getValueType();
                            ArrayData.ElementGetter keyElementGetter = createElementGetter(keyType);
                            ArrayData.ElementGetter valueElementGetter =
                                    createElementGetter(valueType);
                            Map<Object, Object> mapData = new HashMap<>();
                            for (int j = 0; j < map.size(); j++) {
                                mapData.put(
                                        keyElementGetter.getElementOrNull(keyArrayData, j),
                                        valueElementGetter.getElementOrNull(valueArrayData, j));
                            }
                            return mapData;
                        };
                break;
            case ROW:
                final int rowFieldCount = getFieldCount(fieldType);
                fieldGetter =
                        row -> {
                            RecordData recordData = row.getRow(fieldPos, rowFieldCount);
                            RowType rowType = (RowType) fieldType;
                            StructTypeInfo structTypeInfo = (StructTypeInfo) toMaxCompute(rowType);
                            List<DataType> fieldTypes = rowType.getFieldTypes();
                            List<Object> values = new ArrayList<>();
                            for (int j = 0; j < fieldTypes.size(); j++) {
                                values.add(
                                        createFieldGetter(fieldTypes.get(j), j)
                                                .getFieldOrNull(recordData));
                            }
                            return new SimpleStruct(structTypeInfo, values);
                        };
                break;
            default:
                throw new IllegalArgumentException();
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

    /**
     * Creates an accessor for getting elements in an internal array data structure at the given
     * position.
     *
     * <p>This method is a modified version of {@link ArrayData#createElementGetter(DataType)},
     * which return MaxCompute Java type
     *
     * @param elementType the element type of the array
     */
    private static ArrayData.ElementGetter createElementGetter(DataType elementType) {
        final ArrayData.ElementGetter elementGetter;
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                elementGetter = (array, pos) -> array.getString(pos).toString();
                break;
            case BOOLEAN:
                elementGetter = ArrayData::getBoolean;
                break;
            case BINARY:
            case VARBINARY:
                elementGetter = (array, pos) -> new Binary(array.getBinary(pos));
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                final int decimalScale = getScale(elementType);
                elementGetter =
                        (array, pos) ->
                                array.getDecimal(pos, decimalPrecision, decimalScale)
                                        .toBigDecimal();
                break;
            case TINYINT:
                elementGetter = ArrayData::getByte;
                break;
            case SMALLINT:
                elementGetter = ArrayData::getShort;
                break;
            case INTEGER:
                elementGetter = ArrayData::getInt;
                break;
            case DATE:
                elementGetter = (array, pos) -> LocalDate.ofEpochDay(array.getInt(pos));
                break;
            case TIME_WITHOUT_TIME_ZONE:
                elementGetter =
                        (array, pos) -> LocalTime.ofNanoOfDay(array.getInt(pos) * 1000L).toString();
                break;
            case BIGINT:
                elementGetter = ArrayData::getLong;
                break;
            case FLOAT:
                elementGetter = ArrayData::getFloat;
                break;
            case DOUBLE:
                elementGetter = ArrayData::getDouble;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = getPrecision(elementType);
                elementGetter =
                        (array, pos) ->
                                array.getTimestamp(pos, timestampPrecision).toLocalDateTime();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(elementType);
                elementGetter =
                        (array, pos) ->
                                array.getLocalZonedTimestamp(pos, timestampLtzPrecision)
                                        .toInstant();
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                final int timestampTzPrecision = getPrecision(elementType);
                elementGetter =
                        (array, pos) ->
                                array.getZonedTimestamp(pos, timestampTzPrecision).toInstant();
                break;
            case ARRAY:
                elementGetter =
                        (record, fieldPos) -> {
                            ArrayData array = record.getArray(fieldPos);
                            DataType elementTypeInternal =
                                    ((ArrayType) elementType).getElementType();
                            ArrayData.ElementGetter elementGetterInternal =
                                    createElementGetter(elementTypeInternal);
                            List<Object> listData = new ArrayList<>();
                            for (int j = 0; j < array.size(); j++) {
                                listData.add(elementGetterInternal.getElementOrNull(array, j));
                            }
                            return listData;
                        };
                break;
            case MAP:
                elementGetter =
                        (record, fieldPos) -> {
                            MapData map = record.getMap(fieldPos);
                            ArrayData keyArrayData = map.keyArray();
                            ArrayData valueArrayData = map.valueArray();
                            DataType keyType = ((MapType) elementType).getKeyType();
                            DataType valueType = ((MapType) elementType).getValueType();
                            ArrayData.ElementGetter keyElementGetter = createElementGetter(keyType);
                            ArrayData.ElementGetter valueElementGetter =
                                    createElementGetter(valueType);
                            Map<Object, Object> mapData = new HashMap<>();
                            for (int j = 0; j < map.size(); j++) {
                                mapData.put(
                                        keyElementGetter.getElementOrNull(keyArrayData, j),
                                        valueElementGetter.getElementOrNull(valueArrayData, j));
                            }
                            return mapData;
                        };
                break;
            case ROW:
                final int rowFieldCount = getFieldCount(elementType);
                elementGetter =
                        (array, pos) -> {
                            RecordData recordData = array.getRecord(pos, rowFieldCount);
                            RowType rowType = (RowType) elementType;
                            StructTypeInfo structTypeInfo = (StructTypeInfo) toMaxCompute(rowType);
                            List<DataType> fieldTypes = rowType.getFieldTypes();
                            List<Object> values = new ArrayList<>();
                            for (int j = 0; j < fieldTypes.size(); j++) {
                                values.add(
                                        createFieldGetter(fieldTypes.get(j), j)
                                                .getFieldOrNull(recordData));
                            }
                            return new SimpleStruct(structTypeInfo, values);
                        };
                break;
            default:
                throw new IllegalArgumentException();
        }
        if (!elementType.isNullable()) {
            return elementGetter;
        }
        return (array, pos) -> {
            if (array.isNullAt(pos)) {
                return null;
            }
            return elementGetter.getElementOrNull(array, pos);
        };
    }

    public static OdpsType inferMaxComputeType(Object object) {
        if (object instanceof String) {
            return OdpsType.STRING;
        } else if (object instanceof Boolean) {
            return OdpsType.BOOLEAN;
        } else if (object instanceof Binary) {
            return OdpsType.BINARY;
        } else if (object instanceof BigDecimal) {
            return OdpsType.DECIMAL;
        } else if (object instanceof Byte) {
            return OdpsType.TINYINT;
        } else if (object instanceof Short) {
            return OdpsType.SMALLINT;
        } else if (object instanceof Integer) {
            return OdpsType.INT;
        } else if (object instanceof Long) {
            return OdpsType.BIGINT;
        } else if (object instanceof Float) {
            return OdpsType.FLOAT;
        } else if (object instanceof Double) {
            return OdpsType.DOUBLE;
        } else if (object instanceof LocalDate) {
            return OdpsType.DATE;
        } else if (object instanceof LocalDateTime) {
            return OdpsType.TIMESTAMP_NTZ;
        } else if (object instanceof Instant) {
            return OdpsType.TIMESTAMP;
        } else if (object instanceof List) {
            return OdpsType.ARRAY;
        } else if (object instanceof Map) {
            return OdpsType.MAP;
        } else if (object instanceof Struct) {
            return OdpsType.STRUCT;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + object.getClass());
        }
    }
}
