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

package org.apache.flink.cdc.common.converter;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Some shared converters between Java objects and internal objects. None of these functions are
 * null-safe.
 */
public class CommonConverter {

    // ----------------------
    // These are shared converters used for both Internal and Java objects.
    // ----------------------
    static Boolean convertToBoolean(Object obj) {
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to BOOLEAN.");
    }

    static Byte convertToByte(Object obj) {
        if (obj instanceof Byte) {
            return (Byte) obj;
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to TINYINT.");
    }

    static Short convertToShort(Object obj) {
        if (obj instanceof Short) {
            return (Short) obj;
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to SMALLINT.");
    }

    static Integer convertToInt(Object obj) {
        if (obj instanceof Integer) {
            return (Integer) obj;
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to INT.");
    }

    static Long convertToLong(Object obj) {
        if (obj instanceof Long) {
            return (Long) obj;
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to BIGINT.");
    }

    static Float convertToFloat(Object obj) {
        if (obj instanceof Float) {
            return (Float) obj;
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to FLOAT.");
    }

    static Double convertToDouble(Object obj) {
        if (obj instanceof Double) {
            return (Double) obj;
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to DOUBLE.");
    }

    static byte[] convertToBinary(Object obj) {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to BINARY.");
    }

    // ----------------------
    // These are converters to CDC Internal objects.
    // ----------------------

    static StringData convertToStringData(Object obj) {
        if (obj instanceof StringData) {
            return (StringData) obj;
        }
        if (obj instanceof String) {
            return BinaryStringData.fromString((String) obj);
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to STRING DATA.");
    }

    static DecimalData convertToDecimalData(Object obj) {
        if (obj instanceof DecimalData) {
            return (DecimalData) obj;
        }
        if (obj instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) obj;
            return DecimalData.fromBigDecimal(bd, bd.precision(), bd.scale());
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to DECIMAL DATA.");
    }

    static DateData convertToDateData(Object obj) {
        if (obj instanceof DateData) {
            return (DateData) obj;
        }
        if (obj instanceof LocalDate) {
            return DateData.fromLocalDate((LocalDate) obj);
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to DATE DATA.");
    }

    static TimeData convertToTimeData(Object obj) {
        if (obj instanceof TimeData) {
            return (TimeData) obj;
        }
        if (obj instanceof LocalTime) {
            return TimeData.fromLocalTime((LocalTime) obj);
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to TIME DATA.");
    }

    static TimestampData convertToTimestampData(Object obj) {
        if (obj instanceof TimestampData) {
            return (TimestampData) obj;
        }
        if (obj instanceof LocalDateTime) {
            return TimestampData.fromLocalDateTime((LocalDateTime) obj);
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to TIMESTAMP DATA.");
    }

    static ZonedTimestampData convertToZonedTimestampData(Object obj) {
        if (obj instanceof ZonedTimestampData) {
            return (ZonedTimestampData) obj;
        }
        if (obj instanceof ZonedDateTime) {
            return ZonedTimestampData.fromZonedDateTime((ZonedDateTime) obj);
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to TIMESTAMP_TZ DATA.");
    }

    static LocalZonedTimestampData convertToLocalZonedTimestampData(Object obj) {
        if (obj instanceof LocalZonedTimestampData) {
            return (LocalZonedTimestampData) obj;
        }
        if (obj instanceof Instant) {
            return LocalZonedTimestampData.fromInstant((Instant) obj);
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to TIMESTAMP_LTZ DATA.");
    }

    static ArrayData convertToArrayData(Object obj, ArrayType arrayType) {
        if (obj instanceof ArrayData) {
            return (ArrayData) obj;
        }
        if (obj instanceof List) {
            DataType elementType = arrayType.getElementType();
            List<?> objects = (List<?>) obj;
            List<Object> convertedObjects = new ArrayList<>(objects.size());
            for (Object object : objects) {
                convertedObjects.add(
                        InternalObjectConverter.convertToInternal(object, elementType));
            }
            return new GenericArrayData(convertedObjects.toArray());
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to ARRAY DATA.");
    }

    static MapData convertToMapData(Object obj, MapType mapType) {
        if (obj instanceof MapData) {
            return (MapData) obj;
        }
        if (obj instanceof Map) {
            DataType keyType = mapType.getKeyType();
            DataType valueType = mapType.getValueType();
            Map<?, ?> map = (Map<?, ?>) obj;
            Map<Object, Object> convertedMap = new HashMap<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = InternalObjectConverter.convertToInternal(entry.getKey(), keyType);
                Object value =
                        InternalObjectConverter.convertToInternal(entry.getValue(), valueType);
                convertedMap.put(key, value);
            }
            return new GenericMapData(convertedMap);
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to MAP DATA.");
    }

    static RecordData convertToRowData(Object obj, RowType rowType) {
        if (obj instanceof RecordData) {
            return (RecordData) obj;
        }
        if (obj instanceof List) {
            List<DataType> dataTypes = rowType.getFieldTypes();
            List<?> objects = (List<?>) obj;
            List<Object> convertedObjects = new ArrayList<>(objects.size());
            Preconditions.checkArgument(
                    objects.size() == dataTypes.size(),
                    "Cannot convert "
                            + obj
                            + " of type "
                            + obj.getClass()
                            + " with different arity.");
            for (int i = 0; i < objects.size(); i++) {
                convertedObjects.add(
                        InternalObjectConverter.convertToInternal(
                                objects.get(i), dataTypes.get(i)));
            }
            return GenericRecordData.of(convertedObjects.toArray());
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to ROW DATA.");
    }

    // ----------------------
    // These are converters to Java objects.
    // ----------------------

    static String convertToString(Object obj) {
        if (obj instanceof String) {
            return (String) obj;
        }
        if (obj instanceof StringData) {
            return obj.toString();
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to STRING.");
    }

    static BigDecimal convertToBigDecimal(Object obj) {
        if (obj instanceof BigDecimal) {
            return (BigDecimal) obj;
        }
        if (obj instanceof DecimalData) {
            return ((DecimalData) obj).toBigDecimal();
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to DECIMAL.");
    }

    static LocalDate convertToLocalDate(Object obj) {
        if (obj instanceof LocalDate) {
            return (LocalDate) obj;
        }
        if (obj instanceof DateData) {
            return ((DateData) obj).toLocalDate();
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to LOCAL DATE.");
    }

    static LocalTime convertToLocalTime(Object obj) {
        if (obj instanceof LocalTime) {
            return (LocalTime) obj;
        }
        if (obj instanceof TimeData) {
            return ((TimeData) obj).toLocalTime();
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to LOCAL TIME.");
    }

    static LocalDateTime convertToLocalDateTime(Object obj) {
        if (obj instanceof LocalDateTime) {
            return (LocalDateTime) obj;
        }
        if (obj instanceof TimestampData) {
            return ((TimestampData) obj).toLocalDateTime();
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to LOCAL DATETIME.");
    }

    static ZonedDateTime convertToZonedDateTime(Object obj) {
        if (obj instanceof ZonedDateTime) {
            return (ZonedDateTime) obj;
        }
        if (obj instanceof ZonedTimestampData) {
            return ((ZonedTimestampData) obj).getZonedDateTime();
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to ZONED DATETIME.");
    }

    static Instant convertToInstant(Object obj) {
        if (obj instanceof Instant) {
            return (Instant) obj;
        }
        if (obj instanceof LocalZonedTimestampData) {
            return ((LocalZonedTimestampData) obj).toInstant();
        }
        throw new RuntimeException(
                "Cannot convert " + obj + " of type " + obj.getClass() + " to INSTANT.");
    }

    static List<?> convertToList(Object obj, ArrayType arrayType) {
        if (obj instanceof List) {
            return (List<?>) obj;
        }
        if (obj instanceof ArrayData) {
            ArrayData arrayData = (ArrayData) obj;
            DataType elementType = arrayType.getElementType();
            ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
            List<Object> convertedObjects = new ArrayList<>(arrayData.size());
            for (int i = 0; i < arrayData.size(); i++) {
                convertedObjects.add(
                        JavaObjectConverter.convertToJava(
                                elementGetter.getElementOrNull(arrayData, i), elementType));
            }
            return convertedObjects;
        }
        throw new RuntimeException(
                "Cannot convert "
                        + obj
                        + " of type "
                        + obj.getClass()
                        + " to LIST ("
                        + arrayType
                        + ").");
    }

    static Map<?, ?> convertToMap(Object obj, MapType mapType) {
        if (obj instanceof Map) {
            return (Map<?, ?>) obj;
        }
        if (obj instanceof MapData) {
            MapData mapData = (MapData) obj;
            DataType keyType = mapType.getKeyType();
            DataType valueType = mapType.getValueType();
            ArrayData keyArray = mapData.keyArray();
            ArrayData valueArray = mapData.valueArray();
            List<?> keyObjects = convertToList(keyArray, new ArrayType(keyType));
            List<?> valueObjects = convertToList(valueArray, new ArrayType(valueType));
            Map<Object, Object> convertedMap = new HashMap<>(mapData.size());
            for (int i = 0; i < mapData.size(); i++) {
                convertedMap.put(keyObjects.get(i), valueObjects.get(i));
            }
            return convertedMap;
        }
        throw new RuntimeException(
                "Cannot convert "
                        + obj
                        + " of type "
                        + obj.getClass()
                        + " to MAP ("
                        + mapType
                        + ").");
    }

    static List<?> convertToRow(Object obj, RowType rowType) {
        if (obj instanceof List) {
            return (List<?>) obj;
        }
        if (obj instanceof RecordData) {
            RecordData recordData = (RecordData) obj;
            List<DataType> dataTypes = rowType.getFieldTypes();
            List<RecordData.FieldGetter> fieldGetters =
                    SchemaUtils.createFieldGetters(dataTypes.toArray(new DataType[0]));
            List<Object> objects = new ArrayList<>(recordData.getArity());
            for (int i = 0; i < fieldGetters.size(); i++) {
                objects.add(
                        JavaObjectConverter.convertToJava(
                                fieldGetters.get(i).getFieldOrNull(recordData), dataTypes.get(i)));
            }
            return objects;
        }
        throw new RuntimeException(
                "Cannot convert "
                        + obj
                        + " of type "
                        + obj.getClass()
                        + " to ROW ("
                        + rowType
                        + ").");
    }
}
