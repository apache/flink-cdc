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

package org.apache.flink.cdc.runtime.serializer.data;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.types.variant.BinaryVariant;
import org.apache.flink.cdc.common.types.variant.Variant;
import org.apache.flink.cdc.runtime.serializer.data.binary.BinaryRecordDataSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Serializer for {@link GenericRecordData}. Uses a self-describing format where each field is
 * prefixed with a type tag, so no schema information is needed at serialization time.
 */
@Internal
public class GenericRecordDataSerializer {

    // Type tags for self-describing format
    static final byte TAG_NULL = 0;
    static final byte TAG_BOOLEAN = 1;
    static final byte TAG_BYTE = 2;
    static final byte TAG_SHORT = 3;
    static final byte TAG_INT = 4;
    static final byte TAG_LONG = 5;
    static final byte TAG_FLOAT = 6;
    static final byte TAG_DOUBLE = 7;
    static final byte TAG_STRING = 8;
    static final byte TAG_BINARY = 9;
    static final byte TAG_DECIMAL = 10;
    static final byte TAG_TIMESTAMP = 11;
    static final byte TAG_ZONED_TIMESTAMP = 12;
    static final byte TAG_LOCAL_ZONED_TIMESTAMP = 13;
    static final byte TAG_DATE = 14;
    static final byte TAG_TIME = 15;
    static final byte TAG_GENERIC_RECORD = 16;
    static final byte TAG_BINARY_RECORD = 17;
    static final byte TAG_ARRAY = 18;
    static final byte TAG_MAP = 19;
    static final byte TAG_VARIANT = 20;

    private GenericRecordDataSerializer() {}

    /** Serializes a {@link GenericRecordData} to the given output view. */
    public static void serialize(GenericRecordData record, DataOutputView target)
            throws IOException {
        int arity = record.getArity();
        target.writeInt(arity);
        for (int i = 0; i < arity; i++) {
            serializeField(record.getField(i), target);
        }
    }

    /** Deserializes a {@link GenericRecordData} from the given input view. */
    public static GenericRecordData deserialize(DataInputView source) throws IOException {
        int arity = source.readInt();
        GenericRecordData record = new GenericRecordData(arity);
        for (int i = 0; i < arity; i++) {
            record.setField(i, deserializeField(source));
        }
        return record;
    }

    /** Creates a deep copy of the given {@link GenericRecordData}. */
    public static GenericRecordData copy(GenericRecordData from) {
        int arity = from.getArity();
        GenericRecordData copy = new GenericRecordData(arity);
        for (int i = 0; i < arity; i++) {
            copy.setField(i, copyField(from.getField(i)));
        }
        return copy;
    }

    // ---- Field-level serialization ----

    static void serializeField(Object field, DataOutputView target) throws IOException {
        if (field == null) {
            target.writeByte(TAG_NULL);
        } else if (field instanceof Boolean) {
            target.writeByte(TAG_BOOLEAN);
            target.writeBoolean((Boolean) field);
        } else if (field instanceof Byte) {
            target.writeByte(TAG_BYTE);
            target.writeByte((Byte) field);
        } else if (field instanceof Short) {
            target.writeByte(TAG_SHORT);
            target.writeShort((Short) field);
        } else if (field instanceof Integer) {
            target.writeByte(TAG_INT);
            target.writeInt((Integer) field);
        } else if (field instanceof Long) {
            target.writeByte(TAG_LONG);
            target.writeLong((Long) field);
        } else if (field instanceof Float) {
            target.writeByte(TAG_FLOAT);
            target.writeFloat((Float) field);
        } else if (field instanceof Double) {
            target.writeByte(TAG_DOUBLE);
            target.writeDouble((Double) field);
        } else if (field instanceof StringData) {
            target.writeByte(TAG_STRING);
            byte[] bytes = ((StringData) field).toBytes();
            target.writeInt(bytes.length);
            target.write(bytes);
        } else if (field instanceof byte[]) {
            target.writeByte(TAG_BINARY);
            byte[] bytes = (byte[]) field;
            target.writeInt(bytes.length);
            target.write(bytes);
        } else if (field instanceof DecimalData) {
            target.writeByte(TAG_DECIMAL);
            DecimalData decimal = (DecimalData) field;
            // Use DecimalData's precision/scale (SQL DECIMAL(p,s)) instead of BigDecimal's
            target.writeInt(decimal.precision());
            target.writeInt(decimal.scale());
            byte[] unscaled = decimal.toUnscaledBytes();
            target.writeInt(unscaled.length);
            target.write(unscaled);
        } else if (field instanceof TimestampData) {
            target.writeByte(TAG_TIMESTAMP);
            TimestampData ts = (TimestampData) field;
            target.writeLong(ts.getMillisecond());
            target.writeInt(ts.getNanoOfMillisecond());
        } else if (field instanceof ZonedTimestampData) {
            target.writeByte(TAG_ZONED_TIMESTAMP);
            ZonedTimestampData zts = (ZonedTimestampData) field;
            target.writeLong(zts.getMillisecond());
            target.writeInt(zts.getNanoOfMillisecond());
            byte[] zoneBytes = zts.getZoneId().getBytes(StandardCharsets.UTF_8);
            target.writeInt(zoneBytes.length);
            target.write(zoneBytes);
        } else if (field instanceof LocalZonedTimestampData) {
            target.writeByte(TAG_LOCAL_ZONED_TIMESTAMP);
            LocalZonedTimestampData lzts = (LocalZonedTimestampData) field;
            target.writeLong(lzts.getEpochMillisecond());
            target.writeInt(lzts.getEpochNanoOfMillisecond());
        } else if (field instanceof DateData) {
            target.writeByte(TAG_DATE);
            target.writeInt(((DateData) field).toEpochDay());
        } else if (field instanceof TimeData) {
            target.writeByte(TAG_TIME);
            target.writeInt(((TimeData) field).toMillisOfDay());
        } else if (field instanceof GenericRecordData) {
            target.writeByte(TAG_GENERIC_RECORD);
            serialize((GenericRecordData) field, target);
        } else if (field instanceof BinaryRecordData) {
            target.writeByte(TAG_BINARY_RECORD);
            BinaryRecordDataSerializer.INSTANCE.serialize((BinaryRecordData) field, target);
        } else if (field instanceof ArrayData) {
            target.writeByte(TAG_ARRAY);
            serializeArrayData((ArrayData) field, target);
        } else if (field instanceof MapData) {
            target.writeByte(TAG_MAP);
            serializeMapData((MapData) field, target);
        } else if (field instanceof Variant) {
            target.writeByte(TAG_VARIANT);
            serializeVariant((Variant) field, target);
        } else {
            throw new IOException(
                    "Unsupported field type in GenericRecordData: " + field.getClass().getName());
        }
    }

    static Object deserializeField(DataInputView source) throws IOException {
        byte tag = source.readByte();
        switch (tag) {
            case TAG_NULL:
                return null;
            case TAG_BOOLEAN:
                return source.readBoolean();
            case TAG_BYTE:
                return source.readByte();
            case TAG_SHORT:
                return source.readShort();
            case TAG_INT:
                return source.readInt();
            case TAG_LONG:
                return source.readLong();
            case TAG_FLOAT:
                return source.readFloat();
            case TAG_DOUBLE:
                return source.readDouble();
            case TAG_STRING:
                {
                    int len = source.readInt();
                    byte[] bytes = new byte[len];
                    source.readFully(bytes);
                    return BinaryStringData.fromBytes(bytes);
                }
            case TAG_BINARY:
                {
                    int len = source.readInt();
                    byte[] bytes = new byte[len];
                    source.readFully(bytes);
                    return bytes;
                }
            case TAG_DECIMAL:
                {
                    int precision = source.readInt();
                    int scale = source.readInt();
                    int len = source.readInt();
                    byte[] unscaled = new byte[len];
                    source.readFully(unscaled);
                    return DecimalData.fromUnscaledBytes(unscaled, precision, scale);
                }
            case TAG_TIMESTAMP:
                return TimestampData.fromMillis(source.readLong(), source.readInt());
            case TAG_ZONED_TIMESTAMP:
                {
                    long millis = source.readLong();
                    int nanos = source.readInt();
                    int zoneLen = source.readInt();
                    byte[] zoneBytes = new byte[zoneLen];
                    source.readFully(zoneBytes);
                    return ZonedTimestampData.of(
                            millis, nanos, new String(zoneBytes, StandardCharsets.UTF_8));
                }
            case TAG_LOCAL_ZONED_TIMESTAMP:
                return LocalZonedTimestampData.fromEpochMillis(source.readLong(), source.readInt());
            case TAG_DATE:
                return DateData.fromEpochDay(source.readInt());
            case TAG_TIME:
                return TimeData.fromMillisOfDay(source.readInt());
            case TAG_GENERIC_RECORD:
                return deserialize(source);
            case TAG_BINARY_RECORD:
                return BinaryRecordDataSerializer.INSTANCE.deserialize(source);
            case TAG_ARRAY:
                return deserializeArrayData(source);
            case TAG_MAP:
                return deserializeMapData(source);
            case TAG_VARIANT:
                return deserializeVariant(source);
            default:
                throw new IOException("Unknown field type tag: " + tag);
        }
    }

    // ---- ArrayData serialization ----

    private static void serializeArrayData(ArrayData arrayData, DataOutputView target)
            throws IOException {
        if (arrayData instanceof GenericArrayData) {
            Object[] elements = ((GenericArrayData) arrayData).toObjectArray();
            target.writeInt(elements.length);
            for (Object element : elements) {
                serializeField(element, target);
            }
        } else {
            throw new IOException(
                    "Serialization of non-generic ArrayData is not supported in GenericRecordDataSerializer. "
                            + "Actual type: "
                            + arrayData.getClass().getName());
        }
    }

    private static ArrayData deserializeArrayData(DataInputView source) throws IOException {
        int size = source.readInt();
        Object[] elements = new Object[size];
        for (int i = 0; i < size; i++) {
            elements[i] = deserializeField(source);
        }
        return new GenericArrayData(elements);
    }

    // ---- MapData serialization ----

    private static void serializeMapData(MapData mapData, DataOutputView target)
            throws IOException {
        if (mapData instanceof GenericMapData) {
            ArrayData keyArray = mapData.keyArray();
            ArrayData valueArray = mapData.valueArray();
            if (!(keyArray instanceof GenericArrayData)
                    || !(valueArray instanceof GenericArrayData)) {
                throw new IOException(
                        "MapData with non-generic key/value arrays is not supported in GenericRecordDataSerializer.");
            }
            int size = mapData.size();
            target.writeInt(size);
            Object[] keys = ((GenericArrayData) keyArray).toObjectArray();
            Object[] values = ((GenericArrayData) valueArray).toObjectArray();
            for (int i = 0; i < size; i++) {
                serializeField(keys[i], target);
                serializeField(values[i], target);
            }
        } else {
            throw new IOException(
                    "Serialization of non-generic MapData is not supported in GenericRecordDataSerializer. "
                            + "Actual type: "
                            + mapData.getClass().getName());
        }
    }

    private static MapData deserializeMapData(DataInputView source) throws IOException {
        int size = source.readInt();
        Map<Object, Object> map = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++) {
            Object key = deserializeField(source);
            Object value = deserializeField(source);
            map.put(key, value);
        }
        return new GenericMapData(map);
    }

    // ---- Variant serialization ----

    private static void serializeVariant(Variant variant, DataOutputView target)
            throws IOException {
        if (variant instanceof BinaryVariant) {
            BinaryVariant bv = (BinaryVariant) variant;
            byte[] value = bv.getValue();
            byte[] metadata = bv.getMetadata();
            target.writeInt(value.length);
            target.write(value);
            target.writeInt(metadata.length);
            target.write(metadata);
        } else {
            throw new IOException("Unsupported Variant type: " + variant.getClass().getName());
        }
    }

    private static Variant deserializeVariant(DataInputView source) throws IOException {
        int valueLen = source.readInt();
        byte[] value = new byte[valueLen];
        source.readFully(value);
        int metadataLen = source.readInt();
        byte[] metadata = new byte[metadataLen];
        source.readFully(metadata);
        return new BinaryVariant(value, metadata);
    }

    // ---- Field copy ----

    private static Object copyField(Object field) {
        if (field == null) {
            return null;
        }
        // Most CDC internal data types are immutable, so shallow copy is safe
        if (field instanceof byte[]) {
            return ((byte[]) field).clone();
        } else if (field instanceof GenericRecordData) {
            return copy((GenericRecordData) field);
        } else if (field instanceof BinaryRecordData) {
            return ((BinaryRecordData) field).copy();
        } else if (field instanceof GenericArrayData) {
            Object[] elements = ((GenericArrayData) field).toObjectArray();
            Object[] copied = new Object[elements.length];
            for (int i = 0; i < elements.length; i++) {
                copied[i] = copyField(elements[i]);
            }
            return new GenericArrayData(copied);
        } else if (field instanceof GenericMapData) {
            GenericMapData mapData = (GenericMapData) field;
            ArrayData keyArray = mapData.keyArray();
            ArrayData valueArray = mapData.valueArray();
            if (!(keyArray instanceof GenericArrayData)
                    || !(valueArray instanceof GenericArrayData)) {
                throw new IllegalArgumentException(
                        "Expected GenericArrayData for key and value arrays in GenericMapData, but got: keyArray="
                                + keyArray.getClass().getName()
                                + ", valueArray="
                                + valueArray.getClass().getName());
            }
            Object[] keys = ((GenericArrayData) keyArray).toObjectArray();
            Object[] values = ((GenericArrayData) valueArray).toObjectArray();
            Map<Object, Object> newMap = new LinkedHashMap<>(keys.length);
            for (int i = 0; i < keys.length; i++) {
                newMap.put(copyField(keys[i]), copyField(values[i]));
            }
            return new GenericMapData(newMap);
        }
        // Immutable types: Boolean, Byte, Short, Integer, Long, Float, Double,
        // StringData, DecimalData, TimestampData, ZonedTimestampData,
        // LocalZonedTimestampData, DateData, TimeData, Variant
        return field;
    }
}
