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

package org.apache.flink.cdc.common.data.binary;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import java.lang.reflect.Array;

import static org.apache.flink.core.memory.MemoryUtils.UNSAFE;

/**
 * A binary implementation of {@link ArrayData} which is backed by {@link MemorySegment}s.
 *
 * <p>This class provides a way to store array data in a binary format that is compact and
 * efficient. It uses {@link MemorySegment}s to manage the binary representation of the data,
 * allowing for efficient storage and access.
 *
 * <p>The binary layout of {@link BinaryArrayData} is structured as follows:
 *
 * <pre>
 * [size(int)] + [null bits(4-byte word boundaries)] + [values or offset&length] + [variable length part].
 * </pre>
 *
 * <ul>
 *   <li><b>size:</b> The first 4 bytes store the number of elements in the array.
 *   <li><b>null bits:</b> A bitmap to track null values, aligned to 4-byte word boundaries. Each
 *       bit represents whether an element is null.
 *   <li><b>values or offset&length:</b> The values of the array elements. For fixed-length
 *       primitive types, the values are stored directly. For variable-length types (e.g., strings,
 *       maps), this part stores the offset and length of the actual data in the variable length
 *       part.
 *   <li><b>variable length part:</b> This part of the memory segment stores the actual data for
 *       variable-length types (e.g., strings, maps).
 * </ul>
 *
 * <p>The header size is calculated based on the number of elements in the array, ensuring efficient
 * alignment and access.
 *
 * <p>For fields that hold fixed-length primitive types, such as long, double, or int, they are
 * stored compactly in bytes, just like the original Java array.
 *
 * <p>The class also provides methods to convert the binary data back into Java primitive arrays,
 * handling various types such as boolean, byte, short, int, long, float, and double.
 */
public final class BinaryArrayData extends BinarySection implements ArrayData {

    /** Offset for Arrays. */
    private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private static final int BOOLEAN_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(boolean[].class);
    private static final int SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
    private static final int INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
    private static final int LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
    private static final int FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
    private static final int DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);

    /**
     * Calculates the size of the header in bytes for an array with the specified number of fields.
     *
     * <p>The header consists of:
     *
     * <ul>
     *   <li>4 bytes to store the size of the array (number of elements).
     *   <li>A bitmap to track null values, where each bit represents whether an element is null.
     *       This bitmap is aligned to 4-byte word boundaries for efficient memory access and to
     *       facilitate the use of bitwise operations.
     * </ul>
     *
     * <p>The size of the bitmap is determined by the number of elements in the array:
     *
     * <ul>
     *   <li>Each element requires 1 bit in the bitmap.
     *   <li>The total number of bits is rounded up to the nearest multiple of 32 to ensure
     *       alignment to 4-byte word boundaries (i.e., a 32-bit integer).
     * </ul>
     *
     * <p>The formula for calculating the size of the header is:
     *
     * <pre>
     *   header size = 4 bytes (for array size) + ((numFields + 31) / 32) * 4 bytes (for null bitmap)
     * </pre>
     *
     * @param numFields the number of elements in the array
     * @return the size of the header in bytes
     */
    public static int calculateHeaderInBytes(int numFields) {
        return 4 + ((numFields + 31) / 32) * 4;
    }

    /**
     * It store real value when type is primitive. It store the length and offset of variable-length
     * part when type is string, map, etc.
     */
    public static int calculateFixLengthPartSize(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                return 1;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case BIGINT:
            case DOUBLE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case ARRAY:
            case MAP:
            case ROW:
                // long and double are 8 bytes;
                // otherwise it stores the length and offset of the variable-length part for types
                // such as is string, map, etc.
                return 8;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case SMALLINT:
                return 2;
            case INTEGER:
            case FLOAT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return 4;
            default:
                throw new IllegalArgumentException();
        }
    }

    // The number of elements in this array
    private int size;

    /** The position to start storing array elements. */
    private int elementOffset;

    public BinaryArrayData() {}

    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < size : "index (" + index + ") should < " + size;
    }

    private int getElementOffset(int ordinal, int elementSize) {
        return elementOffset + ordinal * elementSize;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        // Read the number of elements from the first 4 bytes.
        final int size = BinarySegmentUtils.getInt(segments, offset);
        assert size >= 0 : "size (" + size + ") should >= 0";

        this.size = size;
        super.pointTo(segments, offset, sizeInBytes);
        this.elementOffset = offset + calculateHeaderInBytes(this.size);
    }

    @Override
    public boolean isNullAt(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.bitGet(segments, offset + 4, pos);
    }

    public void setNullAt(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
    }

    public void setNotNullAt(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitUnSet(segments, offset + 4, pos);
    }

    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getLong(segments, getElementOffset(pos, 8));
    }

    public void setLong(int pos, long value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setLong(segments, getElementOffset(pos, 8), value);
    }

    public void setNullLong(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setLong(segments, getElementOffset(pos, 8), 0L);
    }

    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getInt(segments, getElementOffset(pos, 4));
    }

    public void setInt(int pos, int value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setInt(segments, getElementOffset(pos, 4), value);
    }

    public void setNullInt(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setInt(segments, getElementOffset(pos, 4), 0);
    }

    @Override
    public StringData getString(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readStringData(segments, offset, fieldOffset, offsetAndSize);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        assertIndexIsValid(pos);
        if (DecimalData.isCompact(precision)) {
            return DecimalData.fromUnscaledLong(
                    BinarySegmentUtils.getLong(segments, getElementOffset(pos, 8)),
                    precision,
                    scale);
        }

        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readDecimalData(
                segments, offset, offsetAndSize, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        assertIndexIsValid(pos);

        if (TimestampData.isCompact(precision)) {
            return TimestampData.fromMillis(
                    BinarySegmentUtils.getLong(segments, getElementOffset(pos, 8)));
        }

        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndNanoOfMilli = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readTimestampData(segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public LocalZonedTimestampData getLocalZonedTimestamp(int pos, int precision) {
        throw new UnsupportedOperationException("Not support LocalZonedTimestampData");
    }

    @Override
    public ZonedTimestampData getZonedTimestamp(int pos, int precision) {
        throw new UnsupportedOperationException("Not support ZonedTimestampData");
    }

    @Override
    public byte[] getBinary(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndSize);
    }

    @Override
    public ArrayData getArray(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.readArrayData(segments, offset, getLong(pos));
    }

    @Override
    public MapData getMap(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.readMapData(segments, offset, getLong(pos));
    }

    @Override
    public RecordData getRecord(int pos, int numFields) {
        assertIndexIsValid(pos);
        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readRecordData(segments, numFields, offset, offsetAndSize);
    }

    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getBoolean(segments, getElementOffset(pos, 1));
    }

    public void setBoolean(int pos, boolean value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setBoolean(segments, getElementOffset(pos, 1), value);
    }

    public void setNullBoolean(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setBoolean(segments, getElementOffset(pos, 1), false);
    }

    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getByte(segments, getElementOffset(pos, 1));
    }

    public void setByte(int pos, byte value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setByte(segments, getElementOffset(pos, 1), value);
    }

    public void setNullByte(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setByte(segments, getElementOffset(pos, 1), (byte) 0);
    }

    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getShort(segments, getElementOffset(pos, 2));
    }

    public void setShort(int pos, short value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setShort(segments, getElementOffset(pos, 2), value);
    }

    public void setNullShort(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setShort(segments, getElementOffset(pos, 2), (short) 0);
    }

    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getFloat(segments, getElementOffset(pos, 4));
    }

    public void setFloat(int pos, float value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setFloat(segments, getElementOffset(pos, 4), value);
    }

    public void setNullFloat(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setFloat(segments, getElementOffset(pos, 4), 0F);
    }

    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getDouble(segments, getElementOffset(pos, 8));
    }

    public void setDouble(int pos, double value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setDouble(segments, getElementOffset(pos, 8), value);
    }

    public void setNullDouble(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setDouble(segments, getElementOffset(pos, 8), 0.0);
    }

    public void setDecimal(int pos, DecimalData value, int precision) {
        assertIndexIsValid(pos);

        if (DecimalData.isCompact(precision)) {
            // compact format
            setLong(pos, value.toUnscaledLong());
        } else {
            int fieldOffset = getElementOffset(pos, 8);
            int cursor = (int) (BinarySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;
            // zero-out the bytes
            BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
            BinarySegmentUtils.setLong(segments, offset + cursor + 8, 0L);

            if (value == null) {
                setNullAt(pos);
                // keep the offset for future update
                BinarySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {

                byte[] bytes = value.toUnscaledBytes();
                assert (bytes.length <= 16);

                // Write the bytes to the variable length portion.
                BinarySegmentUtils.copyFromBytes(segments, offset + cursor, bytes, 0, bytes.length);
                setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
            }
        }
    }

    public void setTimestamp(int pos, TimestampData value, int precision) {
        assertIndexIsValid(pos);

        if (TimestampData.isCompact(precision)) {
            setLong(pos, value.getMillisecond());
        } else {
            int fieldOffset = getElementOffset(pos, 8);
            int cursor = (int) (BinarySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // zero-out the bytes
                BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
                // keep the offset for future update
                BinarySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {
                // write millisecond to the variable length portion.
                BinarySegmentUtils.setLong(segments, offset + cursor, value.getMillisecond());
                // write nanoOfMillisecond to the fixed-length portion.
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    public boolean anyNull() {
        for (int i = offset + 4; i < elementOffset; i += 4) {
            if (BinarySegmentUtils.getInt(segments, i) != 0) {
                return true;
            }
        }
        return false;
    }

    private void checkNoNull() {
        if (anyNull()) {
            throw new RuntimeException("Primitive array must not contain a null value.");
        }
    }

    @Override
    public boolean[] toBooleanArray() {
        checkNoNull();
        boolean[] values = new boolean[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, BOOLEAN_ARRAY_OFFSET, size);
        return values;
    }

    @Override
    public byte[] toByteArray() {
        checkNoNull();
        byte[] values = new byte[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, BYTE_ARRAY_BASE_OFFSET, size);
        return values;
    }

    @Override
    public short[] toShortArray() {
        checkNoNull();
        short[] values = new short[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, SHORT_ARRAY_OFFSET, size * 2);
        return values;
    }

    @Override
    public int[] toIntArray() {
        checkNoNull();
        int[] values = new int[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, INT_ARRAY_OFFSET, size * 4);
        return values;
    }

    @Override
    public long[] toLongArray() {
        checkNoNull();
        long[] values = new long[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, LONG_ARRAY_OFFSET, size * 8);
        return values;
    }

    @Override
    public float[] toFloatArray() {
        checkNoNull();
        float[] values = new float[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, FLOAT_ARRAY_OFFSET, size * 4);
        return values;
    }

    @Override
    public double[] toDoubleArray() {
        checkNoNull();
        double[] values = new double[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, DOUBLE_ARRAY_OFFSET, size * 8);
        return values;
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toObjectArray(DataType elementType) {
        Class<T> elementClass = (Class<T>) DataTypeUtils.toInternalConversionClass(elementType);
        ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        T[] values = (T[]) Array.newInstance(elementClass, size);
        for (int i = 0; i < size; i++) {
            if (!isNullAt(i)) {
                values[i] = (T) elementGetter.getElementOrNull(this, i);
            }
        }
        return values;
    }

    public BinaryArrayData copy() {
        return copy(new BinaryArrayData());
    }

    public BinaryArrayData copy(BinaryArrayData reuse) {
        byte[] bytes = BinarySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    @Override
    public int hashCode() {
        return BinarySegmentUtils.hashByWords(segments, offset, sizeInBytes);
    }

    // ------------------------------------------------------------------------------------------
    // Construction Utilities
    // ------------------------------------------------------------------------------------------

    public static BinaryArrayData fromPrimitiveArray(boolean[] arr) {
        return fromPrimitiveArray(arr, BOOLEAN_ARRAY_OFFSET, arr.length, 1);
    }

    public static BinaryArrayData fromPrimitiveArray(byte[] arr) {
        return fromPrimitiveArray(arr, BYTE_ARRAY_BASE_OFFSET, arr.length, 1);
    }

    public static BinaryArrayData fromPrimitiveArray(short[] arr) {
        return fromPrimitiveArray(arr, SHORT_ARRAY_OFFSET, arr.length, 2);
    }

    public static BinaryArrayData fromPrimitiveArray(int[] arr) {
        return fromPrimitiveArray(arr, INT_ARRAY_OFFSET, arr.length, 4);
    }

    public static BinaryArrayData fromPrimitiveArray(long[] arr) {
        return fromPrimitiveArray(arr, LONG_ARRAY_OFFSET, arr.length, 8);
    }

    public static BinaryArrayData fromPrimitiveArray(float[] arr) {
        return fromPrimitiveArray(arr, FLOAT_ARRAY_OFFSET, arr.length, 4);
    }

    public static BinaryArrayData fromPrimitiveArray(double[] arr) {
        return fromPrimitiveArray(arr, DOUBLE_ARRAY_OFFSET, arr.length, 8);
    }

    private static BinaryArrayData fromPrimitiveArray(
            Object arr, int offset, int length, int elementSize) {
        final long headerInBytes = calculateHeaderInBytes(length);
        final long valueRegionInBytes = elementSize * length;

        // must align by 8 bytes
        long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
        if (totalSizeInLongs > Integer.MAX_VALUE / 8) {
            throw new UnsupportedOperationException(
                    "Cannot convert this array to unsafe format as " + "it's too big.");
        }
        long totalSize = totalSizeInLongs * 8;

        final byte[] data = new byte[(int) totalSize];

        UNSAFE.putInt(data, (long) BYTE_ARRAY_BASE_OFFSET, length);
        UNSAFE.copyMemory(
                arr, offset, data, BYTE_ARRAY_BASE_OFFSET + headerInBytes, valueRegionInBytes);

        BinaryArrayData result = new BinaryArrayData();
        result.pointTo(MemorySegmentFactory.wrap(data), 0, (int) totalSize);
        return result;
    }
}
