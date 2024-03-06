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

package org.apache.flink.cdc.common.data;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldCount;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/**
 * Base interface of an internal data structure representing data of {@link ArrayType}.
 *
 * <p>Note: All elements of this data structure must be internal data structures and must be of the
 * same type. See {@link RecordData} for more information about internal data structures.
 *
 * <p>Use {@link GenericArrayData} to construct instances of this interface from regular Java
 * arrays.
 */
@PublicEvolving
public interface ArrayData {

    /** Returns the number of elements in this array. */
    int size();

    // ------------------------------------------------------------------------------------------
    // Read-only accessor methods
    // ------------------------------------------------------------------------------------------

    /** Returns true if the element is null at the given position. */
    boolean isNullAt(int pos);

    /** Returns the boolean value at the given position. */
    boolean getBoolean(int pos);

    /** Returns the byte value at the given position. */
    byte getByte(int pos);

    /** Returns the short value at the given position. */
    short getShort(int pos);

    /** Returns the integer value at the given position. */
    int getInt(int pos);

    /** Returns the long value at the given position. */
    long getLong(int pos);

    /** Returns the float value at the given position. */
    float getFloat(int pos);

    /** Returns the double value at the given position. */
    double getDouble(int pos);

    /** Returns the string value at the given position. */
    StringData getString(int pos);

    /**
     * Returns the decimal value at the given position.
     *
     * <p>The precision and scale are required to determine whether the decimal value was stored in
     * a compact representation (see {@link DecimalData}).
     */
    DecimalData getDecimal(int pos, int precision, int scale);

    /**
     * Returns the timestamp value at the given position.
     *
     * <p>The precision is required to determine whether the timestamp value was stored in a compact
     * representation (see {@link TimestampData}).
     */
    TimestampData getTimestamp(int pos, int precision);

    /**
     * Returns the local zoned timestamp value at the given position.
     *
     * <p>The precision is required to determine whether the timestamp value was stored in a compact
     * representation (see {@link LocalZonedTimestampData}).
     */
    LocalZonedTimestampData getLocalZonedTimestamp(int pos, int precision);

    /**
     * Returns the zoned timestamp value at the given position.
     *
     * <p>The precision is required to determine whether the timestamp value was stored in a compact
     * representation (see {@link ZonedTimestampData}).
     */
    ZonedTimestampData getZonedTimestamp(int pos, int precision);

    /** Returns the binary value at the given position. */
    byte[] getBinary(int pos);

    /** Returns the array value at the given position. */
    ArrayData getArray(int pos);

    /** Returns the map value at the given position. */
    MapData getMap(int pos);

    /**
     * Returns the record value at the given position.
     *
     * <p>The number of fields is required to correctly extract the row.
     */
    RecordData getRecord(int pos, int numFields);

    // ------------------------------------------------------------------------------------------
    // Conversion Utilities
    // ------------------------------------------------------------------------------------------

    boolean[] toBooleanArray();

    byte[] toByteArray();

    short[] toShortArray();

    int[] toIntArray();

    long[] toLongArray();

    float[] toFloatArray();

    double[] toDoubleArray();

    // ------------------------------------------------------------------------------------------
    // Access Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an accessor for getting elements in an internal array data structure at the given
     * position.
     *
     * @param elementType the element type of the array
     */
    static ElementGetter createElementGetter(DataType elementType) {
        final ElementGetter elementGetter;
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                elementGetter = ArrayData::getString;
                break;
            case BOOLEAN:
                elementGetter = ArrayData::getBoolean;
                break;
            case BINARY:
            case VARBINARY:
                elementGetter = ArrayData::getBinary;
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                final int decimalScale = getScale(elementType);
                elementGetter =
                        (array, pos) -> array.getDecimal(pos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                elementGetter = ArrayData::getByte;
                break;
            case SMALLINT:
                elementGetter = ArrayData::getShort;
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                elementGetter = ArrayData::getInt;
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
                elementGetter = (array, pos) -> array.getTimestamp(pos, timestampPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(elementType);
                elementGetter =
                        (array, pos) -> array.getLocalZonedTimestamp(pos, timestampLtzPrecision);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                final int timestampTzPrecision = getPrecision(elementType);
                elementGetter = (array, pos) -> array.getZonedTimestamp(pos, timestampTzPrecision);
                break;
            case ARRAY:
                elementGetter = ArrayData::getArray;
                break;
            case MAP:
                elementGetter = ArrayData::getMap;
                break;
            case ROW:
                final int rowFieldCount = getFieldCount(elementType);
                elementGetter = (array, pos) -> array.getRecord(pos, rowFieldCount);
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

    /**
     * Accessor for getting the elements of an array during runtime.
     *
     * @see #createElementGetter(DataType)
     */
    @PublicEvolving
    interface ElementGetter extends Serializable {
        @Nullable
        Object getElementOrNull(ArrayData array, int pos);
    }
}
