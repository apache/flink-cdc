/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.common.data;

import org.apache.flink.util.StringUtils;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.event.OperationType;
import com.ververica.cdc.common.types.ArrayType;
import com.ververica.cdc.common.types.MapType;
import com.ververica.cdc.common.types.RowType;

import java.io.Serializable;
import java.util.Arrays;

/**
 * An internal data structure representing data of {@link RowType} and other (possibly nested)
 * structured types such as {@link MapType}, {@link ArrayType}.
 *
 * <p>{@link GenericRecordData} is a generic implementation of {@link RecordData} which is backed by
 * an array of Java {@link Object}. A {@link GenericRecordData} can have an arbitrary number of
 * fields of different types. The fields in a row can be accessed by position (0-based) using either
 * the generic {@link #getField(int)} or type-specific getters (such as {@link #getInt(int)}). A
 * field can be updated by the generic {@link #setField(int, Object)}.
 *
 * <p>Note: All fields of this data structure must be internal data structures. See {@link
 * RecordData} for more information about internal data structures.
 *
 * <p>The fields in {@link GenericRecordData} can be null for representing nullability.
 */
@PublicEvolving
public final class GenericRecordData implements RecordData, Serializable {

    /** The array to store the actual internal format values. */
    private final Object[] fields;

    /**
     * Creates an instance of {@link GenericRecordData} with given number of fields.
     *
     * <p>Initially, all fields are set to null. By default, the record describes a {@link
     * OperationType#INSERT} in a changelog.
     *
     * <p>Note: All fields of the record must be internal data structures.
     *
     * @param arity number of fields
     */
    public GenericRecordData(int arity) {
        this.fields = new Object[arity];
    }

    /**
     * Sets the field value at the given position.
     *
     * <p>Note: The given field value must be an internal data structures. Otherwise the {@link
     * GenericRecordData} is corrupted and may throw exception when processing. See {@link
     * RecordData} for more information about internal data structures.
     *
     * <p>The field value can be null for representing nullability.
     */
    public void setField(int pos, Object value) {
        this.fields[pos] = value;
    }

    /**
     * Returns the field value at the given position.
     *
     * <p>Note: The returned value is in internal data structure. See {@link RecordData} for more
     * information about internal data structures.
     *
     * <p>The returned field value can be null for representing nullability.
     */
    public Object getField(int pos) {
        return this.fields[pos];
    }

    @Override
    public int getArity() {
        return fields.length;
    }

    @Override
    public boolean isNullAt(int pos) {
        return this.fields[pos] == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return (boolean) this.fields[pos];
    }

    @Override
    public byte getByte(int pos) {
        return (byte) this.fields[pos];
    }

    @Override
    public short getShort(int pos) {
        return (short) this.fields[pos];
    }

    @Override
    public int getInt(int pos) {
        return (int) this.fields[pos];
    }

    @Override
    public long getLong(int pos) {
        return (long) this.fields[pos];
    }

    @Override
    public float getFloat(int pos) {
        return (float) this.fields[pos];
    }

    @Override
    public double getDouble(int pos) {
        return (double) this.fields[pos];
    }

    @Override
    public byte[] getBinary(int pos) {
        return (byte[]) this.fields[pos];
    }

    @Override
    public StringData getString(int pos) {
        return (StringData) this.fields[pos];
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return (DecimalData) this.fields[pos];
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return (TimestampData) this.fields[pos];
    }

    @Override
    public ZonedTimestampData getZonedTimestamp(int pos, int precision) {
        return (ZonedTimestampData) this.fields[pos];
    }

    @Override
    public LocalZonedTimestampData getLocalZonedTimestampData(int pos, int precision) {
        return (LocalZonedTimestampData) this.fields[pos];
    }

    @Override
    public ArrayData getArray(int pos) {
        return (ArrayData) this.fields[pos];
    }

    @Override
    public MapData getMap(int pos) {
        return (MapData) this.fields[pos];
    }

    @Override
    public RecordData getRow(int pos, int numFields) {
        return (RecordData) this.fields[pos];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GenericRecordData)) {
            return false;
        }
        GenericRecordData that = (GenericRecordData) o;
        return Arrays.deepEquals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(fields);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int i = 0; i < fields.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(StringUtils.arrayAwareToString(fields[i]));
        }
        sb.append(")");
        return sb.toString();
    }

    // ------------------------------------------------------------------------------------------
    // Constructor Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an instance of {@link GenericRecordData} with given field values.
     *
     * <p>Note: All fields of the record must be internal data structures.
     */
    public static GenericRecordData of(Object... values) {
        GenericRecordData record = new GenericRecordData(values.length);

        for (int i = 0; i < values.length; ++i) {
            record.setField(i, values[i]);
        }

        return record;
    }
}
