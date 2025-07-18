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
import org.apache.flink.cdc.common.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldCount;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/**
 * Class {@code RecordData} describes the data of changed record (i.e. row) in the external system.
 *
 * <p>The mappings from external SQL data types to the internal data structures are listed in the
 * following table:
 *
 * <pre>
 * +--------------------------------+-----------------------------------------+
 * | SQL Data Types                 | Internal Data Structures                |
 * +--------------------------------+-----------------------------------------+
 * | BOOLEAN                        | boolean                                 |
 * +--------------------------------+-----------------------------------------+
 * | CHAR / VARCHAR / STRING        | {@link StringData}                    |
 * +--------------------------------+-----------------------------------------+
 * | BINARY / VARBINARY / BYTES     | byte[]                                  |
 * +--------------------------------+-----------------------------------------+
 * | DECIMAL                        | {@link DecimalData}                         |
 * +--------------------------------+-----------------------------------------+
 * | TINYINT                        | byte                                    |
 * +--------------------------------+-----------------------------------------+
 * | SMALLINT                       | short                                   |
 * +--------------------------------+-----------------------------------------+
 * | INT                            | int                                     |
 * +--------------------------------+-----------------------------------------+
 * | BIGINT                         | long                                    |
 * +--------------------------------+-----------------------------------------+
 * | FLOAT                          | float                                   |
 * +--------------------------------+-----------------------------------------+
 * | DOUBLE                         | double                                  |
 * +--------------------------------+-----------------------------------------+
 * | DATE                           | int (number of days since epoch)        |
 * +--------------------------------+-----------------------------------------+
 * | TIME                           | int (number of milliseconds of the day) |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP                      | {@link TimestampData}                   |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITH LOCAL TIME ZONE | {@link LocalZonedTimestampData}         |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITH TIME ZONE       | {@link ZonedTimestampData}              |
 * +--------------------------------+-----------------------------------------+
 * | ROW                            | {@link RecordData}                      |
 * +--------------------------------+-----------------------------------------+
 * | ARRAY                          | {@link ArrayData}                       |
 * +--------------------------------+-----------------------------------------+
 * | MAP                            | {@link MapData}                         |
 * +--------------------------------+-----------------------------------------+
 * </pre>
 *
 * <p>Nullability is always handled by the container data structure.
 */
@PublicEvolving
public interface RecordData {

    /** Returns the number of fields in this record. */
    int getArity();

    // ------------------------------------------------------------------------------------------
    // Read-only accessor methods
    // ------------------------------------------------------------------------------------------

    /** Returns true if the field is null at the given position. */
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

    /** Returns the binary value at the given position. */
    byte[] getBinary(int pos);

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
     * Returns the zoned timestamp value at the given position.
     *
     * <p>The precision is required to determine whether the zoned timestamp value was stored in a
     * compact representation (see {@link ZonedTimestampData}).
     */
    ZonedTimestampData getZonedTimestamp(int pos, int precision);

    /**
     * Returns the local zoned timestamp value at the given position.
     *
     * <p>The precision is required to determine whether the local zoned timestamp value was stored
     * in a compact representation (see {@link LocalZonedTimestampData}).
     */
    LocalZonedTimestampData getLocalZonedTimestampData(int pos, int precision);

    /** Returns the array value at the given position. */
    ArrayData getArray(int pos);

    /** Returns the map value at the given position. */
    MapData getMap(int pos);

    /**
     * Returns the row value at the given position.
     *
     * <p>The number of fields is required to correctly extract the record.
     */
    RecordData getRow(int pos, int numFields);

    /** Returns the Date data at the given position. */
    DateData getDate(int pos);

    /** Returns the Time data at the given position. */
    TimeData getTime(int pos);

    /**
     * Creates an accessor for getting elements in an internal RecordData structure at the given
     * position.
     *
     * @param fieldType the element type of the RecordData
     * @param fieldPos the element position of the RecordData
     */
    static RecordData.FieldGetter createFieldGetter(DataType fieldType, int fieldPos) {
        final RecordData.FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = record -> record.getString(fieldPos);
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
                fieldGetter = record -> record.getDecimal(fieldPos, decimalPrecision, decimalScale);
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
                fieldGetter = record -> record.getDate(fieldPos);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = record -> record.getTime(fieldPos);
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
                fieldGetter = record -> record.getTimestamp(fieldPos, getPrecision(fieldType));
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        record ->
                                record.getLocalZonedTimestampData(
                                        fieldPos, getPrecision(fieldType));
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                fieldGetter = record -> record.getZonedTimestamp(fieldPos, getPrecision(fieldType));
                break;
            case ARRAY:
                fieldGetter = record -> record.getArray(fieldPos);
                break;
            case MAP:
                fieldGetter = record -> record.getMap(fieldPos);
                break;
            case ROW:
                final int rowFieldCount = getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
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
     * Accessor for getting the field of a RecordData during runtime.
     *
     * @see #createFieldGetter(DataType, int)
     */
    @PublicEvolving
    interface FieldGetter extends Serializable {
        @Nullable
        Object getFieldOrNull(RecordData recordData);
    }
}
