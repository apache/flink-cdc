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

package org.apache.flink.cdc.common.types;

import org.apache.flink.cdc.common.annotation.PublicEvolving;

import java.util.Arrays;
import java.util.OptionalInt;

/**
 * A {@link DataType} can be used to declare input and/or output types of operations. This class *
 * enumerates all pre-defined data types of Flink CDC.
 *
 * <p>NOTE:
 */
@PublicEvolving
public class DataTypes {

    /**
     * Data type of a fixed-length binary string (=a sequence of bytes) {@code BINARY(n)} where
     * {@code n} is the number of bytes. {@code n} must have a value between 1 and {@link
     * Integer#MAX_VALUE} (both inclusive).
     *
     * @see BinaryType
     */
    public static BinaryType BINARY(int length) {
        return new BinaryType(length);
    }

    /**
     * Data type of a variable-length binary string (=a sequence of bytes) {@code VARBINARY(n)}
     * where {@code n} is the maximum number of bytes. {@code n} must have a value between 1 and
     * {@link Integer#MAX_VALUE} (both inclusive).
     *
     * @see VarBinaryType
     */
    public static VarBinaryType VARBINARY(int n) {
        return new VarBinaryType(n);
    }

    /**
     * Data type of a variable-length binary string (=a sequence of bytes) with defined maximum
     * length. This is a shortcut for {@code VARBINARY(2147483647)} for representing JVM byte
     * arrays.
     *
     * @see VarBinaryType
     */
    public static VarBinaryType BYTES() {
        return VarBinaryType.bytesType();
    }

    /**
     * Data type of a boolean with a (possibly) three-valued logic of {@code TRUE, FALSE, UNKNOWN}.
     *
     * @see BooleanType
     */
    public static BooleanType BOOLEAN() {
        return new BooleanType();
    }

    /**
     * Data type of a 4-byte signed integer with values from -2,147,483,648 to 2,147,483,647.
     *
     * @see IntType
     */
    public static IntType INT() {
        return new IntType();
    }

    /**
     * Data type of a 1-byte signed integer with values from -128 to 127.
     *
     * @see TinyIntType
     */
    public static TinyIntType TINYINT() {
        return new TinyIntType();
    }

    /**
     * Data type of a 2-byte signed integer with values from -32,768 to 32,767.
     *
     * @see SmallIntType
     */
    public static SmallIntType SMALLINT() {
        return new SmallIntType();
    }

    /**
     * Data type of an 8-byte signed integer with values from -9,223,372,036,854,775,808 to
     * 9,223,372,036,854,775,807.
     *
     * @see BigIntType
     */
    public static BigIntType BIGINT() {
        return new BigIntType();
    }

    /**
     * Data type of a 4-byte single precision floating point number.
     *
     * @see FloatType
     */
    public static FloatType FLOAT() {
        return new FloatType();
    }

    /**
     * Data type of an 8-byte double precision floating point number.
     *
     * @see DoubleType
     */
    public static DoubleType DOUBLE() {
        return new DoubleType();
    }

    /**
     * Data type of a fixed-length character string {@code CHAR(n)} where {@code n} is the number of
     * code points. {@code n} must have a value between 1 and {@link Integer#MAX_VALUE} (both
     * inclusive).
     *
     * @see CharType
     */
    public static CharType CHAR(int length) {
        return new CharType(length);
    }

    /**
     * Data type of a variable-length character string {@code VARCHAR(n)} where {@code n} is the
     * maximum number of code points. {@code n} must have a value between 1 and {@link
     * Integer#MAX_VALUE} (both inclusive).
     *
     * @see VarCharType
     */
    public static DataType VARCHAR(int n) {
        return new VarCharType(n);
    }

    /**
     * Data type of a variable-length character string with defined maximum length. This is a
     * shortcut for {@code VARCHAR(2147483647)} for representing JVM strings.
     *
     * @see VarCharType
     */
    public static DataType STRING() {
        return VarCharType.stringType();
    }

    /**
     * Data type of a decimal number with fixed precision and scale {@code DECIMAL(p, s)} where
     * {@code p} is the number of digits in a number (=precision) and {@code s} is the number of
     * digits to the right of the decimal point in a number (=scale). {@code p} must have a value
     * between 1 and 38 (both inclusive). {@code s} must have a value between 0 and {@code p} (both
     * inclusive).
     *
     * @see DecimalType
     */
    public static DecimalType DECIMAL(int precision, int scale) {
        return new DecimalType(precision, scale);
    }

    /**
     * Data type of a date consisting of {@code year-month-day} with values ranging from {@code
     * 0000-01-01} to {@code 9999-12-31}.
     *
     * <p>Compared to the SQL standard, the range starts at year {@code 0000}.
     *
     * @see DataType
     */
    public static DateType DATE() {
        return new DateType();
    }

    /**
     * Data type of a time WITHOUT time zone {@code TIME} with no fractional seconds by default.
     *
     * <p>An instance consists of {@code hour:minute:second} with up to second precision and values
     * ranging from {@code 00:00:00} to {@code 23:59:59}.
     *
     * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as
     * the semantics are closer to {@link java.time.LocalTime}. A time WITH time zone is not
     * provided.
     *
     * @see #TIME(int)
     * @see TimeType
     */
    public static TimeType TIME() {
        return new TimeType();
    }

    /**
     * Data type of a time WITHOUT time zone {@code TIME(p)} where {@code p} is the number of digits
     * of fractional seconds (=precision). {@code p} must have a value between 0 and 9 (both
     * inclusive).
     *
     * <p>An instance consists of {@code hour:minute:second[.fractional]} with up to nanosecond
     * precision and values ranging from {@code 00:00:00.000000000} to {@code 23:59:59.999999999}.
     *
     * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as
     * the semantics are closer to {@link java.time.LocalTime}. A time WITH time zone is not
     * provided.
     *
     * @see #TIME()
     * @see TimeType
     */
    public static TimeType TIME(int precision) {
        return new TimeType(precision);
    }

    /**
     * Data type of a timestamp WITHOUT time zone {@code TIMESTAMP} with 6 digits of fractional
     * seconds by default.
     *
     * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional]} with up to
     * microsecond precision and values ranging from {@code 0000-01-01 00:00:00.000000} to {@code
     * 9999-12-31 23:59:59.999999}.
     *
     * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as
     * the semantics are closer to {@link java.time.LocalDateTime}.
     *
     * @see #TIMESTAMP(int)
     * @see TimestampType
     */
    public static TimestampType TIMESTAMP() {
        return new TimestampType();
    }

    /**
     * Data type of a timestamp WITHOUT time zone {@code TIMESTAMP(p)} where {@code p} is the number
     * of digits of fractional seconds (=precision). {@code p} must have a value between 0 and 9
     * (both inclusive).
     *
     * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional]} with up to
     * nanosecond precision and values ranging from {@code 0000-01-01 00:00:00.000000000} to {@code
     * 9999-12-31 23:59:59.999999999}.
     *
     * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as
     * the semantics are closer to {@link java.time.LocalDateTime}.
     *
     * @see TimestampType
     */
    public static TimestampType TIMESTAMP(int precision) {
        return new TimestampType(precision);
    }

    /**
     * Data type of a timestamp WITH time zone {@code TIMESTAMP WITH TIME ZONE} with 6 digits of
     * fractional seconds by default.
     *
     * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional] zone} with
     * up to microsecond precision and values ranging from {@code 0000-01-01 00:00:00.000000 +14:59}
     * to {@code 9999-12-31 23:59:59.999999 -14:59}.
     *
     * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as
     * the semantics are closer to {@link java.time.OffsetDateTime}.
     *
     * @see #TIMESTAMP()
     * @see #TIMESTAMP_LTZ()
     * @see ZonedTimestampType
     */
    public static ZonedTimestampType TIMESTAMP_TZ() {
        return new ZonedTimestampType();
    }

    /**
     * Data type of a timestamp WITH time zone {@code TIMESTAMP(p) WITH TIME ZONE} where {@code p}
     * is the number of digits of fractional seconds (=precision). {@code p} must have a value
     * between 0 and 9 (both inclusive).
     *
     * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional] zone} with
     * up to nanosecond precision and values ranging from {@code 0000-01-01 00:00:00.000000000
     * +14:59} to {@code 9999-12-31 23:59:59.999999999 -14:59}.
     *
     * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as
     * the semantics are closer to {@link java.time.OffsetDateTime}.
     *
     * @see #TIMESTAMP(int)
     * @see #TIMESTAMP_LTZ(int)
     * @see ZonedTimestampType
     */
    public static ZonedTimestampType TIMESTAMP_TZ(int precision) {
        return new ZonedTimestampType(precision);
    }

    /**
     * Data type of a timestamp WITH LOCAL time zone {@code TIMESTAMP(p) WITH LOCAL TIME ZONE} where
     * {@code p} is the number of digits of fractional seconds (=precision). {@code p} must have a
     * value between 0 and 9 (both inclusive).
     *
     * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional] zone} with
     * up to nanosecond precision and values ranging from {@code 0000-01-01 00:00:00.000000000
     * +14:59} to {@code 9999-12-31 23:59:59.999999999 -14:59}. Leap seconds (23:59:60 and 23:59:61)
     * are not supported as the semantics are closer to {@link java.time.OffsetDateTime}.
     *
     * <p>Compared to {@link ZonedTimestampType}, the time zone offset information is not stored
     * physically in every datum. Instead, the type assumes {@link java.time.Instant} semantics in
     * UTC time zone at the edges of the table ecosystem. Every datum is interpreted in the local
     * time zone configured in the current session for computation and visualization.
     *
     * <p>This type fills the gap between time zone free and time zone mandatory timestamp types by
     * allowing the interpretation of UTC timestamps according to the configured session timezone.
     *
     * @see #TIMESTAMP()
     * @see #TIMESTAMP_TZ()
     * @see LocalZonedTimestampType
     */
    public static LocalZonedTimestampType TIMESTAMP_LTZ() {
        return new LocalZonedTimestampType();
    }

    /**
     * Data type of a timestamp WITH LOCAL time zone {@code TIMESTAMP WITH LOCAL TIME ZONE} with 6
     * digits of fractional seconds by default.
     *
     * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional] zone} with
     * up to microsecond precision and values ranging from {@code 0000-01-01 00:00:00.000000 +14:59}
     * to {@code 9999-12-31 23:59:59.999999 -14:59}. Leap seconds (23:59:60 and 23:59:61) are not
     * supported as the semantics are closer to {@link java.time.OffsetDateTime}.
     *
     * <p>Compared to {@link ZonedTimestampType}, the time zone offset information is not stored
     * physically in every datum. Instead, the type assumes {@link java.time.Instant} semantics in
     * UTC time zone at the edges of the table ecosystem. Every datum is interpreted in the local
     * time zone configured in the current session for computation and visualization.
     *
     * <p>This type fills the gap between time zone free and time zone mandatory timestamp types by
     * allowing the interpretation of UTC timestamps according to the configured session timezone.
     *
     * @see #TIMESTAMP(int)
     * @see #TIMESTAMP_TZ(int)
     * @see LocalZonedTimestampType
     */
    public static LocalZonedTimestampType TIMESTAMP_LTZ(int precision) {
        return new LocalZonedTimestampType(precision);
    }

    /**
     * Data type of an array of elements with same subtype.
     *
     * <p>Compared to the SQL standard, the maximum cardinality of an array cannot be specified but
     * is fixed at {@link Integer#MAX_VALUE}. Also, any valid type is supported as a subtype.
     *
     * <p>Note: Flink CDC currently doesn't support defining nested array in columns.
     *
     * @see ArrayType
     */
    public static ArrayType ARRAY(DataType element) {
        return new ArrayType(element);
    }

    /**
     * Data type of an associative array that maps keys (including {@code NULL}) to values
     * (including {@code NULL}). A map cannot contain duplicate keys; each key can map to at most
     * one value.
     *
     * <p>There is no restriction of key types; it is the responsibility of the user to ensure
     * uniqueness. The map type is an extension to the SQL standard.
     *
     * <p>Note: Flink CDC currently doesn't support defining nested map in columns.
     *
     * @see MapType
     */
    public static MapType MAP(DataType keyType, DataType valueType) {
        return new MapType(keyType, valueType);
    }

    /** Field definition with field name and data type. */
    public static DataField FIELD(String name, DataType type) {
        return new DataField(name, type);
    }

    /** Field definition with field name, data type, and a description. */
    public static DataField FIELD(String name, DataType type, String description) {
        return new DataField(name, type, description);
    }

    /**
     * Data type of a sequence of fields. A field consists of a field name, field type, and an
     * optional description. The most specific type of a row of a table is a row type. In this case,
     * each column of the row corresponds to the field of the row type that has the same ordinal
     * position as the column.
     *
     * <p>Compared to the SQL standard, an optional field description simplifies the handling with
     * complex structures.
     *
     * <p>Use {@link #FIELD(String, DataType)} or {@link #FIELD(String, DataType, String)} to
     * construct fields.
     *
     * <p>Note: Flink CDC currently doesn't support defining nested row in columns.
     *
     * @see RowType
     */
    public static RowType ROW(DataField... fields) {
        return new RowType(Arrays.asList(fields));
    }

    /**
     * Data type of a sequence of fields.
     *
     * <p>This is shortcut for {@link #ROW(DataField...)} where the field names will be generated
     * using {@code f0, f1, f2, ...}.
     *
     * <p>Note: Flink CDC currently doesn't support defining nested record in columns.
     */
    public static RowType ROW(DataType... fieldTypes) {
        return RowType.builder().fields(fieldTypes).build();
    }

    public static OptionalInt getPrecision(DataType dataType) {
        return dataType.accept(PRECISION_EXTRACTOR);
    }

    public static OptionalInt getLength(DataType dataType) {
        return dataType.accept(LENGTH_EXTRACTOR);
    }

    public static OptionalInt getScale(DataType dataType) {
        return dataType.accept(SCALE_EXTRACTOR);
    }

    private static final PrecisionExtractor PRECISION_EXTRACTOR = new PrecisionExtractor();
    private static final ScaleExtractor SCALE_EXTRACTOR = new ScaleExtractor();
    private static final LengthExtractor LENGTH_EXTRACTOR = new LengthExtractor();

    private static class PrecisionExtractor extends DataTypeDefaultVisitor<OptionalInt> {

        @Override
        public OptionalInt visit(DecimalType decimalType) {
            return OptionalInt.of(decimalType.getPrecision());
        }

        @Override
        public OptionalInt visit(TimeType timeType) {
            return OptionalInt.of(timeType.getPrecision());
        }

        @Override
        public OptionalInt visit(TimestampType timestampType) {
            return OptionalInt.of(timestampType.getPrecision());
        }

        @Override
        public OptionalInt visit(LocalZonedTimestampType localZonedTimestampType) {
            return OptionalInt.of(localZonedTimestampType.getPrecision());
        }

        @Override
        public OptionalInt visit(ZonedTimestampType zonedTimestampType) {
            return OptionalInt.of(zonedTimestampType.getPrecision());
        }

        @Override
        protected OptionalInt defaultMethod(DataType dataType) {
            return OptionalInt.empty();
        }
    }

    private static class ScaleExtractor extends DataTypeDefaultVisitor<OptionalInt> {
        @Override
        public OptionalInt visit(DecimalType decimalType) {
            return OptionalInt.of(decimalType.getScale());
        }

        @Override
        protected OptionalInt defaultMethod(DataType dataType) {
            return OptionalInt.empty();
        }
    }

    private static class LengthExtractor extends DataTypeDefaultVisitor<OptionalInt> {

        @Override
        public OptionalInt visit(CharType charType) {
            return OptionalInt.of(charType.getLength());
        }

        @Override
        public OptionalInt visit(VarCharType varCharType) {
            return OptionalInt.of(varCharType.getLength());
        }

        @Override
        public OptionalInt visit(BinaryType binaryType) {
            return OptionalInt.of(binaryType.getLength());
        }

        @Override
        public OptionalInt visit(VarBinaryType varBinaryType) {
            return OptionalInt.of(varBinaryType.getLength());
        }

        @Override
        protected OptionalInt defaultMethod(DataType dataType) {
            return OptionalInt.empty();
        }
    }
}
