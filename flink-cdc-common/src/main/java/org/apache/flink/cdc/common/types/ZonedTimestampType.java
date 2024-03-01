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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Data type of a timestamp WITH time zone consisting of {@code year-month-day
 * hour:minute:second[.fractional] zone} with up to nanosecond precision and values ranging from
 * {@code 0000-01-01 00:00:00.000000000 +14:59} to {@code 9999-12-31 23:59:59.999999999 -14:59}.
 * Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as the
 * semantics are closer to {@link java.time.OffsetDateTime}.
 *
 * <p>The serialized string representation is {@code TIMESTAMP(p) WITH TIME ZONE} where {@code p} is
 * the number of digits of fractional seconds (=precision). {@code p} must have a value between 0
 * and 9 (both inclusive). If no precision is specified, {@code p} is equal to 6.
 *
 * <p>Compared to {@link LocalZonedTimestampType}, the time zone offset information is physically
 * stored in every datum. It is used individually for every computation, visualization, or
 * communication to external systems.
 *
 * <p>A conversion from {@link java.time.ZonedDateTime} ignores the zone ID.
 *
 * @see TimestampType
 * @see LocalZonedTimestampType
 */
@PublicEvolving
public final class ZonedTimestampType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final int MIN_PRECISION = TimestampType.MIN_PRECISION;

    public static final int MAX_PRECISION = TimestampType.MAX_PRECISION;

    public static final int DEFAULT_PRECISION = TimestampType.DEFAULT_PRECISION;

    private static final String FORMAT = "TIMESTAMP(%d) WITH TIME ZONE";

    private final int precision;

    /**
     * Internal constructor that allows attaching additional metadata about time attribute
     * properties. The additional metadata does not affect equality or serializability.
     */
    public ZonedTimestampType(boolean isNullable, int precision) {
        super(isNullable, DataTypeRoot.TIMESTAMP_WITH_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    String.format(
                            "Timestamp with time zone precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        this.precision = precision;
    }

    public ZonedTimestampType(int precision) {
        this(true, precision);
    }

    public ZonedTimestampType() {
        this(DEFAULT_PRECISION);
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new ZonedTimestampType(isNullable, precision);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, precision);
    }

    @Override
    public String asSummaryString() {
        return asSerializableString();
    }

    @Override
    public List<DataType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ZonedTimestampType that = (ZonedTimestampType) o;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }
}
