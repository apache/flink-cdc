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
 * Data type of a time WITHOUT time zone consisting of {@code hour:minute:second[.fractional]} with
 * up to nanosecond precision and values ranging from {@code 00:00:00.000000000} to {@code
 * 23:59:59.999999999}. Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not
 * supported as the semantics are closer to {@link java.time.LocalTime}. A time WITH time zone is
 * not provided.
 *
 * <p>A conversion from and to {@code int} describes the number of milliseconds of the day. A
 * conversion from and to {@code long} describes the number of nanoseconds of the day.
 */
@PublicEvolving
public final class TimeType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final int MIN_PRECISION = 0;

    public static final int MAX_PRECISION = 9;

    public static final int DEFAULT_PRECISION = 0;

    private static final String FORMAT = "TIME(%d)";

    private final int precision;

    public TimeType(boolean isNullable, int precision) {
        super(isNullable, DataTypeRoot.TIME_WITHOUT_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    String.format(
                            "Time precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        this.precision = precision;
    }

    public TimeType(int precision) {
        this(true, precision);
    }

    public TimeType() {
        this(DEFAULT_PRECISION);
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new TimeType(isNullable, precision);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, precision);
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
        TimeType timeType = (TimeType) o;
        return precision == timeType.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }
}
