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
 * Data type of a time with a time-zone in the ISO-8601 calendar system.
 *
 * <p>The zone time type is used to represent a time with a specific time zone, such as
 * '10:15:30+01:00'.
 */
@PublicEvolving
public class ZoneTimeType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final int MIN_PRECISION = TimeType.MIN_PRECISION;

    public static final int MAX_PRECISION = TimeType.MAX_PRECISION;

    public static final int DEFAULT_PRECISION = TimeType.DEFAULT_PRECISION;

    private static final String FORMAT = "TIME(%d) WITH TIME ZONE";

    private final int precision;

    /** Creates a {@link ZoneTimeType} with default precision. */
    public ZoneTimeType(boolean isNullable, int precision) {
        super(isNullable, DataTypeRoot.TIME_WITH_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    String.format(
                            "Time with time zone precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        this.precision = precision;
    }

    public ZoneTimeType(int precision) {
        this(true, precision);
    }

    public ZoneTimeType() {
        this(DEFAULT_PRECISION);
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new ZoneTimeType(isNullable, precision);
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
    public String toString() {
        return "ZONETIME";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return true; // No additional fields to compare
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }
}
