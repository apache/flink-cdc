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

import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeVisitor;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

/**
 * Converts a {@link org.apache.flink.cdc.common.types.DataType} to its Java representation that
 * could be used in UDF manipulation.
 */
public class JavaClassConverter implements DataTypeVisitor<Class<?>> {

    public static final JavaClassConverter INSTANCE = new JavaClassConverter();

    public static Class<?> toJavaClass(DataType dataType) {
        return dataType.accept(INSTANCE);
    }

    private JavaClassConverter() {
        // No instantiation.
    }

    @Override
    public Class<?> visit(CharType charType) {
        return String.class;
    }

    @Override
    public Class<?> visit(VarCharType varCharType) {
        return String.class;
    }

    @Override
    public Class<?> visit(BooleanType booleanType) {
        return Boolean.class;
    }

    @Override
    public Class<?> visit(BinaryType binaryType) {
        return byte[].class;
    }

    @Override
    public Class<?> visit(VarBinaryType varBinaryType) {
        return byte[].class;
    }

    @Override
    public Class<?> visit(DecimalType decimalType) {
        return BigDecimal.class;
    }

    @Override
    public Class<?> visit(TinyIntType tinyIntType) {
        return Byte.class;
    }

    @Override
    public Class<?> visit(SmallIntType smallIntType) {
        return Short.class;
    }

    @Override
    public Class<?> visit(IntType intType) {
        return Integer.class;
    }

    @Override
    public Class<?> visit(BigIntType bigIntType) {
        return Long.class;
    }

    @Override
    public Class<?> visit(FloatType floatType) {
        return Float.class;
    }

    @Override
    public Class<?> visit(DoubleType doubleType) {
        return Double.class;
    }

    @Override
    public Class<?> visit(DateType dateType) {
        return LocalDate.class;
    }

    @Override
    public Class<?> visit(TimeType timeType) {
        return LocalTime.class;
    }

    @Override
    public Class<?> visit(TimestampType timestampType) {
        return LocalDateTime.class;
    }

    @Override
    public Class<?> visit(ZonedTimestampType zonedTimestampType) {
        return ZonedDateTime.class;
    }

    @Override
    public Class<?> visit(LocalZonedTimestampType localZonedTimestampType) {
        return Instant.class;
    }

    @Override
    public Class<?> visit(ArrayType arrayType) {
        return List.class;
    }

    @Override
    public Class<?> visit(MapType mapType) {
        return Map.class;
    }

    @Override
    public Class<?> visit(RowType rowType) {
        return List.class;
    }
}
