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

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
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
import org.apache.flink.cdc.common.types.VariantType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.common.types.variant.Variant;

/**
 * Converts a {@link org.apache.flink.cdc.common.types.DataType} to its CDC Internal representation.
 */
public class InternalClassConverter implements DataTypeVisitor<Class<?>> {

    public static final InternalClassConverter INSTANCE = new InternalClassConverter();

    public static Class<?> toInternalClass(DataType dataType) {
        return dataType.accept(INSTANCE);
    }

    private InternalClassConverter() {
        // No instantiation.
    }

    @Override
    public Class<?> visit(CharType charType) {
        return StringData.class;
    }

    @Override
    public Class<?> visit(VarCharType varCharType) {
        return StringData.class;
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
        return DecimalData.class;
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
        return DateData.class;
    }

    @Override
    public Class<?> visit(TimeType timeType) {
        return TimeData.class;
    }

    @Override
    public Class<?> visit(TimestampType timestampType) {
        return TimestampData.class;
    }

    @Override
    public Class<?> visit(ZonedTimestampType zonedTimestampType) {
        return ZonedTimestampData.class;
    }

    @Override
    public Class<?> visit(LocalZonedTimestampType localZonedTimestampType) {
        return LocalZonedTimestampData.class;
    }

    @Override
    public Class<?> visit(ArrayType arrayType) {
        return ArrayData.class;
    }

    @Override
    public Class<?> visit(MapType mapType) {
        return MapData.class;
    }

    @Override
    public Class<?> visit(RowType rowType) {
        return RecordData.class;
    }

    @Override
    public Class<?> visit(VariantType variantType) {
        return Variant.class;
    }
}
