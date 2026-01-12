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
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import java.util.function.Function;

/** Common converters from Java objects to CDC Internal objects. */
public class InternalObjectConverter {

    private static final ToInternalObjectConverter converter = new ToInternalObjectConverter();

    static class ToInternalObjectConverter implements DataTypeVisitor<Function<Object, ?>> {

        @Override
        public Function<Object, StringData> visit(CharType charType) {
            return CommonConverter::convertToStringData;
        }

        @Override
        public Function<Object, StringData> visit(VarCharType varCharType) {
            return CommonConverter::convertToStringData;
        }

        @Override
        public Function<Object, Boolean> visit(BooleanType booleanType) {
            return CommonConverter::convertToBoolean;
        }

        @Override
        public Function<Object, byte[]> visit(BinaryType binaryType) {
            return CommonConverter::convertToBinary;
        }

        @Override
        public Function<Object, byte[]> visit(VarBinaryType varBinaryType) {
            return CommonConverter::convertToBinary;
        }

        @Override
        public Function<Object, DecimalData> visit(DecimalType decimalType) {
            return CommonConverter::convertToDecimalData;
        }

        @Override
        public Function<Object, Byte> visit(TinyIntType tinyIntType) {
            return CommonConverter::convertToByte;
        }

        @Override
        public Function<Object, Short> visit(SmallIntType smallIntType) {
            return CommonConverter::convertToShort;
        }

        @Override
        public Function<Object, Integer> visit(IntType intType) {
            return CommonConverter::convertToInt;
        }

        @Override
        public Function<Object, Long> visit(BigIntType bigIntType) {
            return CommonConverter::convertToLong;
        }

        @Override
        public Function<Object, Float> visit(FloatType floatType) {
            return CommonConverter::convertToFloat;
        }

        @Override
        public Function<Object, Double> visit(DoubleType doubleType) {
            return CommonConverter::convertToDouble;
        }

        @Override
        public Function<Object, DateData> visit(DateType dateType) {
            return CommonConverter::convertToDateData;
        }

        @Override
        public Function<Object, TimeData> visit(TimeType timeType) {
            return CommonConverter::convertToTimeData;
        }

        @Override
        public Function<Object, TimestampData> visit(TimestampType timestampType) {
            return CommonConverter::convertToTimestampData;
        }

        @Override
        public Function<Object, ZonedTimestampData> visit(ZonedTimestampType zonedTimestampType) {
            return CommonConverter::convertToZonedTimestampData;
        }

        @Override
        public Function<Object, LocalZonedTimestampData> visit(
                LocalZonedTimestampType localZonedTimestampType) {
            return CommonConverter::convertToLocalZonedTimestampData;
        }

        @Override
        public Function<Object, ArrayData> visit(ArrayType arrayType) {
            return o -> CommonConverter.convertToArrayData(o, arrayType);
        }

        @Override
        public Function<Object, MapData> visit(MapType mapType) {
            return o -> CommonConverter.convertToMapData(o, mapType);
        }

        @Override
        public Function<Object, RecordData> visit(RowType rowType) {
            return o -> CommonConverter.convertToRowData(o, rowType);
        }
    }

    public static Object convertToInternal(Object obj, DataType dataType) {
        if (obj == null) {
            return null;
        }
        return dataType.accept(converter).apply(obj);
    }
}
