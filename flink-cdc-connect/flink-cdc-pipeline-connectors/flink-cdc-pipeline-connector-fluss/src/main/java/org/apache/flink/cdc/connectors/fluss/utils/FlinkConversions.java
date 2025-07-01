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

package org.apache.flink.cdc.connectors.fluss.utils;

import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
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
import org.apache.flink.util.CollectionUtil;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.StringType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Converter from Flink's type to Fluss's type. */
public class FlinkConversions {
    private static final CdcTypeToFlussType INSTANCE = new CdcTypeToFlussType();

    public static TableDescriptor toFlussTable(
            org.apache.flink.cdc.common.schema.Schema cdcSchema,
            List<String> bucketKeys,
            @Nullable Integer bucketNum) {
        // now, build Fluss's table
        Schema.Builder schemBuilder = Schema.newBuilder();
        if (!CollectionUtil.isNullOrEmpty(cdcSchema.primaryKeys())) {
            schemBuilder.primaryKey(cdcSchema.primaryKeys());
        }

        // first build schema with cdc columns
        Schema schema =
                schemBuilder
                        .fromColumns(
                                cdcSchema.getColumns().stream()
                                        .map(
                                                column ->
                                                        new Schema.Column(
                                                                column.getName(),
                                                                toFlussType(column.getType()),
                                                                column.getComment()))
                                        .collect(Collectors.toList()))
                        .build();

        if (CollectionUtil.isNullOrEmpty(bucketKeys)) {
            // use primary keys - partition keys
            bucketKeys =
                    schema.getPrimaryKey()
                            .map(
                                    pk -> {
                                        List<String> keys = new ArrayList<>(pk.getColumnNames());
                                        keys.removeAll(cdcSchema.partitionKeys());
                                        return keys;
                                    })
                            .orElse(Collections.emptyList());
        }

        return TableDescriptor.builder()
                .schema(schema)
                .partitionedBy(cdcSchema.partitionKeys())
                .distributedBy(bucketNum, bucketKeys)
                .comment(cdcSchema.comment())
                .properties(Collections.emptyMap())
                .build();
    }

    @VisibleForTesting
    private static com.alibaba.fluss.types.DataType toFlussType(
            org.apache.flink.cdc.common.types.DataType flinkDataType) {
        return flinkDataType.accept(INSTANCE);
    }

    private static class CdcTypeToFlussType
            implements org.apache.flink.cdc.common.types.DataTypeVisitor<DataType> {

        @Override
        public DataType visit(CharType charType) {
            return new com.alibaba.fluss.types.CharType(
                    charType.isNullable(), charType.getLength());
        }

        @Override
        public DataType visit(VarCharType varCharType) {
            // fluss not support varchar type
            return new StringType(varCharType.isNullable());
        }

        @Override
        public DataType visit(BooleanType booleanType) {
            return new com.alibaba.fluss.types.BooleanType(booleanType.isNullable());
        }

        @Override
        public DataType visit(BinaryType binaryType) {
            return new com.alibaba.fluss.types.BinaryType(
                    binaryType.isNullable(), binaryType.getLength());
        }

        @Override
        public DataType visit(VarBinaryType varBinaryType) {
            // fluss not support varbinary type
            return new BytesType(varBinaryType.isNullable());
        }

        @Override
        public DataType visit(DecimalType decimalType) {
            return new com.alibaba.fluss.types.DecimalType(
                    decimalType.isNullable(), decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public DataType visit(TinyIntType tinyIntType) {
            return new com.alibaba.fluss.types.TinyIntType(tinyIntType.isNullable());
        }

        @Override
        public DataType visit(SmallIntType smallIntType) {
            return new com.alibaba.fluss.types.SmallIntType(smallIntType.isNullable());
        }

        @Override
        public DataType visit(IntType intType) {
            return new com.alibaba.fluss.types.IntType(intType.isNullable());
        }

        @Override
        public DataType visit(BigIntType bigIntType) {
            return new com.alibaba.fluss.types.BigIntType(bigIntType.isNullable());
        }

        @Override
        public DataType visit(FloatType floatType) {
            return new com.alibaba.fluss.types.FloatType(floatType.isNullable());
        }

        @Override
        public DataType visit(DoubleType doubleType) {
            return new com.alibaba.fluss.types.DoubleType(doubleType.isNullable());
        }

        @Override
        public DataType visit(DateType dateType) {
            return new com.alibaba.fluss.types.DateType(dateType.isNullable());
        }

        @Override
        public DataType visit(TimeType timeType) {
            return new com.alibaba.fluss.types.TimeType(
                    timeType.isNullable(), timeType.getPrecision());
        }

        @Override
        public DataType visit(TimestampType timestampType) {
            return new com.alibaba.fluss.types.TimestampType(
                    timestampType.isNullable(), timestampType.getPrecision());
        }

        @Override
        public DataType visit(ZonedTimestampType zonedTimestampType) {
            throw new UnsupportedOperationException(
                    "Unsupported data type in fluss " + zonedTimestampType);
        }

        @Override
        public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
            return new com.alibaba.fluss.types.LocalZonedTimestampType(
                    localZonedTimestampType.isNullable(), localZonedTimestampType.getPrecision());
        }

        @Override
        public DataType visit(ArrayType arrayType) {
            throw new UnsupportedOperationException(
                    "Unsupported data type in fluss version under 0.7: " + arrayType);
        }

        @Override
        public DataType visit(MapType mapType) {
            throw new UnsupportedOperationException(
                    "Unsupported data type in fluss version under 0.7: " + mapType);
        }

        @Override
        public DataType visit(RowType rowType) {
            throw new UnsupportedOperationException(
                    "Unsupported data type in fluss version under 0.7: " + rowType);
        }
    }
}
