/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.maxcompute.utils;

import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.type.TypeInfoFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Partition-related tests for {@link TypeConvertUtils}. */
class TypeConvertUtilsPartitionTest {

    @Test
    void testRecordConversionRejectsMismatchedDataColumnCount() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("pt", DataTypes.STRING())
                        .partitionKey("pt")
                        .build();
        BinaryRecordDataGenerator dataGenerator =
                new BinaryRecordDataGenerator((RowType) schema.toRowDataType());
        BinaryRecordData record =
                dataGenerator.generate(
                        new Object[] {7, BinaryStringData.fromString("partition-value")});

        TableSchema mismatchedSchema = new TableSchema();
        mismatchedSchema.addColumn(new Column("id", TypeInfoFactory.INT));
        mismatchedSchema.addColumn(new Column("extra", TypeInfoFactory.STRING));
        ArrayRecord arrayRecord = new ArrayRecord(mismatchedSchema);

        assertThatThrownBy(() -> TypeConvertUtils.toMaxComputeRecord(schema, record, arrayRecord))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("record data count not match")
                .hasMessageContaining("count 2")
                .hasMessageContaining("count 1");
    }
}
