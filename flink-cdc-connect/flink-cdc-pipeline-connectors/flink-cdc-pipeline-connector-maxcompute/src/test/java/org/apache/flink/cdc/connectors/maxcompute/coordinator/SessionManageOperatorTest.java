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

package org.apache.flink.cdc.connectors.maxcompute.coordinator;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.TypeConvertUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.jobgraph.OperatorID;

import com.aliyun.odps.PartitionSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link SessionManageOperator}. */
class SessionManageOperatorTest {

    @AfterEach
    void tearDown() {
        SessionManageOperator.instance = null;
    }

    @Test
    void testExtractPartitionByColumnName() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("pt", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("dt", DataTypes.STRING())
                        .partitionKey("pt", "dt")
                        .build();
        BinaryRecordDataGenerator dataGenerator =
                new BinaryRecordDataGenerator((RowType) schema.toRowDataType());
        BinaryRecordData record =
                dataGenerator.generate(
                        new Object[] {
                            1,
                            BinaryStringData.fromString("hangzhou"),
                            BinaryStringData.fromString("Alice"),
                            BinaryStringData.fromString("20260728")
                        });

        SessionManageOperator operator =
                new SessionManageOperator(mock(MaxComputeOptions.class), new OperatorID());
        operator.open();
        TableId tableId = TableId.tableId("test_table");
        Map<TableId, Schema> schemaMaps = getField(operator, "schemaMaps");
        Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps =
                getField(operator, "fieldGetterMaps");
        schemaMaps.put(tableId, schema);
        fieldGetterMaps.put(tableId, TypeConvertUtils.createFieldGetters(schema));

        PartitionSpec expected = new PartitionSpec();
        expected.set("pt", "hangzhou");
        expected.set("dt", "20260728");

        assertThat(operator.extractPartition(record, tableId))
                .isEqualTo(expected.toString(true, true));
    }

    @Test
    void testExtractPartitionFromUnpartitionedTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();
        BinaryRecordDataGenerator dataGenerator =
                new BinaryRecordDataGenerator((RowType) schema.toRowDataType());
        BinaryRecordData record =
                dataGenerator.generate(new Object[] {1, BinaryStringData.fromString("Alice")});

        SessionManageOperator operator =
                new SessionManageOperator(mock(MaxComputeOptions.class), new OperatorID());
        operator.open();
        TableId tableId = TableId.tableId("test_table");
        Map<TableId, Schema> schemaMaps = getField(operator, "schemaMaps");
        schemaMaps.put(tableId, schema);

        assertThat(operator.extractPartition(record, tableId)).isNull();
    }

    @Test
    void testExtractPartitionFailsForMissingPartitionColumn() throws Exception {
        Schema schema = mock(Schema.class);
        when(schema.partitionKeys()).thenReturn(Collections.singletonList("missing_pt"));
        when(schema.getColumnCount()).thenReturn(1);
        when(schema.getColumnNames()).thenReturn(Collections.singletonList("id"));

        SessionManageOperator operator =
                new SessionManageOperator(mock(MaxComputeOptions.class), new OperatorID());
        operator.open();
        TableId tableId = TableId.tableId("test_table");
        Map<TableId, Schema> schemaMaps = getField(operator, "schemaMaps");
        Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps =
                getField(operator, "fieldGetterMaps");
        schemaMaps.put(tableId, schema);
        fieldGetterMaps.put(tableId, Collections.emptyList());

        assertThatThrownBy(() -> operator.extractPartition(mock(RecordData.class), tableId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing_pt");
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String fieldName)
            throws ReflectiveOperationException {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }
}
