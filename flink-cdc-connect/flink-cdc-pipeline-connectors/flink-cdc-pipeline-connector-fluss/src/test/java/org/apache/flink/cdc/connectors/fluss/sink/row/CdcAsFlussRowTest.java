/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.flink.cdc.connectors.fluss.sink.row;

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CdcAsFlussRow}. */
class CdcAsFlussRowTest {

    @Test
    void testNestedRowWithIndexMapping() {
        GenericRecordData input = GenericRecordData.of(GenericRecordData.of(7, 8), 42);
        CdcAsFlussRow row = CdcAsFlussRow.replace(input, 2, Map.of(0, 1, 1, 0));

        assertThat(row.getInt(0)).isEqualTo(42);

        InternalRow nested = row.getRow(1, 2);
        assertThat(nested.getInt(0)).isEqualTo(7);
        assertThat(nested.getInt(1)).isEqualTo(8);
    }

    @Test
    void testNestedRowWithMoreChildrenThanOuterRow() {
        GenericRecordData input = GenericRecordData.of(GenericRecordData.of(7, 8, 9), 42);
        CdcAsFlussRow row = CdcAsFlussRow.replace(input);

        InternalRow nested = row.getRow(0, 3);
        assertThat(nested.isNullAt(2)).isFalse();
        assertThat(nested.getInt(0)).isEqualTo(7);
        assertThat(nested.getInt(1)).isEqualTo(8);
        assertThat(nested.getInt(2)).isEqualTo(9);
    }

    @Test
    void testSparseOuterMappingKeepsNestedRowIntact() {
        GenericRecordData input = GenericRecordData.of(GenericRecordData.of(7, 8), 42);
        CdcAsFlussRow row = CdcAsFlussRow.replace(input, 3, Map.of(0, 0, 2, 1));

        assertThat(row.isNullAt(1)).isTrue();

        InternalRow nested = row.getRow(0, 2);
        assertThat(nested.isNullAt(1)).isFalse();
        assertThat(nested.getInt(0)).isEqualTo(7);
        assertThat(nested.getInt(1)).isEqualTo(8);
    }

    @Test
    void testNestedRowWithNullChild() {
        GenericRecordData input = GenericRecordData.of(GenericRecordData.of(7, null, 8), 42);
        CdcAsFlussRow row = CdcAsFlussRow.replace(input, 2, Map.of(0, 1, 1, 0));

        InternalRow nested = row.getRow(1, 3);
        assertThat(nested.isNullAt(0)).isFalse();
        assertThat(nested.getInt(0)).isEqualTo(7);
        assertThat(nested.isNullAt(1)).isTrue();
        assertThat(nested.isNullAt(2)).isFalse();
        assertThat(nested.getInt(2)).isEqualTo(8);
    }

    @Test
    void testNestedRowsWithDifferentArities() {
        GenericRecordData innerA = GenericRecordData.of(7, 8, 9);
        GenericRecordData innerB = GenericRecordData.of(10, 11);
        GenericRecordData input = GenericRecordData.of(GenericRecordData.of(innerA, innerB), 42);
        CdcAsFlussRow row = CdcAsFlussRow.replace(input, 2, Map.of(0, 1, 1, 0));

        InternalRow middle = row.getRow(1, 2);
        InternalRow first = middle.getRow(0, 3);
        InternalRow second = middle.getRow(1, 2);
        assertThat(first.getInt(0)).isEqualTo(7);
        assertThat(first.getInt(1)).isEqualTo(8);
        assertThat(first.getInt(2)).isEqualTo(9);
        assertThat(second.getInt(0)).isEqualTo(10);
        assertThat(second.getInt(1)).isEqualTo(11);
    }

    @Test
    void testBinaryRecordDataNestedRow() {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        DataTypes.ROW(
                                DataTypes.ROW(DataTypes.INT(), DataTypes.INT()), DataTypes.INT()));
        BinaryRecordData input = generator.generate(new Object[] {GenericRecordData.of(7, 8), 42});
        CdcAsFlussRow row = CdcAsFlussRow.replace(input, 2, Map.of(0, 1, 1, 0));

        assertThat(row.getInt(0)).isEqualTo(42);

        InternalRow nested = row.getRow(1, 2);
        assertThat(nested.getInt(0)).isEqualTo(7);
        assertThat(nested.getInt(1)).isEqualTo(8);
    }
}
