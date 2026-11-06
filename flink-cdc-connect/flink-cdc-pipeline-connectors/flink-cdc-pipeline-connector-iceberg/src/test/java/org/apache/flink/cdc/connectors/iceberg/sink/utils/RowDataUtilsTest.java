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

package org.apache.flink.cdc.connectors.iceberg.sink.utils;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for converting CDC events for append-only and equality-delete writers. */
class RowDataUtilsTest {

    private static final TableId TABLE_ID = TableId.parse("test.iceberg_table");

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("value", DataTypes.STRING())
                    .build();

    private final List<RecordData.FieldGetter> fieldGetters =
            IcebergTypeUtils.createFieldGetters(SCHEMA, ZoneId.of("UTC"));

    private final BinaryRecordDataGenerator generator =
            new BinaryRecordDataGenerator(SCHEMA.getColumnDataTypes().toArray(new DataType[0]));

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInsertAndReplacePreserveAfterImage(boolean supportsEqualityDeletes) {
        RecordData after = record("new");
        for (DataChangeEvent event :
                Arrays.asList(
                        DataChangeEvent.insertEvent(TABLE_ID, after),
                        DataChangeEvent.replaceEvent(TABLE_ID, after, null))) {
            List<RowData> rows =
                    RowDataUtils.convertDataChangeEventToRowData(
                            event, fieldGetters, supportsEqualityDeletes);
            assertThat(rows).hasSize(1);
            assertRow(rows.get(0), RowKind.INSERT, "new");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUpdateUsesBeforeImageOnlyWithEqualityDeletes(boolean supportsEqualityDeletes) {
        List<RowData> rows =
                RowDataUtils.convertDataChangeEventToRowData(
                        DataChangeEvent.updateEvent(TABLE_ID, record("old"), record("new")),
                        fieldGetters,
                        supportsEqualityDeletes);

        if (supportsEqualityDeletes) {
            assertThat(rows).hasSize(2);
            assertRow(rows.get(0), RowKind.DELETE, "old");
            assertRow(rows.get(1), RowKind.INSERT, "new");
        } else {
            assertThat(rows).hasSize(1);
            assertRow(rows.get(0), RowKind.INSERT, "new");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDeleteIsIgnoredWithoutEqualityDeletes(boolean supportsEqualityDeletes) {
        List<RowData> rows =
                RowDataUtils.convertDataChangeEventToRowData(
                        DataChangeEvent.deleteEvent(TABLE_ID, record("old")),
                        fieldGetters,
                        supportsEqualityDeletes);

        if (supportsEqualityDeletes) {
            assertThat(rows).hasSize(1);
            assertRow(rows.get(0), RowKind.DELETE, "old");
        } else {
            assertThat(rows).isEmpty();
        }
    }

    private RecordData record(String value) {
        return generator.generate(new Object[] {1L, BinaryStringData.fromString(value)});
    }

    private static void assertRow(RowData row, RowKind rowKind, String value) {
        assertThat(row.getRowKind()).isEqualTo(rowKind);
        assertThat(row.getLong(0)).isEqualTo(1L);
        assertThat(row.getString(1).toString()).isEqualTo(value);
    }
}
