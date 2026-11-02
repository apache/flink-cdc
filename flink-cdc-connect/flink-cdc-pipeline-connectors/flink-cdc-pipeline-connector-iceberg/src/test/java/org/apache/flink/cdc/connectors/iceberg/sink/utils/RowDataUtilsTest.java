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
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
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

    private static final Schema KEYED_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("region", DataTypes.VARCHAR(100))
                    .physicalColumn("city", DataTypes.VARCHAR(100))
                    .physicalColumn("name", DataTypes.VARCHAR(100))
                    .primaryKey("id")
                    .build();

    private static final List<RecordData.FieldGetter> KEYED_FIELD_GETTERS =
            IcebergTypeUtils.createFieldGetters(KEYED_SCHEMA, ZoneId.of("UTC"));

    private static final BinaryRecordDataGenerator KEYED_GENERATOR =
            new BinaryRecordDataGenerator(
                    KEYED_SCHEMA.getColumnDataTypes().toArray(new DataType[0]));

    private static final RecordData.FieldGetter[] ID_AND_REGION =
            RowDataUtils.createDeleteKeyGetters(KEYED_SCHEMA, Arrays.asList("id", "region"));
    private static final RecordData.FieldGetter[] ID_REGION_AND_CITY =
            RowDataUtils.createDeleteKeyGetters(
                    KEYED_SCHEMA, Arrays.asList("id", "region", "city"));
    private static final RecordData.FieldGetter[] ID_ONLY =
            RowDataUtils.createDeleteKeyGetters(KEYED_SCHEMA, Collections.singletonList("id"));

    @Test
    void testUpdateWithUnchangedPartitionEmitsOnlyInsert() {
        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(1L, "north", "oslo", "bob"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, ID_AND_REGION);

        assertThat(rows).hasSize(1);
        assertKeyedRow(rows.get(0), RowKind.INSERT, 1L, "north", "oslo", "bob");
    }

    @Test
    void testUpdateWithChangedPartitionEmitsDeleteThenInsert() {
        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(1L, "east", "oslo", "alice"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, ID_AND_REGION);

        assertThat(rows).hasSize(2);
        assertKeyedRow(rows.get(0), RowKind.DELETE, 1L, "north", "oslo", "alice");
        assertKeyedRow(rows.get(1), RowKind.INSERT, 1L, "east", "oslo", "alice");
        assertThat(rows)
                .isEqualTo(
                        RowDataUtils.convertDataChangeEventToRowData(
                                event, KEYED_FIELD_GETTERS, true));
    }

    @Test
    void testUpdateChangingOnlySecondPartitionColumnEmitsDeleteThenInsert() {
        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(1L, "north", "bergen", "alice"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, ID_REGION_AND_CITY);

        assertThat(rows).hasSize(2);
        assertKeyedRow(rows.get(0), RowKind.DELETE, 1L, "north", "oslo", "alice");
        assertKeyedRow(rows.get(1), RowKind.INSERT, 1L, "north", "bergen", "alice");
    }

    @Test
    void testUpdateWithAllPartitionColumnsUnchangedEmitsOnlyInsert() {
        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(1L, "north", "oslo", "bob"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, ID_REGION_AND_CITY);

        assertThat(rows).hasSize(1);
        assertKeyedRow(rows.get(0), RowKind.INSERT, 1L, "north", "oslo", "bob");
    }

    @Test
    void testUpdatePartitionValueFromNullEmitsDeleteThenInsert() {
        DataChangeEvent event =
                update(row(1L, null, "oslo", "alice"), row(1L, "north", "oslo", "alice"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, ID_AND_REGION);

        assertThat(rows).hasSize(2);
        assertKeyedRow(rows.get(0), RowKind.DELETE, 1L, null, "oslo", "alice");
        assertKeyedRow(rows.get(1), RowKind.INSERT, 1L, "north", "oslo", "alice");
    }

    @Test
    void testUpdatePartitionValueToNullEmitsDeleteThenInsert() {
        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(1L, null, "oslo", "alice"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, ID_AND_REGION);

        assertThat(rows).hasSize(2);
        assertKeyedRow(rows.get(0), RowKind.DELETE, 1L, "north", "oslo", "alice");
        assertKeyedRow(rows.get(1), RowKind.INSERT, 1L, null, "oslo", "alice");
    }

    @Test
    void testUpdateWithNullPartitionValueOnBothSidesEmitsOnlyInsert() {
        DataChangeEvent event =
                update(row(1L, null, "oslo", "alice"), row(1L, null, "oslo", "bob"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, ID_AND_REGION);

        assertThat(rows).hasSize(1);
        assertKeyedRow(rows.get(0), RowKind.INSERT, 1L, null, "oslo", "bob");
    }

    @Test
    void testUpdateOnUnpartitionedTableWithUnchangedKeyEmitsOnlyInsert() {
        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(1L, "east", "bergen", "bob"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, ID_ONLY);

        assertThat(rows).hasSize(1);
        assertKeyedRow(rows.get(0), RowKind.INSERT, 1L, "east", "bergen", "bob");
    }

    @Test
    void testUpdateWithNoDeleteKeyColumnsEmitsOnlyInsert() {
        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(1L, "east", "oslo", "alice"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, new RecordData.FieldGetter[0]);

        assertThat(rows).hasSize(1);
        assertKeyedRow(rows.get(0), RowKind.INSERT, 1L, "east", "oslo", "alice");
    }

    @Test
    void testInsertReplaceAndDeleteMatchBaseline() {
        RecordData record = row(1L, "north", "oslo", "alice");
        List<DataChangeEvent> events =
                Arrays.asList(
                        DataChangeEvent.insertEvent(TABLE_ID, record),
                        DataChangeEvent.replaceEvent(TABLE_ID, record, Collections.emptyMap()),
                        DataChangeEvent.deleteEvent(TABLE_ID, record));

        for (DataChangeEvent event : events) {
            List<RowData> optimized = convert(event, KEYED_FIELD_GETTERS, ID_AND_REGION);
            assertThat(optimized)
                    .as("op %s", event.op())
                    .isEqualTo(
                            RowDataUtils.convertDataChangeEventToRowData(
                                    event, KEYED_FIELD_GETTERS, true));
        }
    }

    @Test
    void testUpdateChangingKeyWithinPartitionEmitsDeleteThenInsert() {
        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(2L, "north", "oslo", "alice"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, ID_AND_REGION);

        assertThat(rows).hasSize(2);
        assertKeyedRow(rows.get(0), RowKind.DELETE, 1L, "north", "oslo", "alice");
        assertKeyedRow(rows.get(1), RowKind.INSERT, 2L, "north", "oslo", "alice");
    }

    @Test
    void testUpdateChangingKeyOnUnpartitionedTableEmitsDeleteThenInsert() {
        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(2L, "north", "oslo", "bob"));

        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, ID_ONLY);

        assertThat(rows).hasSize(2);
        assertKeyedRow(rows.get(0), RowKind.DELETE, 1L, "north", "oslo", "alice");
        assertKeyedRow(rows.get(1), RowKind.INSERT, 2L, "north", "oslo", "bob");
    }

    @Test
    void testBinaryKeyIsComparedByContent() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("k", DataTypes.VARBINARY(16).notNull())
                        .physicalColumn("v", DataTypes.INT())
                        .primaryKey("k")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        RecordData.FieldGetter[] key =
                RowDataUtils.createDeleteKeyGetters(schema, Collections.singletonList("k"));
        List<RecordData.FieldGetter> getters =
                IcebergTypeUtils.createFieldGetters(schema, ZoneId.of("UTC"));

        DataChangeEvent sameKey =
                update(
                        generator.generate(new Object[] {new byte[] {1, 2}, 1}),
                        generator.generate(new Object[] {new byte[] {1, 2}, 2}));
        DataChangeEvent changedKey =
                update(
                        generator.generate(new Object[] {new byte[] {1, 2}, 1}),
                        generator.generate(new Object[] {new byte[] {1, 3}, 1}));

        assertThat(convert(sameKey, getters, key)).hasSize(1);
        assertThat(convert(changedKey, getters, key)).hasSize(2);
    }

    @Test
    void testUnknownDeleteKeyColumnDisablesSkipping() {
        assertThat(
                        RowDataUtils.createDeleteKeyGetters(
                                KEYED_SCHEMA, Arrays.asList("id", "missing")))
                .isNull();

        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(1L, "north", "oslo", "bob"));
        List<RowData> rows = convert(event, KEYED_FIELD_GETTERS, null);

        assertThat(rows).hasSize(2);
        assertKeyedRow(rows.get(0), RowKind.DELETE, 1L, "north", "oslo", "alice");
        assertKeyedRow(rows.get(1), RowKind.INSERT, 1L, "north", "oslo", "bob");
    }

    @Test
    void testAppendOnlyUpdateIgnoresDeleteKeyGetters() {
        DataChangeEvent event =
                update(row(1L, "north", "oslo", "alice"), row(2L, "east", "oslo", "alice"));

        List<RowData> rows =
                RowDataUtils.convertDataChangeEventToRowData(
                        event, KEYED_FIELD_GETTERS, false, ID_AND_REGION);

        assertThat(rows).hasSize(1);
        assertKeyedRow(rows.get(0), RowKind.INSERT, 2L, "east", "oslo", "alice");
    }

    private static List<RowData> convert(
            DataChangeEvent event,
            List<RecordData.FieldGetter> fieldGetters,
            RecordData.FieldGetter[] deleteKeyGetters) {
        return RowDataUtils.convertDataChangeEventToRowData(
                event, fieldGetters, true, deleteKeyGetters);
    }

    private static DataChangeEvent update(RecordData before, RecordData after) {
        return DataChangeEvent.updateEvent(TABLE_ID, before, after);
    }

    private static RecordData row(long id, String region, String city, String name) {
        return KEYED_GENERATOR.generate(
                new Object[] {
                    id,
                    region == null ? null : BinaryStringData.fromString(region),
                    city == null ? null : BinaryStringData.fromString(city),
                    name == null ? null : BinaryStringData.fromString(name)
                });
    }

    private static void assertKeyedRow(
            RowData row, RowKind kind, long id, String region, String city, String name) {
        assertThat(row.getRowKind()).isEqualTo(kind);
        assertThat(row.getLong(0)).isEqualTo(id);
        assertThat(stringAt(row, 1)).isEqualTo(region);
        assertThat(stringAt(row, 2)).isEqualTo(city);
        assertThat(stringAt(row, 3)).isEqualTo(name);
    }

    private static String stringAt(RowData row, int pos) {
        if (row.isNullAt(pos)) {
            return null;
        }
        StringData value = row.getString(pos);
        return value.toString();
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
