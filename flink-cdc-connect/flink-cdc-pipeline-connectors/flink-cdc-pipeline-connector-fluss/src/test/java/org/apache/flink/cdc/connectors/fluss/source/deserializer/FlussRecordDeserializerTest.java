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

package org.apache.flink.cdc.connectors.fluss.source.deserializer;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.fluss.source.reader.FlussSourceRecord;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for schema evolution logic in {@link FlussRecordDeserializer}. */
class FlussRecordDeserializerTest {

    private static final long TABLE_ID = 1001L;
    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final TableId CDC_TABLE_ID = TableId.tableId("test_db", "test_table");

    private FlussRecordDeserializer deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new FlussRecordDeserializer();
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static RowType rowType(DataField... fields) {
        return new RowType(Arrays.asList(fields));
    }

    /** Creates a DataField with an explicit field ID. */
    private static DataField field(String name, org.apache.fluss.types.DataType type, int fieldId) {
        return new DataField(name, type, null, fieldId);
    }

    /** Creates a log-phase ScanRecord with a valid schemaId and RowType. */
    private static FlussSourceRecord logRecord(int schemaId, RowType rowType, GenericRow row) {
        return logRecordWithKeys(
                schemaId, rowType, row, Collections.emptyList(), Collections.emptyList());
    }

    /** Creates a log-phase ScanRecord with table key metadata. */
    private static FlussSourceRecord logRecordWithKeys(
            int schemaId,
            RowType rowType,
            GenericRow row,
            List<String> primaryKeyNames,
            List<String> partitionKeyNames) {
        return new FlussSourceRecord(
                new ScanRecord(
                        TABLE_ID,
                        schemaId,
                        /* offset= */ 0L,
                        System.currentTimeMillis(),
                        ChangeType.INSERT,
                        row,
                        /* sizeInBytes= */ -1),
                TABLE_PATH,
                rowType,
                1,
                primaryKeyNames,
                partitionKeyNames);
    }

    /** Creates a snapshot-phase ScanRecord (schemaId = -1, rowType = null). */
    private static ScanRecord snapshotRecord(GenericRow row) {
        return new ScanRecord(row);
    }

    // ------------------------------------------------------------------
    //  Schema cache seeding
    // ------------------------------------------------------------------

    @Test
    void testFirstLogRecordEmitsCreateTableEventWithKeys() {
        RowType rt =
                rowType(
                        field("id", new IntType(false), 1),
                        field("name", new StringType(true), 2),
                        field("dt", new StringType(false), 3));
        GenericRow row =
                GenericRow.of(
                        1, BinaryString.fromString("Alice"), BinaryString.fromString("2026-07-23"));
        List<String> primaryKeyNames = Arrays.asList("id", "dt");
        List<String> partitionKeyNames = Collections.singletonList("dt");

        List<Event> events =
                deserializer.deserialize(
                        logRecordWithKeys(1, rt, row, primaryKeyNames, partitionKeyNames),
                        TABLE_PATH);

        assertThat(events).hasSize(2);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) events.get(0);
        assertThat(createTableEvent.getSchema().primaryKeys()).containsExactly("id", "dt");
        assertThat(createTableEvent.getSchema().partitionKeys()).containsExactly("dt");
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
    }

    @Test
    void testSameSchemaIdProducesNoSchemaChangeEvents() {
        RowType rt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));

        // Seed cache
        deserializer.deserialize(
                logRecord(1, rt, GenericRow.of(1, BinaryString.fromString("Alice"))), TABLE_PATH);

        // Same schemaId
        List<Event> events =
                deserializer.deserialize(
                        logRecord(1, rt, GenericRow.of(2, BinaryString.fromString("Bob"))),
                        TABLE_PATH);

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(DataChangeEvent.class);
    }

    // ------------------------------------------------------------------
    //  Snapshot records (schemaId = -1) bypass schema detection
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    //  Add column at last (supported)
    // ------------------------------------------------------------------

    @Test
    void testAddSingleColumnAtLast() {
        RowType oldRt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));
        RowType newRt =
                rowType(
                        field("id", new IntType(false), 1),
                        field("name", new StringType(true), 2),
                        field("age", new IntType(true), 3));

        // Seed cache with schema v1
        deserializer.deserialize(
                logRecord(1, oldRt, GenericRow.of(1, BinaryString.fromString("Alice"))),
                TABLE_PATH);

        // Schema v2 — one column added at end
        List<Event> events =
                deserializer.deserialize(
                        logRecord(2, newRt, GenericRow.of(2, BinaryString.fromString("Bob"), 30)),
                        TABLE_PATH);

        assertThat(events).hasSize(2);

        // First event: AddColumnEvent
        assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent addEvent = (AddColumnEvent) events.get(0);
        assertThat(addEvent.tableId()).isEqualTo(CDC_TABLE_ID);
        assertThat(addEvent.getAddedColumns()).hasSize(1);
        assertThat(addEvent.getAddedColumns().get(0).getAddColumn().getName()).isEqualTo("age");
        assertThat(addEvent.getAddedColumns().get(0).getAddColumn().getType())
                .isEqualTo(new org.apache.flink.cdc.common.types.IntType(true));
        assertThat(addEvent.getAddedColumns().get(0).getPosition())
                .isEqualTo(AddColumnEvent.ColumnPosition.LAST);

        // Second event: DataChangeEvent (INSERT)
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
    }

    @Test
    void testAddMultipleColumnsAtLast() {
        RowType oldRt = rowType(field("id", new IntType(false), 1));
        RowType newRt =
                rowType(
                        field("id", new IntType(false), 1),
                        field("name", new StringType(true), 2),
                        field("age", new IntType(true), 3));

        // Seed
        deserializer.deserialize(logRecord(1, oldRt, GenericRow.of(1)), TABLE_PATH);

        // Two columns added
        List<Event> events =
                deserializer.deserialize(
                        logRecord(2, newRt, GenericRow.of(2, BinaryString.fromString("Bob"), 25)),
                        TABLE_PATH);

        assertThat(events).hasSize(2);

        AddColumnEvent addEvent = (AddColumnEvent) events.get(0);
        assertThat(addEvent.getAddedColumns()).hasSize(2);
        assertThat(addEvent.getAddedColumns().get(0).getAddColumn().getName()).isEqualTo("name");
        assertThat(addEvent.getAddedColumns().get(1).getAddColumn().getName()).isEqualTo("age");
    }

    // ------------------------------------------------------------------
    //  SchemaId changes but fields identical — no events
    // ------------------------------------------------------------------

    @Test
    void testSchemaIdChangeSameFieldsNoEvents() {
        RowType rt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));

        // Seed with schemaId 1
        deserializer.deserialize(
                logRecord(1, rt, GenericRow.of(1, BinaryString.fromString("Alice"))), TABLE_PATH);

        // SchemaId bumped to 2, but fields are identical
        List<Event> events =
                deserializer.deserialize(
                        logRecord(2, rt, GenericRow.of(2, BinaryString.fromString("Bob"))),
                        TABLE_PATH);

        // Only the DataChangeEvent, no schema change
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(DataChangeEvent.class);
    }

    // ------------------------------------------------------------------
    //  Unsupported schema changes — must throw
    // ------------------------------------------------------------------

    @Test
    void testDropColumnThrows() {
        RowType oldRt =
                rowType(
                        field("id", new IntType(false), 1),
                        field("name", new StringType(true), 2),
                        field("age", new IntType(true), 3));
        RowType newRt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));

        deserializer.deserialize(
                logRecord(1, oldRt, GenericRow.of(1, BinaryString.fromString("Alice"), 30)),
                TABLE_PATH);

        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        logRecord(
                                                2,
                                                newRt,
                                                GenericRow.of(2, BinaryString.fromString("Bob"))),
                                        TABLE_PATH))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("columns were dropped")
                .hasMessageContaining("Only ADD COLUMN at last is supported");
    }

    @Test
    void testRenameColumnThrows() {
        RowType oldRt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));
        RowType newRt =
                rowType(
                        field("id", new IntType(false), 1),
                        field(
                                "full_name",
                                new StringType(true),
                                2)); // same fieldId, different name

        deserializer.deserialize(
                logRecord(1, oldRt, GenericRow.of(1, BinaryString.fromString("Alice"))),
                TABLE_PATH);

        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        logRecord(
                                                2,
                                                newRt,
                                                GenericRow.of(2, BinaryString.fromString("Bob"))),
                                        TABLE_PATH))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("renamed")
                .hasMessageContaining("Only ADD COLUMN at last is supported");
    }

    @Test
    void testAlterColumnTypeThrows() {
        RowType oldRt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));
        RowType newRt =
                rowType(
                        field("id", new IntType(false), 1),
                        field("name", new IntType(true), 2)); // same fieldId+name, different type

        deserializer.deserialize(
                logRecord(1, oldRt, GenericRow.of(1, BinaryString.fromString("Alice"))),
                TABLE_PATH);

        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        logRecord(2, newRt, GenericRow.of(2, 42)), TABLE_PATH))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("type changed")
                .hasMessageContaining("Only ADD COLUMN at last is supported");
    }

    @Test
    void testReorderColumnsThrows() {
        RowType oldRt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));
        RowType newRt =
                rowType(
                        field("name", new StringType(true), 2), // swapped positions
                        field("id", new IntType(false), 1));

        deserializer.deserialize(
                logRecord(1, oldRt, GenericRow.of(1, BinaryString.fromString("Alice"))),
                TABLE_PATH);

        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        logRecord(
                                                2,
                                                newRt,
                                                GenericRow.of(BinaryString.fromString("Bob"), 2)),
                                        TABLE_PATH))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("different field ID")
                .hasMessageContaining("Only ADD COLUMN at last is supported");
    }

    @Test
    void testInsertColumnInMiddleThrows() {
        RowType oldRt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));
        // New column "age" inserted before "name" — fieldId at position 1 changed
        RowType newRt =
                rowType(
                        field("id", new IntType(false), 1),
                        field("age", new IntType(true), 3),
                        field("name", new StringType(true), 2));

        deserializer.deserialize(
                logRecord(1, oldRt, GenericRow.of(1, BinaryString.fromString("Alice"))),
                TABLE_PATH);

        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        logRecord(
                                                2,
                                                newRt,
                                                GenericRow.of(
                                                        2, 25, BinaryString.fromString("Bob"))),
                                        TABLE_PATH))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("different field ID")
                .hasMessageContaining("Only ADD COLUMN at last is supported");
    }

    // ------------------------------------------------------------------
    //  Multi-table isolation
    // ------------------------------------------------------------------

    @Test
    void testSchemaIdCacheIsPerTable() {
        TablePath tableA = TablePath.of("db", "table_a");
        TablePath tableB = TablePath.of("db", "table_b");

        RowType rtA = rowType(field("id", new IntType(false), 1));
        RowType rtB =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));

        // Seed table A with schemaId 1
        deserializer.deserialize(logRecord(1, rtA, GenericRow.of(1)), tableA);

        // Seed table B with schemaId 5 (different per-table namespace)
        deserializer.deserialize(
                logRecord(5, rtB, GenericRow.of(1, BinaryString.fromString("Alice"))), tableB);

        // Table A schema evolves: add column
        RowType newRtA =
                rowType(field("id", new IntType(false), 1), field("value", new IntType(true), 10));

        List<Event> events =
                deserializer.deserialize(logRecord(2, newRtA, GenericRow.of(2, 100)), tableA);

        // Should emit AddColumnEvent for table A only
        assertThat(events).hasSize(2);
        assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent addEvent = (AddColumnEvent) events.get(0);
        assertThat(addEvent.tableId()).isEqualTo(TableId.tableId("db", "table_a"));
        assertThat(addEvent.getAddedColumns().get(0).getAddColumn().getName()).isEqualTo("value");

        // Table B unchanged — same schemaId, no schema events
        List<Event> eventsB =
                deserializer.deserialize(
                        logRecord(5, rtB, GenericRow.of(2, BinaryString.fromString("Bob"))),
                        tableB);
        assertThat(eventsB).hasSize(1);
        assertThat(eventsB.get(0)).isInstanceOf(DataChangeEvent.class);
    }
}
