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

package org.apache.flink.cdc.connectors.fluss.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.fluss.source.deserializer.FlussRecordDeserializer;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplitState;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FlussRecordEmitter}, focusing on CreateTableEvent emission during state
 * restoration and schema change detection after failover.
 */
class FlussRecordEmitterTest {

    private static final long TABLE_ID = 1001L;
    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final TableId CDC_TABLE_ID = TableId.tableId("test_db", "test_table");
    private static final PhysicalTablePath PHYSICAL_TABLE_PATH = PhysicalTablePath.of(TABLE_PATH);
    private static final TableBucket TABLE_BUCKET = new TableBucket(TABLE_ID, 0);

    private FlussRecordEmitter<Event> emitter;
    private FlussRecordDeserializer deserializer;
    private SimpleReaderOutput output;

    @BeforeEach
    void setUp() {
        deserializer = new FlussRecordDeserializer();
        emitter = new FlussRecordEmitter<>(deserializer);
        output = new SimpleReaderOutput();
    }

    @Test
    void testSchemaChangeDetectedAfterStateRestoration() throws Exception {
        // State was checkpointed with schema v1 (2 columns)
        RowType oldRt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));
        FlussLogSplit split = logSplitWithSchema(1, oldRt);

        emitter.applySplit(split);

        // First record arrives with schema v2 (3 columns — ADD COLUMN)
        RowType newRt =
                rowType(
                        field("id", new IntType(false), 1),
                        field("name", new StringType(true), 2),
                        field("age", new IntType(true), 3));
        FlussLogSplitState splitState = new FlussLogSplitState(split);
        GenericRow row = GenericRow.of(2, BinaryString.fromString("Bob"), 30);
        emitter.emitRecord(logRecord(2, newRt, row, 100L), output, splitState);

        List<Event> events = output.getCollectedEvents();

        // Expect: CreateTableEvent (from restored schema) + AddColumnEvent (from schema change) +
        // DataChangeEvent
        assertThat(events).hasSize(3);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createEvent = (CreateTableEvent) events.get(0);
        assertThat(createEvent.tableId()).isEqualTo(CDC_TABLE_ID);
        // CreateTableEvent uses the checkpointed schema (v1 with 2 columns)
        assertThat(createEvent.getSchema().getColumns()).hasSize(2);

        assertThat(events.get(1)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent addEvent = (AddColumnEvent) events.get(1);
        assertThat(addEvent.getAddedColumns()).hasSize(1);
        assertThat(addEvent.getAddedColumns().get(0).getAddColumn().getName()).isEqualTo("age");

        assertThat(events.get(2)).isInstanceOf(DataChangeEvent.class);
    }

    @Test
    void testMultipleSplitsSameTableOnlyOneCreateTableEvent() throws Exception {
        RowType rt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));

        // Two splits for the same table
        FlussLogSplit split0 =
                new FlussLogSplit(PHYSICAL_TABLE_PATH, new TableBucket(TABLE_ID, 0), 50L, 1, rt);
        FlussLogSplit split1 =
                new FlussLogSplit(PHYSICAL_TABLE_PATH, new TableBucket(TABLE_ID, 1), 80L, 1, rt);

        emitter.applySplit(split0);
        emitter.applySplit(split1);

        // Emit first record from split0
        FlussLogSplitState splitState0 = new FlussLogSplitState(split0);
        GenericRow row0 = GenericRow.of(1, BinaryString.fromString("Alice"));
        emitter.emitRecord(logRecord(1, rt, row0, 50L), output, splitState0);

        List<Event> events = output.getCollectedEvents();

        // Only ONE CreateTableEvent should be emitted (not two)
        long createTableCount = events.stream().filter(e -> e instanceof CreateTableEvent).count();
        assertThat(createTableCount).isEqualTo(1);
    }

    @Test
    void testSecondRecordDoesNotReEmitCreateTableEvent() throws Exception {
        RowType rt =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));
        FlussLogSplit split = logSplitWithSchema(1, rt);

        emitter.applySplit(split);

        FlussLogSplitState splitState = new FlussLogSplitState(split);

        // First record emits CreateTableEvent for the restored schema.
        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("Alice"));
        emitter.emitRecord(logRecord(1, rt, row1, 100L), output, splitState);

        output.clear();

        // Second record — no CreateTableEvent should be emitted
        GenericRow row2 = GenericRow.of(2, BinaryString.fromString("Bob"));
        emitter.emitRecord(logRecord(1, rt, row2, 101L), output, splitState);

        List<Event> events = output.getCollectedEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(DataChangeEvent.class);
    }

    @Test
    void testCreateTableEventContainsPrimaryKey() throws Exception {
        RowType rt =
                rowType(
                        field("id", new IntType(false), 1),
                        field("name", new StringType(true), 2),
                        field("age", new IntType(true), 3));

        List<String> primaryKeyNames = Collections.singletonList("id");
        FlussLogSplit split = logSplitWithSchema(1, rt);

        emitter.applySplit(split);

        FlussLogSplitState splitState = new FlussLogSplitState(split);
        GenericRow row = GenericRow.of(1, BinaryString.fromString("Alice"), 25);
        emitter.emitRecord(logRecordWithPk(1, rt, row, 100L, primaryKeyNames), output, splitState);

        List<Event> events = output.getCollectedEvents();
        assertThat(events).hasSize(2);

        // Verify CreateTableEvent contains correct primary key
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createEvent = (CreateTableEvent) events.get(0);
        assertThat(createEvent.tableId()).isEqualTo(CDC_TABLE_ID);
        assertThat(createEvent.getSchema().primaryKeys()).containsExactly("id");
        assertThat(createEvent.getSchema().getColumns()).hasSize(3);

        // Verify DataChangeEvent
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
    }

    @Test
    void testCreateTableEventContainsPartitionKeyAfterStateRestoration() throws Exception {
        RowType rt =
                rowType(
                        field("id", new IntType(false), 1),
                        field("name", new StringType(true), 2),
                        field("dt", new StringType(false), 3));

        List<String> primaryKeyNames = Arrays.asList("id", "dt");
        List<String> partitionKeyNames = Collections.singletonList("dt");
        FlussLogSplit split = logSplitWithSchema(1, rt);

        emitter.applySplit(split);

        FlussLogSplitState splitState = new FlussLogSplitState(split);
        GenericRow row =
                GenericRow.of(
                        1, BinaryString.fromString("Alice"), BinaryString.fromString("2026-07-23"));
        emitter.emitRecord(
                logRecordWithKeys(1, rt, row, 100L, primaryKeyNames, partitionKeyNames),
                output,
                splitState);

        List<Event> events = output.getCollectedEvents();
        assertThat(events).hasSize(2);

        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createEvent = (CreateTableEvent) events.get(0);
        assertThat(createEvent.getSchema().primaryKeys()).containsExactly("id", "dt");
        assertThat(createEvent.getSchema().partitionKeys()).containsExactly("dt");

        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static RowType rowType(DataField... fields) {
        return new RowType(Arrays.asList(fields));
    }

    private static DataField field(String name, org.apache.fluss.types.DataType type, int fieldId) {
        return new DataField(name, type, null, fieldId);
    }

    /** Creates a FlussLogSplit simulating a recovered split with schema info. */
    private static FlussLogSplit logSplitWithSchema(int schemaId, RowType rowType) {
        return new FlussLogSplit(PHYSICAL_TABLE_PATH, TABLE_BUCKET, 100L, schemaId, rowType);
    }

    /** Creates a log-phase FlussSourceRecord with a valid schemaId and RowType. */
    private static FlussSourceRecord logRecord(
            int schemaId, RowType rowType, GenericRow row, long offset) {
        return logRecordWithPk(schemaId, rowType, row, offset, Collections.emptyList());
    }

    /** Creates a log-phase FlussSourceRecord with primary key names. */
    private static FlussSourceRecord logRecordWithPk(
            int schemaId,
            RowType rowType,
            GenericRow row,
            long offset,
            List<String> primaryKeyNames) {
        return logRecordWithKeys(
                schemaId, rowType, row, offset, primaryKeyNames, Collections.emptyList());
    }

    /** Creates a log-phase FlussSourceRecord with primary and partition key names. */
    private static FlussSourceRecord logRecordWithKeys(
            int schemaId,
            RowType rowType,
            GenericRow row,
            long offset,
            List<String> primaryKeyNames,
            List<String> partitionKeyNames) {
        return new FlussSourceRecord(
                new ScanRecord(
                        TABLE_ID,
                        schemaId,
                        offset,
                        System.currentTimeMillis(),
                        ChangeType.INSERT,
                        row,
                        -1),
                TABLE_PATH,
                rowType,
                FlussSourceRecord.NO_READ_RECORDS_COUNT,
                primaryKeyNames,
                partitionKeyNames);
    }

    private static class SimpleReaderOutput implements SourceOutput<Event> {

        private final List<Event> collected = new ArrayList<>();

        @Override
        public void collect(Event record) {
            collected.add(record);
        }

        @Override
        public void collect(Event record, long timestamp) {
            collected.add(record);
        }

        List<Event> getCollectedEvents() {
            return collected;
        }

        void clear() {
            collected.clear();
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
