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
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.fluss.source.deserializer.FlussDeserializer;
import org.apache.flink.cdc.connectors.fluss.source.deserializer.FlussRecordDeserializer;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplitState;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplitState;
import org.apache.flink.metrics.SimpleCounter;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    private SimpleCounter numRecordsIn;
    private FlussRecordDeserializer deserializer;
    private SimpleReaderOutput<Event> output;

    @BeforeEach
    void setUp() {
        deserializer = new FlussRecordDeserializer();
        numRecordsIn = new SimpleCounter();
        emitter = new FlussRecordEmitter<>(deserializer, numRecordsIn);
        output = new SimpleReaderOutput<>();
    }

    @Test
    void testProgressDoesNotEmitOrConsumePendingTableEvents() throws Exception {
        FlussDeserializer<String> restoringDeserializer =
                new FlussDeserializer<String>() {
                    @Override
                    public List<String> restoreState(
                            TablePath tablePath, int schemaId, RowType rowType) {
                        return Collections.singletonList("schema");
                    }

                    @Override
                    public List<String> deserialize(
                            FlussSourceRecord element, TablePath tablePath) {
                        assertThat(element.isProgressOnly()).isFalse();
                        return Collections.singletonList("row");
                    }
                };
        FlussRecordEmitter<String> restoringEmitter =
                new FlussRecordEmitter<>(restoringDeserializer, numRecordsIn);
        SimpleReaderOutput<String> restoredOutput = new SimpleReaderOutput<>();
        RowType rowType = rowType(field("id", new IntType(false), 1));
        FlussLogSplit split = logSplitWithSchema(1, rowType);
        FlussLogSplitState state = new FlussLogSplitState(split);
        restoringEmitter.applySplit(split);
        numRecordsIn.inc();
        restoringEmitter.emitRecord(
                FlussSourceRecord.forProgress(TABLE_PATH, 105L), restoredOutput, state);
        assertThat(restoredOutput.getCollectedEvents()).isEmpty();
        assertThat(state.toFlussSplit().getStartingOffset()).isEqualTo(105L);
        restoringEmitter.emitRecord(
                logRecord(1, rowType, GenericRow.of(7), 105L), restoredOutput, state);
        assertThat(restoredOutput.getCollectedEvents()).containsExactly("schema", "row");
        restoredOutput.clear();
        restoringEmitter.emitRecord(
                logRecord(1, rowType, GenericRow.of(8), 106L), restoredOutput, state);
        assertThat(restoredOutput.getCollectedEvents()).containsExactly("row");
    }

    @Test
    void testProgressPreservesPendingSchemaUntilARealRecord() throws Exception {
        RowType oldType = rowType(field("id", new IntType(false), 1));
        FlussLogSplit split = logSplitWithSchema(1, oldType);
        FlussLogSplitState state = new FlussLogSplitState(split);
        emitter.applySplit(split);
        numRecordsIn.inc();
        emitter.emitRecord(FlussSourceRecord.forProgress(TABLE_PATH, 105L), output, state);

        assertThat(state.toFlussSplit().getStartingOffset()).isEqualTo(105L);
        assertThat(state.toFlussSplit().getSchemaId()).isEqualTo(1);
        assertThat(state.toFlussSplit().getRowType()).isEqualTo(oldType);
        assertThat(output.getCollectedEvents()).isEmpty();
        assertThat(numRecordsIn.getCount()).isZero();

        RowType newType =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));
        numRecordsIn.inc();
        emitter.emitRecord(
                logRecord(2, newType, GenericRow.of(7, BinaryString.fromString("name")), 105L),
                output,
                state);
        assertThat(output.getCollectedEvents())
                .extracting(Event::getClass)
                .containsExactly(
                        CreateTableEvent.class, AddColumnEvent.class, DataChangeEvent.class);
        assertThat(
                        ((CreateTableEvent) output.getCollectedEvents().get(0))
                                .getSchema()
                                .getColumnNames())
                .containsExactly("id");
        assertThat(state.toFlussSplit().getStartingOffset()).isEqualTo(106L);
        assertThat(numRecordsIn.getCount()).isEqualTo(1L);
    }

    @Test
    void testFirstLogProgressFinishesSnapshotAndPreservesSnapshotPositionAndSchema()
            throws Exception {
        RowType rowType = rowType(field("id", new IntType(false), 1));
        FlussHybridSnapshotLogSplit split =
                new FlussHybridSnapshotLogSplit(
                        PHYSICAL_TABLE_PATH, TABLE_BUCKET, 1L, 3L, 100L, false, 1, rowType);
        FlussHybridSnapshotLogSplitState state = new FlussHybridSnapshotLogSplitState(split);
        emitter.applySplit(split);
        emitter.emitRecord(
                new FlussSourceRecord(
                        new ScanRecord(
                                TABLE_ID, 1, -1L, -1L, ChangeType.INSERT, GenericRow.of(7), 1),
                        TABLE_PATH,
                        rowType,
                        4L,
                        Collections.singletonList("id")),
                output,
                state);
        assertThat(state.toFlussSplit().isSnapshotFinished()).isFalse();
        assertThat(state.toFlussSplit().getRecordsToSkip()).isEqualTo(4L);
        output.clear();

        numRecordsIn.inc();
        emitter.emitRecord(FlussSourceRecord.forProgress(TABLE_PATH, 105L), output, state);
        FlussHybridSnapshotLogSplit saved = state.toFlussSplit();
        assertThat(saved.isSnapshotFinished()).isTrue();
        assertThat(saved.getLogStartingOffset()).isEqualTo(105L);
        assertThat(saved.getRecordsToSkip()).isEqualTo(4L);
        assertThat(saved.getSchemaId()).isEqualTo(1);
        assertThat(saved.getRowType()).isEqualTo(rowType);
        assertThat(output.getCollectedEvents()).isEmpty();
        assertThat(numRecordsIn.getCount()).isZero();
    }

    @Test
    void testInactiveProgressDoesNotAdvanceStateOrCountAsInput() throws Exception {
        RowType rowType = rowType(field("id", new IntType(false), 1));
        FlussLogSplit split = logSplitWithSchema(1, rowType);
        FlussLogSplitState state = new FlussLogSplitState(split);
        emitter.applySplit(split);
        emitter.removeTable(TABLE_PATH);
        numRecordsIn.inc();
        emitter.emitRecord(FlussSourceRecord.forProgress(TABLE_PATH, 105L), output, state);
        assertThat(state.toFlussSplit().getStartingOffset()).isEqualTo(100L);
        assertThat(state.toFlussSplit().getSchemaId()).isEqualTo(1);
        assertThat(output.getCollectedEvents()).isEmpty();
        assertThat(numRecordsIn.getCount()).isZero();
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

    @ParameterizedTest(name = "olderSplitFirst = {0}")
    @ValueSource(booleans = {true, false})
    void testHighestRestoredSchemaIsRetained(boolean olderSplitFirst) throws Exception {
        RowType schema1 =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));
        RowType schema2 =
                rowType(
                        field("id", new IntType(false), 1),
                        field("name", new StringType(true), 2),
                        field("age", new IntType(true), 3));
        FlussLogSplit schema1Split =
                new FlussLogSplit(
                        PHYSICAL_TABLE_PATH, new TableBucket(TABLE_ID, 0), 50L, 1, schema1);
        FlussLogSplit schema2Split =
                new FlussLogSplit(
                        PHYSICAL_TABLE_PATH, new TableBucket(TABLE_ID, 1), 80L, 2, schema2);

        emitter.applySplit(olderSplitFirst ? schema1Split : schema2Split);
        emitter.applySplit(olderSplitFirst ? schema2Split : schema1Split);

        emitter.emitRecord(
                logRecord(1, schema1, GenericRow.of(2, BinaryString.fromString("Bob")), 50L),
                output,
                new FlussLogSplitState(schema1Split));

        assertThat(output.getCollectedEvents()).hasSize(2);
        assertThat(output.getCollectedEvents().get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) output.getCollectedEvents().get(0);
        assertThat(createTableEvent.getSchema().getColumnNames())
                .containsExactly("id", "name", "age");
        assertThat(output.getCollectedEvents().get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent = (DataChangeEvent) output.getCollectedEvents().get(1);
        assertThat(dataChangeEvent.after().getArity()).isEqualTo(3);
        assertThat(dataChangeEvent.after().isNullAt(2)).isTrue();
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
    void testRemovedTableDoesNotEmitOrAdvanceStateUntilItIsReadded() throws Exception {
        RowType rowType =
                rowType(field("id", new IntType(false), 1), field("name", new StringType(true), 2));
        FlussLogSplit split = logSplitWithSchema(1, rowType);
        FlussLogSplitState splitState = new FlussLogSplitState(split);
        emitter.applySplit(split);
        emitter.removeTable(TABLE_PATH);

        emitter.emitRecord(
                logRecord(1, rowType, GenericRow.of(1, BinaryString.fromString("late")), 100L),
                output,
                splitState);

        assertThat(output.getCollectedEvents()).isEmpty();
        assertThat(splitState.toFlussSplit().getStartingOffset()).isEqualTo(100L);

        emitter.applySplit(split);
        emitter.emitRecord(
                logRecord(1, rowType, GenericRow.of(2, BinaryString.fromString("readded")), 100L),
                output,
                splitState);
        assertThat(output.getCollectedEvents())
                .extracting(Event::getClass)
                .containsExactly(CreateTableEvent.class, DataChangeEvent.class);
        assertThat(splitState.toFlussSplit().getStartingOffset()).isEqualTo(101L);
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

    private static DataField field(String name, DataType type, int fieldId) {
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

    static class SimpleReaderOutput<T> implements ReaderOutput<T> {

        private final List<T> collected = new ArrayList<>();

        @Override
        public SourceOutput<T> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}

        @Override
        public void collect(T record) {
            collected.add(record);
        }

        @Override
        public void collect(T record, long timestamp) {
            collected.add(record);
        }

        List<T> getCollectedEvents() {
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
