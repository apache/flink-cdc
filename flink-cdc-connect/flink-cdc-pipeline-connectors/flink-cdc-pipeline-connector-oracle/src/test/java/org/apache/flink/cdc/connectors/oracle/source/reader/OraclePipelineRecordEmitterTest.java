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

package org.apache.flink.cdc.connectors.oracle.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import io.debezium.relational.TableId;
import oracle.sql.ROWID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link OraclePipelineRecordEmitter}. */
class OraclePipelineRecordEmitterTest {

    private static final String LOWER_ROWID = "AAAzIdACKAAABWCAAA";
    private static final String UPPER_ROWID = "AAAzIdAC/AACWIPAAB";
    private static final String TABLE_TOPIC = "ORCLCDB.DEBEZIUM.CHUNK_KEY_NO_PK";
    private static final TableId SOURCE_TABLE_ID =
            new TableId("ORCLCDB", "DEBEZIUM", "CHUNK_KEY_NO_PK");

    @Test
    void testRowIdSnapshotSplitSkipsRecordOutsideUpperBound() throws Exception {
        OraclePipelineRecordEmitter emitter = createEmitter();
        SnapshotSplitState splitState = createSnapshotSplitState(null, UPPER_ROWID, true);
        CollectingSourceOutput output = new CollectingSourceOutput();

        emitter.emitRecord(
                SourceRecords.fromSingleRecord(createDataChangeRecord(UPPER_ROWID)),
                output,
                splitState);

        assertThat(output.events).isEmpty();
    }

    @Test
    void testRowIdSnapshotSplitEmitsRecordWithinRange() throws Exception {
        OraclePipelineRecordEmitter emitter = createEmitter();
        SnapshotSplitState splitState = createSnapshotSplitState(null, UPPER_ROWID, true);
        CollectingSourceOutput output = new CollectingSourceOutput();

        emitter.emitRecord(
                SourceRecords.fromSingleRecord(createDataChangeRecord(LOWER_ROWID)),
                output,
                splitState);

        assertThat(output.events).hasSize(1);
    }

    @Test
    void testRowIdSnapshotSplitAcceptsRecordWithoutRowId() throws Exception {
        OraclePipelineRecordEmitter emitter = createEmitter();
        SnapshotSplitState splitState = createSnapshotSplitState(null, LOWER_ROWID, true);
        CollectingSourceOutput output = new CollectingSourceOutput();

        emitter.emitRecord(
                SourceRecords.fromSingleRecord(createDataChangeRecord(null)), output, splitState);

        assertThat(output.events).hasSize(1);
    }

    @Test
    void testIsRowIdChunkKeyUsesSplitTypeInsteadOfBoundaryNullability() throws SQLException {
        SnapshotSplit rowIdSplit =
                new SnapshotSplit(
                        SOURCE_TABLE_ID,
                        0,
                        RowType.of(
                                new org.apache.flink.table.types.logical.LogicalType[] {
                                    DataTypes.STRING().getLogicalType()
                                },
                                new String[] {ROWID.class.getSimpleName()}),
                        null,
                        null,
                        null,
                        Collections.emptyMap());
        SnapshotSplit pkSplit =
                new SnapshotSplit(
                        SOURCE_TABLE_ID,
                        1,
                        RowType.of(
                                new org.apache.flink.table.types.logical.LogicalType[] {
                                    DataTypes.INT().getLogicalType()
                                },
                                new String[] {"ID"}),
                        null,
                        null,
                        null,
                        Collections.emptyMap());

        assertThat(OraclePipelineRecordEmitter.isRowIdChunkKey(rowIdSplit)).isTrue();
        assertThat(OraclePipelineRecordEmitter.isRowIdChunkKey(pkSplit)).isFalse();
    }

    private OraclePipelineRecordEmitter createEmitter() {
        SourceReaderContext readerContext = new TestingReaderContext();
        SourceReaderMetrics metrics = new SourceReaderMetrics(readerContext.metricGroup());
        HashMap<TableId, CreateTableEvent> createTableEventCache = new HashMap<>();
        createTableEventCache.put(
                SOURCE_TABLE_ID,
                new CreateTableEvent(
                        org.apache.flink.cdc.common.event.TableId.tableId(
                                SOURCE_TABLE_ID.schema(), SOURCE_TABLE_ID.table()),
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "ID", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .build()));

        return new OraclePipelineRecordEmitter(
                new TestEventDeserializer(),
                metrics,
                false,
                new NoOpOffsetFactory(),
                null,
                createTableEventCache,
                new HashSet<>(Collections.singleton(SOURCE_TABLE_ID)),
                false);
    }

    private SnapshotSplitState createSnapshotSplitState(
            String splitStart, String splitEnd, boolean rowIdChunkKey) throws SQLException {
        RowType splitType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            (rowIdChunkKey ? DataTypes.STRING() : DataTypes.INT()).getLogicalType()
                        },
                        new String[] {rowIdChunkKey ? ROWID.class.getSimpleName() : "ID"});
        SnapshotSplit split =
                new SnapshotSplit(
                        SOURCE_TABLE_ID,
                        0,
                        splitType,
                        splitStart == null ? null : new Object[] {new ROWID(splitStart)},
                        splitEnd == null ? null : new Object[] {new ROWID(splitEnd)},
                        null,
                        Collections.emptyMap());
        return new SnapshotSplitState(split);
    }

    private SourceRecord createDataChangeRecord(String rowId) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field(DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .build();
        Schema rowSchema =
                SchemaBuilder.struct()
                        .field("ID", Schema.INT32_SCHEMA)
                        .field("ROWID", Schema.OPTIONAL_STRING_SCHEMA)
                        .optional()
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .field("op", Schema.STRING_SCHEMA)
                        .field("source", sourceSchema)
                        .field("after", rowSchema)
                        .build();

        Struct value =
                new Struct(valueSchema)
                        .put("op", "r")
                        .put(
                                "source",
                                new Struct(sourceSchema)
                                        .put(DATABASE_NAME_KEY, SOURCE_TABLE_ID.catalog())
                                        .put(SCHEMA_NAME_KEY, SOURCE_TABLE_ID.schema())
                                        .put(TABLE_NAME_KEY, SOURCE_TABLE_ID.table()));
        if (rowId != null) {
            value.put("after", new Struct(rowSchema).put("ID", 1).put("ROWID", rowId));
        } else {
            value.put("after", new Struct(rowSchema).put("ID", 1).put("ROWID", null));
        }

        return new SourceRecord(
                Collections.singletonMap("server", "oracle"),
                Collections.emptyMap(),
                TABLE_TOPIC,
                null,
                null,
                valueSchema,
                value);
    }

    private static class TestEventDeserializer
            implements org.apache.flink.cdc.debezium.DebeziumDeserializationSchema<Event> {

        @Override
        public void deserialize(SourceRecord record, Collector<Event> out) {
            out.collect(new TestEvent());
        }

        @Override
        public TypeInformation<Event> getProducedType() {
            return TypeInformation.of(Event.class);
        }
    }

    private static class TestEvent implements Event {}

    private static class CollectingSourceOutput implements SourceOutput<Event> {
        private final java.util.List<Event> events = new java.util.ArrayList<>();

        @Override
        public void collect(Event record) {
            events.add(record);
        }

        @Override
        public void collect(Event record, long timestamp) {
            events.add(record);
        }

        @Override
        public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }

    private static class NoOpOffsetFactory extends OffsetFactory {
        @Override
        public Offset newOffset(Map<String, String> offset) {
            throw new UnsupportedOperationException("Offset creation is not used in this test.");
        }

        @Override
        public Offset newOffset(String filename, Long position) {
            throw new UnsupportedOperationException("Offset creation is not used in this test.");
        }

        @Override
        public Offset newOffset(Long position) {
            throw new UnsupportedOperationException("Offset creation is not used in this test.");
        }

        @Override
        public Offset createTimestampOffset(long timestampMillis) {
            throw new UnsupportedOperationException("Offset creation is not used in this test.");
        }

        @Override
        public Offset createInitialOffset() {
            throw new UnsupportedOperationException("Offset creation is not used in this test.");
        }

        @Override
        public Offset createNoStoppingOffset() {
            throw new UnsupportedOperationException("Offset creation is not used in this test.");
        }
    }
}
