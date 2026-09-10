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

package org.apache.flink.cdc.connectors.mysql.source.reader;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumSchemaDataTypeInference;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import io.debezium.relational.Tables;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MySqlPipelineRecordEmitter}. */
class MySqlPipelineRecordEmitterTest {

    @Test
    void testBatchEmitCreateTableEventsWhenEnabled() throws Exception {
        MySqlSourceConfig config = createSourceConfig(true);
        DebeziumEventDeserializationSchema schema = createStubSchema();

        // Pre-populate cache with CreateTableEvents for 3 tables
        Map<io.debezium.relational.TableId, CreateTableEvent> cache =
                schema.getCreateTableEventCache();
        TableId table1 = TableId.tableId("test_db", "table1");
        TableId table2 = TableId.tableId("test_db", "table2");
        TableId table3 = TableId.tableId("test_db", "table3");
        cache.put(
                new io.debezium.relational.TableId("test_db", null, "table1"),
                createTableEvent(table1));
        cache.put(
                new io.debezium.relational.TableId("test_db", null, "table2"),
                createTableEvent(table2));
        cache.put(
                new io.debezium.relational.TableId("test_db", null, "table3"),
                createTableEvent(table3));

        MySqlPipelineRecordEmitter emitter =
                new MySqlPipelineRecordEmitter(
                        schema,
                        new MySqlSourceReaderMetrics(
                                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                        config,
                        false);

        // Feed a dummy record to trigger batch emit
        TestingReaderOutput<Event> output = new TestingReaderOutput<>();
        emitter.emitRecord(
                SourceRecords.fromSingleRecord(createDummyRecord()),
                output,
                createBinlogSplitState());

        List<Event> emitted = new ArrayList<>(output.getEmittedRecords());
        // Should emit exactly 3 CreateTableEvents (dummy record is unknown type, skipped by
        // parent)
        assertThat(emitted).hasSize(3);
        assertThat(emitted.get(0)).isInstanceOf(CreateTableEvent.class);
        assertThat(emitted.get(1)).isInstanceOf(CreateTableEvent.class);
        assertThat(emitted.get(2)).isInstanceOf(CreateTableEvent.class);
    }

    @Test
    void testBatchEmitOnlyOnce() throws Exception {
        MySqlSourceConfig config = createSourceConfig(true);
        DebeziumEventDeserializationSchema schema = createStubSchema();

        Map<io.debezium.relational.TableId, CreateTableEvent> cache =
                schema.getCreateTableEventCache();
        cache.put(
                new io.debezium.relational.TableId("test_db", null, "table1"),
                createTableEvent(TableId.tableId("test_db", "table1")));

        MySqlPipelineRecordEmitter emitter =
                new MySqlPipelineRecordEmitter(
                        schema,
                        new MySqlSourceReaderMetrics(
                                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                        config,
                        false);

        // First record: should batch emit all CreateTableEvents
        TestingReaderOutput<Event> output1 = new TestingReaderOutput<>();
        emitter.emitRecord(
                SourceRecords.fromSingleRecord(createDummyRecord()),
                output1,
                createBinlogSplitState());
        assertThat(output1.getEmittedRecords()).hasSize(1);

        // Second record: no more batch emit (shouldEmitAllCreateTableEventsInSnapshotMode=false)
        TestingReaderOutput<Event> output2 = new TestingReaderOutput<>();
        emitter.emitRecord(
                SourceRecords.fromSingleRecord(createDummyRecord()),
                output2,
                createBinlogSplitState());
        assertThat(output2.getEmittedRecords()).isEmpty();
    }

    @Test
    void testNoBatchEmitWhenDisabled() throws Exception {
        MySqlSourceConfig config = createSourceConfig(false);
        DebeziumEventDeserializationSchema schema = createStubSchema();

        Map<io.debezium.relational.TableId, CreateTableEvent> cache =
                schema.getCreateTableEventCache();
        cache.put(
                new io.debezium.relational.TableId("test_db", null, "table1"),
                createTableEvent(TableId.tableId("test_db", "table1")));

        MySqlPipelineRecordEmitter emitter =
                new MySqlPipelineRecordEmitter(
                        schema,
                        new MySqlSourceReaderMetrics(
                                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                        config,
                        false);

        // With batch disabled and not in snapshot mode, no batch emit should occur
        TestingReaderOutput<Event> output = new TestingReaderOutput<>();
        emitter.emitRecord(
                SourceRecords.fromSingleRecord(createDummyRecord()),
                output,
                createBinlogSplitState());
        // No CreateTableEvents emitted because batch is disabled and the record is unrecognized
        assertThat(output.getEmittedRecords()).isEmpty();
    }

    @Test
    void testBatchEmitEmptyCacheDoesNotEmit() throws Exception {
        MySqlSourceConfig config = createSourceConfig(true);
        DebeziumEventDeserializationSchema schema = createStubSchema();
        // Cache is empty

        MySqlPipelineRecordEmitter emitter =
                new MySqlPipelineRecordEmitter(
                        schema,
                        new MySqlSourceReaderMetrics(
                                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                        config,
                        false);

        TestingReaderOutput<Event> output = new TestingReaderOutput<>();
        emitter.emitRecord(
                SourceRecords.fromSingleRecord(createDummyRecord()),
                output,
                createBinlogSplitState());
        // Cache is empty, so nothing is emitted
        assertThat(output.getEmittedRecords()).isEmpty();
    }

    @Test
    void testConfigDefaultFalse() {
        MySqlSourceConfig config = createSourceConfig(false);
        assertThat(config.isEmitCreateTableEventsInBatchEnabled()).isFalse();
    }

    @Test
    void testConfigEnabledTrue() {
        MySqlSourceConfig config = createSourceConfig(true);
        assertThat(config.isEmitCreateTableEventsInBatchEnabled()).isTrue();
    }

    // ---- helpers ----

    private static MySqlSourceConfig createSourceConfig(boolean emitCreateTableEventsInBatch) {
        return new MySqlSourceConfigFactory()
                .hostname("localhost")
                .port(3306)
                .username("test")
                .password("test")
                .databaseList("test_db")
                .tableList("test_db\\.test_table")
                .emitCreateTableEventsInBatchEnabled(emitCreateTableEventsInBatch)
                .createConfig(0);
    }

    private static DebeziumEventDeserializationSchema createStubSchema() {
        return new DebeziumEventDeserializationSchema(
                new DebeziumSchemaDataTypeInference(), DebeziumChangelogMode.ALL) {
            @Override
            protected boolean isDataChangeRecord(SourceRecord record) {
                return false;
            }

            @Override
            protected boolean isSchemaChangeRecord(SourceRecord record) {
                return false;
            }

            @Override
            public List<DataChangeEvent> deserializeDataChangeRecord(SourceRecord record) {
                return Collections.emptyList();
            }

            @Override
            public List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
                return Collections.emptyList();
            }

            @Override
            protected org.apache.flink.cdc.common.event.TableId getTableId(
                    SourceRecord record) {
                return null;
            }

            @Override
            protected Map<String, String> getMetadata(SourceRecord record) {
                return Collections.emptyMap();
            }
        };
    }

    private static MySqlBinlogSplitState createBinlogSplitState() {
        return new MySqlBinlogSplitState(
                new MySqlBinlogSplit(
                        "binlog-split",
                        BinlogOffset.ofEarliest(),
                        BinlogOffset.ofNonStopping(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0));
    }

    /**
     * Creates a SourceRecord that is not recognized as any special type (not data change, not
     * schema change, not watermark, etc.), so the parent emitter just skips it. This lets us test
     * the batch CreateTableEvent emission in isolation.
     */
    private static SourceRecord createDummyRecord() {
        org.apache.kafka.connect.data.Schema valueSchema =
                SchemaBuilder.struct().field("dummy", org.apache.kafka.connect.data.Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("dummy", "test");
        return new SourceRecord(
                Collections.singletonMap("server", "mysql"),
                Collections.singletonMap("file", "binlog.000001"),
                "test_db.test_table",
                null,
                null,
                valueSchema,
                value);
    }

    private static CreateTableEvent createTableEvent(TableId tableId) {
        return new CreateTableEvent(tableId, Schema.newBuilder().build());
    }
}
