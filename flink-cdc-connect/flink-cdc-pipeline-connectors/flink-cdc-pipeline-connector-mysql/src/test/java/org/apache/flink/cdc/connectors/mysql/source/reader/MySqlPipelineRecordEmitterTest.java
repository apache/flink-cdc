/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.mysql.source.MySqlEventDeserializer;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.Collector;

import io.debezium.data.Envelope;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.Table;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl.HISTORY_RECORD_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

/** Regression tests for filtering the actual tables in multi-table schema changes. */
class MySqlPipelineRecordEmitterTest {

    private static final String DATABASE = "cdc_filter_repro";
    private static final String INCLUDED = "cdc_filter_repro\\..*";
    private static final String EXCLUDED =
            "cdc_filter_repro.excluded_single,cdc_filter_repro.excluded_multi";
    private static final BinlogOffset OFFSET =
            BinlogOffset.ofBinlogFilePosition("binlog.000374", 751768L);

    @Test
    void testMixedDropRetainsSubscribedTable() throws Exception {
        assertDrop(
                "DROP TABLE keep_multi, excluded_multi",
                "keep_multi,excluded_multi",
                INCLUDED,
                EXCLUDED,
                "keep_multi");
    }

    @Test
    void testMixedDropWithExcludedTableFirst() throws Exception {
        assertDrop(
                "DROP TABLE excluded_multi, keep_multi",
                "excluded_multi,keep_multi",
                INCLUDED,
                EXCLUDED,
                "keep_multi");
    }

    @Test
    void testMixedDropWithSubscribedSourceTable() throws Exception {
        assertDrop(
                "DROP TABLE keep_multi, excluded_multi",
                "keep_multi",
                INCLUDED,
                EXCLUDED,
                "keep_multi");
    }

    @Test
    void testMixedDropWithExcludedSourceTable() throws Exception {
        assertDrop(
                "DROP TABLE keep_multi, excluded_multi",
                "excluded_multi",
                INCLUDED,
                EXCLUDED,
                "keep_multi");
    }

    @Test
    void testDropAllSubscribedTables() throws Exception {
        assertDrop(
                "DROP TABLE keep_multi, keep_other",
                "keep_multi,keep_other",
                INCLUDED,
                EXCLUDED,
                "keep_multi",
                "keep_other");
    }

    @Test
    void testDropAllExcludedTables() throws Exception {
        assertDrop(
                "DROP TABLE excluded_single, excluded_multi",
                "excluded_single,excluded_multi",
                INCLUDED,
                EXCLUDED);
    }

    @Test
    void testDropSingleSubscribedTable() throws Exception {
        assertDrop("DROP TABLE keep_multi", "keep_multi", INCLUDED, EXCLUDED, "keep_multi");
    }

    @Test
    void testDropSingleExcludedTable() throws Exception {
        assertDrop("DROP TABLE excluded_single", "excluded_single", INCLUDED, EXCLUDED);
    }

    @Test
    void testDropIfExistsMixedTables() throws Exception {
        assertDrop(
                "DROP TABLE IF EXISTS keep_multi, excluded_multi",
                "keep_multi,excluded_multi",
                INCLUDED,
                EXCLUDED,
                "keep_multi");
    }

    @Test
    void testDropOnlyExplicitlyIncludedTables() throws Exception {
        assertDrop(
                "DROP TABLE keep_multi, keep_other",
                "keep_multi,keep_other",
                "cdc_filter_repro\\.keep_multi",
                null,
                "keep_multi");
    }

    @Test
    void testDropExcludedRegexTakesPrecedence() throws Exception {
        assertDrop(
                "DROP TABLE keep_multi, excluded_multi",
                "keep_multi,excluded_multi",
                INCLUDED,
                "cdc_filter_repro.excluded_\\.*",
                "keep_multi");
    }

    @Test
    void testDropQualifiedTablesOutsideIncludedDatabase() throws Exception {
        assertDrop(
                "DROP TABLE cdc_filter_repro.keep_multi, other_database.keep_multi",
                "keep_multi",
                INCLUDED,
                EXCLUDED,
                "keep_multi");
    }

    @Test
    void testDropQuotedTableNameContainingComma() throws Exception {
        assertDrop(
                "DROP TABLE `keep,comma`, excluded_multi",
                "keep,comma,excluded_multi",
                INCLUDED,
                EXCLUDED,
                "keep,comma");
    }

    @Test
    void testSchemaChangesDisabled() throws Exception {
        MySqlSourceConfig config = sourceConfig(INCLUDED, EXCLUDED, false);
        TestingReaderOutput<Event> output = new TestingReaderOutput<>();
        MySqlBinlogSplitState state = splitState();
        emitter(config)
                .emitRecord(
                        SourceRecords.fromSingleRecord(
                                schemaRecord(
                                        "DROP TABLE keep_multi, excluded_multi",
                                        "keep_multi,excluded_multi")),
                        output,
                        state);
        assertThat(output.getEmittedRecords()).isEmpty();
    }

    @Test
    void testDeserializerParsesWholeDdlRegardlessOfAggregatedSourceTable() throws Exception {
        MySqlEventDeserializer deserializer =
                new MySqlEventDeserializer(DebeziumChangelogMode.ALL, true, true, false);
        List<Event> events = new ArrayList<>();
        deserializer.deserialize(
                schemaRecord(
                        "DROP TABLE IF EXISTS `keep,comma`, excluded_multi",
                        "keep,comma,excluded_multi"),
                new Collector<Event>() {
                    @Override
                    public void collect(Event event) {
                        events.add(event);
                    }

                    @Override
                    public void close() {}
                });
        assertThat(events)
                .containsExactlyInAnyOrder(
                        new DropTableEvent(TableId.tableId(DATABASE, "keep,comma")),
                        new DropTableEvent(TableId.tableId(DATABASE, "excluded_multi")));
    }

    @Test
    void testFilteredRecordDoesNotLeakEventsIntoNextRecord() throws Exception {
        MySqlPipelineRecordEmitter emitter = emitter(sourceConfig(INCLUDED, EXCLUDED, true));
        TestingReaderOutput<Event> output = new TestingReaderOutput<>();
        MySqlBinlogSplitState state = splitState();
        emitter.emitRecord(
                SourceRecords.fromSingleRecord(
                        schemaRecord(
                                "DROP TABLE excluded_single, excluded_multi",
                                "excluded_single,excluded_multi")),
                output,
                state);
        assertThat(output.getEmittedRecords()).isEmpty();
        emitter.emitRecord(
                SourceRecords.fromSingleRecord(schemaRecord("DROP TABLE keep_multi", "keep_multi")),
                output,
                state);
        assertThat(output.getEmittedRecords())
                .containsExactly(new DropTableEvent(TableId.tableId(DATABASE, "keep_multi")));
    }

    @Test
    void testFilteredCreateStillUpdatesParserSchema() throws Exception {
        MySqlSourceConfig config = sourceConfig(INCLUDED, EXCLUDED, true);
        MySqlEventDeserializer filtered =
                new MySqlEventDeserializer(DebeziumChangelogMode.ALL, true, true, false);
        MySqlEventDeserializer raw =
                new MySqlEventDeserializer(DebeziumChangelogMode.ALL, true, true, false);
        MySqlPipelineRecordEmitter emitter = emitter(config, filtered);
        TestingReaderOutput<Event> output = new TestingReaderOutput<>();
        MySqlBinlogSplitState state = splitState();
        List<Event> rawEvents = new ArrayList<>();
        Collector<Event> collector =
                new Collector<Event>() {
                    @Override
                    public void collect(Event event) {
                        rawEvents.add(event);
                    }

                    @Override
                    public void close() {}
                };
        SourceRecord create =
                schemaRecord(
                        "CREATE TABLE excluded_single (id INT NOT NULL PRIMARY KEY)",
                        "excluded_single");
        raw.deserialize(create, collector);
        emitter.emitRecord(SourceRecords.fromSingleRecord(create), output, state);
        assertThat(output.getEmittedRecords()).isEmpty();
        assertThat(parserTable(filtered)).isNotNull();
        assertThat(parserTable(filtered).columns())
                .extracting(io.debezium.relational.Column::name)
                .containsExactly("id");
        assertThat(parserTable(filtered).primaryKeyColumnNames()).containsExactly("id");
        assertThat(parserTable(filtered)).isEqualTo(parserTable(raw));

        SourceRecord alter =
                schemaRecord(
                        "ALTER TABLE excluded_single ADD COLUMN amount INT", "excluded_single");
        raw.deserialize(alter, collector);
        emitter.emitRecord(SourceRecords.fromSingleRecord(alter), output, state);
        assertThat(output.getEmittedRecords()).isEmpty();
        assertThat(rawEvents).hasSize(2);
        assertThat(parserTable(filtered)).isEqualTo(parserTable(raw));
    }

    @Test
    void testNonSchemaRecordPassesThroughRealDeserializer() throws Exception {
        MySqlSourceConfig config = sourceConfig(INCLUDED, EXCLUDED, true);
        List<Event> events = new ArrayList<>();
        Collector<Event> collector = collecting(events);
        wrappedDeserializer(emitter(config)).deserialize(dataRecord(), collector);
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent event = (DataChangeEvent) events.get(0);
        assertThat(event.tableId()).isEqualTo(TableId.tableId(DATABASE, "excluded_single"));
        assertThat(event.op()).isEqualTo(OperationType.INSERT);
        assertThat(event.before()).isNull();
        assertThat(event.after().getInt(0)).isEqualTo(7);
    }

    @Test
    void testSchemaWrapperPreservesOtherEventsAndCollectorClose() throws Exception {
        Event marker =
                DataChangeEvent.insertEvent(
                        TableId.tableId(DATABASE, "excluded_single"), GenericRecordData.of(7));
        MySqlEventDeserializer delegate =
                new MySqlEventDeserializer(DebeziumChangelogMode.ALL, true, true, false) {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void deserialize(SourceRecord record, Collector<Event> out)
                            throws Exception {
                        super.deserialize(record, out);
                        out.collect(marker);
                        out.close();
                    }
                };
        List<Event> events = new ArrayList<>();
        int[] closeCalls = {0};
        Collector<Event> collector =
                new Collector<Event>() {
                    @Override
                    public void collect(Event event) {
                        events.add(event);
                    }

                    @Override
                    public void close() {
                        closeCalls[0]++;
                    }
                };
        wrappedDeserializer(emitter(sourceConfig(INCLUDED, EXCLUDED, true), delegate))
                .deserialize(
                        schemaRecord("DROP TABLE excluded_single", "excluded_single"), collector);
        assertThat(events).containsExactly(marker);
        assertThat(closeCalls[0]).isEqualTo(1);
    }

    @Test
    void testWrapperDelegatesProducedType() throws Exception {
        TypeInformation<Event> producedType = new EventTypeInfo();
        MySqlEventDeserializer delegate =
                new MySqlEventDeserializer(DebeziumChangelogMode.ALL, true, true, false) {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public TypeInformation<Event> getProducedType() {
                        return producedType;
                    }
                };
        assertThat(
                        wrappedDeserializer(
                                        emitter(sourceConfig(INCLUDED, EXCLUDED, true), delegate))
                                .getProducedType())
                .isSameAs(producedType);
    }

    @SuppressWarnings("unchecked")
    private DebeziumDeserializationSchema<Event> wrappedDeserializer(
            MySqlRecordEmitter<Event> emitter) throws Exception {
        Field field = MySqlRecordEmitter.class.getDeclaredField("debeziumDeserializationSchema");
        field.setAccessible(true);
        return (DebeziumDeserializationSchema<Event>) field.get(emitter);
    }

    private Collector<Event> collecting(List<Event> events) {
        return new Collector<Event>() {
            @Override
            public void collect(Event event) {
                events.add(event);
            }

            @Override
            public void close() {}
        };
    }

    private SourceRecord dataRecord() {
        Schema rowSchema =
                SchemaBuilder.struct().name("test.Row").field("id", Schema.INT32_SCHEMA).build();
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .name("test.Source")
                        .field("db", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .build();
        Envelope envelope =
                Envelope.defineSchema()
                        .withName("test.Envelope")
                        .withRecord(rowSchema)
                        .withSource(sourceSchema)
                        .build();
        Struct value =
                envelope.create(
                        new Struct(rowSchema).put("id", 7),
                        new Struct(sourceSchema)
                                .put("db", DATABASE)
                                .put("table", "excluded_single"),
                        Instant.EPOCH);
        return new SourceRecord(
                Collections.emptyMap(),
                OFFSET.getOffset(),
                "mysql_binlog_source." + DATABASE + ".excluded_single",
                null,
                null,
                envelope.schema(),
                value);
    }

    private Table parserTable(MySqlEventDeserializer deserializer) throws Exception {
        Field field = MySqlEventDeserializer.class.getDeclaredField("tables");
        field.setAccessible(true);
        return ((Tables) field.get(deserializer))
                .forTable(new io.debezium.relational.TableId(DATABASE, null, "excluded_single"));
    }

    private void assertDrop(
            String ddl,
            String sourceTable,
            String include,
            String exclude,
            String... expectedTables)
            throws Exception {
        MySqlSourceConfig config = sourceConfig(include, exclude, true);
        TestingReaderOutput<Event> output = new TestingReaderOutput<>();
        MySqlBinlogSplitState state = splitState();
        emitter(config)
                .emitRecord(
                        SourceRecords.fromSingleRecord(schemaRecord(ddl, sourceTable)),
                        output,
                        state);
        List<Event> expected = new ArrayList<>();
        Arrays.stream(expectedTables)
                .forEach(
                        table ->
                                expected.add(new DropTableEvent(TableId.tableId(DATABASE, table))));
        assertThat(output.getEmittedRecords()).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(state.getStartingOffset()).isEqualByComparingTo(OFFSET);
    }

    private MySqlPipelineRecordEmitter emitter(MySqlSourceConfig config) throws Exception {
        return emitter(
                config,
                new MySqlEventDeserializer(
                        DebeziumChangelogMode.ALL, config.isIncludeSchemaChanges(), true, false));
    }

    private MySqlPipelineRecordEmitter emitter(
            MySqlSourceConfig config, DebeziumDeserializationSchema<Event> deserializer)
            throws Exception {
        return new MySqlPipelineRecordEmitter(
                deserializer,
                new MySqlSourceReaderMetrics(
                        UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                config,
                false);
    }

    private MySqlSourceConfig sourceConfig(String include, String exclude, boolean schemaChanges) {
        MySqlSourceConfigFactory factory =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .username("test-user")
                        .password("<REDACTED>")
                        .serverTimeZone("UTC")
                        .databaseList(DATABASE)
                        .tableList(include)
                        .includeSchemaChanges(schemaChanges);
        if (exclude != null) {
            factory.excludeTableList(exclude);
        }
        return factory.createConfig(0);
    }

    private MySqlBinlogSplitState splitState() {
        return new MySqlBinlogSplitState(
                new MySqlBinlogSplit(
                        "binlog-split",
                        BinlogOffset.ofEarliest(),
                        BinlogOffset.ofNonStopping(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0));
    }

    private SourceRecord schemaRecord(String ddl, String sourceTable) throws Exception {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .name("test.Source")
                        .field("db", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .build();
        Schema keySchema =
                SchemaBuilder.struct()
                        .name(MySqlEventDeserializer.SCHEMA_CHANGE_EVENT_KEY_NAME)
                        .field("databaseName", Schema.STRING_SCHEMA)
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("test.SchemaChange")
                        .field("source", sourceSchema)
                        .field(HISTORY_RECORD_FIELD, Schema.STRING_SCHEMA)
                        .build();
        HistoryRecord history =
                new HistoryRecord(
                        Collections.emptyMap(),
                        OFFSET.getOffset(),
                        DATABASE,
                        null,
                        ddl,
                        new TableChanges());
        Struct value =
                new Struct(valueSchema)
                        .put(
                                "source",
                                new Struct(sourceSchema)
                                        .put("db", DATABASE)
                                        .put("table", sourceTable))
                        .put(
                                HISTORY_RECORD_FIELD,
                                DocumentWriter.defaultWriter().write(history.document()));
        return new SourceRecord(
                Collections.emptyMap(),
                OFFSET.getOffset(),
                "mysql_binlog_source",
                keySchema,
                new Struct(keySchema).put("databaseName", DATABASE),
                valueSchema,
                value);
    }
}
