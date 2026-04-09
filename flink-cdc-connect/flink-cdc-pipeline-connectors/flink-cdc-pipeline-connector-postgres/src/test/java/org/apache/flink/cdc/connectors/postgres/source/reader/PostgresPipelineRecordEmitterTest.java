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

package org.apache.flink.cdc.connectors.postgres.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.source.schema.PostgresSchemaRecord;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumSchemaDataTypeInference;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.data.Envelope;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PostgresPipelineRecordEmitter}. */
class PostgresPipelineRecordEmitterTest {

    private static final TableId DBZ_TABLE_ID = new TableId(null, "public", "orders");
    private static final org.apache.flink.cdc.common.event.TableId CDC_TABLE_ID =
            org.apache.flink.cdc.common.event.TableId.tableId("public", "orders");

    @Test
    void testHiddenSchemaChangeRefreshesCacheLazilyWithoutReemittingCreateTable() throws Exception {
        TestingDebeziumEventDeserializer deserializer = new TestingDebeziumEventDeserializer();
        CreateTableEvent initialCreateTableEvent = createTableEvent(tableWithIdColumn());
        CreateTableEvent refreshedCreateTableEvent = createTableEvent(tableWithIdAndNameColumns());
        deserializer.applyChangeEvent(initialCreateTableEvent);

        TestingPostgresPipelineRecordEmitter emitter =
                new TestingPostgresPipelineRecordEmitter(
                        deserializer,
                        createSourceConfig(false),
                        refreshedCreateTableEvent,
                        Collections.singletonList(createAddNameColumnEvent()));
        StreamSplitState splitState = createStreamSplitState(tableWithIdColumn());
        CollectingSourceOutput output = new CollectingSourceOutput();

        emitter.emitRecord(SourceRecords.fromSingleRecord(dataChangeRecord()), output, splitState);

        assertThat(output.events).containsExactly(initialCreateTableEvent);
        assertThat(emitter.getCreateTableEventCalls).isZero();

        emitter.emitRecord(
                SourceRecords.fromSingleRecord(
                        new PostgresSchemaRecord(tableWithIdAndNameColumns())),
                output,
                splitState);

        assertThat(output.events).containsExactly(initialCreateTableEvent);
        assertThat(emitter.inferSchemaChangeCalls).isZero();
        assertThat(deserializer.getCreateTableEventCache()).doesNotContainKey(DBZ_TABLE_ID);

        emitter.emitRecord(SourceRecords.fromSingleRecord(dataChangeRecord()), output, splitState);

        assertThat(output.events).containsExactly(initialCreateTableEvent);
        assertThat(emitter.getCreateTableEventCalls).isEqualTo(1);
        assertThat(
                        deserializer
                                .getCreateTableEventCache()
                                .get(DBZ_TABLE_ID)
                                .getSchema()
                                .getColumnNames())
                .containsExactly("id", "name");
    }

    @Test
    void testSchemaChangeEmissionStillUsesInferredChangesWhenEnabled() throws Exception {
        TestingDebeziumEventDeserializer deserializer = new TestingDebeziumEventDeserializer();
        CreateTableEvent initialCreateTableEvent = createTableEvent(tableWithIdColumn());
        deserializer.applyChangeEvent(initialCreateTableEvent);

        AddColumnEvent addNameColumnEvent = createAddNameColumnEvent();
        TestingPostgresPipelineRecordEmitter emitter =
                new TestingPostgresPipelineRecordEmitter(
                        deserializer,
                        createSourceConfig(true),
                        createTableEvent(tableWithIdAndNameColumns()),
                        Collections.singletonList(addNameColumnEvent));
        StreamSplitState splitState = createStreamSplitState(tableWithIdColumn());
        CollectingSourceOutput output = new CollectingSourceOutput();

        emitter.emitRecord(
                SourceRecords.fromSingleRecord(
                        new PostgresSchemaRecord(tableWithIdAndNameColumns())),
                output,
                splitState);

        assertThat(output.events).containsExactly(initialCreateTableEvent, addNameColumnEvent);
        assertThat(emitter.inferSchemaChangeCalls).isEqualTo(1);
        assertThat(
                        deserializer
                                .getCreateTableEventCache()
                                .get(DBZ_TABLE_ID)
                                .getSchema()
                                .getColumnNames())
                .containsExactly("id", "name");
    }

    private static PostgresSourceConfig createSourceConfig(boolean includeSchemaChanges) {
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.hostname("localhost");
        factory.port(5432);
        factory.username("postgres");
        factory.password("postgres");
        factory.database("test_db");
        factory.databaseList("test_db");
        factory.tableList("public.orders");
        factory.startupOptions(StartupOptions.latest());
        factory.serverTimeZone("UTC");
        factory.slotName("test_slot");
        factory.decodingPluginName("pgoutput");
        factory.includeSchemaChanges(includeSchemaChanges);
        return factory.create(0);
    }

    private static StreamSplitState createStreamSplitState(Table table) {
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        tableSchemas.put(
                DBZ_TABLE_ID,
                new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table));
        StreamSplit split =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        new PostgresOffsetFactory().createInitialOffset(),
                        new PostgresOffsetFactory().createNoStoppingOffset(),
                        Collections.emptyList(),
                        tableSchemas,
                        0);
        return new StreamSplitState(split);
    }

    private static Table tableWithIdColumn() {
        return Table.editor()
                .tableId(DBZ_TABLE_ID)
                .addColumn(
                        io.debezium.relational.Column.editor()
                                .name("id")
                                .jdbcType(INTEGER)
                                .type("INT4", "INT4")
                                .position(1)
                                .optional(false)
                                .create())
                .setPrimaryKeyNames(Collections.singletonList("id"))
                .create();
    }

    private static Table tableWithIdAndNameColumns() {
        return Table.editor()
                .tableId(DBZ_TABLE_ID)
                .addColumn(
                        io.debezium.relational.Column.editor()
                                .name("id")
                                .jdbcType(INTEGER)
                                .type("INT4", "INT4")
                                .position(1)
                                .optional(false)
                                .create())
                .addColumn(
                        io.debezium.relational.Column.editor()
                                .name("name")
                                .jdbcType(VARCHAR)
                                .type("VARCHAR", "VARCHAR")
                                .position(2)
                                .optional(true)
                                .create())
                .setPrimaryKeyNames(Collections.singletonList("id"))
                .create();
    }

    private static CreateTableEvent createTableEvent(Table table) {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().physicalColumn("id", DataTypes.INT().notNull());
        if (table.columnWithName("name") != null) {
            schemaBuilder.physicalColumn("name", DataTypes.STRING());
        }
        return new CreateTableEvent(CDC_TABLE_ID, schemaBuilder.primaryKey("id").build());
    }

    private static AddColumnEvent createAddNameColumnEvent() {
        return new AddColumnEvent(
                CDC_TABLE_ID,
                Collections.singletonList(
                        AddColumnEvent.last(Column.physicalColumn("name", DataTypes.STRING()))));
    }

    private static SourceRecord dataChangeRecord() {
        org.apache.kafka.connect.data.Schema sourceSchema =
                SchemaBuilder.struct()
                        .field(
                                DATABASE_NAME_KEY,
                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .field(SCHEMA_NAME_KEY, org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .field(TABLE_NAME_KEY, org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .build();
        org.apache.kafka.connect.data.Schema valueSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.SOURCE, sourceSchema)
                        .field(
                                Envelope.FieldName.OPERATION,
                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .build();
        Struct source =
                new Struct(sourceSchema)
                        .put(DATABASE_NAME_KEY, "test_db")
                        .put(SCHEMA_NAME_KEY, DBZ_TABLE_ID.schema())
                        .put(TABLE_NAME_KEY, DBZ_TABLE_ID.table());
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code());

        return new SourceRecord(
                Collections.singletonMap("server", "postgres"),
                Collections.singletonMap(SourceInfo.LSN_KEY, 1L),
                "postgres.public.orders",
                null,
                null,
                valueSchema,
                value);
    }

    private static final class TestingDebeziumEventDeserializer
            extends DebeziumEventDeserializationSchema {

        private TestingDebeziumEventDeserializer() {
            super(new DebeziumSchemaDataTypeInference(), DebeziumChangelogMode.ALL);
        }

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
        protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
            return Collections.emptyList();
        }

        @Override
        protected org.apache.flink.cdc.common.event.TableId getTableId(SourceRecord record) {
            return CDC_TABLE_ID;
        }

        @Override
        protected Map<String, String> getMetadata(SourceRecord record) {
            return Collections.emptyMap();
        }
    }

    private static final class TestingPostgresPipelineRecordEmitter
            extends PostgresPipelineRecordEmitter<Event> {

        private final CreateTableEvent refreshedCreateTableEvent;
        private final List<SchemaChangeEvent> inferredSchemaChangeEvents;
        private int getCreateTableEventCalls;
        private int inferSchemaChangeCalls;

        private TestingPostgresPipelineRecordEmitter(
                TestingDebeziumEventDeserializer deserializer,
                PostgresSourceConfig sourceConfig,
                CreateTableEvent refreshedCreateTableEvent,
                List<SchemaChangeEvent> inferredSchemaChangeEvents) {
            super(
                    deserializer,
                    new SourceReaderMetrics(new TestingReaderContext().metricGroup()),
                    sourceConfig,
                    new PostgresOffsetFactory(),
                    new PostgresDialect(sourceConfig));
            this.refreshedCreateTableEvent = refreshedCreateTableEvent;
            this.inferredSchemaChangeEvents = inferredSchemaChangeEvents;
        }

        @Override
        protected List<SchemaChangeEvent> inferSchemaChangeEvents(
                TableId tableId, Table schemaBefore, Table schemaAfter) {
            inferSchemaChangeCalls++;
            return inferredSchemaChangeEvents;
        }

        @Override
        protected CreateTableEvent getCreateTableEvent(TableId tableId) {
            getCreateTableEventCalls++;
            return refreshedCreateTableEvent;
        }

        @Override
        protected Map<TableId, CreateTableEvent> generateCreateTableEvents() {
            return Collections.emptyMap();
        }
    }

    private static final class CollectingSourceOutput implements SourceOutput<Event> {
        private final List<Event> events = new ArrayList<>();

        @Override
        public void collect(Event record) {
            events.add(record);
        }

        @Override
        public void collect(Event record, long timestamp) {
            events.add(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
