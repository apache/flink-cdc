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

package org.apache.flink.cdc.connectors.db2.source;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.util.Collector;

import io.debezium.document.DocumentWriter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Db2EventDeserializer} schema change handling. */
class Db2EventDeserializerTest {

    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();

    @Test
    void testDb2CdcDecimalBytesAreDecoded() {
        Db2EventDeserializer deserializer =
                new Db2EventDeserializer(DebeziumChangelogMode.ALL, true);
        org.apache.kafka.connect.data.Schema dbzDecimalSchema = Decimal.schema(2);

        assertThat(
                        ((DecimalData)
                                        deserializer.convertToDecimal(
                                                new DecimalType(false, 14, 2),
                                                new byte[] {
                                                    (byte) 0x87,
                                                    0x36,
                                                    0x35,
                                                    0x2e,
                                                    0x34,
                                                    0x33,
                                                    0x32,
                                                    0x31
                                                },
                                                dbzDecimalSchema))
                                .toBigDecimal())
                .isEqualByComparingTo("1234.56");
        assertThat(
                        ((DecimalData)
                                        deserializer.convertToDecimal(
                                                new DecimalType(false, 14, 2),
                                                new byte[] {
                                                    (byte) 0x86,
                                                    0x00,
                                                    0x30,
                                                    0x30,
                                                    0x2e,
                                                    0x38,
                                                    0x39,
                                                    0x34
                                                },
                                                dbzDecimalSchema))
                                .toBigDecimal())
                .isEqualByComparingTo("498.00");
    }

    @Test
    void testDb2CdcDecimalBigDecimalPayloadIsDecoded() {
        Db2EventDeserializer deserializer =
                new Db2EventDeserializer(DebeziumChangelogMode.ALL, true);
        org.apache.kafka.connect.data.Schema dbzDecimalSchema = Decimal.schema(2);

        assertThat(
                        ((DecimalData)
                                        deserializer.convertToDecimal(
                                                new DecimalType(false, 14, 2),
                                                new BigDecimal("-87037107572863666.71"),
                                                dbzDecimalSchema))
                                .toBigDecimal())
                .isEqualByComparingTo("1234.56");
        assertThat(
                        ((DecimalData)
                                        deserializer.convertToDecimal(
                                                new DecimalType(false, 14, 2),
                                                new BigDecimal("-87909734891352081.40"),
                                                dbzDecimalSchema))
                                .toBigDecimal())
                .isEqualByComparingTo("498.00");
    }

    @Test
    void testDb2CdcDecimalStringPayloadIsDecoded() {
        Db2EventDeserializer deserializer =
                new Db2EventDeserializer(DebeziumChangelogMode.ALL, true);
        org.apache.kafka.connect.data.Schema dbzDecimalSchema = Decimal.schema(2);

        assertThat(
                        ((DecimalData)
                                        deserializer.convertToDecimal(
                                                new DecimalType(false, 14, 2),
                                                "-87037107572863666.71",
                                                dbzDecimalSchema))
                                .toBigDecimal())
                .isEqualByComparingTo("1234.56");
        assertThat(
                        ((DecimalData)
                                        deserializer.convertToDecimal(
                                                new DecimalType(false, 14, 2),
                                                "1234.56",
                                                dbzDecimalSchema))
                                .toBigDecimal())
                .isEqualByComparingTo("1234.56");
    }

    @Test
    void testCreateAlterDropAreEmitted() throws Exception {
        Db2EventDeserializer deserializer =
                new Db2EventDeserializer(DebeziumChangelogMode.ALL, true);
        List<Event> events = new ArrayList<>();
        TestCollector collector = new TestCollector(events);

        SourceRecord createRecord =
                buildSchemaChangeRecord(
                        TableChangeType.CREATE, Collections.singletonList(col("ID", false, 1)));
        deserializer.deserialize(createRecord, collector);

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        Schema createSchema = ((CreateTableEvent) events.get(0)).getSchema();
        assertThat(createSchema.getColumns()).hasSize(1);

        SourceRecord alterRecord =
                buildSchemaChangeRecord(
                        TableChangeType.ALTER,
                        Arrays.asList(col("ID", false, 1), col("AGE", true, 2)));
        deserializer.deserialize(alterRecord, collector);

        assertThat(events).hasSize(2);
        assertThat(events.get(1)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent addColumnEvent = (AddColumnEvent) events.get(1);
        assertThat(addColumnEvent.getAddedColumns()).hasSize(1);
        assertThat(addColumnEvent.getAddedColumns().get(0).getAddColumn().getName())
                .isEqualTo("AGE");

        SourceRecord dropRecord =
                buildSchemaChangeRecord(TableChangeType.DROP, Collections.emptyList());
        deserializer.deserialize(dropRecord, collector);

        assertThat(events).hasSize(3);
        assertThat(events.get(2)).isInstanceOf(DropTableEvent.class);
    }

    @Test
    void testAlterDiffUsesRestoredSchemaCache() throws Exception {
        Db2EventDeserializer deserializer =
                new Db2EventDeserializer(DebeziumChangelogMode.ALL, true);
        TableChange restoredTableChange =
                buildTableChanges(
                                TableChangeType.CREATE,
                                Collections.singletonList(col("ID", false, 1)))
                        .iterator()
                        .next();
        Map<TableId, TableChange> tableSchemas = new HashMap<>();
        tableSchemas.put(restoredTableChange.getId(), restoredTableChange);
        deserializer.initializeTableSchemaCacheFromSplitSchemas(tableSchemas);

        List<Event> events = new ArrayList<>();
        TestCollector collector = new TestCollector(events);
        SourceRecord alterRecord =
                buildSchemaChangeRecord(
                        TableChangeType.ALTER,
                        Arrays.asList(col("ID", false, 1), col("AGE", true, 2)));
        deserializer.deserialize(alterRecord, collector);

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent addColumnEvent = (AddColumnEvent) events.get(0);
        assertThat(addColumnEvent.getAddedColumns()).hasSize(1);
        assertThat(addColumnEvent.getAddedColumns().get(0).getAddColumn().getName())
                .isEqualTo("AGE");
    }

    private static Column col(String name, boolean optional, int position) {
        return Column.editor()
                .name(name)
                .jdbcType(Types.INTEGER)
                .type("INTEGER", "INTEGER")
                .position(position)
                .optional(optional)
                .create();
    }

    private static SourceRecord buildSchemaChangeRecord(TableChangeType type, List<Column> columns)
            throws Exception {
        TableId tableId = new TableId("TESTDB", "DB2INST1", "USERS");
        TableChanges tableChanges = buildTableChanges(type, columns);

        HistoryRecord historyRecord =
                new HistoryRecord(
                        Collections.singletonMap("file", "test"),
                        Collections.singletonMap("pos", "1"),
                        tableId.catalog(),
                        tableId.schema(),
                        "ddl",
                        tableChanges);

        String historyJson = DOCUMENT_WRITER.write(historyRecord.document());

        org.apache.kafka.connect.data.Schema keySchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.db2.SchemaChangeKey")
                        .field("databaseName", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .build();
        Struct keyStruct = new Struct(keySchema).put("databaseName", tableId.catalog());

        org.apache.kafka.connect.data.Schema sourceSchema =
                SchemaBuilder.struct()
                        .name("source")
                        .field("dummy", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                        .optional()
                        .build();
        org.apache.kafka.connect.data.Schema valueSchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.db2.SchemaChangeValue")
                        .field("source", sourceSchema)
                        .field(
                                org.apache.flink.cdc.connectors.base.relational
                                        .JdbcSourceEventDispatcher.HISTORY_RECORD_FIELD,
                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .build();

        Struct valueStruct =
                new Struct(valueSchema)
                        .put("source", new Struct(sourceSchema))
                        .put(
                                org.apache.flink.cdc.connectors.base.relational
                                        .JdbcSourceEventDispatcher.HISTORY_RECORD_FIELD,
                                historyJson);

        Map<String, String> partition = new HashMap<>();
        partition.put("server", "server1");
        Map<String, String> offset = new HashMap<>();
        offset.put("lsn", "1");

        return new SourceRecord(
                partition,
                offset,
                "server1.TESTDB.DB2INST1.USERS",
                null,
                keySchema,
                keyStruct,
                valueSchema,
                valueStruct);
    }

    private static TableChanges buildTableChanges(TableChangeType type, List<Column> columns) {
        TableId tableId = new TableId("TESTDB", "DB2INST1", "USERS");
        TableEditor editor = Table.editor().tableId(tableId);
        columns.forEach(editor::addColumn);
        if (!columns.isEmpty()) {
            editor.setPrimaryKeyNames("ID");
        }
        Table table = editor.create();
        TableChanges tableChanges = new TableChanges();
        switch (type) {
            case CREATE:
                tableChanges.create(table);
                break;
            case ALTER:
                tableChanges.alter(table);
                break;
            case DROP:
                tableChanges.drop(table);
                break;
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
        return tableChanges;
    }

    private static class TestCollector implements Collector<Event> {
        private final List<Event> results;

        private TestCollector(List<Event> results) {
            this.results = results;
        }

        @Override
        public void collect(Event record) {
            results.add(record);
        }

        @Override
        public void close() {}
    }
}
