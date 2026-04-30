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

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.util.Collector;

import io.debezium.document.DocumentWriter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlServerEventDeserializer} schema change handling. */
class SqlServerEventDeserializerTest {

    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();

    @Test
    void testCreateAlterDropAreEmitted() throws Exception {
        SqlServerEventDeserializer deserializer =
                new SqlServerEventDeserializer(DebeziumChangelogMode.ALL, true);
        List<Event> events = new ArrayList<>();
        TestCollector collector = new TestCollector(events);

        // CREATE
        SourceRecord createRecord =
                buildSchemaChangeRecord(
                        TableChangeType.CREATE, Collections.singletonList(col("id", false, 1)));
        deserializer.deserialize(createRecord, collector);

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        Schema createSchema = ((CreateTableEvent) events.get(0)).getSchema();
        assertThat(createSchema.getColumns()).hasSize(1);

        // ALTER add column
        SourceRecord alterRecord =
                buildSchemaChangeRecord(
                        TableChangeType.ALTER,
                        Arrays.asList(col("id", false, 1), col("age", true, 2)));
        deserializer.deserialize(alterRecord, collector);

        assertThat(events).hasSize(2);
        assertThat(events.get(1)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent addColumnEvent = (AddColumnEvent) events.get(1);
        assertThat(addColumnEvent.getAddedColumns()).hasSize(1);
        assertThat(addColumnEvent.getAddedColumns().get(0).getAddColumn().getName())
                .isEqualTo("age");

        // DROP
        SourceRecord dropRecord =
                buildSchemaChangeRecord(TableChangeType.DROP, Collections.emptyList());
        deserializer.deserialize(dropRecord, collector);

        assertThat(events).hasSize(3);
        assertThat(events.get(2)).isInstanceOf(DropTableEvent.class);
    }

    private static Column col(String name, boolean optional, int position) {
        return Column.editor()
                .name(name)
                .jdbcType(java.sql.Types.INTEGER)
                .type("INT", "INT")
                .position(position)
                .optional(optional)
                .create();
    }

    private static SourceRecord buildSchemaChangeRecord(TableChangeType type, List<Column> columns)
            throws Exception {
        TableId tableId = new TableId("db0", "dbo", "users");
        TableEditor editor = Table.editor().tableId(tableId);
        columns.forEach(editor::addColumn);
        if (!columns.isEmpty()) {
            editor.setPrimaryKeyNames("id");
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
                        .name("io.debezium.connector.sqlserver.SchemaChangeKey")
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
                        .name("io.debezium.connector.sqlserver.SchemaChangeValue")
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
                "server1.db0.dbo.users",
                null,
                keySchema,
                keyStruct,
                valueSchema,
                valueStruct);
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
