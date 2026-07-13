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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.util.Collector;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresEventDeserializer} data change event handling. */
class PostgresEventDeserializerTest {

    private static final Schema ROW_SCHEMA =
            SchemaBuilder.struct()
                    .name("server1.inventory.products.Value")
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build();

    private static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct()
                    .name("server1.inventory.products.Envelope")
                    .field(Envelope.FieldName.BEFORE, ROW_SCHEMA)
                    .field(Envelope.FieldName.AFTER, ROW_SCHEMA)
                    .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                    .build();

    @Test
    void testUpdateEmitsUpdateEventInAllMode() throws Exception {
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL);

        List<Event> events =
                deserialize(deserializer, buildUpdateRecord(row(1, "spare"), row(1, "scooter")));

        assertThat(events).hasSize(1);
        DataChangeEvent event = (DataChangeEvent) events.get(0);
        assertThat(event.op()).isEqualTo(OperationType.UPDATE);
        assertThat(event.tableId()).isEqualTo(TableId.tableId("inventory", "products"));
        assertThat(event.before()).isNotNull();
        assertThat(event.before().getInt(0)).isEqualTo(1);
        assertThat(event.before().getString(1).toString()).isEqualTo("spare");
        assertThat(event.after().getInt(0)).isEqualTo(1);
        assertThat(event.after().getString(1).toString()).isEqualTo("scooter");
    }

    @Test
    void testUpdateEmitsReplaceEventInUpsertMode() throws Exception {
        PostgresEventDeserializer deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.UPSERT);

        // With REPLICA IDENTITY DEFAULT the before image is unavailable for UPDATE events.
        List<Event> events = deserialize(deserializer, buildUpdateRecord(null, row(1, "scooter")));

        assertThat(events).hasSize(1);
        DataChangeEvent event = (DataChangeEvent) events.get(0);
        assertThat(event.op()).isEqualTo(OperationType.REPLACE);
        assertThat(event.tableId()).isEqualTo(TableId.tableId("inventory", "products"));
        assertThat(event.before()).isNull();
        assertThat(event.after().getInt(0)).isEqualTo(1);
        assertThat(event.after().getString(1).toString()).isEqualTo("scooter");
    }

    private static List<Event> deserialize(
            PostgresEventDeserializer deserializer, SourceRecord record) throws Exception {
        List<Event> events = new ArrayList<>();
        deserializer.deserialize(
                record,
                new Collector<Event>() {
                    @Override
                    public void collect(Event event) {
                        events.add(event);
                    }

                    @Override
                    public void close() {}
                });
        return events;
    }

    private static Struct row(int id, String name) {
        return new Struct(ROW_SCHEMA).put("id", id).put("name", name);
    }

    private static SourceRecord buildUpdateRecord(Struct before, Struct after) {
        Struct value =
                new Struct(VALUE_SCHEMA)
                        .put(Envelope.FieldName.OPERATION, Envelope.Operation.UPDATE.code())
                        .put(Envelope.FieldName.AFTER, after);
        if (before != null) {
            value.put(Envelope.FieldName.BEFORE, before);
        }
        return new SourceRecord(
                Collections.singletonMap("server", "server1"),
                Collections.singletonMap("lsn", 1L),
                "server1.inventory.products",
                null,
                null,
                null,
                VALUE_SCHEMA,
                value);
    }
}
