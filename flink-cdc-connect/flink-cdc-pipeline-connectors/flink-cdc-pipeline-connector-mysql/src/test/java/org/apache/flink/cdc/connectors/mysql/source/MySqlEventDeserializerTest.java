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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MySqlEventDeserializer}. */
class MySqlEventDeserializerTest {

    private static final TableId PRODUCTS = TableId.tableId("inventory", "products");

    @Test
    void testDataTypeCacheLifecycle() throws Exception {
        MySqlEventDeserializer deserializer = createDeserializer();
        Schema rowSchema = rowSchema();

        DataChangeEvent first = deserializeDataRecord(deserializer, rowSchema, 1, "scooter");
        assertRecord(first.after(), 1, "scooter");
        assertThat(getDataTypeCache(deserializer)).hasSize(1).containsKey(PRODUCTS);

        DataChangeEvent second = deserializeDataRecord(deserializer, rowSchema, 2, "car battery");
        assertRecord(second.after(), 2, "car battery");
        assertThat(getDataTypeCache(deserializer)).hasSize(1).containsKey(PRODUCTS);

        List<? extends Event> schemaChangeEvents = deserializer.deserialize(schemaChangeRecord());
        assertThat(schemaChangeEvents).isEmpty();
        assertThat(getDataTypeCache(deserializer)).isEmpty();

        DataChangeEvent third = deserializeDataRecord(deserializer, rowSchema, 3, "12-pack drill");
        assertRecord(third.after(), 3, "12-pack drill");
        assertThat(getDataTypeCache(deserializer)).hasSize(1).containsKey(PRODUCTS);
    }

    @Test
    void testDataTypeCacheIsSeparatedByTableId() throws Exception {
        MySqlEventDeserializer deserializer = createDeserializer();
        Schema rowSchema = rowSchema();
        TableId customers = TableId.tableId("inventory", "customers");

        deserializeDataRecord(deserializer, rowSchema, PRODUCTS, 1, "scooter");
        assertThat(getDataTypeCache(deserializer)).hasSize(1).containsKey(PRODUCTS);

        deserializeDataRecord(deserializer, rowSchema, customers, 2, "alice");
        assertThat(getDataTypeCache(deserializer)).hasSize(2).containsKeys(PRODUCTS, customers);

        deserializeDataRecord(deserializer, rowSchema, PRODUCTS, 3, "hammer");
        assertThat(getDataTypeCache(deserializer)).hasSize(2).containsKeys(PRODUCTS, customers);
    }

    @Test
    void testDataTypeCacheNormalizesCaseInsensitiveTableId() throws Exception {
        MySqlEventDeserializer deserializer = createDeserializer(true);
        Schema rowSchema = rowSchema();
        TableId upperCaseProducts = TableId.tableId("Inventory", "Products");

        deserializeDataRecord(deserializer, rowSchema, upperCaseProducts, 1, "scooter");
        assertThat(getDataTypeCache(deserializer)).hasSize(1).containsKey(PRODUCTS);

        deserializeDataRecord(deserializer, rowSchema, PRODUCTS, 2, "car battery");
        assertThat(getDataTypeCache(deserializer)).hasSize(1).containsKey(PRODUCTS);
    }

    private static MySqlEventDeserializer createDeserializer() {
        return createDeserializer(false);
    }

    private static MySqlEventDeserializer createDeserializer(boolean isTableIdCaseInsensitive) {
        return new MySqlEventDeserializer(
                DebeziumChangelogMode.ALL,
                false,
                Collections.emptyList(),
                false,
                false,
                isTableIdCaseInsensitive);
    }

    private static DataChangeEvent deserializeDataRecord(
            MySqlEventDeserializer deserializer, Schema rowSchema, int id, String name)
            throws Exception {
        return deserializeDataRecord(deserializer, rowSchema, PRODUCTS, id, name);
    }

    private static DataChangeEvent deserializeDataRecord(
            MySqlEventDeserializer deserializer,
            Schema rowSchema,
            TableId tableId,
            int id,
            String name)
            throws Exception {
        return deserializer
                .deserializeDataChangeRecord(dataRecord(tableId, rowSchema, id, name))
                .get(0);
    }

    private static SourceRecord dataRecord(TableId tableId, Schema rowSchema, int id, String name) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field("db", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("mysql-envelope")
                        .field(Envelope.FieldName.BEFORE, rowSchema)
                        .field(Envelope.FieldName.AFTER, rowSchema)
                        .field(Envelope.FieldName.SOURCE, sourceSchema)
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .build();
        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.BEFORE, null)
                        .put(Envelope.FieldName.AFTER, row(rowSchema, id, name))
                        .put(Envelope.FieldName.SOURCE, source(tableId, sourceSchema))
                        .put(Envelope.FieldName.OPERATION, "c");
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "mysql_binlog_source." + tableId.identifier(),
                null,
                null,
                null,
                valueSchema,
                value);
    }

    private static Struct source(TableId tableId, Schema sourceSchema) {
        return new Struct(sourceSchema)
                .put("db", tableId.getSchemaName())
                .put("table", tableId.getTableName());
    }

    private static SourceRecord schemaChangeRecord() {
        Schema keySchema =
                SchemaBuilder.struct()
                        .name(MySqlEventDeserializer.SCHEMA_CHANGE_EVENT_KEY_NAME)
                        .build();
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "mysql_binlog_source",
                null,
                keySchema,
                new Struct(keySchema),
                null,
                null);
    }

    private static Schema rowSchema() {
        return SchemaBuilder.struct()
                .name("inventory.products.Value")
                .optional()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
    }

    private static Struct row(Schema schema, int id, String name) {
        return new Struct(schema).put("id", id).put("name", name);
    }

    private static void assertRecord(RecordData recordData, int id, String name) {
        assertThat(recordData.getInt(0)).isEqualTo(id);
        assertThat(recordData.getString(1)).isEqualTo(BinaryStringData.fromString(name));
    }

    @SuppressWarnings("unchecked")
    private static Map<TableId, ?> getDataTypeCache(MySqlEventDeserializer deserializer)
            throws Exception {
        Field field = MySqlEventDeserializer.class.getDeclaredField("dataTypeCache");
        field.setAccessible(true);
        Object cache = field.get(deserializer);
        return cache == null ? Collections.emptyMap() : (Map<TableId, ?>) cache;
    }
}
