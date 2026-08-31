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

package org.apache.flink.cdc.connectors.kafka.source;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Tests for {@link PipelineKafkaRecordDeserializationSchema}. */
class PipelineKafkaRecordDeserializationSchemaTest {

    private static final byte[] KEY =
            bytes(
                    "{\"schema\":{\"type\":\"struct\",\"fields\":["
                            + field("int32", "id", false)
                            + "]},\"payload\":{\"id\":1}}");

    @Test
    void testDeserializeOperationsMetadataAndIgnoredRecords() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String fields = field("int32", "id", false) + "," + field("string", "name", true);

        deserializer.deserialize(
                record(3, 11, KEY, value(fields, "c", "null", row(1, "Alice"))), collector);
        deserializer.deserialize(
                record(3, 12, KEY, value(fields, "u", row(1, "Alice"), row(1, "Bob"))), collector);
        deserializer.deserialize(
                record(3, 13, KEY, value(fields, "d", row(1, "Bob"), "null")), collector);
        deserializer.deserialize(record(3, 14, KEY, null), collector);
        deserializer.deserialize(
                record(
                        3,
                        15,
                        KEY,
                        bytes("{\"schema\":{},\"payload\":{\"source\":{},\"ts_ms\":1}}")),
                collector);

        Assertions.assertThat(collector.events)
                .hasSize(4)
                .element(0)
                .isInstanceOf(CreateTableEvent.class);
        Assertions.assertThat(
                        ((CreateTableEvent) collector.events.get(0)).getSchema().primaryKeys())
                .isEmpty();
        Assertions.assertThat(
                        collector.events.subList(1, 4).stream()
                                .map(event -> ((DataChangeEvent) event).op()))
                .containsExactly(OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE);
        DataChangeEvent insert = (DataChangeEvent) collector.events.get(1);
        Assertions.assertThat(insert.tableId().toString()).isEqualTo("inventory.customers");
        Assertions.assertThat(insert.meta())
                .containsEntry("topic", "dbserver.inventory.customers")
                .containsEntry("partition", "3")
                .containsEntry("offset", "11");
        Assertions.assertThat(insert.after().getInt(0)).isEqualTo(1);
        Assertions.assertThat(insert.after().getString(1).toString()).isEqualTo("Alice");
    }

    @Test
    void testInterleavedOldNewOldSchemasUseWidestSchema() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String oldFields = field("int32", "id", false) + "," + field("string", "name", true);
        String newFields =
                field("int64", "id", false)
                        + ","
                        + field("string", "name", true)
                        + ","
                        + field("string", "email", true);

        deserializer.deserialize(
                record(0, 1, KEY, value(oldFields, "c", "null", row(1, "Alice"))), collector);
        deserializer.deserialize(
                record(
                        1,
                        1,
                        KEY,
                        value(
                                newFields,
                                "c",
                                "null",
                                "{\"id\":2147483648,\"name\":\"Bob\",\"email\":\"b@example.com\"}")),
                collector);
        deserializer.deserialize(
                record(0, 2, KEY, value(oldFields, "c", "null", row(2, "Carol"))), collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly(
                        "CreateTableEvent",
                        "DataChangeEvent",
                        "AddColumnEvent",
                        "AlterColumnTypeEvent",
                        "DataChangeEvent",
                        "DataChangeEvent");
        AlterColumnTypeEvent alter = (AlterColumnTypeEvent) collector.events.get(3);
        Assertions.assertThat(alter.getTypeMapping().get("id").getTypeRoot())
                .isEqualTo(DataTypeRoot.BIGINT);
        DataChangeEvent oldAfterWidening = (DataChangeEvent) collector.events.get(5);
        RecordData converted = oldAfterWidening.after();
        Assertions.assertThat(converted.getArity()).isEqualTo(3);
        Assertions.assertThat(converted.getLong(0)).isEqualTo(2L);
        Assertions.assertThat(converted.isNullAt(2)).isTrue();
    }

    @Test
    void testNewSchemaFirstThenOldSchemaOnAnotherPartition() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String oldFields = field("int32", "id", false) + "," + field("string", "name", true);
        String newFields =
                field("int64", "id", false)
                        + ","
                        + field("string", "name", true)
                        + ","
                        + field("string", "email", true);

        deserializer.deserialize(
                record(
                        1,
                        1,
                        KEY,
                        value(
                                newFields,
                                "c",
                                "null",
                                "{\"id\":2147483648,\"name\":\"Bob\",\"email\":\"b@example.com\"}")),
                collector);
        deserializer.deserialize(
                record(0, 1, KEY, value(oldFields, "c", "null", row(2, "Carol"))), collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly("CreateTableEvent", "DataChangeEvent", "DataChangeEvent");
        RecordData converted = ((DataChangeEvent) collector.events.get(2)).after();
        Assertions.assertThat(converted.getArity()).isEqualTo(3);
        Assertions.assertThat(converted.getLong(0)).isEqualTo(2L);
        Assertions.assertThat(converted.isNullAt(2)).isTrue();
    }

    @Test
    void testIntToStringWideningOnSamePartition() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String intFields = field("int32", "id", false) + "," + field("int32", "age", true);
        String stringFields = field("int32", "id", false) + "," + field("string", "age", true);

        deserializer.deserialize(
                record(0, 1, KEY, value(intFields, "c", "null", "{\"id\":1,\"age\":18}")),
                collector);
        deserializer.deserialize(
                record(0, 2, KEY, value(stringFields, "c", "null", "{\"id\":2,\"age\":\"hello\"}")),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly(
                        "CreateTableEvent",
                        "DataChangeEvent",
                        "AlterColumnTypeEvent",
                        "DataChangeEvent");
        CreateTableEvent createTable = (CreateTableEvent) collector.events.get(0);
        Assertions.assertThat(
                        createTable
                                .getSchema()
                                .getColumn("age")
                                .orElseThrow(AssertionError::new)
                                .getType()
                                .getTypeRoot())
                .isEqualTo(DataTypeRoot.INTEGER);
        AlterColumnTypeEvent alter = (AlterColumnTypeEvent) collector.events.get(2);
        Assertions.assertThat(alter.getTypeMapping().get("age"))
                .isEqualTo(DataTypes.STRING().nullable());
        DataChangeEvent intRecord = (DataChangeEvent) collector.events.get(1);
        Assertions.assertThat(intRecord.after().getInt(1)).isEqualTo(18);
        DataChangeEvent stringRecord = (DataChangeEvent) collector.events.get(3);
        Assertions.assertThat(stringRecord.after().getString(1).toString()).isEqualTo("hello");
    }

    @Test
    void testStringThenHistoricalIntFromAnotherPartition() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String intFields = field("int32", "id", false) + "," + field("int32", "age", true);
        String stringFields = field("int32", "id", false) + "," + field("string", "age", true);

        deserializer.deserialize(
                record(1, 1, KEY, value(stringFields, "c", "null", "{\"id\":1,\"age\":\"hello\"}")),
                collector);
        deserializer.deserialize(
                record(0, 1, KEY, value(intFields, "c", "null", "{\"id\":2,\"age\":19}")),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly("CreateTableEvent", "DataChangeEvent", "DataChangeEvent");
        DataChangeEvent historical = (DataChangeEvent) collector.events.get(2);
        Assertions.assertThat(historical.after().getString(1).toString()).isEqualTo("19");
    }

    @Test
    void testStringToIntWithinPartitionFailsClearly() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String stringFields = field("int32", "id", false) + "," + field("string", "age", true);
        String intFields = field("int32", "id", false) + "," + field("int32", "age", true);
        deserializer.deserialize(
                record(0, 1, KEY, value(stringFields, "c", "null", "{\"id\":1,\"age\":\"hello\"}")),
                collector);

        Assertions.assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        record(
                                                0,
                                                2,
                                                KEY,
                                                value(
                                                        intFields,
                                                        "c",
                                                        "null",
                                                        "{\"id\":2,\"age\":19}")),
                                        collector))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Incompatible or narrowing type change")
                .hasMessageContaining("age")
                .hasMessageContaining("@2");
    }

    @Test
    void testDroppedColumnStaysInSchemaAndReadsAsNull() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String fields = field("int32", "id", false) + "," + field("string", "name", true);
        deserializer.deserialize(
                record(0, 1, KEY, value(fields, "c", "null", row(1, "Alice"))), collector);
        deserializer.deserialize(
                record(0, 2, KEY, value(field("int32", "id", false), "c", "null", "{\"id\":2}")),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly("CreateTableEvent", "DataChangeEvent", "DataChangeEvent");
        CreateTableEvent createTable = (CreateTableEvent) collector.events.get(0);
        Assertions.assertThat(createTable.getSchema().getColumnNames())
                .containsExactly("id", "name");
        DataChangeEvent dropped = (DataChangeEvent) collector.events.get(2);
        Assertions.assertThat(dropped.after().getArity()).isEqualTo(2);
        Assertions.assertThat(dropped.after().getInt(0)).isEqualTo(2);
        Assertions.assertThat(dropped.after().isNullAt(1)).isTrue();
    }

    @Test
    void testRenamedColumnKeepsOldNameAndAddsNewColumn() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String oldFields = field("int32", "id", false) + "," + field("string", "name", true);
        String renamedFields =
                field("int32", "id", false) + "," + field("string", "full_name", true);

        deserializer.deserialize(
                record(0, 1, KEY, value(oldFields, "c", "null", row(1, "Alice"))), collector);
        deserializer.deserialize(
                record(
                        0,
                        2,
                        KEY,
                        value(renamedFields, "c", "null", "{\"id\":2,\"full_name\":\"Bob\"}")),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly(
                        "CreateTableEvent", "DataChangeEvent", "AddColumnEvent", "DataChangeEvent");
        AddColumnEvent addColumn = (AddColumnEvent) collector.events.get(2);
        Assertions.assertThat(addColumn.getAddedColumns())
                .extracting(column -> column.getAddColumn().getName())
                .containsExactly("full_name");
        DataChangeEvent beforeRename = (DataChangeEvent) collector.events.get(1);
        Assertions.assertThat(beforeRename.after().getArity()).isEqualTo(2);
        Assertions.assertThat(beforeRename.after().getString(1).toString()).isEqualTo("Alice");
        DataChangeEvent afterRename = (DataChangeEvent) collector.events.get(3);
        Assertions.assertThat(afterRename.after().getArity()).isEqualTo(3);
        Assertions.assertThat(afterRename.after().isNullAt(1)).isTrue();
        Assertions.assertThat(afterRename.after().getString(2).toString()).isEqualTo("Bob");
    }

    @Test
    void testNonNullableDroppedColumnBecomesNullable() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String fields = field("int32", "id", false) + "," + field("string", "name", false);
        deserializer.deserialize(
                record(0, 1, KEY, value(fields, "c", "null", row(1, "Alice"))), collector);
        deserializer.deserialize(
                record(0, 2, KEY, value(field("int32", "id", false), "c", "null", "{\"id\":2}")),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly(
                        "CreateTableEvent",
                        "DataChangeEvent",
                        "AlterColumnTypeEvent",
                        "DataChangeEvent");
        AlterColumnTypeEvent alter = (AlterColumnTypeEvent) collector.events.get(2);
        Assertions.assertThat(alter.getTypeMapping().get("name"))
                .isEqualTo(DataTypes.STRING().nullable());
        DataChangeEvent dropped = (DataChangeEvent) collector.events.get(3);
        Assertions.assertThat(dropped.after().isNullAt(1)).isTrue();
    }

    @Test
    void testHistoricalNameAfterRenameFromAnotherPartition() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String oldFields = field("int32", "id", false) + "," + field("string", "name", true);
        String renamedFields =
                field("int32", "id", false) + "," + field("string", "full_name", true);

        deserializer.deserialize(
                record(0, 1, KEY, value(oldFields, "c", "null", row(1, "Alice"))), collector);
        deserializer.deserialize(
                record(
                        0,
                        2,
                        KEY,
                        value(renamedFields, "c", "null", "{\"id\":2,\"full_name\":\"Bob\"}")),
                collector);
        deserializer.deserialize(
                record(1, 1, KEY, value(oldFields, "c", "null", row(3, "Carol"))), collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly(
                        "CreateTableEvent",
                        "DataChangeEvent",
                        "AddColumnEvent",
                        "DataChangeEvent",
                        "DataChangeEvent");
        DataChangeEvent historical = (DataChangeEvent) collector.events.get(4);
        Assertions.assertThat(historical.after().getArity()).isEqualTo(3);
        Assertions.assertThat(historical.after().getString(1).toString()).isEqualTo("Carol");
        Assertions.assertThat(historical.after().isNullAt(2)).isTrue();
    }

    @Test
    void testKafkaConnectFloatingTypesAndStringMapsToString() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();
        String fields =
                field("int32", "id", false)
                        + ","
                        + field("float", "score", true)
                        + ","
                        + field("double", "ratio", true)
                        + ","
                        + field("string", "name", true);

        deserializer.deserialize(
                record(
                        0,
                        1,
                        KEY,
                        value(
                                fields,
                                "c",
                                "null",
                                "{\"id\":1,\"score\":1.5,\"ratio\":2.5,\"name\":\"Alice\"}")),
                collector);
        deserializer.deserialize(
                record(
                        0,
                        2,
                        KEY,
                        value(
                                fields,
                                "c",
                                "null",
                                "{\"id\":2,\"score\":3.5,\"ratio\":4.5,\"name\":\"Bob\"}")),
                collector);

        CreateTableEvent createTable = (CreateTableEvent) collector.events.get(0);
        Assertions.assertThat(
                        createTable
                                .getSchema()
                                .getColumn("name")
                                .orElseThrow(AssertionError::new)
                                .getType())
                .isEqualTo(DataTypes.STRING().nullable());
        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly("CreateTableEvent", "DataChangeEvent", "DataChangeEvent");
        DataChangeEvent firstRecord = (DataChangeEvent) collector.events.get(1);
        Assertions.assertThat(firstRecord.after().getFloat(1)).isEqualTo(1.5f);
        Assertions.assertThat(firstRecord.after().getDouble(2)).isEqualTo(2.5d);
    }

    @Test
    void testTablesIncludeKeepsMatchingTableOnly() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema("inventory.customers", null);
        TestCollector collector = new TestCollector();
        String fields = field("int32", "id", false) + "," + field("string", "name", true);

        deserializer.deserialize(
                record(0, 1, KEY, value(fields, "c", "null", row(1, "Alice"))), collector);
        deserializer.deserialize(
                record(0, 2, KEY, value("inventory", "orders", fields, "c", "null", row(2, "Bob"))),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly("CreateTableEvent", "DataChangeEvent");
        Assertions.assertThat(((CreateTableEvent) collector.events.get(0)).tableId().toString())
                .isEqualTo("inventory.customers");
        Assertions.assertThat(((DataChangeEvent) collector.events.get(1)).tableId().toString())
                .isEqualTo("inventory.customers");
    }

    @Test
    void testTablesExcludeDropsMatchingTable() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema(null, "inventory.orders");
        TestCollector collector = new TestCollector();
        String fields = field("int32", "id", false) + "," + field("string", "name", true);

        deserializer.deserialize(
                record(0, 1, KEY, value(fields, "c", "null", row(1, "Alice"))), collector);
        deserializer.deserialize(
                record(0, 2, KEY, value("inventory", "orders", fields, "c", "null", row(2, "Bob"))),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly("CreateTableEvent", "DataChangeEvent");
        Assertions.assertThat(((DataChangeEvent) collector.events.get(1)).tableId().toString())
                .isEqualTo("inventory.customers");
    }

    @Test
    void testTablesIncludeAndExclude() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema("inventory.\\.*", "inventory.orders");
        TestCollector collector = new TestCollector();
        String fields = field("int32", "id", false) + "," + field("string", "name", true);

        deserializer.deserialize(
                record(0, 1, KEY, value(fields, "c", "null", row(1, "Alice"))), collector);
        deserializer.deserialize(
                record(0, 2, KEY, value("inventory", "orders", fields, "c", "null", row(2, "Bob"))),
                collector);
        deserializer.deserialize(
                record(
                        0,
                        3,
                        KEY,
                        value("other", "customers", fields, "c", "null", row(3, "Carol"))),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly("CreateTableEvent", "DataChangeEvent");
        Assertions.assertThat(((DataChangeEvent) collector.events.get(1)).tableId().toString())
                .isEqualTo("inventory.customers");
    }

    @Test
    void testDebeziumColumnLengthParameterDoesNotCreateVarchar() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema();
        TestCollector collector = new TestCollector();

        String fields = field("int32", "id", false) + "," + stringFieldWithLength("name", 32);
        deserializer.deserialize(
                record(0, 1, KEY, value(fields, "c", "null", "{\"id\":1,\"name\":\"Alice\"}")),
                collector);

        CreateTableEvent createTable = (CreateTableEvent) collector.events.get(0);
        Assertions.assertThat(
                        createTable
                                .getSchema()
                                .getColumn("name")
                                .orElseThrow(AssertionError::new)
                                .getType())
                .isEqualTo(DataTypes.STRING().nullable());
    }

    private static ConsumerRecord<byte[], byte[]> record(
            int partition, long offset, byte[] key, byte[] value) {
        return new ConsumerRecord<>("dbserver.inventory.customers", partition, offset, key, value);
    }

    private static byte[] value(String fields, String operation, String before, String after) {
        return value("inventory", "customers", fields, operation, before, after);
    }

    private static byte[] value(
            String db, String table, String fields, String operation, String before, String after) {
        String rowSchema =
                "{\"type\":\"struct\",\"fields\":["
                        + fields
                        + "],\"optional\":true,\"name\":\""
                        + db
                        + "."
                        + table
                        + ".Value\"}";
        return bytes(
                "{\"schema\":{\"type\":\"struct\",\"fields\":["
                        + withField(rowSchema, "before")
                        + ","
                        + withField(rowSchema, "after")
                        + "]},\"payload\":{\"before\":"
                        + before
                        + ",\"after\":"
                        + after
                        + ",\"source\":{\"db\":\""
                        + db
                        + "\",\"table\":\""
                        + table
                        + "\"},\"op\":\""
                        + operation
                        + "\"}}");
    }

    private static String withField(String schema, String field) {
        return schema.substring(0, schema.length() - 1) + ",\"field\":\"" + field + "\"}";
    }

    private static String field(String type, String name, boolean optional) {
        return "{\"type\":\""
                + type
                + "\",\"optional\":"
                + optional
                + ",\"field\":\""
                + name
                + "\"}";
    }

    private static String stringFieldWithLength(String name, int length) {
        return "{\"type\":\"string\",\"optional\":true,\"parameters\":{"
                + "\"__debezium.source.column.length\":\""
                + length
                + "\"},\"field\":\""
                + name
                + "\"}";
    }

    private static String row(long id, String name) {
        return "{\"id\":" + id + ",\"name\":\"" + name + "\"}";
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static class TestCollector implements Collector<Event> {
        private final List<Event> events = new ArrayList<>();

        @Override
        public void collect(Event event) {
            events.add(event);
        }

        @Override
        public void close() {}
    }
}
