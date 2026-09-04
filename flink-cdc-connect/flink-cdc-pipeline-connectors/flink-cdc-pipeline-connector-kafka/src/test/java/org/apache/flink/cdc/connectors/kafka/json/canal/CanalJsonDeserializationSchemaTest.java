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

package org.apache.flink.cdc.connectors.kafka.json.canal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CanalJsonDeserializationSchema}. */
class CanalJsonDeserializationSchemaTest {

    private CanalJsonDeserializationSchema deserializationSchema;
    private TestCollector collector;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws Exception {
        deserializationSchema = new CanalJsonDeserializationSchema(true, null, null, null);
        deserializationSchema.open(createMockContext());
        collector = new TestCollector();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testInsertEventParsing() throws IOException {
        String json =
                "{\"old\":null,\"data\":[{\"id\":1,\"name\":\"Alice\",\"age\":30}],"
                        + "\"type\":\"INSERT\",\"database\":\"mydb\",\"table\":\"users\","
                        + "\"pkNames\":[\"id\"]}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(2);

        // First event should be CreateTableEvent
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) events.get(0);
        assertThat(createTableEvent.tableId()).isEqualTo(TableId.tableId("mydb", "users"));
        Schema schema = createTableEvent.getSchema();
        assertThat(schema.getColumnNames()).containsExactly("id", "name", "age");
        assertThat(schema.primaryKeys()).containsExactly("id");

        // Second event should be DataChangeEvent with INSERT
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent = (DataChangeEvent) events.get(1);
        assertThat(dataChangeEvent.op()).isEqualTo(OperationType.INSERT);
        assertThat(dataChangeEvent.before()).isNull();
        assertThat(dataChangeEvent.after()).isNotNull();
    }

    @Test
    void testUpdateEventParsing() throws IOException {
        String json =
                "{\"old\":[{\"id\":1,\"name\":\"Alice\",\"age\":25}],"
                        + "\"data\":[{\"id\":1,\"name\":\"Alice\",\"age\":26}],"
                        + "\"type\":\"UPDATE\",\"database\":\"mydb\",\"table\":\"users\","
                        + "\"pkNames\":[\"id\"]}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(2);

        DataChangeEvent dataChangeEvent = (DataChangeEvent) events.get(1);
        assertThat(dataChangeEvent.op()).isEqualTo(OperationType.UPDATE);
        assertThat(dataChangeEvent.before()).isNotNull();
        assertThat(dataChangeEvent.after()).isNotNull();
    }

    @Test
    void testDeleteEventParsing() throws IOException {
        String json =
                "{\"old\":null,\"data\":[{\"id\":1,\"name\":\"Alice\",\"age\":30}],"
                        + "\"type\":\"DELETE\",\"database\":\"mydb\",\"table\":\"users\","
                        + "\"pkNames\":[\"id\"]}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(2);

        DataChangeEvent dataChangeEvent = (DataChangeEvent) events.get(1);
        assertThat(dataChangeEvent.op()).isEqualTo(OperationType.DELETE);
        assertThat(dataChangeEvent.after()).isNull();
        assertThat(dataChangeEvent.before()).isNotNull();
    }

    @Test
    void testSchemaInferenceDisabled() throws Exception {
        CanalJsonDeserializationSchema noInferSchema =
                new CanalJsonDeserializationSchema(false, null, null, null);
        noInferSchema.open(createMockContext());

        String json =
                "{\"old\":null,\"data\":[{\"id\":1,\"name\":\"Alice\"}],"
                        + "\"type\":\"INSERT\",\"database\":\"mydb\",\"table\":\"users\","
                        + "\"pkNames\":[\"id\"]}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        TestCollector testCollector = new TestCollector();
        noInferSchema.deserialize(record, testCollector);

        CreateTableEvent createTableEvent = (CreateTableEvent) testCollector.getEvents().get(0);
        Schema schema = createTableEvent.getSchema();
        // All columns should be STRING type when schema inference is disabled
        assertThat(schema.getColumns())
                .allMatch(col -> col.getType().getTypeRoot() == DataTypeRoot.VARCHAR);
    }

    @Test
    void testNullValueSkipped() throws IOException {
        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>("test-topic", 0, 0L, null, null);
        deserializationSchema.deserialize(record, collector);
        assertThat(collector.getEvents()).isEmpty();
    }

    @Test
    void testDefaultTableName() throws Exception {
        CanalJsonDeserializationSchema schemaWithDefault =
                new CanalJsonDeserializationSchema(true, "defaultdb", null, "default_table");
        schemaWithDefault.open(createMockContext());

        // Canal JSON without table field
        String json =
                "{\"old\":null,\"data\":[{\"id\":1,\"name\":\"Alice\"}],"
                        + "\"type\":\"INSERT\",\"database\":\"mydb\","
                        + "\"pkNames\":[\"id\"]}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        TestCollector testCollector = new TestCollector();
        schemaWithDefault.deserialize(record, testCollector);

        CreateTableEvent createTableEvent = (CreateTableEvent) testCollector.getEvents().get(0);
        assertThat(createTableEvent.tableId().getTableName()).isEqualTo("default_table");
        assertThat(createTableEvent.tableId().getSchemaName()).isEqualTo("mydb");
    }

    @Test
    void testMultipleInsertsInOneMessage() throws IOException {
        String json =
                "{\"old\":null,\"data\":["
                        + "{\"id\":1,\"name\":\"Alice\"},"
                        + "{\"id\":2,\"name\":\"Bob\"}"
                        + "],\"type\":\"INSERT\",\"database\":\"mydb\",\"table\":\"users\","
                        + "\"pkNames\":[\"id\"]}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        // 1 CreateTableEvent + 2 DataChangeEvents
        assertThat(collector.getEvents()).hasSize(3);
        assertThat(collector.getEvents().get(1)).isInstanceOf(DataChangeEvent.class);
        assertThat(collector.getEvents().get(2)).isInstanceOf(DataChangeEvent.class);
    }

    @Test
    void testCreateTableEventOnlyEmittedOnce() throws IOException {
        String json1 =
                "{\"old\":null,\"data\":[{\"id\":1,\"name\":\"Alice\"}],"
                        + "\"type\":\"INSERT\",\"database\":\"mydb\",\"table\":\"users\","
                        + "\"pkNames\":[\"id\"]}";
        String json2 =
                "{\"old\":null,\"data\":[{\"id\":2,\"name\":\"Bob\"}],"
                        + "\"type\":\"INSERT\",\"database\":\"mydb\",\"table\":\"users\","
                        + "\"pkNames\":[\"id\"]}";

        ConsumerRecord<byte[], byte[]> record1 =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json1.getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<byte[], byte[]> record2 =
                new ConsumerRecord<>(
                        "test-topic", 0, 1L, null, json2.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record1, collector);
        deserializationSchema.deserialize(record2, collector);

        // First message: 1 CreateTableEvent + 1 DataChangeEvent = 2
        // Second message: 1 DataChangeEvent = 1
        // Total: 3
        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(3);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
        assertThat(events.get(2)).isInstanceOf(DataChangeEvent.class);
    }

    @Test
    void testDataValueExtraction() throws IOException {
        String json =
                "{\"old\":null,\"data\":[{\"id\":42,\"name\":\"Charlie\",\"active\":true}],"
                        + "\"type\":\"INSERT\",\"database\":\"mydb\",\"table\":\"users\","
                        + "\"pkNames\":[\"id\"]}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        DataChangeEvent dataChangeEvent = (DataChangeEvent) collector.getEvents().get(1);
        RecordData after = dataChangeEvent.after();
        assertThat(after.getInt(0)).isEqualTo(42);
        assertThat(after.getString(1).toString()).isEqualTo("Charlie");
        assertThat(after.getBoolean(2)).isTrue();
    }

    @Test
    void testCreateTableDdl() throws IOException {
        String json =
                "{\"isDdl\":true,\"sql\":\"CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2))\","
                        + "\"type\":\"CREATE\",\"database\":\"mydb\",\"table\":\"products\"}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) events.get(0);
        assertThat(createTableEvent.tableId()).isEqualTo(TableId.tableId("mydb", "products"));
        Schema schema = createTableEvent.getSchema();
        assertThat(schema.getColumnNames()).containsExactly("id", "name", "price");
        assertThat(schema.primaryKeys()).containsExactly("id");
    }

    @Test
    void testDropTableDdl() throws IOException {
        String json =
                "{\"isDdl\":true,\"sql\":\"DROP TABLE users\",\"type\":\"DROP\","
                        + "\"database\":\"mydb\",\"table\":\"users\"}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(DropTableEvent.class);
        DropTableEvent dropTableEvent = (DropTableEvent) events.get(0);
        assertThat(dropTableEvent.tableId()).isEqualTo(TableId.tableId("mydb", "users"));
    }

    @Test
    void testTruncateTableDdl() throws IOException {
        String json =
                "{\"isDdl\":true,\"sql\":\"TRUNCATE TABLE orders\",\"type\":\"TRUNCATE\","
                        + "\"database\":\"mydb\",\"table\":\"orders\"}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(TruncateTableEvent.class);
        TruncateTableEvent truncateTableEvent = (TruncateTableEvent) events.get(0);
        assertThat(truncateTableEvent.tableId()).isEqualTo(TableId.tableId("mydb", "orders"));
    }

    @Test
    void testAlterAddColumnDdl() throws IOException {
        String json =
                "{\"isDdl\":true,\"sql\":\"ALTER TABLE users ADD COLUMN email VARCHAR(255)\","
                        + "\"type\":\"ALTER\",\"database\":\"mydb\",\"table\":\"users\"}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent addColumnEvent = (AddColumnEvent) events.get(0);
        assertThat(addColumnEvent.tableId()).isEqualTo(TableId.tableId("mydb", "users"));
    }

    @Test
    void testAlterDropColumnDdl() throws IOException {
        String json =
                "{\"isDdl\":true,\"sql\":\"ALTER TABLE users DROP COLUMN email\","
                        + "\"type\":\"ALTER\",\"database\":\"mydb\",\"table\":\"users\"}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(DropColumnEvent.class);
        DropColumnEvent dropColumnEvent = (DropColumnEvent) events.get(0);
        assertThat(dropColumnEvent.tableId()).isEqualTo(TableId.tableId("mydb", "users"));
    }

    @Test
    void testAlterModifyColumnDdl() throws IOException {
        String json =
                "{\"isDdl\":true,\"sql\":\"ALTER TABLE users MODIFY COLUMN age BIGINT\","
                        + "\"type\":\"ALTER\",\"database\":\"mydb\",\"table\":\"users\"}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        AlterColumnTypeEvent alterColumnTypeEvent = (AlterColumnTypeEvent) events.get(0);
        assertThat(alterColumnTypeEvent.tableId()).isEqualTo(TableId.tableId("mydb", "users"));
    }

    @Test
    void testDdlWithoutIsDdlFlag() throws IOException {
        String json =
                "{\"isDdl\":true,\"sql\":\"CREATE TABLE test_table (id INT)\","
                        + "\"type\":\"CREATE TABLE\",\"database\":\"mydb\",\"table\":\"test_table\"}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
    }

    @Test
    void testDdlWithoutTypeField() throws IOException {
        String json =
                "{\"isDdl\":true,\"sql\":\"CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100))\","
                        + "\"database\":\"mydb\",\"table\":\"products\"}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) events.get(0);
        assertThat(createTableEvent.tableId()).isEqualTo(TableId.tableId("mydb", "products"));
        Schema schema = createTableEvent.getSchema();
        assertThat(schema.getColumnNames()).containsExactly("id", "name");
        assertThat(schema.primaryKeys()).containsExactly("id");
    }

    @Test
    void testDdlWithQueryType() throws IOException {
        String json =
                "{\"isDdl\":true,\"sql\":\"ALTER TABLE users ADD COLUMN email VARCHAR(255)\","
                        + "\"type\":\"QUERY\",\"database\":\"mydb\",\"table\":\"users\"}";

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "test-topic", 0, 0L, null, json.getBytes(StandardCharsets.UTF_8));

        deserializationSchema.deserialize(record, collector);

        List<Event> events = collector.getEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent addColumnEvent = (AddColumnEvent) events.get(0);
        assertThat(addColumnEvent.tableId()).isEqualTo(TableId.tableId("mydb", "users"));
    }

    private static DeserializationSchema.InitializationContext createMockContext() {
        return new DeserializationSchema.InitializationContext() {
            @Override
            public org.apache.flink.metrics.MetricGroup getMetricGroup() {
                return null;
            }

            @Override
            public org.apache.flink.util.UserCodeClassLoader getUserCodeClassLoader() {
                return null;
            }
        };
    }

    /** A test collector that collects emitted events into a list. */
    private static class TestCollector implements Collector<Event> {
        private final List<Event> events = new ArrayList<>();

        @Override
        public void collect(Event record) {
            events.add(record);
        }

        @Override
        public void close() {
            // no-op
        }

        public List<Event> getEvents() {
            return events;
        }
    }
}
