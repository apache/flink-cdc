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

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Canal JSON tests for {@link PipelineKafkaRecordDeserializationSchema}. */
class PipelineKafkaRecordDeserializationSchemaCanalTest {

    @Test
    void testInsertUpdateDeleteAndPrimaryKeys() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema(
                        JsonSerializationType.CANAL_JSON, null, null);
        TestCollector collector = new TestCollector();

        deserializer.deserialize(
                record(
                        1,
                        canal(
                                "INSERT",
                                "inventory",
                                "products",
                                "[\"id\"]",
                                "{\"id\":\"INTEGER\",\"name\":\"VARCHAR(255)\",\"weight\":\"FLOAT\"}",
                                "[{\"id\":\"111\",\"name\":\"scooter\",\"weight\":\"5.18\"}]",
                                "null")),
                collector);
        deserializer.deserialize(
                record(
                        2,
                        canal(
                                "UPDATE",
                                "inventory",
                                "products",
                                "[\"id\"]",
                                "{\"id\":\"INTEGER\",\"name\":\"VARCHAR(255)\",\"weight\":\"FLOAT\"}",
                                "[{\"id\":\"111\",\"name\":\"scooter\",\"weight\":\"5.18\"}]",
                                "[{\"weight\":\"5.15\"}]")),
                collector);
        deserializer.deserialize(
                record(
                        3,
                        canal(
                                "DELETE",
                                "inventory",
                                "products",
                                "[\"id\"]",
                                "{\"id\":\"INTEGER\",\"name\":\"VARCHAR(255)\",\"weight\":\"FLOAT\"}",
                                "[{\"id\":\"111\",\"name\":\"scooter\",\"weight\":\"5.18\"}]",
                                "null")),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly(
                        "CreateTableEvent",
                        "DataChangeEvent",
                        "DataChangeEvent",
                        "DataChangeEvent");
        CreateTableEvent createTable = (CreateTableEvent) collector.events.get(0);
        Assertions.assertThat(createTable.getSchema().primaryKeys()).containsExactly("id");
        Assertions.assertThat(
                        createTable
                                .getSchema()
                                .getColumn("name")
                                .orElseThrow(AssertionError::new)
                                .getType())
                .isEqualTo(DataTypes.STRING().nullable());
        Assertions.assertThat(
                        collector.events.subList(1, 4).stream()
                                .map(event -> ((DataChangeEvent) event).op()))
                .containsExactly(OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE);

        DataChangeEvent insert = (DataChangeEvent) collector.events.get(1);
        Assertions.assertThat(insert.tableId().toString()).isEqualTo("inventory.products");
        Assertions.assertThat(insert.after().getInt(0)).isEqualTo(111);
        Assertions.assertThat(insert.after().getFloat(2)).isEqualTo(5.18f);

        DataChangeEvent update = (DataChangeEvent) collector.events.get(2);
        Assertions.assertThat(update.before().getInt(0)).isEqualTo(111);
        Assertions.assertThat(update.before().getString(1).toString()).isEqualTo("scooter");
        Assertions.assertThat(update.before().getFloat(2)).isEqualTo(5.15f);
        Assertions.assertThat(update.after().getFloat(2)).isEqualTo(5.18f);
    }

    @Test
    void testMultipleRowsInOneRecord() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema(
                        JsonSerializationType.CANAL_JSON, null, null);
        TestCollector collector = new TestCollector();
        deserializer.deserialize(
                record(
                        1,
                        canal(
                                "INSERT",
                                "inventory",
                                "products",
                                "[\"id\"]",
                                "{\"id\":\"INTEGER\",\"name\":\"VARCHAR(255)\"}",
                                "[{\"id\":\"1\",\"name\":\"a\"},{\"id\":\"2\",\"name\":\"b\"}]",
                                "null")),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly("CreateTableEvent", "DataChangeEvent", "DataChangeEvent");
        Assertions.assertThat(((DataChangeEvent) collector.events.get(1)).after().getInt(0))
                .isEqualTo(1);
        Assertions.assertThat(((DataChangeEvent) collector.events.get(2)).after().getInt(0))
                .isEqualTo(2);
    }

    @Test
    void testSkipDdlAndUnknownType() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema(
                        JsonSerializationType.CANAL_JSON, null, null);
        TestCollector collector = new TestCollector();
        deserializer.deserialize(
                record(
                        1,
                        bytes(
                                "{\"data\":null,\"database\":\"inventory\",\"isDdl\":true,\"table\":\"products\",\"type\":\"CREATE\"}")),
                collector);
        deserializer.deserialize(
                record(
                        2,
                        bytes(
                                "{\"data\":[],\"database\":\"inventory\",\"table\":\"products\",\"type\":\"QUERY\"}")),
                collector);

        Assertions.assertThat(collector.events).isEmpty();
    }

    @Test
    void testTablesFilter() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema(
                        JsonSerializationType.CANAL_JSON, "inventory.products", "inventory.orders");
        TestCollector collector = new TestCollector();
        deserializer.deserialize(
                record(
                        1,
                        canal(
                                "INSERT",
                                "inventory",
                                "products",
                                "[\"id\"]",
                                "{\"id\":\"INTEGER\"}",
                                "[{\"id\":\"1\"}]",
                                "null")),
                collector);
        deserializer.deserialize(
                record(
                        2,
                        canal(
                                "INSERT",
                                "inventory",
                                "orders",
                                "[\"id\"]",
                                "{\"id\":\"INTEGER\"}",
                                "[{\"id\":\"2\"}]",
                                "null")),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly("CreateTableEvent", "DataChangeEvent");
        Assertions.assertThat(((DataChangeEvent) collector.events.get(1)).tableId().toString())
                .isEqualTo("inventory.products");
    }

    @Test
    void testAddColumnAndTypeWidening() throws Exception {
        PipelineKafkaRecordDeserializationSchema deserializer =
                new PipelineKafkaRecordDeserializationSchema(
                        JsonSerializationType.CANAL_JSON, null, null);
        TestCollector collector = new TestCollector();
        deserializer.deserialize(
                record(
                        1,
                        canal(
                                "INSERT",
                                "inventory",
                                "products",
                                "[\"id\"]",
                                "{\"id\":\"INTEGER\",\"name\":\"VARCHAR(255)\"}",
                                "[{\"id\":\"1\",\"name\":\"a\"}]",
                                "null")),
                collector);
        deserializer.deserialize(
                record(
                        2,
                        canal(
                                "INSERT",
                                "inventory",
                                "products",
                                "[\"id\"]",
                                "{\"id\":\"BIGINT\",\"name\":\"VARCHAR(255)\",\"email\":\"VARCHAR(255)\"}",
                                "[{\"id\":\"2\",\"name\":\"b\",\"email\":\"b@example.com\"}]",
                                "null")),
                collector);

        Assertions.assertThat(collector.events)
                .extracting(event -> event.getClass().getSimpleName())
                .containsExactly(
                        "CreateTableEvent",
                        "DataChangeEvent",
                        "AddColumnEvent",
                        "AlterColumnTypeEvent",
                        "DataChangeEvent");
        AddColumnEvent addColumn = (AddColumnEvent) collector.events.get(2);
        Assertions.assertThat(addColumn.getAddedColumns())
                .extracting(column -> column.getAddColumn().getName())
                .containsExactly("email");
        AlterColumnTypeEvent alter = (AlterColumnTypeEvent) collector.events.get(3);
        Assertions.assertThat(alter.getTypeMapping().get("id").getTypeRoot())
                .isEqualTo(DataTypeRoot.BIGINT);
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset, byte[] value) {
        return new ConsumerRecord<>("canal.inventory.products", 0, offset, null, value);
    }

    private static byte[] canal(
            String type,
            String database,
            String table,
            String pkNames,
            String mysqlType,
            String data,
            String old) {
        return bytes(
                "{\"data\":"
                        + data
                        + ",\"database\":\""
                        + database
                        + "\",\"isDdl\":false,\"mysqlType\":"
                        + mysqlType
                        + ",\"old\":"
                        + old
                        + ",\"pkNames\":"
                        + pkNames
                        + ",\"table\":\""
                        + table
                        + "\",\"ts\":1589373560798,\"type\":\""
                        + type
                        + "\"}");
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
