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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafka.json.ChangeLogJsonFormatFactory;
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;
import org.apache.flink.cdc.connectors.kafka.json.MockInitializationContext;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

/** Tests for {@link CanalJsonSerializationSchema}. */
class CanalJsonSerializationSchemaTest {

    public static final TableId TABLE_1 =
            TableId.tableId("default_namespace", "default_schema", "table1");

    @Test
    void testSerialize() throws Exception {
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        SerializationSchema<Event> serializationSchema =
                ChangeLogJsonFormatFactory.createSerializationSchema(
                        new Configuration(),
                        JsonSerializationType.CANAL_JSON,
                        ZoneId.systemDefault());
        serializationSchema.open(new MockInitializationContext());

        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        Assertions.assertThat(serializationSchema.serialize(createTableEvent)).isNull();

        // insert
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        JsonNode expected =
                mapper.readTree(
                        "{\"old\":null,\"data\":[{\"col1\":\"1\",\"col2\":\"1\"}],\"type\":\"INSERT\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
        JsonNode actual = mapper.readTree(serializationSchema.serialize(insertEvent1));
        Assertions.assertThat(actual).isEqualTo(expected);

        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        expected =
                mapper.readTree(
                        "{\"old\":null,\"data\":[{\"col1\":\"2\",\"col2\":\"2\"}],\"type\":\"INSERT\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
        actual = mapper.readTree(serializationSchema.serialize(insertEvent2));
        Assertions.assertThat(actual).isEqualTo(expected);

        DataChangeEvent deleteEvent =
                DataChangeEvent.deleteEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        expected =
                mapper.readTree(
                        "{\"old\":null,\"data\":[{\"col1\":\"2\",\"col2\":\"2\"}],\"type\":\"DELETE\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
        actual = mapper.readTree(serializationSchema.serialize(deleteEvent));
        Assertions.assertThat(actual).isEqualTo(expected);

        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("x")
                                }));
        expected =
                mapper.readTree(
                        "{\"old\":[{\"col1\":\"1\",\"col2\":\"1\"}],\"data\":[{\"col1\":\"1\",\"col2\":\"x\"}],\"type\":\"UPDATE\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
        actual = mapper.readTree(serializationSchema.serialize(updateEvent));
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testSerializeComplexTypes() throws Exception {
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        SerializationSchema<Event> serializationSchema =
                ChangeLogJsonFormatFactory.createSerializationSchema(
                        new Configuration(),
                        JsonSerializationType.CANAL_JSON,
                        ZoneId.systemDefault());
        serializationSchema.open(new MockInitializationContext());

        // create table with complex types
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("arr", DataTypes.ARRAY(DataTypes.STRING()))
                        .physicalColumn("map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .physicalColumn(
                                "row",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f1", DataTypes.STRING()),
                                        DataTypes.FIELD("f2", DataTypes.INT())))
                        .primaryKey("id")
                        .build();

        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.ROW(
                                DataTypes.FIELD("f1", DataTypes.STRING()),
                                DataTypes.FIELD("f2", DataTypes.INT())));

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        Assertions.assertThat(serializationSchema.serialize(createTableEvent)).isNull();

        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);

        // Create test data with complex types
        org.apache.flink.cdc.common.data.GenericArrayData arrayData =
                new org.apache.flink.cdc.common.data.GenericArrayData(
                        new Object[] {
                            BinaryStringData.fromString("item1"),
                            BinaryStringData.fromString("item2")
                        });

        java.util.Map<Object, Object> mapValues = new java.util.HashMap<>();
        mapValues.put(BinaryStringData.fromString("key1"), 100);
        mapValues.put(BinaryStringData.fromString("key2"), 200);
        org.apache.flink.cdc.common.data.GenericMapData mapData =
                new org.apache.flink.cdc.common.data.GenericMapData(mapValues);

        BinaryRecordDataGenerator nestedRowGenerator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.INT()));
        org.apache.flink.cdc.common.data.RecordData nestedRow =
                nestedRowGenerator.generate(
                        new Object[] {BinaryStringData.fromString("nested"), 42});

        // insert event with complex types
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(new Object[] {1, arrayData, mapData, nestedRow}));

        byte[] serialized = serializationSchema.serialize(insertEvent);
        JsonNode actual = mapper.readTree(serialized);

        // Verify the structure contains complex types
        Assertions.assertThat(actual.has("old")).isTrue();
        Assertions.assertThat(actual.has("data")).isTrue();
        Assertions.assertThat(actual.get("data").isArray()).isTrue();
        Assertions.assertThat(actual.get("data").get(0).has("id")).isTrue();
        Assertions.assertThat(actual.get("data").get(0).has("arr")).isTrue();
        Assertions.assertThat(actual.get("data").get(0).has("map")).isTrue();
        Assertions.assertThat(actual.get("data").get(0).has("row")).isTrue();

        // Verify array content
        JsonNode arrNode = actual.get("data").get(0).get("arr");
        Assertions.assertThat(arrNode.isArray()).isTrue();
        Assertions.assertThat(arrNode.size()).isEqualTo(2);

        // Verify map content
        JsonNode mapNode = actual.get("data").get(0).get("map");
        Assertions.assertThat(mapNode.isObject()).isTrue();

        // Verify row content
        JsonNode rowNode = actual.get("data").get(0).get("row");
        Assertions.assertThat(rowNode.isObject()).isTrue();
        Assertions.assertThat(rowNode.has("f1")).isTrue();
        Assertions.assertThat(rowNode.has("f2")).isTrue();
    }
}
