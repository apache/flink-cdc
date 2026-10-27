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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafka.json.ChangeLogJsonFormatFactory;
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;
import org.apache.flink.cdc.connectors.kafka.json.MockInitializationContext;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Collections;

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
                        "{\"old\":[{\"col2\":\"1\"}],\"data\":[{\"col1\":\"1\",\"col2\":\"x\"}],\"type\":\"UPDATE\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
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

        org.apache.flink.cdc.common.data.GenericArrayData equalArray =
                new org.apache.flink.cdc.common.data.GenericArrayData(
                        new Object[] {
                            BinaryStringData.fromString("item1"),
                            BinaryStringData.fromString("item2")
                        });
        java.util.Map<Object, Object> reversedMapValues = new java.util.LinkedHashMap<>();
        reversedMapValues.put(BinaryStringData.fromString("key2"), 200);
        reversedMapValues.put(BinaryStringData.fromString("key1"), 100);
        org.apache.flink.cdc.common.data.GenericMapData equalMap =
                new org.apache.flink.cdc.common.data.GenericMapData(reversedMapValues);
        org.apache.flink.cdc.common.data.RecordData equalRow =
                nestedRowGenerator.generate(
                        new Object[] {BinaryStringData.fromString("nested"), 42});
        JsonNode unchanged =
                mapper.readTree(
                        serializationSchema.serialize(
                                DataChangeEvent.updateEvent(
                                        TABLE_1,
                                        generator.generate(
                                                new Object[] {1, arrayData, mapData, nestedRow}),
                                        generator.generate(
                                                new Object[] {
                                                    1, equalArray, equalMap, equalRow
                                                }))));
        Assertions.assertThat(unchanged.get("old").get(0).size()).isZero();
        Assertions.assertThat(unchanged.get("data")).isEqualTo(actual.get("data"));

        org.apache.flink.cdc.common.data.GenericArrayData changedArray =
                new org.apache.flink.cdc.common.data.GenericArrayData(
                        new Object[] {BinaryStringData.fromString("changed")});
        java.util.Map<Object, Object> changedMapValues = new java.util.HashMap<>(mapValues);
        changedMapValues.put(BinaryStringData.fromString("key1"), 101);
        org.apache.flink.cdc.common.data.GenericMapData changedMap =
                new org.apache.flink.cdc.common.data.GenericMapData(changedMapValues);
        org.apache.flink.cdc.common.data.RecordData changedRow =
                nestedRowGenerator.generate(
                        new Object[] {BinaryStringData.fromString("nested"), 43});
        JsonNode changed =
                mapper.readTree(
                        serializationSchema.serialize(
                                DataChangeEvent.updateEvent(
                                        TABLE_1,
                                        generator.generate(
                                                new Object[] {1, arrayData, mapData, nestedRow}),
                                        generator.generate(
                                                new Object[] {
                                                    1, changedArray, changedMap, changedRow
                                                }))));
        Assertions.assertThat(changed.get("old").get(0).size()).isEqualTo(3);
        Assertions.assertThat(changed.get("old").get(0).get("arr")).isEqualTo(arrNode);
        Assertions.assertThat(changed.get("old").get(0).get("map")).isEqualTo(mapNode);
        Assertions.assertThat(changed.get("old").get(0).get("row")).isEqualTo(rowNode);
        Assertions.assertThat(changed.get("data").get(0))
                .isEqualTo(
                        mapper.readTree(
                                "{\"id\":1,\"arr\":[\"changed\"],\"map\":{\"key1\":101,\"key2\":200},\"row\":{\"f1\":\"nested\",\"f2\":43}}"));
    }

    @Test
    void testUpdateNullsAndUnchangedFields() throws Exception {
        SerializationSchema<Event> serializer = createStringSerializer(false);
        BinaryRecordDataGenerator generator = stringGenerator(3);
        assertJson(
                serializer.serialize(
                        DataChangeEvent.updateEvent(
                                TABLE_1,
                                generator.generate(
                                        new Object[] {
                                            null, BinaryStringData.fromString("old"), null
                                        }),
                                generator.generate(
                                        new Object[] {
                                            BinaryStringData.fromString("new"), null, null
                                        }))),
                "{\"old\":[{\"col1\":null,\"col2\":\"old\"}],\"data\":[{\"col1\":\"new\",\"col2\":null,\"col3\":null}],\"type\":\"UPDATE\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
        assertJson(
                serializer.serialize(
                        DataChangeEvent.updateEvent(
                                TABLE_1,
                                generator.generate(
                                        new Object[] {
                                            BinaryStringData.fromString("new"), null, null
                                        }),
                                generator.generate(
                                        new Object[] {
                                            BinaryStringData.fromString("new"), null, null
                                        }))),
                "{\"old\":[{}],\"data\":[{\"col1\":\"new\",\"col2\":null,\"col3\":null}],\"type\":\"UPDATE\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
        JsonNode inserted =
                JacksonMapperFactory.createObjectMapper()
                        .readTree(
                                serializer.serialize(
                                        DataChangeEvent.insertEvent(
                                                TABLE_1,
                                                generator.generate(
                                                        new Object[] {
                                                            BinaryStringData.fromString("new"),
                                                            null,
                                                            null
                                                        }))));
        Assertions.assertThat(inserted.get("old").isNull()).isTrue();
        Assertions.assertThat(inserted.get("data").get(0).size()).isEqualTo(3);
        // A field removed from old must be available again in the next record.
        JsonNode next =
                JacksonMapperFactory.createObjectMapper()
                        .readTree(
                                serializer.serialize(
                                        DataChangeEvent.updateEvent(
                                                TABLE_1,
                                                generator.generate(
                                                        new Object[] {
                                                            BinaryStringData.fromString("new"),
                                                            null,
                                                            null
                                                        }),
                                                generator.generate(
                                                        new Object[] {
                                                            BinaryStringData.fromString("changed"),
                                                            null,
                                                            null
                                                        }))));
        Assertions.assertThat(next.get("old").get(0).toString()).isEqualTo("{\"col1\":\"new\"}");
    }

    @Test
    void testIgnoreNullFields() throws Exception {
        SerializationSchema<Event> serializer = createStringSerializer(true);
        BinaryRecordDataGenerator generator = stringGenerator(3);
        assertJson(
                serializer.serialize(
                        DataChangeEvent.updateEvent(
                                TABLE_1,
                                generator.generate(
                                        new Object[] {
                                            null,
                                            BinaryStringData.fromString("old"),
                                            BinaryStringData.fromString("same")
                                        }),
                                generator.generate(
                                        new Object[] {
                                            BinaryStringData.fromString("new"),
                                            null,
                                            BinaryStringData.fromString("same")
                                        }))),
                "{\"old\":[{\"col2\":\"old\"}],\"data\":[{\"col1\":\"new\",\"col3\":\"same\"}],\"type\":\"UPDATE\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
        ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();
        JsonNode inserted =
                mapper.readTree(
                        serializer.serialize(
                                DataChangeEvent.insertEvent(
                                        TABLE_1,
                                        generator.generate(
                                                new Object[] {
                                                    BinaryStringData.fromString("new"), null, null
                                                }))));
        Assertions.assertThat(inserted.has("old")).isFalse();
        Assertions.assertThat(inserted.get("data").get(0).toString())
                .isEqualTo("{\"col1\":\"new\"}");
        JsonNode deleted =
                mapper.readTree(
                        serializer.serialize(
                                DataChangeEvent.deleteEvent(
                                        TABLE_1,
                                        generator.generate(
                                                new Object[] {
                                                    BinaryStringData.fromString("new"), null, null
                                                }))));
        Assertions.assertThat(deleted.has("old")).isFalse();
        Assertions.assertThat(deleted.get("data")).isEqualTo(inserted.get("data"));
    }

    @Test
    void testSchemaChangeAndReplaceWithBefore() throws Exception {
        SerializationSchema<Event> serializer = createStringSerializer(false);
        serializer.serialize(
                new AddColumnEvent(
                        TABLE_1,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("col4", DataTypes.STRING())))));
        BinaryRecordDataGenerator generator = stringGenerator(4);
        JsonNode actual =
                JacksonMapperFactory.createObjectMapper()
                        .readTree(
                                serializer.serialize(
                                        DataChangeEvent.projectBefore(
                                                DataChangeEvent.replaceEvent(
                                                        TABLE_1,
                                                        generator.generate(
                                                                new Object[] {
                                                                    BinaryStringData.fromString(
                                                                            "id"),
                                                                    null,
                                                                    null,
                                                                    BinaryStringData.fromString(
                                                                            "after")
                                                                })),
                                                generator.generate(
                                                        new Object[] {
                                                            BinaryStringData.fromString("id"),
                                                            null,
                                                            null,
                                                            BinaryStringData.fromString("before")
                                                        }))));
        Assertions.assertThat(actual.get("old").get(0).toString())
                .isEqualTo("{\"col4\":\"before\"}");
        Assertions.assertThat(actual.get("data").get(0).size()).isEqualTo(4);
        Assertions.assertThat(actual.get("type").asText()).isEqualTo("UPDATE");
    }

    private static SerializationSchema<Event> createStringSerializer(boolean ignoreNullFields)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(JsonFormatOptions.ENCODE_IGNORE_NULL_FIELDS, ignoreNullFields);
        SerializationSchema<Event> serializer =
                ChangeLogJsonFormatFactory.createSerializationSchema(
                        configuration, JsonSerializationType.CANAL_JSON, ZoneId.systemDefault());
        serializer.open(new MockInitializationContext());
        serializer.serialize(
                new CreateTableEvent(
                        TABLE_1,
                        Schema.newBuilder()
                                .physicalColumn("col1", DataTypes.STRING())
                                .physicalColumn("col2", DataTypes.STRING())
                                .physicalColumn("col3", DataTypes.STRING())
                                .primaryKey("col1")
                                .build()));
        return serializer;
    }

    private static BinaryRecordDataGenerator stringGenerator(int count) {
        org.apache.flink.cdc.common.types.DataType[] types =
                new org.apache.flink.cdc.common.types.DataType[count];
        java.util.Arrays.fill(types, DataTypes.STRING());
        return new BinaryRecordDataGenerator(RowType.of(types));
    }

    private static void assertJson(byte[] actual, String expected) throws Exception {
        ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();
        Assertions.assertThat(mapper.readTree(actual)).isEqualTo(mapper.readTree(expected));
    }
}
