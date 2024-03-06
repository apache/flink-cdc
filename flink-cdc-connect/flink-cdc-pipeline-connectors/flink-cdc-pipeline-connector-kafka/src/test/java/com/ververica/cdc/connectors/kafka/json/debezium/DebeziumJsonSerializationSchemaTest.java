/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.kafka.json.debezium;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.connectors.kafka.json.ChangeLogJsonFormatFactory;
import com.ververica.cdc.connectors.kafka.json.JsonSerializationType;
import com.ververica.cdc.connectors.kafka.json.MockInitializationContext;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link DebeziumJsonSerializationSchema}. */
public class DebeziumJsonSerializationSchemaTest {

    public static final TableId TABLE_1 =
            TableId.tableId("default_namespace", "default_schema", "table1");

    @Test
    public void testSerialize() throws Exception {
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        SerializationSchema<Event> serializationSchema =
                ChangeLogJsonFormatFactory.createSerializationSchema(
                        new Configuration(), JsonSerializationType.DEBEZIUM_JSON);
        serializationSchema.open(new MockInitializationContext());
        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        Assertions.assertNull(serializationSchema.serialize(createTableEvent));
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        // insert
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
                        "{\"before\":null,\"after\":{\"f0\":\"1\",\"f1\":\"1\"},\"op\":\"c\"}");
        JsonNode actual = mapper.readTree(serializationSchema.serialize(insertEvent1));
        Assertions.assertEquals(expected, actual);
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
                        "{\"before\":null,\"after\":{\"f0\":\"2\",\"f1\":\"2\"},\"op\":\"c\"}");
        actual = mapper.readTree(serializationSchema.serialize(insertEvent2));
        Assertions.assertEquals(expected, actual);
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
                        "{\"before\":{\"f0\":\"2\",\"f1\":\"2\"},\"after\":null,\"op\":\"d\"}");
        actual = mapper.readTree(serializationSchema.serialize(deleteEvent));
        Assertions.assertEquals(expected, actual);
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
                        "{\"before\":{\"f0\":\"1\",\"f1\":\"1\"},\"after\":{\"f0\":\"1\",\"f1\":\"x\"},\"op\":\"u\"}");
        actual = mapper.readTree(serializationSchema.serialize(updateEvent));
        Assertions.assertEquals(expected, actual);
    }
}
