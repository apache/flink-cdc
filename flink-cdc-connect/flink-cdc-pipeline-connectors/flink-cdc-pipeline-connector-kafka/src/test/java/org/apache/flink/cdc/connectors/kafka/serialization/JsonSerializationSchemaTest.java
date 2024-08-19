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

package org.apache.flink.cdc.connectors.kafka.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafka.json.MockInitializationContext;
import org.apache.flink.cdc.connectors.kafka.sink.KeyFormat;
import org.apache.flink.cdc.connectors.kafka.sink.KeySerializationFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

/** Tests for {@link JsonSerializationSchema}. */
public class JsonSerializationSchemaTest {

    public static final TableId TABLE_1 =
            TableId.tableId("default_namespace", "default_schema", "table1");

    @Test
    public void testSerialize() throws Exception {
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        SerializationSchema<Event> serializationSchema =
                KeySerializationFactory.createSerializationSchema(
                        new Configuration(), KeyFormat.JSON, ZoneId.systemDefault());
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
                        "{\"TableId\":\"default_namespace.default_schema.table1\",\"col1\":\"1\"}");
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
                        "{\"TableId\":\"default_namespace.default_schema.table1\",\"col1\":\"2\"}");
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
                        "{\"TableId\":\"default_namespace.default_schema.table1\",\"col1\":\"2\"}");
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
                        "{\"TableId\":\"default_namespace.default_schema.table1\",\"col1\":\"1\"}");
        actual = mapper.readTree(serializationSchema.serialize(updateEvent));
        Assertions.assertEquals(expected, actual);
    }
}
