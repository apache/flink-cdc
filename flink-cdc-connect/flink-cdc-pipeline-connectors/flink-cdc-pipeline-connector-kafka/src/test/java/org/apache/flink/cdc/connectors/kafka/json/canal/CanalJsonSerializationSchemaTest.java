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
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafka.format.JsonFormatOptionsUtil;
import org.apache.flink.cdc.connectors.kafka.format.canal.CanalJsonSerializationSchema;
import org.apache.flink.cdc.connectors.kafka.json.MockInitializationContext;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.apache.flink.cdc.connectors.kafka.format.JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
import static org.apache.flink.cdc.connectors.kafka.format.canal.CanalJsonFormatOptions.JSON_MAP_NULL_KEY_LITERAL;

/** Tests for {@link CanalJsonSerializationSchema}. */
public class CanalJsonSerializationSchemaTest {

    public static final TableId TABLE_1 =
            TableId.tableId("default_namespace", "default_schema", "table1");

    @Test
    public void testSerialize() throws Exception {
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        Configuration formatOptions = new Configuration();
        TimestampFormat timestampFormat = JsonFormatOptionsUtil.getTimestampFormat(formatOptions);
        JsonFormatOptions.MapNullKeyMode mapNullKeyMode =
                JsonFormatOptionsUtil.getMapNullKeyMode(formatOptions);
        String mapNullKeyLiteral = formatOptions.get(JSON_MAP_NULL_KEY_LITERAL);

        Boolean encodeDecimalAsPlainNumber = formatOptions.get(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        SerializationSchema<Event> serializationSchema =
                new CanalJsonSerializationSchema(
                        timestampFormat,
                        mapNullKeyMode,
                        mapNullKeyLiteral,
                        ZoneId.systemDefault(),
                        encodeDecimalAsPlainNumber);
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
                        "{\"old\":null,\"data\":[{\"col1\":\"1\",\"col2\":\"1\"}],\"type\":\"INSERT\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
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
                        "{\"old\":null,\"data\":[{\"col1\":\"2\",\"col2\":\"2\"}],\"type\":\"INSERT\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
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
                        "{\"old\":[{\"col1\":\"2\",\"col2\":\"2\"}],\"data\":null,\"type\":\"DELETE\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
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
                        "{\"old\":[{\"col1\":\"1\",\"col2\":\"1\"}],\"data\":[{\"col1\":\"1\",\"col2\":\"x\"}],\"type\":\"UPDATE\",\"database\":\"default_schema\",\"table\":\"table1\",\"pkNames\":[\"col1\"]}");
        actual = mapper.readTree(serializationSchema.serialize(updateEvent));
        Assertions.assertEquals(expected, actual);
    }
}
