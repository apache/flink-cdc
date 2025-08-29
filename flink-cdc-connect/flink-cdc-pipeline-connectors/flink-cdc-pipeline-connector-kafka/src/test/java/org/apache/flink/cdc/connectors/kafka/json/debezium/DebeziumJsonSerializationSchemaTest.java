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

package org.apache.flink.cdc.connectors.kafka.json.debezium;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DebeziumJsonSerializationSchema}. */
class DebeziumJsonSerializationSchemaTest {

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
                        JsonSerializationType.DEBEZIUM_JSON,
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
        assertThat(serializationSchema.serialize(createTableEvent)).isNull();
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
                        "{\"before\":null,\"after\":{\"col1\":\"1\",\"col2\":\"1\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"table1\"}}");
        JsonNode actual = mapper.readTree(serializationSchema.serialize(insertEvent1));
        assertThat(actual).isEqualTo(expected);
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
                        "{\"before\":null,\"after\":{\"col1\":\"2\",\"col2\":\"2\"},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"table1\"}}");
        actual = mapper.readTree(serializationSchema.serialize(insertEvent2));
        assertThat(actual).isEqualTo(expected);
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
                        "{\"before\":{\"col1\":\"2\",\"col2\":\"2\"},\"after\":null,\"op\":\"d\",\"source\":{\"db\":\"default_schema\",\"table\":\"table1\"}}");
        actual = mapper.readTree(serializationSchema.serialize(deleteEvent));
        assertThat(actual).isEqualTo(expected);
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
                        "{\"before\":{\"col1\":\"1\",\"col2\":\"1\"},\"after\":{\"col1\":\"1\",\"col2\":\"x\"},\"op\":\"u\",\"source\":{\"db\":\"default_schema\",\"table\":\"table1\"}}");
        actual = mapper.readTree(serializationSchema.serialize(updateEvent));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testSerializeWithSchemaAllDataTypes() throws Exception {
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        Map<String, String> properties = new HashMap<>();
        properties.put("include-schema.enabled", "true");
        Configuration configuration = Configuration.fromMap(properties);
        SerializationSchema<Event> serializationSchema =
                ChangeLogJsonFormatFactory.createSerializationSchema(
                        configuration, JsonSerializationType.DEBEZIUM_JSON, ZoneId.systemDefault());
        serializationSchema.open(new MockInitializationContext());
        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("_boolean", DataTypes.BOOLEAN(), "_boolean comment")
                        .physicalColumn("_binary", DataTypes.BINARY(3))
                        .physicalColumn("_varbinary", DataTypes.VARBINARY(10))
                        .physicalColumn("_bytes", DataTypes.BYTES())
                        .physicalColumn("_tinyint", DataTypes.TINYINT())
                        .physicalColumn("_smallint", DataTypes.SMALLINT())
                        .physicalColumn("_int", DataTypes.INT())
                        .physicalColumn("_bigint", DataTypes.BIGINT())
                        .physicalColumn("_float", DataTypes.FLOAT())
                        .physicalColumn("_double", DataTypes.DOUBLE())
                        .physicalColumn("_decimal", DataTypes.DECIMAL(6, 3))
                        .physicalColumn("_char", DataTypes.CHAR(5))
                        .physicalColumn("_varchar", DataTypes.VARCHAR(10))
                        .physicalColumn("_string", DataTypes.STRING())
                        .physicalColumn("_date", DataTypes.DATE())
                        .physicalColumn("_time", DataTypes.TIME())
                        .physicalColumn("_time_6", DataTypes.TIME(6))
                        .physicalColumn("_timestamp", DataTypes.TIMESTAMP())
                        .physicalColumn("_timestamp_3", DataTypes.TIMESTAMP(3))
                        .physicalColumn("_timestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                        .physicalColumn("_timestamp_ltz_3", DataTypes.TIMESTAMP_LTZ(3))
                        .physicalColumn("pt", DataTypes.STRING())
                        .primaryKey("pt")
                        .build();

        RowType rowType =
                RowType.of(
                        DataTypes.BOOLEAN(),
                        DataTypes.BINARY(3),
                        DataTypes.VARBINARY(10),
                        DataTypes.BYTES(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DECIMAL(6, 3),
                        DataTypes.CHAR(5),
                        DataTypes.VARCHAR(10),
                        DataTypes.STRING(),
                        DataTypes.DATE(),
                        DataTypes.TIME(),
                        DataTypes.TIME(6),
                        DataTypes.TIMESTAMP(),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP_LTZ(),
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.STRING());

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        assertThat(serializationSchema.serialize(createTableEvent)).isNull();
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        // insert
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    true,
                                    new byte[] {1, 2},
                                    new byte[] {3, 4},
                                    new byte[] {5, 6, 7},
                                    (byte) 1,
                                    (short) 2,
                                    3,
                                    4L,
                                    5.1f,
                                    6.2,
                                    DecimalData.fromBigDecimal(new BigDecimal("7.123"), 6, 3),
                                    BinaryStringData.fromString("test1"),
                                    BinaryStringData.fromString("test2"),
                                    BinaryStringData.fromString("test3"),
                                    DateData.fromEpochDay(100),
                                    TimeData.fromNanoOfDay(200_000_000L),
                                    TimeData.fromNanoOfDay(300_000_000L),
                                    TimestampData.fromTimestamp(
                                            java.sql.Timestamp.valueOf("2023-01-01 00:00:00.000")),
                                    TimestampData.fromTimestamp(
                                            java.sql.Timestamp.valueOf("2023-01-01 00:00:00")),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z")),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z")),
                                    null
                                }));
        JsonNode expected =
                mapper.readTree(
                        "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"boolean\",\"optional\":true,\"doc\":\"_boolean comment\",\"field\":\"_boolean\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"io.debezium.data.Bits\",\"version\":1,\"parameters\":{\"length\":\"3\"},\"field\":\"_binary\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"io.debezium.data.Bits\",\"version\":1,\"parameters\":{\"length\":\"10\"},\"field\":\"_varbinary\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"io.debezium.data.Bits\",\"version\":1,\"parameters\":{\"length\":\"2147483647\"},\"field\":\"_bytes\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"_tinyint\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"_smallint\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"_int\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"_bigint\"},{\"type\":\"float\",\"optional\":true,\"field\":\"_float\"},{\"type\":\"double\",\"optional\":true,\"field\":\"_double\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"3\",\"connect.decimal.precision\":\"6\"},\"field\":\"_decimal\"},{\"type\":\"string\",\"optional\":true,\"field\":\"_char\"},{\"type\":\"string\",\"optional\":true,\"field\":\"_varchar\"},{\"type\":\"string\",\"optional\":true,\"field\":\"_string\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"_date\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"_time\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"_time_6\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTimestamp\",\"version\":1,\"field\":\"_timestamp\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"field\":\"_timestamp_3\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.time.ZonedTimestamp\",\"version\":1,\"field\":\"_timestamp_ltz\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.time.ZonedTimestamp\",\"version\":1,\"field\":\"_timestamp_ltz_3\"},{\"type\":\"string\",\"optional\":true,\"field\":\"pt\"}],\"optional\":true,\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"boolean\",\"optional\":true,\"doc\":\"_boolean comment\",\"field\":\"_boolean\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"io.debezium.data.Bits\",\"version\":1,\"parameters\":{\"length\":\"3\"},\"field\":\"_binary\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"io.debezium.data.Bits\",\"version\":1,\"parameters\":{\"length\":\"10\"},\"field\":\"_varbinary\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"io.debezium.data.Bits\",\"version\":1,\"parameters\":{\"length\":\"2147483647\"},\"field\":\"_bytes\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"_tinyint\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"_smallint\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"_int\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"_bigint\"},{\"type\":\"float\",\"optional\":true,\"field\":\"_float\"},{\"type\":\"double\",\"optional\":true,\"field\":\"_double\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"3\",\"connect.decimal.precision\":\"6\"},\"field\":\"_decimal\"},{\"type\":\"string\",\"optional\":true,\"field\":\"_char\"},{\"type\":\"string\",\"optional\":true,\"field\":\"_varchar\"},{\"type\":\"string\",\"optional\":true,\"field\":\"_string\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"_date\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"_time\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"_time_6\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTimestamp\",\"version\":1,\"field\":\"_timestamp\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"field\":\"_timestamp_3\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.time.ZonedTimestamp\",\"version\":1,\"field\":\"_timestamp_ltz\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.time.ZonedTimestamp\",\"version\":1,\"field\":\"_timestamp_ltz_3\"},{\"type\":\"string\",\"optional\":true,\"field\":\"pt\"}],\"optional\":true,\"field\":\"after\"}],\"optional\":false},\"payload\":{\"before\":null,\"after\":{\"_boolean\":true,\"_binary\":\"AQI=\",\"_varbinary\":\"AwQ=\",\"_bytes\":\"BQYH\",\"_tinyint\":1,\"_smallint\":2,\"_int\":3,\"_bigint\":4,\"_float\":5.1,\"_double\":6.2,\"_decimal\":7.123,\"_char\":\"test1\",\"_varchar\":\"test2\",\"_string\":\"test3\",\"_date\":\"1970-04-11\",\"_time\":\"00:00:00\",\"_time_6\":\"00:00:00\",\"_timestamp\":\"2023-01-01 00:00:00\",\"_timestamp_3\":\"2023-01-01 00:00:00\",\"_timestamp_ltz\":\"2023-01-01 00:00:00Z\",\"_timestamp_ltz_3\":\"2023-01-01 00:00:00Z\",\"pt\":null},\"op\":\"c\",\"source\":{\"db\":\"default_schema\",\"table\":\"table1\"}}}");
        JsonNode actual = mapper.readTree(serializationSchema.serialize(insertEvent1));
        assertThat(actual).isEqualTo(expected);
    }
}
