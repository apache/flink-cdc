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

package org.apache.flink.cdc.connectors.jdbc.sink.v2;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.assertj.core.api.Assertions;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;

/** Test class for {@link EventRecordSerializationSchema}. */
class EventRecordSerializationSchemaTest {

    private static final TableId TABLE_ID = TableId.tableId("foo", "bar", "baz");

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("boolean_col", DataTypes.BOOLEAN())
                    .physicalColumn("decimal_col", DataTypes.DECIMAL(20, 0))
                    .physicalColumn("tinyint_col", DataTypes.TINYINT())
                    .physicalColumn("smallint_col", DataTypes.SMALLINT())
                    .physicalColumn("int_col", DataTypes.INT())
                    .physicalColumn("bigint_col", DataTypes.BIGINT())
                    .physicalColumn("float_col", DataTypes.FLOAT())
                    .physicalColumn("double_col", DataTypes.DOUBLE())
                    .physicalColumn("char_col", DataTypes.CHAR(255))
                    .physicalColumn("varchar_col", DataTypes.VARCHAR(255))
                    .physicalColumn("string_col", DataTypes.STRING())
                    .physicalColumn("binary_col", DataTypes.BINARY(255))
                    .physicalColumn("varbinary_col", DataTypes.VARBINARY(255))
                    .physicalColumn("bytes_col", DataTypes.BYTES())
                    .physicalColumn("ts_col", DataTypes.TIMESTAMP(6))
                    .physicalColumn("ts_ltz_col", DataTypes.TIMESTAMP_LTZ(6))
                    .primaryKey("id")
                    .build();

    private static final BinaryRecordDataGenerator GENERATOR =
            new BinaryRecordDataGenerator(SCHEMA.getColumnDataTypes().toArray(new DataType[0]));

    private static final CreateTableEvent CREATE_TABLE_EVENT =
            new CreateTableEvent(TABLE_ID, SCHEMA);

    private static final BinaryRecordData RECORD_NON_NULL =
            GENERATOR.generate(
                    new Object[] {
                        1L,
                        true,
                        DecimalData.fromBigDecimal(new BigDecimal("12345678901234567890"), 20, 0),
                        (byte) 17,
                        (short) 91102,
                        1234567890,
                        1234567890123456L,
                        123.456f,
                        123.456d,
                        BinaryStringData.fromString("Alice"),
                        BinaryStringData.fromString("Zorro"),
                        BinaryStringData.fromString("A long long long story..."),
                        "Cicada".getBytes(),
                        "Vera".getBytes(),
                        "...that never ends ends ends".getBytes(),
                        TimestampData.fromTimestamp(
                                Timestamp.valueOf("2020-07-17 18:00:22.123456")),
                        LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    });

    private static final BinaryRecordData RECORD_NULL =
            GENERATOR.generate(
                    new Object[] {
                        0L, null, null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null
                    });

    private static final String RECORD_NON_NULL_IN_JSON =
            "{\"int_col\":1234567890,\"char_col\":\"Alice\",\"bytes_col\":\"Li4udGhhdCBuZXZlciBlbmRzIGVuZHMgZW5kcw==\",\"binary_col\":\"Q2ljYWRh\",\"bigint_col\":1234567890123456,\"boolean_col\":true,\"float_col\":123.456,\"varchar_col\":\"Zorro\",\"string_col\":\"A long long long story...\",\"ts_col\":\"2020-07-17T18:00:22.123456\",\"smallint_col\":25566,\"ts_ltz_col\":{\"epochMillisecond\":1595008822000,\"epochNanoOfMillisecond\":0},\"tinyint_col\":17,\"id\":1,\"double_col\":123.456,\"varbinary_col\":\"VmVyYQ==\",\"decimal_col\":\"12345678901234567890\"}";

    private static final String RECORD_NULL_IN_JSON =
            "{\"int_col\":null,\"char_col\":null,\"bytes_col\":null,\"binary_col\":null,\"bigint_col\":null,\"boolean_col\":null,\"float_col\":null,\"varchar_col\":null,\"string_col\":null,\"ts_col\":null,\"smallint_col\":null,\"ts_ltz_col\":null,\"tinyint_col\":null,\"id\":0,\"double_col\":null,\"varbinary_col\":null,\"decimal_col\":null}";

    @Test
    void testSerialization() throws Exception {
        EventRecordSerializationSchema schema = new EventRecordSerializationSchema();

        Assertions.assertThat(schema.serialize(CREATE_TABLE_EVENT))
                .singleElement()
                .extracting("tableId", "schema", "rowKind", "rows")
                .containsExactly(TABLE_ID, null, RowKind.SCHEMA_CHANGE, null);

        Assertions.assertThat(
                        schema.serialize(DataChangeEvent.insertEvent(TABLE_ID, RECORD_NON_NULL)))
                .singleElement()
                .extracting("tableId", "schema", "rowKind", "rows")
                .containsExactly(
                        TABLE_ID, SCHEMA, RowKind.INSERT, RECORD_NON_NULL_IN_JSON.getBytes());

        Assertions.assertThat(schema.serialize(DataChangeEvent.insertEvent(TABLE_ID, RECORD_NULL)))
                .singleElement()
                .extracting("tableId", "schema", "rowKind", "rows")
                .containsExactly(TABLE_ID, SCHEMA, RowKind.INSERT, RECORD_NULL_IN_JSON.getBytes());

        Assertions.assertThat(
                        schema.serialize(
                                DataChangeEvent.updateEvent(
                                        TABLE_ID, RECORD_NON_NULL, RECORD_NULL)))
                .hasSize(2)
                .extracting("tableId", "schema", "rowKind", "rows")
                .containsExactly(
                        Tuple.tuple(
                                TABLE_ID,
                                SCHEMA,
                                RowKind.DELETE,
                                RECORD_NON_NULL_IN_JSON.getBytes()),
                        Tuple.tuple(
                                TABLE_ID, SCHEMA, RowKind.INSERT, RECORD_NULL_IN_JSON.getBytes()));

        Assertions.assertThat(
                        schema.serialize(
                                DataChangeEvent.updateEvent(
                                        TABLE_ID, RECORD_NULL, RECORD_NON_NULL)))
                .hasSize(2)
                .extracting("tableId", "schema", "rowKind", "rows")
                .containsExactly(
                        Tuple.tuple(
                                TABLE_ID, SCHEMA, RowKind.DELETE, RECORD_NULL_IN_JSON.getBytes()),
                        Tuple.tuple(
                                TABLE_ID,
                                SCHEMA,
                                RowKind.INSERT,
                                RECORD_NON_NULL_IN_JSON.getBytes()));

        Assertions.assertThat(schema.serialize(DataChangeEvent.deleteEvent(TABLE_ID, RECORD_NULL)))
                .singleElement()
                .extracting("tableId", "schema", "rowKind", "rows")
                .containsExactly(TABLE_ID, SCHEMA, RowKind.DELETE, RECORD_NULL_IN_JSON.getBytes());
    }

    private static Instant toInstant(String ts) {
        return Timestamp.valueOf(ts).toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant();
    }
}
