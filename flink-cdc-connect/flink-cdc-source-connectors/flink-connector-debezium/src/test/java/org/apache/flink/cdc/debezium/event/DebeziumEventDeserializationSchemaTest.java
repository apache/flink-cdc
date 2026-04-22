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

package org.apache.flink.cdc.debezium.event;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.data.Bits;
import io.debezium.data.Envelope;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.time.Year;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DebeziumEventDeserializationSchema}. */
class DebeziumEventDeserializationSchemaTest {

    private static final TableId TABLE_ID = TableId.tableId("test_db", "orders");

    @Test
    void testCachedSchemaMismatchFallsBackToValueInference() throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.DECIMAL(20, 0).notNull())
                                .physicalColumn("name", DataTypes.STRING())
                                .primaryKey("id")
                                .build()));

        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct()
                                        .field(
                                                "name",
                                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                                        .build(),
                                Collections.singletonMap("name", "alice")));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getString(0)).isEqualTo(BinaryStringData.fromString("alice"));
    }

    @Test
    void testCachedFieldTypeMismatchFallsBackToValueInference() throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("name", DataTypes.STRING())
                                .primaryKey("id")
                                .build()));

        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct()
                                        .field(
                                                "id",
                                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                                        .field(
                                                "name",
                                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                                        .build(),
                                Map.of("id", "abc", "name", "alice")));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(2);
        assertThat(after.getString(0)).isEqualTo(BinaryStringData.fromString("abc"));
        assertThat(after.getString(1)).isEqualTo(BinaryStringData.fromString("alice"));
    }

    @Test
    void testDecimalSchemaWithoutPrecisionParameterRemainsCompatibleWithCachedPrecision()
            throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.DECIMAL(18, 0).notNull())
                                .physicalColumn("name", DataTypes.STRING())
                                .primaryKey("id")
                                .build()));

        org.apache.kafka.connect.data.Schema decimalSchema =
                org.apache.kafka.connect.data.Decimal.builder(0).build();
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct()
                                        .field("id", decimalSchema)
                                        .field(
                                                "name",
                                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                                        .build(),
                                Map.of("id", BigDecimal.ONE, "name", "alice")));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();
        DecimalData id = after.getDecimal(0, 18, 0);

        assertThat(id.toBigDecimal()).isEqualByComparingTo(BigDecimal.ONE);
        assertThat(after.getString(1)).isEqualTo(BinaryStringData.fromString("alice"));
    }

    @Test
    void testLargePrecisionDecimalSchemaCompatibleWithCachedString() throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("left_value", DataTypes.STRING().notNull())
                                .build()));

        org.apache.kafka.connect.data.Schema decimalSchema =
                org.apache.kafka.connect.data.Decimal.builder(1)
                        .parameter(DebeziumSchemaDataTypeInference.PRECISION_PARAMETER_KEY, "65")
                        .build();
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct().field("left_value", decimalSchema).build(),
                                Collections.singletonMap(
                                        "left_value", new BigDecimal("12345678901234567890.1"))));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getString(0))
                .isEqualTo(BinaryStringData.fromString("12345678901234567890.1"));
    }

    @Test
    void testLargePrecisionDecimalBytesCompatibleWithCachedString() throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        BigDecimal decimalValue = new BigDecimal("12345678901234567890.1");
        org.apache.kafka.connect.data.Schema decimalSchema =
                org.apache.kafka.connect.data.Decimal.builder(1)
                        .parameter(DebeziumSchemaDataTypeInference.PRECISION_PARAMETER_KEY, "65")
                        .build();
        byte[] rawDecimal =
                org.apache.kafka.connect.data.Decimal.fromLogical(decimalSchema, decimalValue);

        assertThat(deserializer.convertToStringForTest(rawDecimal, decimalSchema))
                .isEqualTo(BinaryStringData.fromString("12345678901234567890.1"));
    }

    @Test
    void testRegularPrecisionDecimalSchemaIncompatibleWithCachedStringFallsBackToValueInference()
            throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("left_value", DataTypes.STRING().notNull())
                                .build()));

        org.apache.kafka.connect.data.Schema decimalSchema =
                org.apache.kafka.connect.data.Decimal.builder(1)
                        .parameter(DebeziumSchemaDataTypeInference.PRECISION_PARAMETER_KEY, "10")
                        .build();
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct().field("left_value", decimalSchema).build(),
                                Collections.singletonMap("left_value", new BigDecimal("12345.6"))));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getDecimal(0, 10, 1).toBigDecimal()).isEqualByComparingTo("12345.6");
    }

    @Test
    void testLogicalDateSchemaIncompatibleWithCachedIntegerFallsBackToValueInference()
            throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("created_on", DataTypes.INT().notNull())
                                .build()));

        org.apache.kafka.connect.data.Schema dateSchema =
                org.apache.kafka.connect.data.Date.builder().build();
        LocalDate expectedDate = LocalDate.of(2026, 4, 10);
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct().field("created_on", dateSchema).build(),
                                Collections.singletonMap(
                                        "created_on",
                                        org.apache.kafka.connect.data.Date.toLogical(
                                                dateSchema,
                                                Math.toIntExact(expectedDate.toEpochDay())))));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getDate(0).toLocalDate()).isEqualTo(expectedDate);
    }

    @Test
    void testLogicalTimestampSchemaIncompatibleWithCachedBigintFallsBackToValueInference()
            throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("created_at", DataTypes.BIGINT().notNull())
                                .build()));

        org.apache.kafka.connect.data.Schema timestampSchema =
                org.apache.kafka.connect.data.Timestamp.builder().build();
        LocalDateTime expectedTimestamp = LocalDateTime.of(2026, 4, 10, 10, 5, 6, 789_000_000);
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct().field("created_at", timestampSchema).build(),
                                Collections.singletonMap(
                                        "created_at",
                                        org.apache.kafka.connect.data.Timestamp.toLogical(
                                                timestampSchema,
                                                expectedTimestamp
                                                        .toInstant(ZoneOffset.UTC)
                                                        .toEpochMilli()))));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getTimestamp(0, 3).toLocalDateTime()).isEqualTo(expectedTimestamp);
    }

    @Test
    void testDecimalLogicalBytesSchemaIncompatibleWithCachedBinaryFallsBackToValueInference()
            throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("amount", DataTypes.BYTES().notNull())
                                .build()));

        org.apache.kafka.connect.data.Schema decimalSchema =
                org.apache.kafka.connect.data.Decimal.builder(1)
                        .parameter(DebeziumSchemaDataTypeInference.PRECISION_PARAMETER_KEY, "10")
                        .build();
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct().field("amount", decimalSchema).build(),
                                Collections.singletonMap("amount", new BigDecimal("12345.6"))));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getDecimal(0, 10, 1).toBigDecimal()).isEqualByComparingTo("12345.6");
    }

    @Test
    void testGeometryStructSchemaCompatibleWithCachedString() throws Exception {
        GeometryTestingDebeziumEventDeserializationSchema deserializer =
                new GeometryTestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder().physicalColumn("geom", DataTypes.STRING()).build()));

        org.apache.kafka.connect.data.Schema pointSchema = Point.builder().build();
        org.apache.kafka.connect.data.Schema geometrySchema = Geometry.builder().build();
        org.apache.kafka.connect.data.Schema geographySchema = Geography.builder().build();

        assertThat(
                        deserializeGeometryValue(
                                deserializer, pointSchema, createPointValue(pointSchema)))
                .isEqualTo(Point.LOGICAL_NAME);
        assertThat(
                        deserializeGeometryValue(
                                deserializer,
                                geometrySchema,
                                createGeometryValue(geometrySchema, new byte[] {1, 2, 3})))
                .isEqualTo(Geometry.LOGICAL_NAME);
        assertThat(
                        deserializeGeometryValue(
                                deserializer,
                                geographySchema,
                                createGeometryValue(geographySchema, new byte[] {4, 5, 6})))
                .isEqualTo(Geography.LOGICAL_NAME);
    }

    @Test
    void testUnexpectedActualFieldFallsBackToValueInference() throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .primaryKey("id")
                                .build()));

        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct()
                                        .field(
                                                "id",
                                                org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                                        .field(
                                                "name",
                                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                                        .build(),
                                Map.of("id", 1, "name", "alice")));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(2);
        assertThat(after.getInt(0)).isEqualTo(1);
        assertThat(after.getString(1)).isEqualTo(BinaryStringData.fromString("alice"));
    }

    @Test
    void testUnexpectedNestedActualFieldFallsBackToValueInference() throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn(
                                        "payload",
                                        DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())))
                                .build()));

        org.apache.kafka.connect.data.Schema payloadSchema =
                SchemaBuilder.struct()
                        .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                        .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .build();
        Struct payload = new Struct(payloadSchema).put("id", 1).put("name", "alice");

        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct().field("payload", payloadSchema).build(),
                                Collections.singletonMap("payload", payload)));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();
        RecordData nested = after.getRow(0, 2);

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(nested.getArity()).isEqualTo(2);
        assertThat(nested.getInt(0)).isEqualTo(1);
        assertThat(nested.getString(1)).isEqualTo(BinaryStringData.fromString("alice"));
    }

    @Test
    void testMySqlYearColumnWithCachedIntSchemaIsAccepted() throws Exception {
        // MySQL `YEAR` is mapped to DataTypes.INT() by MySqlTypeUtils.
        // At runtime, Debezium emits INT32 with logical name io.debezium.time.Year.
        // The cached-schema validator must treat this as compatible, not throw a mismatch.
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("birth_year", DataTypes.INT().notNull())
                                .build()));

        org.apache.kafka.connect.data.Schema yearSchema = Year.builder().build();
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct().field("birth_year", yearSchema).build(),
                                Collections.singletonMap("birth_year", 1990)));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getInt(0)).isEqualTo(1990);
    }

    @Test
    void testMySqlYearColumnWithoutCachedSchemaFallsBackToValueInference() throws Exception {
        // Fallback path: no CreateTableEvent cached. Runtime INT32/io.debezium.time.Year must
        // also be accepted by the validator (otherwise the fallback re-throws the same
        // exception and escapes the operator uncaught).
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();

        org.apache.kafka.connect.data.Schema yearSchema = Year.builder().build();
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct().field("birth_year", yearSchema).build(),
                                Collections.singletonMap("birth_year", 2026)));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getInt(0)).isEqualTo(2026);
    }

    @Test
    void testFallbackPathDoubleMismatchIsWrappedWithClearMessage() throws Exception {
        // If a future Debezium logical name makes the validator reject BOTH the cached type
        // and the freshly-inferred type, the second exception used to escape the operator
        // uncaught with confusing "cached type" wording. The fallback is now wrapped so the
        // underlying validator gap surfaces clearly instead.
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema(
                        new DebeziumSchemaDataTypeInference() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            protected org.apache.flink.cdc.common.types.DataType inferInt32(
                                    Object value, org.apache.kafka.connect.data.Schema schema) {
                                // Deliberately infer a type that will not match the cached check
                                // to simulate a "validator gap" scenario.
                                return DataTypes.STRING();
                            }
                        });
        org.apache.kafka.connect.data.Schema pretendYearLikeSchema =
                SchemaBuilder.int32().name("io.acme.futurelogical.Gadget").build();

        assertThatThrownBy(
                        () ->
                                deserializer.deserialize(
                                        insertRecord(
                                                SchemaBuilder.struct()
                                                        .field("col", pretendYearLikeSchema)
                                                        .build(),
                                                Collections.singletonMap("col", 42))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("validator gap")
                .hasMessageContaining("io.acme.futurelogical.Gadget");
    }

    @Test
    void testMySqlBitColumnWithCachedBinarySchemaIsAccepted() throws Exception {
        // MySQL `bit(3)` is parsed to DataTypes.BINARY(1) by MySqlTypeUtils.
        // At runtime, Debezium emits the value as BYTES with logical name io.debezium.data.Bits.
        // The cached-schema validator must treat this as compatible, not throw a mismatch.
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn(
                                        "check_medium_limit_detail", DataTypes.BINARY(1).notNull())
                                .build()));

        org.apache.kafka.connect.data.Schema bitSchema = Bits.builder(3).build();
        byte[] rawBits = new byte[] {0b101};
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct()
                                        .field("check_medium_limit_detail", bitSchema)
                                        .build(),
                                Collections.singletonMap("check_medium_limit_detail", rawBits)));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getBinary(0)).containsExactly(rawBits);
    }

    @Test
    void testMySqlBit8ColumnWithCachedBinarySchemaIsAccepted() throws Exception {
        // MySQL `bit(8)` → DataTypes.BINARY(1).
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("flags", DataTypes.BINARY(1).notNull())
                                .build()));

        org.apache.kafka.connect.data.Schema bitSchema = Bits.builder(8).build();
        byte[] rawBits = new byte[] {(byte) 0xFF};
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct().field("flags", bitSchema).build(),
                                Collections.singletonMap("flags", rawBits)));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getBinary(0)).containsExactly(rawBits);
    }

    @Test
    void testMySqlBit17ColumnWithCachedBinarySchemaIsAccepted() throws Exception {
        // MySQL `bit(17)` → DataTypes.BINARY(3). Debezium still emits BYTES/io.debezium.data.Bits.
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("wide_flags", DataTypes.BINARY(3).notNull())
                                .build()));

        org.apache.kafka.connect.data.Schema bitSchema = Bits.builder(17).build();
        byte[] rawBits = new byte[] {0x01, 0x02, 0x03};
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct().field("wide_flags", bitSchema).build(),
                                Collections.singletonMap("wide_flags", rawBits)));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getBinary(0)).containsExactly(rawBits);
    }

    @Test
    void testMySqlBitColumnWithoutCachedSchemaFallsBackToValueInference() throws Exception {
        // When there is no CreateTableEvent cache, the fallback path must also accept
        // BYTES/io.debezium.data.Bits without throwing a CachedSchemaMismatchException.
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();

        org.apache.kafka.connect.data.Schema bitSchema = Bits.builder(3).build();
        byte[] rawBits = new byte[] {0b010};
        List<? extends Event> events =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct()
                                        .field("check_medium_limit_detail", bitSchema)
                                        .build(),
                                Collections.singletonMap("check_medium_limit_detail", rawBits)));

        DataChangeEvent event = (DataChangeEvent) events.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(1);
        assertThat(after.getBinary(0)).containsExactly(rawBits);
    }

    @Test
    void testSchemaChangeEventsUpdateCachedSchema() throws Exception {
        TestingDebeziumEventDeserializationSchema deserializer =
                new TestingDebeziumEventDeserializationSchema();
        deserializer.applyChangeEvent(
                new CreateTableEvent(
                        TABLE_ID,
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .primaryKey("id")
                                .build()));
        deserializer.setSchemaChangeEvents(
                Collections.singletonList(
                        new AddColumnEvent(
                                TABLE_ID,
                                Collections.singletonList(
                                        AddColumnEvent.last(
                                                Column.physicalColumn(
                                                        "name", DataTypes.STRING()))))));

        List<? extends Event> schemaEvents = deserializer.deserialize(schemaChangeRecord());

        assertThat(schemaEvents).hasSize(1);
        assertThat(
                        deserializer
                                .getCreateTableEventCache()
                                .get(new io.debezium.relational.TableId(null, "test_db", "orders"))
                                .getSchema()
                                .getColumnNames())
                .containsExactly("id", "name");

        List<? extends Event> dataEvents =
                deserializer.deserialize(
                        insertRecord(
                                SchemaBuilder.struct()
                                        .field(
                                                "id",
                                                org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
                                        .field(
                                                "name",
                                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                                        .build(),
                                Map.of("id", 1, "name", "alice")));

        DataChangeEvent event = (DataChangeEvent) dataEvents.get(0);
        RecordData after = event.after();

        assertThat(after.getArity()).isEqualTo(2);
        assertThat(after.getInt(0)).isEqualTo(1);
        assertThat(after.getString(1)).isEqualTo(BinaryStringData.fromString("alice"));
    }

    private static SourceRecord insertRecord(
            org.apache.kafka.connect.data.Schema afterSchema, Map<String, ?> fieldValues) {
        Struct after = new Struct(afterSchema);
        fieldValues.forEach(after::put);

        org.apache.kafka.connect.data.Schema envelopeSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.AFTER, afterSchema)
                        .field(
                                Envelope.FieldName.OPERATION,
                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .build();
        Struct envelope = new Struct(envelopeSchema);
        envelope.put(Envelope.FieldName.AFTER, after);
        envelope.put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code());

        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "data",
                null,
                null,
                null,
                envelopeSchema,
                envelope);
    }

    private static SourceRecord schemaChangeRecord() {
        return new SourceRecord(
                Collections.emptyMap(), Collections.emptyMap(), "schema", null, null, null, null);
    }

    private static String deserializeGeometryValue(
            TestingDebeziumEventDeserializationSchema deserializer,
            org.apache.kafka.connect.data.Schema geometrySchema,
            Struct geometryValue)
            throws Exception {
        return ((DataChangeEvent)
                        deserializer
                                .deserialize(
                                        insertRecord(
                                                SchemaBuilder.struct()
                                                        .field("geom", geometrySchema)
                                                        .build(),
                                                Collections.singletonMap("geom", geometryValue)))
                                .get(0))
                .after()
                .getString(0)
                .toString();
    }

    private static Struct createPointValue(org.apache.kafka.connect.data.Schema pointSchema) {
        return new Struct(pointSchema).put("x", 1.0d).put("y", 2.0d);
    }

    private static Struct createGeometryValue(
            org.apache.kafka.connect.data.Schema geometrySchema, byte[] wkb) {
        return new Struct(geometrySchema).put("wkb", wkb);
    }

    private static final class GeometryTestingDebeziumEventDeserializationSchema
            extends TestingDebeziumEventDeserializationSchema {

        private GeometryTestingDebeziumEventDeserializationSchema() {
            super(new GeometryFailingSchemaDataTypeInference());
        }

        @Override
        protected Object convertToString(
                Object dbzObj, org.apache.kafka.connect.data.Schema schema) {
            return BinaryStringData.fromString(schema.name());
        }
    }

    private static final class GeometryFailingSchemaDataTypeInference
            extends DebeziumSchemaDataTypeInference {

        @Override
        protected org.apache.flink.cdc.common.types.DataType inferStruct(
                Object value, org.apache.kafka.connect.data.Schema schema) {
            if (Point.LOGICAL_NAME.equals(schema.name())
                    || Geometry.LOGICAL_NAME.equals(schema.name())
                    || Geography.LOGICAL_NAME.equals(schema.name())) {
                throw new AssertionError(
                        "Geometry payload should use cached STRING schema without fallback.");
            }
            return super.inferStruct(value, schema);
        }
    }

    private static class TestingDebeziumEventDeserializationSchema
            extends DebeziumEventDeserializationSchema {

        private List<SchemaChangeEvent> schemaChangeEvents = Collections.emptyList();

        private TestingDebeziumEventDeserializationSchema() {
            this(new DebeziumSchemaDataTypeInference());
        }

        private TestingDebeziumEventDeserializationSchema(SchemaDataTypeInference inference) {
            super(inference, DebeziumChangelogMode.ALL);
        }

        private void setSchemaChangeEvents(List<SchemaChangeEvent> schemaChangeEvents) {
            this.schemaChangeEvents = schemaChangeEvents;
        }

        @Override
        protected boolean isDataChangeRecord(SourceRecord record) {
            return "data".equals(record.topic());
        }

        @Override
        protected boolean isSchemaChangeRecord(SourceRecord record) {
            return "schema".equals(record.topic());
        }

        @Override
        protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
            return schemaChangeEvents;
        }

        @Override
        protected TableId getTableId(SourceRecord record) {
            return TABLE_ID;
        }

        @Override
        protected Map<String, String> getMetadata(SourceRecord record) {
            return Collections.emptyMap();
        }

        private BinaryStringData convertToStringForTest(
                Object value, org.apache.kafka.connect.data.Schema schema) {
            return (BinaryStringData) convertToString(value, schema);
        }
    }
}
