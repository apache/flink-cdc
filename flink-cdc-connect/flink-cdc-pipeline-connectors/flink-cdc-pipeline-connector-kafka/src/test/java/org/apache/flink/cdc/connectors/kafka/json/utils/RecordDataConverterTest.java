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

package org.apache.flink.cdc.connectors.kafka.json.utils;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Unit tests for {@link RecordDataConverter}. */
class RecordDataConverterTest {

    private static final ZoneId TEST_ZONE_ID = ZoneId.of("UTC");

    /** Tests RecordDataConverter.createFieldGetters method for all supported data types. */
    @Test
    void testCreateFieldGettersForAllTypes() {
        // Define nested ROW types for complex type testing
        DataType innerRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("inner_name", DataTypes.STRING()),
                        DataTypes.FIELD("inner_value", DataTypes.INT()));

        DataType outerRowType =
                DataTypes.ROW(
                        DataTypes.FIELD("row_id", DataTypes.BIGINT()),
                        DataTypes.FIELD("row_inner", innerRowType));

        Schema schema =
                Schema.newBuilder()
                        // Basic types
                        .physicalColumn("col_boolean", DataTypes.BOOLEAN())
                        .physicalColumn("col_tinyint", DataTypes.TINYINT())
                        .physicalColumn("col_smallint", DataTypes.SMALLINT())
                        .physicalColumn("col_int", DataTypes.INT())
                        .physicalColumn("col_bigint", DataTypes.BIGINT())
                        .physicalColumn("col_float", DataTypes.FLOAT())
                        .physicalColumn("col_double", DataTypes.DOUBLE())
                        // String types
                        .physicalColumn("col_char", DataTypes.CHAR(10))
                        .physicalColumn("col_varchar", DataTypes.VARCHAR(50))
                        .physicalColumn("col_string", DataTypes.STRING())
                        // Binary types
                        .physicalColumn("col_binary", DataTypes.BINARY(5))
                        .physicalColumn("col_varbinary", DataTypes.VARBINARY(20))
                        .physicalColumn("col_bytes", DataTypes.BYTES())
                        // Decimal types
                        .physicalColumn("col_decimal_small", DataTypes.DECIMAL(6, 3))
                        .physicalColumn("col_decimal_large", DataTypes.DECIMAL(38, 18))
                        // Temporal types
                        .physicalColumn("col_date", DataTypes.DATE())
                        .physicalColumn("col_time", DataTypes.TIME())
                        .physicalColumn("col_time_precision", DataTypes.TIME(6))
                        .physicalColumn("col_timestamp", DataTypes.TIMESTAMP())
                        .physicalColumn("col_timestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                        // Array types
                        .physicalColumn("col_array_int", DataTypes.ARRAY(DataTypes.INT()))
                        .physicalColumn("col_array_string", DataTypes.ARRAY(DataTypes.STRING()))
                        .physicalColumn("col_array_row", DataTypes.ARRAY(innerRowType))
                        // Map types
                        .physicalColumn(
                                "col_map_string_int",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .physicalColumn(
                                "col_map_int_row", DataTypes.MAP(DataTypes.INT(), innerRowType))
                        // Row types
                        .physicalColumn("col_row", innerRowType)
                        .physicalColumn("col_nested_row", outerRowType)
                        // NOT NULL type
                        .physicalColumn("col_int_not_null", DataTypes.INT().notNull())
                        .primaryKey("col_int")
                        .build();

        List<RecordData.FieldGetter> fieldGetters =
                RecordDataConverter.createFieldGetters(schema, TEST_ZONE_ID);

        Assertions.assertThat(fieldGetters).hasSize(28);

        // Prepare test data - create Row data generators
        BinaryRecordDataGenerator innerRowGenerator =
                new BinaryRecordDataGenerator(new DataType[] {DataTypes.STRING(), DataTypes.INT()});
        BinaryRecordDataGenerator outerRowGenerator =
                new BinaryRecordDataGenerator(new DataType[] {DataTypes.BIGINT(), innerRowType});
        RecordData innerRow1 =
                innerRowGenerator.generate(
                        new Object[] {BinaryStringData.fromString("inner1"), 100});
        RecordData innerRow2 =
                innerRowGenerator.generate(
                        new Object[] {BinaryStringData.fromString("inner2"), 200});
        RecordData innerRow3 =
                innerRowGenerator.generate(
                        new Object[] {BinaryStringData.fromString("inner3"), 300});

        RecordData outerRow = outerRowGenerator.generate(new Object[] {999L, innerRow1});
        GenericArrayData intArray = new GenericArrayData(new Integer[] {1, 2, 3, 4, 5});
        GenericArrayData stringArray =
                new GenericArrayData(
                        new Object[] {
                            BinaryStringData.fromString("a"),
                            BinaryStringData.fromString("b"),
                            BinaryStringData.fromString("c")
                        });
        GenericArrayData rowArray = new GenericArrayData(new RecordData[] {innerRow2, innerRow3});

        // Create Map data
        Map<Object, Object> stringIntMap = new HashMap<>();
        stringIntMap.put(BinaryStringData.fromString("key1"), 10);
        stringIntMap.put(BinaryStringData.fromString("key2"), 20);
        GenericMapData mapStringInt = new GenericMapData(stringIntMap);

        Map<Object, Object> intRowMap = new HashMap<>();
        intRowMap.put(1, innerRow1);
        intRowMap.put(2, innerRow2);
        GenericMapData mapIntRow = new GenericMapData(intRowMap);

        Instant testInstant = Instant.parse("2023-06-15T10:30:00.123456Z");
        Object[] testData =
                new Object[] {
                    true,
                    (byte) 127,
                    (short) 32767,
                    2147483647,
                    9223372036854775807L,
                    3.14f,
                    2.718281828,
                    BinaryStringData.fromString("char_val"),
                    BinaryStringData.fromString("varchar_value"),
                    BinaryStringData.fromString("string_value"),
                    new byte[] {1, 2, 3, 4, 5},
                    new byte[] {6, 7, 8, 9, 10, 11, 12},
                    new byte[] {13, 14, 15},
                    DecimalData.fromBigDecimal(new BigDecimal("123.456"), 6, 3),
                    DecimalData.fromBigDecimal(
                            new BigDecimal("12345678901234567890.123456789012345678"), 38, 18),
                    DateData.fromEpochDay(19523),
                    TimeData.fromMillisOfDay(37800000),
                    TimeData.fromNanoOfDay(37800123456000L),
                    TimestampData.fromTimestamp(
                            java.sql.Timestamp.valueOf("2023-06-15 10:30:00.123456")),
                    LocalZonedTimestampData.fromInstant(testInstant),
                    intArray,
                    stringArray,
                    rowArray,
                    mapStringInt,
                    mapIntRow,
                    innerRow1,
                    outerRow,
                    42
                };

        BinaryRecordData recordData =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]))
                        .generate(testData);

        int idx = 0;
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData)).isEqualTo(true);
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData))
                .isEqualTo((byte) 127);
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData))
                .isEqualTo((short) 32767);
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData))
                .isEqualTo(2147483647);
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData))
                .isEqualTo(9223372036854775807L);
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData)).isEqualTo(3.14f);
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData))
                .isEqualTo(2.718281828);

        // Verify string types
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData).toString())
                .isEqualTo("char_val");
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData).toString())
                .isEqualTo("varchar_value");
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData).toString())
                .isEqualTo("string_value");

        // Verify binary types
        Assertions.assertThat((byte[]) fieldGetters.get(idx++).getFieldOrNull(recordData))
                .isEqualTo(new byte[] {1, 2, 3, 4, 5});
        Assertions.assertThat((byte[]) fieldGetters.get(idx++).getFieldOrNull(recordData))
                .isEqualTo(new byte[] {6, 7, 8, 9, 10, 11, 12});
        Assertions.assertThat((byte[]) fieldGetters.get(idx++).getFieldOrNull(recordData))
                .isEqualTo(new byte[] {13, 14, 15});

        // Verify decimal types
        org.apache.flink.table.data.DecimalData decimalSmall =
                (org.apache.flink.table.data.DecimalData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(decimalSmall.toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("123.456"));

        org.apache.flink.table.data.DecimalData decimalLarge =
                (org.apache.flink.table.data.DecimalData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(decimalLarge.toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12345678901234567890.123456789012345678"));

        // Verify temporal types
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData)).isEqualTo(19523);
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData))
                .isEqualTo(37800000);
        Assertions.assertThat(fieldGetters.get(idx++).getFieldOrNull(recordData))
                .isEqualTo(37800123);

        org.apache.flink.table.data.TimestampData timestamp =
                (org.apache.flink.table.data.TimestampData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(timestamp.toTimestamp())
                .isEqualTo(java.sql.Timestamp.valueOf("2023-06-15 10:30:00.123456"));

        org.apache.flink.table.data.TimestampData timestampLtz =
                (org.apache.flink.table.data.TimestampData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(timestampLtz.toInstant()).isEqualTo(testInstant);

        // Verify array types
        org.apache.flink.table.data.ArrayData resultIntArray =
                (org.apache.flink.table.data.ArrayData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(resultIntArray.size()).isEqualTo(5);
        Assertions.assertThat(resultIntArray.getInt(0)).isEqualTo(1);
        Assertions.assertThat(resultIntArray.getInt(4)).isEqualTo(5);

        org.apache.flink.table.data.ArrayData resultStringArray =
                (org.apache.flink.table.data.ArrayData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(resultStringArray.size()).isEqualTo(3);
        Assertions.assertThat(resultStringArray.getString(0).toString()).isEqualTo("a");
        Assertions.assertThat(resultStringArray.getString(2).toString()).isEqualTo("c");

        org.apache.flink.table.data.ArrayData resultRowArray =
                (org.apache.flink.table.data.ArrayData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(resultRowArray.size()).isEqualTo(2);
        org.apache.flink.table.data.RowData arrayRow0 = resultRowArray.getRow(0, 2);
        Assertions.assertThat(arrayRow0.getString(0).toString()).isEqualTo("inner2");
        Assertions.assertThat(arrayRow0.getInt(1)).isEqualTo(200);

        // Verify map types
        org.apache.flink.table.data.MapData resultMapStringInt =
                (org.apache.flink.table.data.MapData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(resultMapStringInt).isNotNull();
        Assertions.assertThat(resultMapStringInt.keyArray().size()).isEqualTo(2);

        org.apache.flink.table.data.MapData resultMapIntRow =
                (org.apache.flink.table.data.MapData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(resultMapIntRow).isNotNull();
        Assertions.assertThat(resultMapIntRow.keyArray().size()).isEqualTo(2);

        // Verify row types
        org.apache.flink.table.data.RowData resultRow =
                (org.apache.flink.table.data.RowData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(resultRow).isNotNull();
        Assertions.assertThat(resultRow.getString(0).toString()).isEqualTo("inner1");
        Assertions.assertThat(resultRow.getInt(1)).isEqualTo(100);

        org.apache.flink.table.data.RowData resultNestedRow =
                (org.apache.flink.table.data.RowData)
                        fieldGetters.get(idx++).getFieldOrNull(recordData);
        Assertions.assertThat(resultNestedRow).isNotNull();
        Assertions.assertThat(resultNestedRow.getLong(0)).isEqualTo(999L);
        org.apache.flink.table.data.RowData nestedInnerRow = resultNestedRow.getRow(1, 2);
        Assertions.assertThat(nestedInnerRow.getString(0).toString()).isEqualTo("inner1");
        Assertions.assertThat(nestedInnerRow.getInt(1)).isEqualTo(100);

        // Verify NOT NULL type
        Assertions.assertThat(fieldGetters.get(idx).getFieldOrNull(recordData)).isEqualTo(42);
    }

    /** Tests the case when all field values are null. */
    @Test
    void testCreateFieldGettersWithAllNullValues() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("string_col", DataTypes.STRING())
                        .physicalColumn("int_col", DataTypes.INT())
                        .physicalColumn("decimal_col", DataTypes.DECIMAL(10, 2))
                        .physicalColumn("timestamp_col", DataTypes.TIMESTAMP())
                        .physicalColumn("array_col", DataTypes.ARRAY(DataTypes.INT()))
                        .physicalColumn(
                                "map_col", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .physicalColumn(
                                "row_col", DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.STRING())))
                        .build();

        List<RecordData.FieldGetter> fieldGetters =
                RecordDataConverter.createFieldGetters(schema, TEST_ZONE_ID);

        Object[] testData = new Object[] {null, null, null, null, null, null, null};

        BinaryRecordData recordData =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]))
                        .generate(testData);

        Assertions.assertThat(fieldGetters).hasSize(7);
        for (int i = 0; i < fieldGetters.size(); i++) {
            Assertions.assertThat(fieldGetters.get(i).getFieldOrNull(recordData)).isNull();
        }
    }

    /** Tests the case with mixed null and non-null values. */
    @Test
    void testCreateFieldGettersWithMixedNullValues() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("string_col", DataTypes.STRING())
                        .physicalColumn("int_col", DataTypes.INT())
                        .physicalColumn("nullable_string", DataTypes.STRING())
                        .physicalColumn("bigint_col", DataTypes.BIGINT())
                        .physicalColumn("nullable_array", DataTypes.ARRAY(DataTypes.INT()))
                        .build();

        List<RecordData.FieldGetter> fieldGetters =
                RecordDataConverter.createFieldGetters(schema, TEST_ZONE_ID);

        Object[] testData =
                new Object[] {BinaryStringData.fromString("test"), 42, null, 100L, null};

        BinaryRecordData recordData =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]))
                        .generate(testData);

        Assertions.assertThat(fieldGetters.get(0).getFieldOrNull(recordData).toString())
                .isEqualTo("test");
        Assertions.assertThat(fieldGetters.get(1).getFieldOrNull(recordData)).isEqualTo(42);
        Assertions.assertThat(fieldGetters.get(2).getFieldOrNull(recordData)).isNull();
        Assertions.assertThat(fieldGetters.get(3).getFieldOrNull(recordData)).isEqualTo(100L);
        Assertions.assertThat(fieldGetters.get(4).getFieldOrNull(recordData)).isNull();
    }

    /** Tests the case with an empty Schema. */
    @Test
    void testCreateFieldGettersForEmptySchema() {
        Schema schema = Schema.newBuilder().build();

        List<RecordData.FieldGetter> fieldGetters =
                RecordDataConverter.createFieldGetters(schema, TEST_ZONE_ID);

        Assertions.assertThat(fieldGetters).isEmpty();
    }

    /** Tests the impact of different time zones on TIMESTAMP_LTZ type. */
    @Test
    void testCreateFieldGettersWithDifferentTimeZones() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("timestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                        .build();

        Instant testInstant = Instant.parse("2023-01-01T12:00:00.000Z");
        Object[] testData = new Object[] {LocalZonedTimestampData.fromInstant(testInstant)};

        List<RecordData.FieldGetter> utcFieldGetters =
                RecordDataConverter.createFieldGetters(schema, ZoneId.of("UTC"));
        BinaryRecordData recordData1 =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]))
                        .generate(testData);
        org.apache.flink.table.data.TimestampData utcResult =
                (org.apache.flink.table.data.TimestampData)
                        utcFieldGetters.get(0).getFieldOrNull(recordData1);
        Assertions.assertThat(utcResult.toInstant()).isEqualTo(testInstant);

        // Test with Asia/Shanghai timezone
        List<RecordData.FieldGetter> shanghaiFieldGetters =
                RecordDataConverter.createFieldGetters(schema, ZoneId.of("Asia/Shanghai"));
        BinaryRecordData recordData2 =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]))
                        .generate(testData);
        org.apache.flink.table.data.TimestampData shanghaiResult =
                (org.apache.flink.table.data.TimestampData)
                        shanghaiFieldGetters.get(0).getFieldOrNull(recordData2);
        Assertions.assertThat(shanghaiResult.toInstant()).isEqualTo(testInstant);
    }

    /** Tests edge cases with empty Array and empty Map. */
    @Test
    void testCreateFieldGettersForEmptyCollections() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("empty_array", DataTypes.ARRAY(DataTypes.INT()))
                        .physicalColumn(
                                "empty_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();

        List<RecordData.FieldGetter> fieldGetters =
                RecordDataConverter.createFieldGetters(schema, TEST_ZONE_ID);

        GenericArrayData emptyArray = new GenericArrayData(new Integer[] {});
        GenericMapData emptyMap = new GenericMapData(new HashMap<>());

        Object[] testData = new Object[] {emptyArray, emptyMap};

        BinaryRecordData recordData =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]))
                        .generate(testData);

        org.apache.flink.table.data.ArrayData resultArray =
                (org.apache.flink.table.data.ArrayData)
                        fieldGetters.get(0).getFieldOrNull(recordData);
        Assertions.assertThat(resultArray.size()).isEqualTo(0);

        org.apache.flink.table.data.MapData resultMap =
                (org.apache.flink.table.data.MapData)
                        fieldGetters.get(1).getFieldOrNull(recordData);
        Assertions.assertThat(resultMap.keyArray().size()).isEqualTo(0);
    }
}
