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

package org.apache.flink.cdc.runtime.typeutils;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/** Test cases for {@link BinaryRecordDataExtractor}. */
public class BinaryRecordDataExtractorTest {
    public static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("bool_col", DataTypes.BOOLEAN())
                    .physicalColumn("tinyint_col", DataTypes.TINYINT())
                    .physicalColumn("smallint_col", DataTypes.SMALLINT())
                    .physicalColumn("int_col", DataTypes.INT())
                    .physicalColumn("bigint_col", DataTypes.BIGINT())
                    .physicalColumn("float_col", DataTypes.FLOAT())
                    .physicalColumn("double_col", DataTypes.DOUBLE())
                    .physicalColumn("decimal_col", DataTypes.DECIMAL(17, 10))
                    .physicalColumn("char_col", DataTypes.CHAR(17))
                    .physicalColumn("varchar_col", DataTypes.VARCHAR(17))
                    .physicalColumn("bin_col", DataTypes.BINARY(17))
                    .physicalColumn("varbin_col", DataTypes.VARBINARY(17))
                    .physicalColumn("date_col", DataTypes.DATE())
                    .physicalColumn("time_col", DataTypes.TIME(9))
                    .physicalColumn("ts_col", DataTypes.TIMESTAMP(3))
                    .physicalColumn("ts_tz_col", DataTypes.TIMESTAMP_TZ(3))
                    .physicalColumn("ts_ltz_col", DataTypes.TIMESTAMP_LTZ(3))
                    .physicalColumn("array_col", DataTypes.ARRAY(DataTypes.STRING()))
                    .physicalColumn("map_col", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                    .physicalColumn("row_col", DataTypes.ROW(DataTypes.INT(), DataTypes.DOUBLE()))
                    .build();

    public static List<RecordData> generateEventWithAllTypes() {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(SCHEMA.getColumnDataTypes().toArray(new DataType[0]));
        BinaryRecordDataGenerator nestedGenerator =
                new BinaryRecordDataGenerator(DataTypes.ROW(DataTypes.INT(), DataTypes.DOUBLE()));
        List<RecordData> events = new ArrayList<>();
        events.add(
                generator.generate(
                        new Object[] {
                            1,
                            true,
                            (byte) 2,
                            (short) 3,
                            4,
                            5L,
                            6.0F,
                            7.0D,
                            DecimalData.fromUnscaledLong(1234567890, 17, 10),
                            BinaryStringData.fromString("Eight"),
                            BinaryStringData.fromString("Nine"),
                            "Ten\1".getBytes(),
                            "Eleven\2".getBytes(),
                            DateData.fromLocalDate(LocalDate.of(2019, 12, 31)),
                            TimeData.fromLocalTime(LocalTime.of(8, 30, 17, 123456789)),
                            TimestampData.fromLocalDateTime(
                                    LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)),
                            ZonedTimestampData.fromZonedDateTime(
                                    LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)
                                            .atZone(ZoneId.of("+05:00"))),
                            LocalZonedTimestampData.fromInstant(
                                    LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)
                                            .atZone(ZoneId.of("+05:00"))
                                            .toInstant()),
                            new GenericArrayData(
                                    new BinaryStringData[] {
                                        BinaryStringData.fromString("One"),
                                        BinaryStringData.fromString("Two"),
                                        BinaryStringData.fromString("Three")
                                    }),
                            new GenericMapData(
                                    ImmutableMap.of(
                                            1,
                                            BinaryStringData.fromString("yi"),
                                            2,
                                            BinaryStringData.fromString("er"),
                                            3,
                                            BinaryStringData.fromString("san"))),
                            nestedGenerator.generate(new Object[] {3, .1415926})
                        }));
        events.add(
                generator.generate(
                        new Object[] {
                            -1,
                            false,
                            (byte) -2,
                            (short) -3,
                            -4,
                            -5L,
                            -6.0F,
                            -7.0D,
                            DecimalData.fromUnscaledLong(-1234567890, 17, 10),
                            BinaryStringData.fromString("-Eight"),
                            BinaryStringData.fromString("-Nine"),
                            "-Ten\1".getBytes(),
                            "-Eleven\2".getBytes(),
                            DateData.fromLocalDate(LocalDate.of(2019, 12, 31)),
                            TimeData.fromLocalTime(LocalTime.of(8, 30, 17, 123456789)),
                            TimestampData.fromLocalDateTime(
                                    LocalDateTime.of(2021, 11, 11, 11, 11, 11, 11)),
                            ZonedTimestampData.fromZonedDateTime(
                                    LocalDateTime.of(2021, 11, 11, 11, 11, 11, 11)
                                            .atZone(ZoneId.of("+05:00"))),
                            LocalZonedTimestampData.fromInstant(
                                    LocalDateTime.of(2021, 11, 11, 11, 11, 11, 11)
                                            .atZone(ZoneId.of("+05:00"))
                                            .toInstant()),
                            new GenericArrayData(
                                    new BinaryStringData[] {
                                        BinaryStringData.fromString("Ninety"),
                                        BinaryStringData.fromString("Eighty"),
                                        BinaryStringData.fromString("Seventy")
                                    }),
                            new GenericMapData(
                                    ImmutableMap.of(
                                            7, BinaryStringData.fromString("qi"),
                                            8, BinaryStringData.fromString("ba"),
                                            9, BinaryStringData.fromString("jiu"))),
                            nestedGenerator.generate(new Object[] {2, .718281828})
                        }));
        events.add(
                generator.generate(
                        new Object[] {
                            0, null, null, null, null, null, null, null, null, null, null, null,
                            null, null, null, null, null, null, null, null, null
                        }));
        events.add(null);
        return events;
    }

    @Test
    void testConvertingBinaryRecordData() {
        Assertions.assertThat(generateEventWithAllTypes())
                .map(e -> BinaryRecordDataExtractor.extractRecord(e, SCHEMA.toRowDataType()))
                .containsExactly(
                        "{id: INT NOT NULL -> 1, bool_col: BOOLEAN -> true, tinyint_col: TINYINT -> 2, smallint_col: SMALLINT -> 3, int_col: INT -> 4, bigint_col: BIGINT -> 5, float_col: FLOAT -> 6.0, double_col: DOUBLE -> 7.0, decimal_col: DECIMAL(17, 10) -> 0.1234567890, char_col: CHAR(17) -> Eight, varchar_col: VARCHAR(17) -> Nine, bin_col: BINARY(17) -> VGVuAQ==, varbin_col: VARBINARY(17) -> RWxldmVuAg==, date_col: DATE -> 2019-12-31, time_col: TIME(9) -> 08:30:17.123, ts_col: TIMESTAMP(3) -> 2023-11-11T11:11:11.000000011, ts_tz_col: TIMESTAMP(3) WITH TIME ZONE -> 2023-11-11T11:11:11.000000011+05:00, ts_ltz_col: TIMESTAMP_LTZ(3) -> 2023-11-11T06:11:11.000000011, array_col: ARRAY<STRING> -> [One, Two, Three], map_col: MAP<INT, STRING> -> {1 -> yi, 2 -> er, 3 -> san}, row_col: ROW<`f0` INT, `f1` DOUBLE> -> {f0: INT -> 3, f1: DOUBLE -> 0.1415926}}",
                        "{id: INT NOT NULL -> -1, bool_col: BOOLEAN -> false, tinyint_col: TINYINT -> -2, smallint_col: SMALLINT -> -3, int_col: INT -> -4, bigint_col: BIGINT -> -5, float_col: FLOAT -> -6.0, double_col: DOUBLE -> -7.0, decimal_col: DECIMAL(17, 10) -> -0.1234567890, char_col: CHAR(17) -> -Eight, varchar_col: VARCHAR(17) -> -Nine, bin_col: BINARY(17) -> LVRlbgE=, varbin_col: VARBINARY(17) -> LUVsZXZlbgI=, date_col: DATE -> 2019-12-31, time_col: TIME(9) -> 08:30:17.123, ts_col: TIMESTAMP(3) -> 2021-11-11T11:11:11.000000011, ts_tz_col: TIMESTAMP(3) WITH TIME ZONE -> 2021-11-11T11:11:11.000000011+05:00, ts_ltz_col: TIMESTAMP_LTZ(3) -> 2021-11-11T06:11:11.000000011, array_col: ARRAY<STRING> -> [Ninety, Eighty, Seventy], map_col: MAP<INT, STRING> -> {7 -> qi, 8 -> ba, 9 -> jiu}, row_col: ROW<`f0` INT, `f1` DOUBLE> -> {f0: INT -> 2, f1: DOUBLE -> 0.718281828}}",
                        "{id: INT NOT NULL -> 0, bool_col: BOOLEAN -> null, tinyint_col: TINYINT -> null, smallint_col: SMALLINT -> null, int_col: INT -> null, bigint_col: BIGINT -> null, float_col: FLOAT -> null, double_col: DOUBLE -> null, decimal_col: DECIMAL(17, 10) -> null, char_col: CHAR(17) -> null, varchar_col: VARCHAR(17) -> null, bin_col: BINARY(17) -> null, varbin_col: VARBINARY(17) -> null, date_col: DATE -> null, time_col: TIME(9) -> null, ts_col: TIMESTAMP(3) -> null, ts_tz_col: TIMESTAMP(3) WITH TIME ZONE -> null, ts_ltz_col: TIMESTAMP_LTZ(3) -> null, array_col: ARRAY<STRING> -> null, map_col: MAP<INT, STRING> -> null, row_col: ROW<`f0` INT, `f1` DOUBLE> -> null}",
                        "null");
    }

    @Test
    void testConvertingBinaryRecordDataWithSchema() {
        Assertions.assertThat(generateEventWithAllTypes())
                .map(e -> BinaryRecordDataExtractor.extractRecord(e, SCHEMA))
                .containsExactly(
                        "{id: INT NOT NULL -> 1, bool_col: BOOLEAN -> true, tinyint_col: TINYINT -> 2, smallint_col: SMALLINT -> 3, int_col: INT -> 4, bigint_col: BIGINT -> 5, float_col: FLOAT -> 6.0, double_col: DOUBLE -> 7.0, decimal_col: DECIMAL(17, 10) -> 0.1234567890, char_col: CHAR(17) -> Eight, varchar_col: VARCHAR(17) -> Nine, bin_col: BINARY(17) -> VGVuAQ==, varbin_col: VARBINARY(17) -> RWxldmVuAg==, date_col: DATE -> 2019-12-31, time_col: TIME(9) -> 08:30:17.123, ts_col: TIMESTAMP(3) -> 2023-11-11T11:11:11.000000011, ts_tz_col: TIMESTAMP(3) WITH TIME ZONE -> 2023-11-11T11:11:11.000000011+05:00, ts_ltz_col: TIMESTAMP_LTZ(3) -> 2023-11-11T06:11:11.000000011, array_col: ARRAY<STRING> -> [One, Two, Three], map_col: MAP<INT, STRING> -> {1 -> yi, 2 -> er, 3 -> san}, row_col: ROW<`f0` INT, `f1` DOUBLE> -> {f0: INT -> 3, f1: DOUBLE -> 0.1415926}}",
                        "{id: INT NOT NULL -> -1, bool_col: BOOLEAN -> false, tinyint_col: TINYINT -> -2, smallint_col: SMALLINT -> -3, int_col: INT -> -4, bigint_col: BIGINT -> -5, float_col: FLOAT -> -6.0, double_col: DOUBLE -> -7.0, decimal_col: DECIMAL(17, 10) -> -0.1234567890, char_col: CHAR(17) -> -Eight, varchar_col: VARCHAR(17) -> -Nine, bin_col: BINARY(17) -> LVRlbgE=, varbin_col: VARBINARY(17) -> LUVsZXZlbgI=, date_col: DATE -> 2019-12-31, time_col: TIME(9) -> 08:30:17.123, ts_col: TIMESTAMP(3) -> 2021-11-11T11:11:11.000000011, ts_tz_col: TIMESTAMP(3) WITH TIME ZONE -> 2021-11-11T11:11:11.000000011+05:00, ts_ltz_col: TIMESTAMP_LTZ(3) -> 2021-11-11T06:11:11.000000011, array_col: ARRAY<STRING> -> [Ninety, Eighty, Seventy], map_col: MAP<INT, STRING> -> {7 -> qi, 8 -> ba, 9 -> jiu}, row_col: ROW<`f0` INT, `f1` DOUBLE> -> {f0: INT -> 2, f1: DOUBLE -> 0.718281828}}",
                        "{id: INT NOT NULL -> 0, bool_col: BOOLEAN -> null, tinyint_col: TINYINT -> null, smallint_col: SMALLINT -> null, int_col: INT -> null, bigint_col: BIGINT -> null, float_col: FLOAT -> null, double_col: DOUBLE -> null, decimal_col: DECIMAL(17, 10) -> null, char_col: CHAR(17) -> null, varchar_col: VARCHAR(17) -> null, bin_col: BINARY(17) -> null, varbin_col: VARBINARY(17) -> null, date_col: DATE -> null, time_col: TIME(9) -> null, ts_col: TIMESTAMP(3) -> null, ts_tz_col: TIMESTAMP(3) WITH TIME ZONE -> null, ts_ltz_col: TIMESTAMP_LTZ(3) -> null, array_col: ARRAY<STRING> -> null, map_col: MAP<INT, STRING> -> null, row_col: ROW<`f0` INT, `f1` DOUBLE> -> null}",
                        "null");
    }
}
