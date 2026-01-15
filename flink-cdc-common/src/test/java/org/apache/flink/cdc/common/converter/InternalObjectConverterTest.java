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

package org.apache.flink.cdc.common.converter;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.converter.InternalObjectConverter.convertToInternal;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test cases for {@link InternalObjectConverter}. */
class InternalObjectConverterTest {

    @Test
    void testConvertToBoolean() {
        assertThat(convertToInternal(true, DataTypes.BOOLEAN())).isEqualTo(true);
        assertThat(convertToInternal(false, DataTypes.BOOLEAN())).isEqualTo(false);
        assertThat(convertToInternal(null, DataTypes.BOOLEAN())).isNull();
    }

    @Test
    void testConvertToBytes() {
        assertThat(convertToInternal("Alice".getBytes(), DataTypes.BYTES()))
                .isInstanceOf(byte[].class)
                .extracting(byte[].class::cast)
                .extracting(Arrays::toString)
                .isEqualTo("[65, 108, 105, 99, 101]");
        assertThat(
                        convertToInternal(
                                new byte[] {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe},
                                DataTypes.BYTES()))
                .isInstanceOf(byte[].class)
                .extracting(byte[].class::cast)
                .extracting(Arrays::toString)
                .isEqualTo("[-54, -2, -70, -66]");
        assertThat(convertToInternal(null, DataTypes.BYTES())).isNull();
    }

    @Test
    void testConvertToBinary() {
        assertThat(convertToInternal("Alice".getBytes(), DataTypes.BINARY(5)))
                .isEqualTo(new byte[] {65, 108, 105, 99, 101});
        assertThat(
                        convertToInternal(
                                new byte[] {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe},
                                DataTypes.BINARY(4)))
                .isEqualTo(new byte[] {-54, -2, -70, -66});
        assertThat(convertToInternal(null, DataTypes.BINARY(3))).isNull();
    }

    @Test
    void testConvertToVarBinary() {
        assertThat(convertToInternal("Alice".getBytes(), DataTypes.VARBINARY(5)))
                .isEqualTo(new byte[] {65, 108, 105, 99, 101});
        assertThat(
                        convertToInternal(
                                new byte[] {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe},
                                DataTypes.VARBINARY(4)))
                .isEqualTo(new byte[] {-54, -2, -70, -66});
        assertThat(convertToInternal(null, DataTypes.VARBINARY(3))).isNull();
    }

    @Test
    void testConvertToChar() {
        assertThat(convertToInternal("Alice", DataTypes.CHAR(5)))
                .isInstanceOf(StringData.class)
                .hasToString("Alice");

        assertThat(convertToInternal(BinaryStringData.fromString("Bob"), DataTypes.CHAR(5)))
                .isInstanceOf(StringData.class)
                .hasToString("Bob");

        assertThat(convertToInternal(null, DataTypes.CHAR(5))).isNull();
    }

    @Test
    void testConvertToVarChar() {
        assertThat(convertToInternal("Alice", DataTypes.VARCHAR(5)))
                .isInstanceOf(StringData.class)
                .hasToString("Alice");

        assertThat(convertToInternal(BinaryStringData.fromString("Bob"), DataTypes.VARCHAR(5)))
                .isInstanceOf(StringData.class)
                .hasToString("Bob");

        assertThat(convertToInternal(null, DataTypes.VARCHAR(5))).isNull();
    }

    @Test
    void testConvertToString() {
        assertThat(convertToInternal("Alice", DataTypes.STRING()))
                .isInstanceOf(StringData.class)
                .hasToString("Alice");
        assertThat(convertToInternal(BinaryStringData.fromString("Bob"), DataTypes.STRING()))
                .isInstanceOf(StringData.class)
                .hasToString("Bob");
        assertThat(convertToInternal(null, DataTypes.STRING())).isNull();
    }

    @Test
    void testConvertToInt() {
        assertThat(convertToInternal(11, DataTypes.INT())).isEqualTo(11);
        assertThat(convertToInternal(-14, DataTypes.INT())).isEqualTo(-14);
        assertThat(convertToInternal(17, DataTypes.INT())).isEqualTo(17);
        assertThat(convertToInternal(null, DataTypes.INT())).isNull();
    }

    @Test
    void testConvertToTinyInt() {
        assertThat(convertToInternal((byte) 11, DataTypes.TINYINT())).isEqualTo((byte) 11);
        assertThat(convertToInternal((byte) -14, DataTypes.TINYINT())).isEqualTo((byte) -14);
        assertThat(convertToInternal((byte) 17, DataTypes.TINYINT())).isEqualTo((byte) 17);
        assertThat(convertToInternal(null, DataTypes.TINYINT())).isNull();
    }

    @Test
    void testConvertToSmallInt() {
        assertThat(convertToInternal((short) 11, DataTypes.SMALLINT())).isEqualTo((short) 11);
        assertThat(convertToInternal((short) -14, DataTypes.SMALLINT())).isEqualTo((short) -14);
        assertThat(convertToInternal((short) 17, DataTypes.SMALLINT())).isEqualTo((short) 17);
        assertThat(convertToInternal(null, DataTypes.SMALLINT())).isNull();
    }

    @Test
    void testConvertToBigInt() {
        assertThat(convertToInternal((long) 11, DataTypes.BIGINT())).isEqualTo((long) 11);
        assertThat(convertToInternal((long) -14, DataTypes.BIGINT())).isEqualTo((long) -14);
        assertThat(convertToInternal((long) 17, DataTypes.BIGINT())).isEqualTo((long) 17);
        assertThat(convertToInternal(null, DataTypes.BIGINT())).isNull();
    }

    @Test
    void testConvertToFloat() {
        assertThat(convertToInternal((float) 11, DataTypes.FLOAT())).isEqualTo((float) 11);
        assertThat(convertToInternal((float) -14, DataTypes.FLOAT())).isEqualTo((float) -14);
        assertThat(convertToInternal((float) 17, DataTypes.FLOAT())).isEqualTo((float) 17);
        assertThat(convertToInternal(null, DataTypes.FLOAT())).isNull();
    }

    @Test
    void testConvertToDouble() {
        assertThat(convertToInternal((double) 11, DataTypes.DOUBLE())).isEqualTo((double) 11);
        assertThat(convertToInternal((double) -14, DataTypes.DOUBLE())).isEqualTo((double) -14);
        assertThat(convertToInternal((double) 17, DataTypes.DOUBLE())).isEqualTo((double) 17);
        assertThat(convertToInternal(null, DataTypes.DOUBLE())).isNull();
    }

    @Test
    void testConvertToDecimal() {
        assertThat(convertToInternal(new BigDecimal("4.2"), DataTypes.DECIMAL(2, 1)))
                .isInstanceOf(DecimalData.class)
                .hasToString("4.2");
        assertThat(convertToInternal(new BigDecimal("-3.1415926"), DataTypes.DECIMAL(20, 10)))
                .isInstanceOf(DecimalData.class)
                .hasToString("-3.1415926");

        assertThat(
                        convertToInternal(
                                DecimalData.fromUnscaledLong(42, 2, 1), DataTypes.DECIMAL(2, 1)))
                .isInstanceOf(DecimalData.class)
                .hasToString("4.2");
        assertThat(
                        convertToInternal(
                                DecimalData.fromUnscaledLong(-31415926, 14, 7),
                                DataTypes.DECIMAL(14, 7)))
                .isInstanceOf(DecimalData.class)
                .hasToString("-3.1415926");

        assertThat(convertToInternal(null, DataTypes.DECIMAL(20, 10))).isNull();
    }

    @Test
    void testConvertToDate() {
        assertThat(convertToInternal(LocalDate.of(2017, 12, 31), DataTypes.DATE()))
                .isInstanceOf(DateData.class)
                .hasToString("2017-12-31");
        assertThat(convertToInternal(DateData.fromEpochDay(14417), DataTypes.DATE()))
                .isInstanceOf(DateData.class)
                .hasToString("2009-06-22");
        assertThat(convertToInternal(null, DataTypes.DATE())).isNull();
    }

    @Test
    void testConvertToTime() {
        assertThat(convertToInternal(LocalTime.of(21, 48, 25), DataTypes.TIME(0)))
                .isInstanceOf(TimeData.class)
                .hasToString("21:48:25");
        assertThat(convertToInternal(LocalTime.ofSecondOfDay(14419), DataTypes.TIME(0)))
                .isInstanceOf(TimeData.class)
                .hasToString("04:00:19");
        assertThat(convertToInternal(LocalTime.of(21, 48, 25, 123456789), DataTypes.TIME(3)))
                .isInstanceOf(TimeData.class)
                .hasToString("21:48:25.123");
        assertThat(convertToInternal(LocalTime.ofNanoOfDay(14419123456789L), DataTypes.TIME(3)))
                .isInstanceOf(TimeData.class)
                .hasToString("04:00:19.123");

        assertThat(
                        convertToInternal(
                                TimeData.fromLocalTime(LocalTime.of(21, 48, 25)),
                                DataTypes.TIME(0)))
                .isInstanceOf(TimeData.class)
                .hasToString("21:48:25");
        assertThat(convertToInternal(TimeData.fromSecondOfDay(14419), DataTypes.TIME(0)))
                .isInstanceOf(TimeData.class)
                .hasToString("04:00:19");
        assertThat(
                        convertToInternal(
                                TimeData.fromLocalTime(LocalTime.of(21, 48, 25, 123456789)),
                                DataTypes.TIME(3)))
                .isInstanceOf(TimeData.class)
                .hasToString("21:48:25.123");
        assertThat(convertToInternal(TimeData.fromNanoOfDay(14419123456789L), DataTypes.TIME(3)))
                .isInstanceOf(TimeData.class)
                .hasToString("04:00:19.123");
        assertThat(convertToInternal(null, DataTypes.TIME())).isNull();
    }

    @Test
    void testConvertToTimestamp() {
        assertThat(
                        convertToInternal(
                                TimestampData.fromMillis(2147483648491L), DataTypes.TIMESTAMP(3)))
                .isInstanceOf(TimestampData.class)
                .hasToString("2038-01-19T03:14:08.491");
        assertThat(
                        convertToInternal(
                                LocalDateTime.of(2019, 12, 25, 21, 48, 25, 123456789),
                                DataTypes.TIMESTAMP(9)))
                .isInstanceOf(TimestampData.class)
                .hasToString("2019-12-25T21:48:25.123456789");
        assertThat(convertToInternal(null, DataTypes.TIMESTAMP())).isNull();
    }

    @Test
    void testConvertToZonedTimestamp() {
        assertThat(
                        convertToInternal(
                                ZonedTimestampData.of(2143658709L, 0, "UTC"),
                                DataTypes.TIMESTAMP_TZ(3)))
                .isInstanceOf(ZonedTimestampData.class)
                .hasToString("1970-01-25T19:27:38.709Z");
        assertThat(
                        convertToInternal(
                                ZonedDateTime.of(
                                        2019,
                                        12,
                                        25,
                                        21,
                                        48,
                                        25,
                                        123456789,
                                        ZoneId.of("UTC+08:00")),
                                DataTypes.TIMESTAMP_TZ(9)))
                .isInstanceOf(ZonedTimestampData.class)
                .hasToString("2019-12-25T21:48:25.123456789+08:00");
        assertThat(convertToInternal(null, DataTypes.TIMESTAMP_TZ())).isNull();
    }

    @Test
    void testConvertToLocalZonedTimestamp() {
        assertThat(
                        convertToInternal(
                                LocalZonedTimestampData.fromEpochMillis(3141592653589L),
                                DataTypes.TIMESTAMP_LTZ(3)))
                .isInstanceOf(LocalZonedTimestampData.class)
                .hasToString("2069-07-21T00:37:33.589");
        assertThat(
                        convertToInternal(
                                Instant.ofEpochSecond(2718281828L, 123456789),
                                DataTypes.TIMESTAMP_LTZ(9)))
                .isInstanceOf(LocalZonedTimestampData.class)
                .hasToString("2056-02-20T14:17:08.123456789");
        assertThat(convertToInternal(null, DataTypes.TIMESTAMP_LTZ())).isNull();
    }

    @Test
    void testConvertToArray() {
        assertThat(
                        convertToInternal(
                                Arrays.asList("Alice", "Bob", "Charlie"),
                                DataTypes.ARRAY(DataTypes.STRING())))
                .isInstanceOf(ArrayData.class)
                .hasToString("[Alice, Bob, Charlie]")
                .extracting(ArrayData.class::cast)
                .extracting(s -> s.getString(0))
                .isInstanceOf(StringData.class);
        assertThat(
                        convertToInternal(
                                new GenericArrayData(
                                        new StringData[] {
                                            BinaryStringData.fromString("Derrida"),
                                            BinaryStringData.fromString("Enigma"),
                                            BinaryStringData.fromString("Fall")
                                        }),
                                DataTypes.ARRAY(DataTypes.STRING())))
                .isInstanceOf(ArrayData.class)
                .hasToString("[Derrida, Enigma, Fall]")
                .extracting(ArrayData.class::cast)
                .extracting(s -> s.getString(0))
                .isInstanceOf(StringData.class);
        assertThat(convertToInternal(null, DataTypes.TIMESTAMP_LTZ())).isNull();
    }

    @Test
    void testConvertToMap() {
        MapType targetType =
                DataTypes.MAP(
                        DataTypes.STRING(), DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()));
        assertThat(
                        convertToInternal(
                                Map.of(
                                        "Alice", List.of(5, "AILISI"),
                                        "Bob", List.of(3, "BAOBO"),
                                        "Cancan", List.of(6, "KANGKANG")),
                                targetType))
                .isInstanceOf(MapData.class)
                .hasToString("{Alice=(5,AILISI), Cancan=(6,KANGKANG), Bob=(3,BAOBO)}")
                .extracting(MapData.class::cast)
                .extracting(MapData::keyArray, MapData::valueArray)
                .map(Object::toString)
                .containsExactly("[Alice, Cancan, Bob]", "[(5,AILISI), (6,KANGKANG), (3,BAOBO)]");

        assertThat(
                        convertToInternal(
                                new GenericMapData(
                                        Map.of(
                                                BinaryStringData.fromString("Derrida"),
                                                GenericRecordData.of(
                                                        7, BinaryStringData.fromString("DELIDA")))),
                                targetType))
                .isInstanceOf(MapData.class)
                .hasToString("{Derrida=(7,DELIDA)}")
                .extracting(MapData.class::cast)
                .extracting(MapData::keyArray, MapData::valueArray)
                .map(Object::toString)
                .containsExactly("[Derrida]", "[(7,DELIDA)]");
        assertThat(convertToInternal(null, targetType)).isNull();
    }

    @Test
    void testConvertToRow() {
        RowType targetType =
                DataTypes.ROW(
                        DataTypes.STRING(), DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()));
        assertThat(convertToInternal(List.of("Alice", List.of(5, "AILISI")), targetType))
                .isInstanceOf(RecordData.class)
                .hasToString("(Alice,(5,AILISI))")
                .extracting(RecordData.class::cast)
                .extracting(o -> o.getString(0))
                .isInstanceOf(StringData.class)
                .hasToString("Alice");

        assertThat(convertToInternal(List.of("Bob", List.of(3, "BAOBO")), targetType))
                .isInstanceOf(RecordData.class)
                .hasToString("(Bob,(3,BAOBO))")
                .extracting(RecordData.class::cast)
                .extracting(o -> o.getRow(1, 2))
                .isInstanceOf(RecordData.class)
                .hasToString("(3,BAOBO)")
                .extracting(o -> o.getString(1))
                .isInstanceOf(StringData.class);

        assertThat(convertToInternal(null, targetType)).isNull();
    }

    @Test
    void testConvertToVariant() {

        assertThat(convertToInternal(true, DataTypes.BOOLEAN())).isEqualTo(true);
        assertThat(convertToInternal(false, DataTypes.BOOLEAN())).isEqualTo(false);
        assertThat(convertToInternal(null, DataTypes.BOOLEAN())).isNull();
    }
}
