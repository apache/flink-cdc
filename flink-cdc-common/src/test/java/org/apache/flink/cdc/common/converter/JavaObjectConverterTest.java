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

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
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

import static org.apache.flink.cdc.common.converter.JavaObjectConverter.convertToJava;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.map;

/** Unit test cases for {@link JavaObjectConverter}. */
class JavaObjectConverterTest {

    @Test
    void testConvertToBoolean() {
        assertThat(convertToJava(true, DataTypes.BOOLEAN())).isEqualTo(true);
        assertThat(convertToJava(false, DataTypes.BOOLEAN())).isEqualTo(false);
        assertThat(convertToJava(null, DataTypes.BOOLEAN())).isNull();
    }

    @Test
    void testConvertToBytes() {
        assertThat(convertToJava("Alice".getBytes(), DataTypes.BYTES()))
                .isInstanceOf(byte[].class)
                .extracting(byte[].class::cast)
                .extracting(Arrays::toString)
                .isEqualTo("[65, 108, 105, 99, 101]");
        assertThat(
                        convertToJava(
                                new byte[] {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe},
                                DataTypes.BYTES()))
                .isInstanceOf(byte[].class)
                .extracting(byte[].class::cast)
                .extracting(Arrays::toString)
                .isEqualTo("[-54, -2, -70, -66]");
        assertThat(convertToJava(null, DataTypes.BYTES())).isNull();
    }

    @Test
    void testConvertToBinary() {
        assertThat(convertToJava("Alice".getBytes(), DataTypes.BINARY(5)))
                .isEqualTo(new byte[] {65, 108, 105, 99, 101});
        assertThat(
                        convertToJava(
                                new byte[] {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe},
                                DataTypes.BINARY(4)))
                .isEqualTo(new byte[] {-54, -2, -70, -66});
        assertThat(convertToJava(null, DataTypes.BINARY(3))).isNull();
    }

    @Test
    void testConvertToVarBinary() {
        assertThat(convertToJava("Alice".getBytes(), DataTypes.VARBINARY(5)))
                .isEqualTo(new byte[] {65, 108, 105, 99, 101});
        assertThat(
                        convertToJava(
                                new byte[] {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe},
                                DataTypes.VARBINARY(4)))
                .isEqualTo(new byte[] {-54, -2, -70, -66});
        assertThat(convertToJava(null, DataTypes.VARBINARY(3))).isNull();
    }

    @Test
    void testConvertToChar() {
        assertThat(convertToJava("Alice", DataTypes.CHAR(5)))
                .isInstanceOf(String.class)
                .hasToString("Alice");

        assertThat(convertToJava(BinaryStringData.fromString("Bob"), DataTypes.CHAR(5)))
                .isInstanceOf(String.class)
                .hasToString("Bob");

        assertThat(convertToJava(null, DataTypes.CHAR(5))).isNull();
    }

    @Test
    void testConvertToVarChar() {
        assertThat(convertToJava("Alice", DataTypes.VARCHAR(5)))
                .isInstanceOf(String.class)
                .hasToString("Alice");

        assertThat(convertToJava(BinaryStringData.fromString("Bob"), DataTypes.VARCHAR(5)))
                .isInstanceOf(String.class)
                .hasToString("Bob");

        assertThat(convertToJava(null, DataTypes.VARCHAR(5))).isNull();
    }

    @Test
    void testConvertToString() {
        assertThat(convertToJava("Alice", DataTypes.STRING()))
                .isInstanceOf(String.class)
                .hasToString("Alice");
        assertThat(convertToJava(BinaryStringData.fromString("Bob"), DataTypes.STRING()))
                .isInstanceOf(String.class)
                .hasToString("Bob");
        assertThat(convertToJava(null, DataTypes.STRING())).isNull();
    }

    @Test
    void testConvertToInt() {
        assertThat(convertToJava(11, DataTypes.INT())).isEqualTo(11);
        assertThat(convertToJava(-14, DataTypes.INT())).isEqualTo(-14);
        assertThat(convertToJava(17, DataTypes.INT())).isEqualTo(17);
        assertThat(convertToJava(null, DataTypes.INT())).isNull();
    }

    @Test
    void testConvertToTinyInt() {
        assertThat(convertToJava((byte) 11, DataTypes.TINYINT())).isEqualTo((byte) 11);
        assertThat(convertToJava((byte) -14, DataTypes.TINYINT())).isEqualTo((byte) -14);
        assertThat(convertToJava((byte) 17, DataTypes.TINYINT())).isEqualTo((byte) 17);
        assertThat(convertToJava(null, DataTypes.TINYINT())).isNull();
    }

    @Test
    void testConvertToSmallInt() {
        assertThat(convertToJava((short) 11, DataTypes.SMALLINT())).isEqualTo((short) 11);
        assertThat(convertToJava((short) -14, DataTypes.SMALLINT())).isEqualTo((short) -14);
        assertThat(convertToJava((short) 17, DataTypes.SMALLINT())).isEqualTo((short) 17);
        assertThat(convertToJava(null, DataTypes.SMALLINT())).isNull();
    }

    @Test
    void testConvertToBigInt() {
        assertThat(convertToJava((long) 11, DataTypes.BIGINT())).isEqualTo((long) 11);
        assertThat(convertToJava((long) -14, DataTypes.BIGINT())).isEqualTo((long) -14);
        assertThat(convertToJava((long) 17, DataTypes.BIGINT())).isEqualTo((long) 17);
        assertThat(convertToJava(null, DataTypes.BIGINT())).isNull();
    }

    @Test
    void testConvertToFloat() {
        assertThat(convertToJava((float) 11, DataTypes.FLOAT())).isEqualTo((float) 11);
        assertThat(convertToJava((float) -14, DataTypes.FLOAT())).isEqualTo((float) -14);
        assertThat(convertToJava((float) 17, DataTypes.FLOAT())).isEqualTo((float) 17);
        assertThat(convertToJava(null, DataTypes.FLOAT())).isNull();
    }

    @Test
    void testConvertToDouble() {
        assertThat(convertToJava((double) 11, DataTypes.DOUBLE())).isEqualTo((double) 11);
        assertThat(convertToJava((double) -14, DataTypes.DOUBLE())).isEqualTo((double) -14);
        assertThat(convertToJava((double) 17, DataTypes.DOUBLE())).isEqualTo((double) 17);
        assertThat(convertToJava(null, DataTypes.DOUBLE())).isNull();
    }

    @Test
    void testConvertToDecimal() {
        assertThat(convertToJava(new BigDecimal("4.2"), DataTypes.DECIMAL(2, 1)))
                .isInstanceOf(BigDecimal.class)
                .hasToString("4.2");
        assertThat(convertToJava(new BigDecimal("-3.1415926"), DataTypes.DECIMAL(20, 10)))
                .isInstanceOf(BigDecimal.class)
                .hasToString("-3.1415926");

        assertThat(convertToJava(DecimalData.fromUnscaledLong(42, 2, 1), DataTypes.DECIMAL(2, 1)))
                .isInstanceOf(BigDecimal.class)
                .hasToString("4.2");
        assertThat(
                        convertToJava(
                                DecimalData.fromUnscaledLong(-31415926, 14, 7),
                                DataTypes.DECIMAL(14, 7)))
                .isInstanceOf(BigDecimal.class)
                .hasToString("-3.1415926");

        assertThat(convertToJava(null, DataTypes.DECIMAL(20, 10))).isNull();
    }

    @Test
    void testConvertToDate() {
        assertThat(convertToJava(LocalDate.of(2017, 12, 31), DataTypes.DATE()))
                .isInstanceOf(LocalDate.class)
                .hasToString("2017-12-31");
        assertThat(convertToJava(DateData.fromEpochDay(14417), DataTypes.DATE()))
                .isInstanceOf(LocalDate.class)
                .hasToString("2009-06-22");
        assertThat(convertToJava(null, DataTypes.DATE())).isNull();
    }

    @Test
    void testConvertToTime() {
        assertThat(convertToJava(LocalTime.of(21, 48, 25), DataTypes.TIME(0)))
                .isInstanceOf(LocalTime.class)
                .hasToString("21:48:25");
        assertThat(convertToJava(LocalTime.ofSecondOfDay(14419), DataTypes.TIME(0)))
                .isInstanceOf(LocalTime.class)
                .hasToString("04:00:19");
        assertThat(convertToJava(LocalTime.of(21, 48, 25, 123456789), DataTypes.TIME(3)))
                .isInstanceOf(LocalTime.class)
                .hasToString("21:48:25.123456789");
        assertThat(convertToJava(LocalTime.ofNanoOfDay(14419123456789L), DataTypes.TIME(3)))
                .isInstanceOf(LocalTime.class)
                .hasToString("04:00:19.123456789");

        assertThat(
                        convertToJava(
                                TimeData.fromLocalTime(LocalTime.of(21, 48, 25)),
                                DataTypes.TIME(0)))
                .isInstanceOf(LocalTime.class)
                .hasToString("21:48:25");
        assertThat(convertToJava(TimeData.fromSecondOfDay(14419), DataTypes.TIME(0)))
                .isInstanceOf(LocalTime.class)
                .hasToString("04:00:19");
        assertThat(
                        convertToJava(
                                TimeData.fromLocalTime(LocalTime.of(21, 48, 25, 123456789)),
                                DataTypes.TIME(3)))
                .isInstanceOf(LocalTime.class)
                .hasToString("21:48:25.123");
        assertThat(convertToJava(TimeData.fromNanoOfDay(14419123456789L), DataTypes.TIME(3)))
                .isInstanceOf(LocalTime.class)
                .hasToString("04:00:19.123");
        assertThat(convertToJava(null, DataTypes.TIME())).isNull();
    }

    @Test
    void testConvertToTimestamp() {
        assertThat(convertToJava(TimestampData.fromMillis(2147483648491L), DataTypes.TIMESTAMP(3)))
                .isInstanceOf(LocalDateTime.class)
                .hasToString("2038-01-19T03:14:08.491");
        assertThat(
                        convertToJava(
                                LocalDateTime.of(2019, 12, 25, 21, 48, 25, 123456789),
                                DataTypes.TIMESTAMP(9)))
                .isInstanceOf(LocalDateTime.class)
                .hasToString("2019-12-25T21:48:25.123456789");
        assertThat(convertToJava(null, DataTypes.TIMESTAMP())).isNull();
    }

    @Test
    void testConvertToZonedTimestamp() {
        assertThat(
                        convertToJava(
                                ZonedTimestampData.of(2143658709L, 0, "UTC"),
                                DataTypes.TIMESTAMP_TZ(3)))
                .isInstanceOf(ZonedDateTime.class)
                .hasToString("1970-01-25T19:27:38.709Z[UTC]");
        assertThat(
                        convertToJava(
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
                .isInstanceOf(ZonedDateTime.class)
                .hasToString("2019-12-25T21:48:25.123456789+08:00[UTC+08:00]");
        assertThat(convertToJava(null, DataTypes.TIMESTAMP_TZ())).isNull();
    }

    @Test
    void testConvertToLocalZonedTimestamp() {
        assertThat(
                        convertToJava(
                                LocalZonedTimestampData.fromEpochMillis(3141592653589L),
                                DataTypes.TIMESTAMP_LTZ(3)))
                .isInstanceOf(Instant.class)
                .hasToString("2069-07-21T00:37:33.589Z");
        assertThat(
                        convertToJava(
                                Instant.ofEpochSecond(2718281828L, 123456789),
                                DataTypes.TIMESTAMP_LTZ(9)))
                .isInstanceOf(Instant.class)
                .hasToString("2056-02-20T14:17:08.123456789Z");
        assertThat(convertToJava(null, DataTypes.TIMESTAMP_LTZ())).isNull();
    }

    @Test
    void testConvertToArray() {
        assertThat(
                        convertToJava(
                                Arrays.asList("Alice", "Bob", "Charlie"),
                                DataTypes.ARRAY(DataTypes.STRING())))
                .isInstanceOf(List.class)
                .hasToString("[Alice, Bob, Charlie]")
                .extracting(List.class::cast)
                .extracting(s -> s.get(0))
                .isInstanceOf(String.class);
        assertThat(
                        convertToJava(
                                new GenericArrayData(
                                        new StringData[] {
                                            BinaryStringData.fromString("Derrida"),
                                            BinaryStringData.fromString("Enigma"),
                                            BinaryStringData.fromString("Fall")
                                        }),
                                DataTypes.ARRAY(DataTypes.STRING())))
                .isInstanceOf(List.class)
                .hasToString("[Derrida, Enigma, Fall]")
                .extracting(List.class::cast)
                .extracting(s -> s.get(0))
                .isInstanceOf(String.class);
        assertThat(convertToJava(null, DataTypes.TIMESTAMP_LTZ())).isNull();
    }

    @Test
    void testConvertToMap() {
        MapType targetType =
                DataTypes.MAP(
                        DataTypes.STRING(), DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()));
        Map<String, List<Object>> originalMap =
                Map.of(
                        "Alice", List.of(5, "AILISI"),
                        "Bob", List.of(3, "BAOBO"),
                        "Cancan", List.of(6, "KANGKANG"));
        assertThat(convertToJava(originalMap, targetType))
                .isInstanceOf(Map.class)
                .asInstanceOf(map(String.class, List.class))
                .containsExactlyEntriesOf(originalMap);

        assertThat(
                        convertToJava(
                                new GenericMapData(
                                        Map.of(
                                                BinaryStringData.fromString("Derrida"),
                                                GenericRecordData.of(
                                                        7, BinaryStringData.fromString("DELIDA")))),
                                targetType))
                .isInstanceOf(Map.class)
                .asInstanceOf(map(String.class, List.class))
                .containsExactlyEntriesOf(Map.of("Derrida", List.of(7, "DELIDA")));
        assertThat(convertToJava(null, targetType)).isNull();
    }

    @Test
    void testConvertToRow() {
        RowType targetType =
                DataTypes.ROW(
                        DataTypes.STRING(), DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()));
        assertThat(convertToJava(List.of("Alice", List.of(5, "AILISI")), targetType))
                .isInstanceOf(List.class)
                .asInstanceOf(list(Object.class))
                .containsExactly("Alice", List.of(5, "AILISI"));

        assertThat(convertToJava(List.of("Bob", List.of(3, "BAOBO")), targetType))
                .isInstanceOf(List.class)
                .asInstanceOf(list(Object.class))
                .containsExactly("Bob", List.of(3, "BAOBO"));

        assertThat(convertToJava(null, targetType)).isNull();
    }
}
