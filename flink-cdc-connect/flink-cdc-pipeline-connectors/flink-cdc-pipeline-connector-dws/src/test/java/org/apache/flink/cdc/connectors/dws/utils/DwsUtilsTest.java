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

package org.apache.flink.cdc.connectors.dws.utils;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DwsUtils}. */
class DwsUtilsTest {

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/New_York"})
    void preservesLocalZonedTimestampInstant(String zone) {
        Instant instant = Instant.parse("2026-10-10T01:02:03.123456789Z");
        Object converted =
                DwsUtils.createFieldGetter(DataTypes.TIMESTAMP_LTZ(9), 0, ZoneId.of(zone))
                        .getFieldOrNull(
                                GenericRecordData.of(LocalZonedTimestampData.fromInstant(instant)));
        assertThat(((Timestamp) converted).toInstant()).isEqualTo(instant);
    }

    @Test
    void preservesNestedDecimalsInAllContainers() {
        assertNestedJson(
                DataTypes.DECIMAL(12, 2),
                DecimalData.fromBigDecimal(new BigDecimal("123456.70"), 12, 2),
                "123456.70");
        assertNestedJson(
                DataTypes.DECIMAL(20, 2),
                DecimalData.fromBigDecimal(new BigDecimal("123456789012345678.70"), 20, 2),
                "123456789012345678.70");
        assertNestedJson(DataTypes.DECIMAL(20, 2), null, "null");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3, 6, 9})
    void preservesNestedTimestampPrecisionInAllContainers(int precision) {
        String fraction = precision == 0 ? "" : "." + "123456789".substring(0, precision);
        LocalDateTime timestamp = LocalDateTime.parse("2026-10-10T01:02:03" + fraction);
        assertNestedJson(
                DataTypes.TIMESTAMP(precision),
                TimestampData.fromLocalDateTime(timestamp),
                "\"2026-10-10 01:02:03" + fraction + "\"");
        assertNestedJson(
                DataTypes.TIMESTAMP_LTZ(precision),
                LocalZonedTimestampData.fromInstant(
                        Instant.parse("2026-10-10T01:02:03" + fraction + "Z")),
                "\"2026-10-10 09:02:03" + fraction + "\"");
    }

    private void assertNestedJson(DataType type, Object value, String expected) {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(BinaryStringData.fromString("value"), value);
        assertThat(
                        convertComplex(
                                DataTypes.ROW(DataTypes.FIELD("value", type)),
                                GenericRecordData.of(value)))
                .isEqualTo("{\"value\":" + expected + "}");
        assertThat(
                        convertComplex(
                                DataTypes.ARRAY(type), new GenericArrayData(new Object[] {value})))
                .isEqualTo("[" + expected + "]");
        assertThat(convertComplex(DataTypes.MAP(DataTypes.STRING(), type), new GenericMapData(map)))
                .isEqualTo("{\"value\":" + expected + "}");
    }

    private Object convertComplex(DataType type, Object value) {
        return DwsUtils.createFieldGetter(type, 0, ZoneId.of("Asia/Shanghai"))
                .getFieldOrNull(GenericRecordData.of(value));
    }

    @Test
    void testCreateFieldGetterSerializesComplexTypesAsJson() {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(BinaryStringData.fromString("k1"), BinaryStringData.fromString("v1"));
        map.put(BinaryStringData.fromString("k2"), BinaryStringData.fromString("v2"));

        GenericRecordData record =
                GenericRecordData.of(
                        new GenericArrayData(
                                new Object[] {
                                    BinaryStringData.fromString("alpha"),
                                    BinaryStringData.fromString("beta")
                                }),
                        new GenericMapData(map),
                        GenericRecordData.of(
                                BinaryStringData.fromString("nested"),
                                7,
                                DateData.fromIsoLocalDateString("2024-01-02"),
                                TimeData.fromIsoLocalTimeString("03:04:05")));

        RecordData.FieldGetter arrayGetter =
                DwsUtils.createFieldGetter(
                        DataTypes.ARRAY(DataTypes.STRING()), 0, ZoneId.of("UTC"));
        RecordData.FieldGetter mapGetter =
                DwsUtils.createFieldGetter(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()), 1, ZoneId.of("UTC"));
        RecordData.FieldGetter rowGetter =
                DwsUtils.createFieldGetter(
                        DataTypes.ROW(
                                DataTypes.FIELD("name", DataTypes.STRING()),
                                DataTypes.FIELD("count", DataTypes.INT()),
                                DataTypes.FIELD("created_on", DataTypes.DATE()),
                                DataTypes.FIELD("created_at", DataTypes.TIME())),
                        2,
                        ZoneId.of("UTC"));

        assertThat(arrayGetter.getFieldOrNull(record)).isEqualTo("[\"alpha\",\"beta\"]");
        assertThat(mapGetter.getFieldOrNull(record)).isEqualTo("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        assertThat(rowGetter.getFieldOrNull(record))
                .isEqualTo(
                        "{\"name\":\"nested\",\"count\":7,"
                                + "\"created_on\":\"2024-01-02\","
                                + "\"created_at\":\"03:04:05\"}");
    }

    @Test
    void testCreateFieldGetterReadsTimestampsAsJdbcTimestamp() {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse("2024-01-02T03:04:05.123Z");
        GenericRecordData record =
                GenericRecordData.of(
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2024-01-02T03:04:05.123")),
                        LocalZonedTimestampData.fromInstant(
                                Instant.parse("2024-01-02T03:04:05.123Z")),
                        ZonedTimestampData.fromZonedDateTime(zonedDateTime));

        RecordData.FieldGetter timestampGetter =
                DwsUtils.createFieldGetter(DataTypes.TIMESTAMP(3), 0, ZoneId.of("UTC"));
        RecordData.FieldGetter localZonedGetter =
                DwsUtils.createFieldGetter(DataTypes.TIMESTAMP_LTZ(3), 1, ZoneId.of("UTC"));
        RecordData.FieldGetter zonedGetter =
                DwsUtils.createFieldGetter(DataTypes.TIMESTAMP_TZ(3), 2, ZoneId.of("UTC"));

        assertThat(timestampGetter.getFieldOrNull(record))
                .isEqualTo(Timestamp.valueOf("2024-01-02 03:04:05.123"));
        assertThat(localZonedGetter.getFieldOrNull(record))
                .isEqualTo(Timestamp.from(Instant.parse("2024-01-02T03:04:05.123Z")));
        assertThat(zonedGetter.getFieldOrNull(record))
                .isEqualTo(Timestamp.from(Instant.parse("2024-01-02T03:04:05.123Z")));
    }
}
