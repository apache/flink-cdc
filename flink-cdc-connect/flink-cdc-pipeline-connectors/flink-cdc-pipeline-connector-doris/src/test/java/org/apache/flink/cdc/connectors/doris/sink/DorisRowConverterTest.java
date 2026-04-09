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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** A test for {@link org.apache.flink.cdc.connectors.doris.sink.DorisRowConverter} . */
class DorisRowConverterTest {

    @Test
    void testExternalConvert() {
        List<Column> columns =
                Arrays.asList(
                        Column.physicalColumn("f2", DataTypes.BOOLEAN()),
                        Column.physicalColumn("f3", DataTypes.FLOAT()),
                        Column.physicalColumn("f4", DataTypes.DOUBLE()),
                        Column.physicalColumn("f7", DataTypes.TINYINT()),
                        Column.physicalColumn("f8", DataTypes.SMALLINT()),
                        Column.physicalColumn("f9", DataTypes.INT()),
                        Column.physicalColumn("f10", DataTypes.BIGINT()),
                        Column.physicalColumn("f12", DataTypes.TIMESTAMP()),
                        Column.physicalColumn("f14", DataTypes.DATE()),
                        Column.physicalColumn("f15", DataTypes.CHAR(1)),
                        Column.physicalColumn("f16", DataTypes.VARCHAR(256)),
                        Column.physicalColumn("f17", DataTypes.TIMESTAMP()),
                        Column.physicalColumn("f18", DataTypes.TIMESTAMP()),
                        Column.physicalColumn("f19", DataTypes.TIMESTAMP()),
                        Column.physicalColumn("f20", DataTypes.TIMESTAMP_LTZ()),
                        Column.physicalColumn("f21", DataTypes.TIMESTAMP_LTZ()),
                        Column.physicalColumn("f22", DataTypes.TIMESTAMP_LTZ()),
                        Column.physicalColumn("f23", DataTypes.TIME(0)),
                        Column.physicalColumn("f24", DataTypes.TIME(3)),
                        Column.physicalColumn("f24", DataTypes.TIME(6)));

        List<DataType> dataTypes =
                columns.stream().map(Column::getType).collect(Collectors.toList());
        LocalDateTime time1 =
                LocalDateTime.ofInstant(Instant.parse("2021-01-01T08:00:00Z"), ZoneId.of("Z"));
        LocalDate date1 = LocalDate.of(2021, 1, 1);

        LocalDateTime f17 =
                LocalDateTime.ofInstant(Instant.parse("2021-01-01T08:01:11Z"), ZoneId.of("Z"));
        LocalDateTime f18 =
                LocalDateTime.ofInstant(Instant.parse("2021-01-01T08:01:11.123Z"), ZoneId.of("Z"));
        LocalDateTime f19 =
                LocalDateTime.ofInstant(
                        Instant.parse("2021-01-01T08:01:11.123456Z"), ZoneId.of("Z"));

        Instant f20 = Instant.parse("2021-01-01T08:01:11Z");
        Instant f21 = Instant.parse("2021-01-01T08:01:11.123Z");
        Instant f22 = Instant.parse("2021-01-01T08:01:11.123456Z");

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(dataTypes.toArray(new DataType[] {})));
        BinaryRecordData recordData =
                generator.generate(
                        new Object[] {
                            true,
                            1.2F,
                            1.2345D,
                            (byte) 1,
                            (short) 32,
                            64,
                            128L,
                            TimestampData.fromLocalDateTime(time1),
                            DateData.fromLocalDate(date1),
                            BinaryStringData.fromString("a"),
                            BinaryStringData.fromString("doris"),
                            TimestampData.fromLocalDateTime(f17),
                            TimestampData.fromLocalDateTime(f18),
                            TimestampData.fromLocalDateTime(f19),
                            LocalZonedTimestampData.fromInstant(f20),
                            LocalZonedTimestampData.fromInstant(f21),
                            LocalZonedTimestampData.fromInstant(f22),
                            TimeData.fromNanoOfDay(3661000_000000L),
                            TimeData.fromNanoOfDay(3661123_000000L),
                            TimeData.fromNanoOfDay(3661123_000000L)
                        });
        List<Object> row = new ArrayList<>();
        for (int i = 0; i < recordData.getArity(); i++) {
            DorisRowConverter.SerializationConverter converter =
                    DorisRowConverter.createNullableExternalConverter(
                            columns.get(i).getType(), ZoneId.of("GMT+08:00"));
            row.add(converter.serialize(i, recordData));
        }
        Assertions.assertThat(row)
                .hasToString(
                        "[true, 1.2, 1.2345, 1, 32, 64, 128, 2021-01-01 08:00:00.000000, 2021-01-01, a, doris, 2021-01-01 "
                                + "08:01:11.000000, 2021-01-01 08:01:11.123000, 2021-01-01 08:01:11.123456, 2021-01-01 "
                                + "16:01:11.000000, 2021-01-01 16:01:11.123000, 2021-01-01 16:01:11.123456, 01:01:01, 01:01:01.123, 01:01:01.123]");
    }
}
