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

package org.apache.flink.cdc.connectors.kafka.json;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;

/** Tests for {@link TableSchemaInfo}. */
class TableSchemaInfoTest {

    @Test
    void testGetRowDataFromRecordData() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn(
                                "col1",
                                org.apache.flink.cdc.common.types.DataTypes.STRING().notNull())
                        .physicalColumn(
                                "boolean", org.apache.flink.cdc.common.types.DataTypes.BOOLEAN())
                        .physicalColumn(
                                "binary", org.apache.flink.cdc.common.types.DataTypes.BINARY(3))
                        .physicalColumn(
                                "varbinary",
                                org.apache.flink.cdc.common.types.DataTypes.VARBINARY(10))
                        .physicalColumn(
                                "bytes", org.apache.flink.cdc.common.types.DataTypes.BYTES())
                        .physicalColumn(
                                "tinyint", org.apache.flink.cdc.common.types.DataTypes.TINYINT())
                        .physicalColumn(
                                "smallint", org.apache.flink.cdc.common.types.DataTypes.SMALLINT())
                        .physicalColumn("int", org.apache.flink.cdc.common.types.DataTypes.INT())
                        .physicalColumn(
                                "big_int", org.apache.flink.cdc.common.types.DataTypes.BIGINT())
                        .physicalColumn(
                                "float", org.apache.flink.cdc.common.types.DataTypes.FLOAT())
                        .physicalColumn(
                                "double", org.apache.flink.cdc.common.types.DataTypes.DOUBLE())
                        .physicalColumn(
                                "decimal",
                                org.apache.flink.cdc.common.types.DataTypes.DECIMAL(6, 3))
                        .physicalColumn("char", org.apache.flink.cdc.common.types.DataTypes.CHAR(5))
                        .physicalColumn(
                                "varchar", org.apache.flink.cdc.common.types.DataTypes.VARCHAR(10))
                        .physicalColumn(
                                "string", org.apache.flink.cdc.common.types.DataTypes.STRING())
                        .physicalColumn("date", org.apache.flink.cdc.common.types.DataTypes.DATE())
                        .physicalColumn("time", org.apache.flink.cdc.common.types.DataTypes.TIME())
                        .physicalColumn(
                                "time_with_precision",
                                org.apache.flink.cdc.common.types.DataTypes.TIME(6))
                        .physicalColumn(
                                "timestamp",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP())
                        .physicalColumn(
                                "timestamp_with_precision",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(3))
                        .physicalColumn(
                                "timestamp_ltz",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ())
                        .physicalColumn(
                                "timestamp_ltz_with_precision",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(3))
                        .physicalColumn(
                                "null_string", org.apache.flink.cdc.common.types.DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        TableSchemaInfo tableSchemaInfo =
                new TableSchemaInfo(
                        TableId.parse("testDatabase.testTable"), schema, null, ZoneId.of("UTC+8"));
        Object[] testData =
                new Object[] {
                    BinaryStringData.fromString("pk"),
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
                    TimestampData.fromTimestamp(java.sql.Timestamp.valueOf("2023-01-01 00:00:00")),
                    LocalZonedTimestampData.fromInstant(Instant.parse("2023-01-01T00:00:00.000Z")),
                    LocalZonedTimestampData.fromInstant(Instant.parse("2023-01-01T00:00:00.000Z")),
                    null
                };
        BinaryRecordData recordData =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]))
                        .generate(testData);

        Assertions.assertThat(tableSchemaInfo.getRowDataFromRecordData(recordData, false))
                .isEqualTo(
                        GenericRowData.ofKind(
                                RowKind.INSERT,
                                org.apache.flink.table.data.binary.BinaryStringData.fromString(
                                        "pk"),
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
                                org.apache.flink.table.data.DecimalData.fromBigDecimal(
                                        new BigDecimal("7.123"), 6, 3),
                                org.apache.flink.table.data.binary.BinaryStringData.fromString(
                                        "test1"),
                                org.apache.flink.table.data.binary.BinaryStringData.fromString(
                                        "test2"),
                                org.apache.flink.table.data.binary.BinaryStringData.fromString(
                                        "test3"),
                                100,
                                200,
                                300,
                                org.apache.flink.table.data.TimestampData.fromTimestamp(
                                        Timestamp.valueOf("2023-01-01 00:00:00.000")),
                                org.apache.flink.table.data.TimestampData.fromTimestamp(
                                        Timestamp.valueOf("2023-01-01 00:00:00")),
                                org.apache.flink.table.data.TimestampData.fromInstant(
                                        Instant.parse("2023-01-01T00:00:00.000Z")),
                                org.apache.flink.table.data.TimestampData.fromInstant(
                                        Instant.parse("2023-01-01T00:00:00.000Z")),
                                null));
    }
}
