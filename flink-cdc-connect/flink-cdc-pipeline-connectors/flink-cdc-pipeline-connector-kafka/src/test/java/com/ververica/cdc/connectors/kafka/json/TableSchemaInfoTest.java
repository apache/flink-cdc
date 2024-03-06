/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.kafka.json;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;

import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;

/** Tests for {@link TableSchemaInfo}. */
public class TableSchemaInfoTest {

    @Test
    public void testGetRowDataFromRecordData() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn(
                                "col1", com.ververica.cdc.common.types.DataTypes.STRING().notNull())
                        .physicalColumn(
                                "boolean", com.ververica.cdc.common.types.DataTypes.BOOLEAN())
                        .physicalColumn(
                                "binary", com.ververica.cdc.common.types.DataTypes.BINARY(3))
                        .physicalColumn(
                                "varbinary", com.ververica.cdc.common.types.DataTypes.VARBINARY(10))
                        .physicalColumn("bytes", com.ververica.cdc.common.types.DataTypes.BYTES())
                        .physicalColumn(
                                "tinyint", com.ververica.cdc.common.types.DataTypes.TINYINT())
                        .physicalColumn(
                                "smallint", com.ververica.cdc.common.types.DataTypes.SMALLINT())
                        .physicalColumn("int", com.ververica.cdc.common.types.DataTypes.INT())
                        .physicalColumn(
                                "big_int", com.ververica.cdc.common.types.DataTypes.BIGINT())
                        .physicalColumn("float", com.ververica.cdc.common.types.DataTypes.FLOAT())
                        .physicalColumn("double", com.ververica.cdc.common.types.DataTypes.DOUBLE())
                        .physicalColumn(
                                "decimal", com.ververica.cdc.common.types.DataTypes.DECIMAL(6, 3))
                        .physicalColumn("char", com.ververica.cdc.common.types.DataTypes.CHAR(5))
                        .physicalColumn(
                                "varchar", com.ververica.cdc.common.types.DataTypes.VARCHAR(10))
                        .physicalColumn("string", com.ververica.cdc.common.types.DataTypes.STRING())
                        .physicalColumn("date", com.ververica.cdc.common.types.DataTypes.DATE())
                        .physicalColumn("time", com.ververica.cdc.common.types.DataTypes.TIME())
                        .physicalColumn(
                                "time_with_precision",
                                com.ververica.cdc.common.types.DataTypes.TIME(6))
                        .physicalColumn(
                                "timestamp", com.ververica.cdc.common.types.DataTypes.TIMESTAMP())
                        .physicalColumn(
                                "timestamp_with_precision",
                                com.ververica.cdc.common.types.DataTypes.TIMESTAMP(3))
                        .physicalColumn(
                                "timestamp_ltz",
                                com.ververica.cdc.common.types.DataTypes.TIMESTAMP_LTZ())
                        .physicalColumn(
                                "timestamp_ltz_with_precision",
                                com.ververica.cdc.common.types.DataTypes.TIMESTAMP_LTZ(3))
                        .physicalColumn(
                                "null_string", com.ververica.cdc.common.types.DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        TableSchemaInfo tableSchemaInfo = new TableSchemaInfo(schema, null, ZoneId.of("UTC+8"));
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
                    100,
                    200,
                    300,
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

        Assertions.assertEquals(
                GenericRowData.ofKind(
                        RowKind.INSERT,
                        org.apache.flink.table.data.binary.BinaryStringData.fromString("pk"),
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
                        org.apache.flink.table.data.binary.BinaryStringData.fromString("test1"),
                        org.apache.flink.table.data.binary.BinaryStringData.fromString("test2"),
                        org.apache.flink.table.data.binary.BinaryStringData.fromString("test3"),
                        100,
                        200,
                        300,
                        org.apache.flink.table.data.TimestampData.fromTimestamp(
                                Timestamp.valueOf("2023-01-01 00:00:00.000")),
                        org.apache.flink.table.data.TimestampData.fromTimestamp(
                                Timestamp.valueOf("2023-01-01 00:00:00")),
                        // plus 8 hours.
                        org.apache.flink.table.data.TimestampData.fromInstant(
                                Instant.parse("2023-01-01T08:00:00.000Z")),
                        org.apache.flink.table.data.TimestampData.fromInstant(
                                Instant.parse("2023-01-01T08:00:00.000Z")),
                        null),
                tableSchemaInfo.getRowDataFromRecordData(recordData));
    }
}
