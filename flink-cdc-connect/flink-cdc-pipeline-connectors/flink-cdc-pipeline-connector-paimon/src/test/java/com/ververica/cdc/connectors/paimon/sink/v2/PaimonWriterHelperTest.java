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

package com.ververica.cdc.connectors.paimon.sink.v2;

import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

/** Tests for {@link PaimonWriterHelper}. */
public class PaimonWriterHelperTest {

    @Test
    public void testConvertEventToGenericRowOfAllDataTypes() {
        RowType rowType =
                RowType.of(
                        DataTypes.BOOLEAN(),
                        DataTypes.BINARY(3),
                        DataTypes.VARBINARY(10),
                        DataTypes.BYTES(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DECIMAL(6, 3),
                        DataTypes.CHAR(5),
                        DataTypes.VARCHAR(10),
                        DataTypes.STRING(),
                        DataTypes.DATE(),
                        DataTypes.TIME(),
                        DataTypes.TIME(6),
                        DataTypes.TIMESTAMP(),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP_LTZ(),
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.STRING());
        Object[] testData =
                new Object[] {
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
        BinaryRecordData recordData = new BinaryRecordDataGenerator(rowType).generate(testData);
        Schema schema = Schema.newBuilder().fromRowDataType(rowType).build();
        List<RecordData.FieldGetter> fieldGetters =
                PaimonWriterHelper.createFieldGetters(schema, ZoneId.of("UTC+8"));
        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(TableId.parse("database.table"), recordData);
        GenericRow genericRow =
                PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);
        Assertions.assertEquals(
                GenericRow.ofKind(
                        RowKind.INSERT,
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
                        Decimal.fromBigDecimal(new BigDecimal("7.123"), 6, 3),
                        BinaryString.fromString("test1"),
                        BinaryString.fromString("test2"),
                        BinaryString.fromString("test3"),
                        100,
                        200,
                        300,
                        Timestamp.fromSQLTimestamp(
                                java.sql.Timestamp.valueOf("2023-01-01 00:00:00.000")),
                        Timestamp.fromSQLTimestamp(
                                java.sql.Timestamp.valueOf("2023-01-01 00:00:00")),
                        // plus 8 hours.
                        Timestamp.fromInstant(Instant.parse("2023-01-01T08:00:00.000Z")),
                        Timestamp.fromInstant(Instant.parse("2023-01-01T08:00:00.000Z")),
                        null),
                genericRow);
    }

    @Test
    public void testConvertEventToGenericRowOfDataChangeTypes() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .build();
        List<RecordData.FieldGetter> fieldGetters =
                PaimonWriterHelper.createFieldGetters(schema, ZoneId.systemDefault());
        TableId tableId = TableId.parse("database.table");
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        BinaryRecordData recordData =
                generator.generate(
                        new Object[] {
                            BinaryStringData.fromString("1"), BinaryStringData.fromString("1")
                        });

        DataChangeEvent dataChangeEvent = DataChangeEvent.insertEvent(tableId, recordData);
        GenericRow genericRow =
                PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);
        Assertions.assertEquals(genericRow.getRowKind(), RowKind.INSERT);

        dataChangeEvent = DataChangeEvent.deleteEvent(tableId, recordData);
        genericRow = PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);
        Assertions.assertEquals(genericRow.getRowKind(), RowKind.DELETE);

        dataChangeEvent = DataChangeEvent.updateEvent(tableId, recordData, recordData);
        genericRow = PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);
        Assertions.assertEquals(genericRow.getRowKind(), RowKind.INSERT);

        dataChangeEvent = DataChangeEvent.replaceEvent(tableId, recordData, null);
        genericRow = PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);
        Assertions.assertEquals(genericRow.getRowKind(), RowKind.INSERT);
    }
}
