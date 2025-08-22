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

package org.apache.flink.cdc.connectors.oceanbase.sink;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import com.oceanbase.connector.flink.table.Record;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OceanBaseEventSerializationSchema}. */
class OceanBaseEventSerializationSchemaTest {

    private static final OceanBaseEventSerializationSchema serializer =
            new OceanBaseEventSerializationSchema(ZoneId.of("+08"));

    @Test
    void testMixedSchemaAndDataChanges() throws Exception {
        // 1. create table1, and insert/delete/update data
        TableId table1 = TableId.parse("test.tbl1");
        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType(false))
                        .physicalColumn("col2", new BooleanType())
                        .physicalColumn("col3", new TimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent1 = new CreateTableEvent(table1, schema1);
        assertThat(serializer.serialize(createTableEvent1)).isNull();

        BinaryRecordDataGenerator generator1 =
                new BinaryRecordDataGenerator(
                        schema1.getColumnDataTypes().toArray(new DataType[0]));
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator1.generate(
                                new Object[] {
                                    1,
                                    true,
                                    TimestampData.fromTimestamp(
                                            Timestamp.valueOf("2023-11-27 18:00:00"))
                                }));
        Record insert1 = serializer.serialize(insertEvent1);
        verifySerializeResult(table1, "[1, true, 2023-11-27 18:00:00.0]", insert1);

        DataChangeEvent deleteEvent1 =
                DataChangeEvent.deleteEvent(
                        table1,
                        generator1.generate(
                                new Object[] {
                                    2,
                                    false,
                                    TimestampData.fromTimestamp(
                                            Timestamp.valueOf("2023-11-27 19:00:00"))
                                }));
        verifySerializeResult(
                table1, "[2, false, 2023-11-27 19:00:00.0]", serializer.serialize(deleteEvent1));

        DataChangeEvent updateEvent1 =
                DataChangeEvent.updateEvent(
                        table1,
                        generator1.generate(
                                new Object[] {
                                    3,
                                    false,
                                    TimestampData.fromTimestamp(
                                            Timestamp.valueOf("2023-11-27 20:00:00"))
                                }),
                        generator1.generate(
                                new Object[] {
                                    3,
                                    true,
                                    TimestampData.fromTimestamp(
                                            Timestamp.valueOf("2023-11-27 21:00:00"))
                                }));
        verifySerializeResult(
                table1, "[3, true, 2023-11-27 21:00:00.0]", serializer.serialize(updateEvent1));

        // 2. create table2, and insert data
        TableId table2 = TableId.parse("test.tbl2");
        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("col1", new DateType())
                        .physicalColumn("col2", new FloatType())
                        .physicalColumn("col3", new VarCharType(20))
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent2 = new CreateTableEvent(table2, schema2);
        assertThat(serializer.serialize(createTableEvent2)).isNull();

        BinaryRecordDataGenerator generator2 =
                new BinaryRecordDataGenerator(
                        schema2.getColumnDataTypes().toArray(new DataType[0]));
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table2,
                        generator2.generate(
                                new Object[] {
                                    DateData.fromLocalDate(LocalDate.of(2023, 11, 27)),
                                    3.4f,
                                    BinaryStringData.fromString("insert table2")
                                }));
        verifySerializeResult(
                table2, "[2023-11-27, 3.4, insert table2]", serializer.serialize(insertEvent2));

        // 3. add columns to table1, and delete data
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        table1,
                        Arrays.asList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("col4", new DecimalType(20, 5))),
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("col5", new SmallIntType())),
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn(
                                                "col6", new LocalZonedTimestampType()))));
        Schema newSchema1 = SchemaUtils.applySchemaChangeEvent(schema1, addColumnEvent);
        BinaryRecordDataGenerator newGenerator1 =
                new BinaryRecordDataGenerator(
                        newSchema1.getColumnDataTypes().toArray(new DataType[0]));
        assertThat(serializer.serialize(addColumnEvent)).isNull();

        DataChangeEvent deleteEvent2 =
                DataChangeEvent.deleteEvent(
                        table1,
                        newGenerator1.generate(
                                new Object[] {
                                    4,
                                    true,
                                    TimestampData.fromTimestamp(
                                            Timestamp.valueOf("2023-11-27 21:00:00")),
                                    DecimalData.fromBigDecimal(new BigDecimal("83.23"), 20, 5),
                                    (short) 9,
                                    LocalZonedTimestampData.fromInstant(
                                            LocalDateTime.of(2023, 11, 27, 21, 0, 0)
                                                    .toInstant(ZoneOffset.of("+10")))
                                }));
        verifySerializeResult(
                table1,
                "[4, true, 2023-11-27 21:00:00.0, 83.23000, 9, 2023-11-27 19:00:00.0]",
                Objects.requireNonNull(serializer.serialize(deleteEvent2)));

        // 4. drop columns from table2, and insert data
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(table2, Arrays.asList("col2", "col3"));
        Schema newSchema2 = SchemaUtils.applySchemaChangeEvent(schema2, dropColumnEvent);
        BinaryRecordDataGenerator newGenerator2 =
                new BinaryRecordDataGenerator(
                        newSchema2.getColumnDataTypes().toArray(new DataType[0]));
        assertThat(serializer.serialize(dropColumnEvent)).isNull();

        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table2,
                        newGenerator2.generate(
                                new Object[] {DateData.fromLocalDate(LocalDate.of(2023, 11, 28))}));
        verifySerializeResult(
                table2, "[2023-11-28]", Objects.requireNonNull(serializer.serialize(insertEvent3)));
    }

    private void verifySerializeResult(TableId expectTable, String expectRow, Record record)
            throws Exception {
        assertThat(record.getTableId().getSchemaName()).isEqualTo(expectTable.getSchemaName());
        assertThat(record.getTableId().getTableName()).isEqualTo(expectTable.getTableName());
        int start = record.toString().indexOf('[');
        int end = record.toString().indexOf(']');
        assertThat(record.toString().substring(start, end + 1)).isEqualTo(expectRow);
    }
}
