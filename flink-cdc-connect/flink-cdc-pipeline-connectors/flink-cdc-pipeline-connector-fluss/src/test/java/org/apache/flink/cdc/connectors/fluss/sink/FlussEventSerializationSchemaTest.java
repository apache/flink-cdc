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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import com.alibaba.fluss.flink.row.OperationType;
import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.RowType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

public class FlussEventSerializationSchemaTest {
    private FlussEventSerializationSchema serializer;

    @BeforeEach
    public void setup() throws Exception {
        this.serializer = new FlussEventSerializationSchema(ZoneId.of("+08"));
        this.serializer.open(new MockInitializationContext());
    }

    @Test
    void testMixedSchemaAndDataChanges() throws Exception {
        // 1. create table1, and insert/delete/update data
        TableId table1 = TableId.parse("test.tbl1");
        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType())
                        .physicalColumn("col2", new BooleanType())
                        .physicalColumn("col3", new TimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent1 = new CreateTableEvent(table1, schema1);
        Assertions.assertThat(serializer.serialize(createTableEvent1)).isNull();

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
                                    TimestampData.fromMillis(
                                            Timestamp.valueOf("2023-11-27 18:00:00").getTime())
                                }));

        verifySerializeResult(
                new RowWithOp(
                        GenericRow.of(
                                1,
                                true,
                                TimestampNtz.fromMillis(
                                        Timestamp.valueOf("2023-11-27 18:00:00").getTime())),
                        OperationType.UPSERT),
                serializer.serialize(insertEvent1));

        DataChangeEvent deleteEvent1 =
                DataChangeEvent.deleteEvent(
                        table1,
                        generator1.generate(
                                new Object[] {
                                    2,
                                    false,
                                    TimestampData.fromMillis(
                                            Timestamp.valueOf("2023-11-27 19:00:00").getTime())
                                }));
        verifySerializeResult(
                new RowWithOp(
                        GenericRow.of(
                                2,
                                false,
                                TimestampNtz.fromMillis(
                                        Timestamp.valueOf("2023-11-27 19:00:00").getTime())),
                        OperationType.DELETE),
                serializer.serialize(deleteEvent1));

        DataChangeEvent updateEvent1 =
                DataChangeEvent.updateEvent(
                        table1,
                        generator1.generate(
                                new Object[] {
                                    3,
                                    false,
                                    TimestampData.fromMillis(
                                            Timestamp.valueOf("2023-11-27 20:00:00").getTime())
                                }),
                        generator1.generate(
                                new Object[] {
                                    3,
                                    true,
                                    TimestampData.fromMillis(
                                            Timestamp.valueOf("2023-11-27 21:00:00").getTime())
                                }));
        verifySerializeResult(
                new RowWithOp(
                        GenericRow.of(
                                3,
                                true,
                                TimestampNtz.fromMillis(
                                        Timestamp.valueOf("2023-11-27 21:00:00").getTime())),
                        OperationType.UPSERT),
                serializer.serialize(updateEvent1));

        // 2. create table2, and insert data
        TableId table2 = TableId.parse("test.tbl2");
        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("col1", new DateType())
                        .physicalColumn("col2", new FloatType())
                        .physicalColumn("col3", new VarCharType(20))
                        .physicalColumn("col4", new DecimalType(20, 5))
                        .physicalColumn("col5", new LocalZonedTimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent2 = new CreateTableEvent(table2, schema2);
        Assertions.assertThat(serializer.serialize(createTableEvent2)).isNull();

        BinaryRecordDataGenerator generator2 =
                new BinaryRecordDataGenerator(
                        schema2.getColumnDataTypes().toArray(new DataType[0]));
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table2,
                        generator2.generate(
                                new Object[] {
                                    (int) LocalDate.of(2023, 11, 27).toEpochDay(),
                                    3.4f,
                                    BinaryStringData.fromString("insert table2"),
                                    DecimalData.fromBigDecimal(new BigDecimal("83.23"), 20, 5),
                                    LocalZonedTimestampData.fromInstant(
                                            LocalDateTime.of(2023, 11, 27, 21, 0, 0)
                                                    .toInstant(ZoneOffset.of("+10")))
                                }));
        verifySerializeResult(
                new RowWithOp(
                        GenericRow.of(
                                (int) LocalDate.of(2023, 11, 27).toEpochDay(),
                                3.4f,
                                BinaryString.fromString("insert table2"),
                                Decimal.fromBigDecimal(new BigDecimal("83.23"), 20, 5),
                                TimestampLtz.fromInstant(
                                        LocalDateTime.of(2023, 11, 27, 21, 0, 0)
                                                .toInstant(ZoneOffset.of("+10")))),
                        OperationType.UPSERT),
                serializer.serialize(insertEvent2));

        DataChangeEvent deleteEvent2 =
                DataChangeEvent.deleteEvent(
                        table1,
                        generator1.generate(
                                new Object[] {
                                    4,
                                    true,
                                    TimestampData.fromMillis(
                                            Timestamp.valueOf("2023-11-27 21:00:00").getTime())
                                }));
        verifySerializeResult(
                new RowWithOp(
                        GenericRow.of(
                                4,
                                true,
                                TimestampNtz.fromMillis(
                                        Timestamp.valueOf("2023-11-27 21:00:00").getTime())),
                        OperationType.DELETE),
                Objects.requireNonNull(serializer.serialize(deleteEvent2)));
    }

    private void verifySerializeResult(RowWithOp expectRow, RowWithOp flussRowData)
            throws Exception {
        Assertions.assertThat(expectRow).isEqualTo(flussRowData);
    }

    /** A mock context for serialization schema testing. */
    private static class MockInitializationContext
            implements FlussSerializationSchema.InitializationContext {

        @Override
        public RowType getRowSchema() {
            return null;
        }
    }
}
