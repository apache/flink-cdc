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

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
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
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlussEvent;
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlussOperationType;
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlussRowWithOp;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlussEventSerializationSchema}. */
public class FlussEventSerializationSchemaTest {
    private static FlussEventSerializationSchema serializer;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    private static Connection conn;
    private static FlussMetaDataApplier flussMetaDataApplier;

    @BeforeAll
    static void setup() {
        conn = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        flussMetaDataApplier =
                new FlussMetaDataApplier(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        serializer = new FlussEventSerializationSchema();
        serializer.open(conn);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (flussMetaDataApplier != null) {
            flussMetaDataApplier.close();
        }
        if (conn != null) {
            conn.close();
        }
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
        flussMetaDataApplier.applySchemaChange(createTableEvent1);
        verifySchemaChangeEvent(table1, serializer.serialize(createTableEvent1));

        BinaryRecordDataGenerator generator1 =
                new BinaryRecordDataGenerator(
                        schema1.getColumnDataTypes().toArray(new DataType[0]));

        RecordData record =
                generator1.generate(
                        new Object[] {
                            1,
                            true,
                            TimestampData.fromMillis(
                                    Timestamp.valueOf("2023-11-27 18:00:00").getTime())
                        });
        DataChangeEvent insertEvent1 = DataChangeEvent.insertEvent(table1, record);

        verifyDataChangeEvent(
                table1,
                new FlussRowWithOp(CdcAsFlussRow.replace(record), FlussOperationType.UPSERT),
                serializer.serialize(insertEvent1));

        record =
                generator1.generate(
                        new Object[] {
                            2,
                            false,
                            TimestampData.fromMillis(
                                    Timestamp.valueOf("2023-11-27 19:00:00").getTime())
                        });

        DataChangeEvent deleteEvent1 = DataChangeEvent.deleteEvent(table1, record);
        verifyDataChangeEvent(
                table1,
                new FlussRowWithOp(CdcAsFlussRow.replace(record), FlussOperationType.DELETE),
                serializer.serialize(deleteEvent1));

        record =
                generator1.generate(
                        new Object[] {
                            3,
                            true,
                            TimestampData.fromMillis(
                                    Timestamp.valueOf("2023-11-27 21:00:00").getTime())
                        });
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
                        record);

        verifyDataChangeEvent(
                table1,
                new FlussRowWithOp(CdcAsFlussRow.replace(record), FlussOperationType.UPSERT),
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
        flussMetaDataApplier.applySchemaChange(createTableEvent2);
        verifySchemaChangeEvent(table2, serializer.serialize(createTableEvent2));

        BinaryRecordDataGenerator generator2 =
                new BinaryRecordDataGenerator(
                        schema2.getColumnDataTypes().toArray(new DataType[0]));
        record =
                generator2.generate(
                        new Object[] {
                            DateData.fromLocalDate(LocalDate.of(2023, 11, 27)),
                            3.4f,
                            BinaryStringData.fromString("insert table2"),
                            DecimalData.fromBigDecimal(new BigDecimal("83.23"), 20, 5),
                            LocalZonedTimestampData.fromInstant(
                                    LocalDateTime.of(2023, 11, 27, 21, 0, 0)
                                            .toInstant(ZoneOffset.of("+10")))
                        });
        DataChangeEvent insertEvent2 = DataChangeEvent.insertEvent(table2, record);
        verifyDataChangeEvent(
                table2,
                new FlussRowWithOp(CdcAsFlussRow.replace(record), FlussOperationType.UPSERT),
                serializer.serialize(insertEvent2));

        record =
                generator2.generate(
                        new Object[] {
                            DateData.fromEpochDay(4),
                            3.4f,
                            BinaryStringData.fromString("insert table2"),
                            DecimalData.fromBigDecimal(new BigDecimal("83.23"), 20, 5),
                            LocalZonedTimestampData.fromInstant(
                                    LocalDateTime.of(2023, 11, 27, 21, 0, 0)
                                            .toInstant(ZoneOffset.of("+10")))
                        });
        DataChangeEvent deleteEvent2 = DataChangeEvent.deleteEvent(table2, record);
        verifyDataChangeEvent(
                table2,
                new FlussRowWithOp(CdcAsFlussRow.replace(record), FlussOperationType.DELETE),
                Objects.requireNonNull(serializer.serialize(deleteEvent2)));
    }

    private void verifySchemaChangeEvent(TableId tableId, FlussEvent flussEvent) throws Exception {
        verifySerializeResult(
                new FlussEvent(
                        TablePath.of(tableId.getSchemaName(), tableId.getTableName()), null, true),
                flussEvent);
    }

    private void verifyDataChangeEvent(TableId tableId, FlussRowWithOp row, FlussEvent flussEvent)
            throws Exception {
        verifySerializeResult(
                new FlussEvent(
                        TablePath.of(tableId.getSchemaName(), tableId.getTableName()),
                        Collections.singletonList(row),
                        false),
                flussEvent);
    }

    private void verifySerializeResult(FlussEvent expectedEvent, FlussEvent actualflussEvent) {

        assertThat(actualflussEvent.getTablePath()).isEqualTo(expectedEvent.getTablePath());
        assertThat(actualflussEvent.isShouldRefreshSchema())
                .isEqualTo(expectedEvent.isShouldRefreshSchema());
        List<FlussRowWithOp> actualRowWithOps = actualflussEvent.getRowWithOps();
        List<FlussRowWithOp> expectedRowWithOps = expectedEvent.getRowWithOps();
        if (actualRowWithOps == null) {
            assertThat(expectedRowWithOps).isNull();
            return;
        }
        assertThat(actualRowWithOps.size()).isEqualTo(expectedRowWithOps.size());
        for (int i = 0; i < actualRowWithOps.size(); i++) {
            FlussRowWithOp actualRowWithOp = actualRowWithOps.get(i);
            FlussRowWithOp expectedRowWithOp = expectedRowWithOps.get(i);
            assertThat(actualRowWithOp.getOperationType())
                    .isEqualTo(expectedRowWithOp.getOperationType());
            assertThat(actualRowWithOp.getRow()).isInstanceOf(CdcAsFlussRow.class);
            CdcAsFlussRow actualRow = (CdcAsFlussRow) actualRowWithOp.getRow();
            CdcAsFlussRow expectedRow = (CdcAsFlussRow) expectedRowWithOp.getRow();
            assertThat(actualRow.getCdcRecord()).isEqualTo(expectedRow.getCdcRecord());
            assertThat(actualRow.getIndexMapping()).isEqualTo(expectedRow.getIndexMapping());
        }
    }
}
