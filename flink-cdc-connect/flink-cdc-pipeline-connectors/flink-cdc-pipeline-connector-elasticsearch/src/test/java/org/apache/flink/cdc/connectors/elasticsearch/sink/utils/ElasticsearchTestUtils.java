/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.cdc.connectors.elasticsearch.sink.utils;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Utility class for creating test events for Elasticsearch integration tests. */
public class ElasticsearchTestUtils {

    /**
     * Creates a list of test events that simulate inserting rows into a table.
     *
     * @param tableId the identifier of the table.
     * @return a list of events simulating inserts into the table.
     */
    public static List<Event> createTestEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .column(new PhysicalColumn("bool", DataTypes.BOOLEAN(), null))
                        .column(new PhysicalColumn("tinyint", DataTypes.TINYINT(), null))
                        .column(new PhysicalColumn("smallint", DataTypes.SMALLINT(), null))
                        .column(new PhysicalColumn("bigint", DataTypes.BIGINT(), null))
                        .column(new PhysicalColumn("float", DataTypes.FLOAT(), null))
                        .column(new PhysicalColumn("decimal", DataTypes.DECIMAL(10, 2), null))
                        .column(new PhysicalColumn("timestamp", DataTypes.TIMESTAMP(), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        org.apache.flink.cdc.common.types.RowType.of(
                                org.apache.flink.cdc.common.types.DataTypes.INT(),
                                org.apache.flink.cdc.common.types.DataTypes.DOUBLE(),
                                org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                org.apache.flink.cdc.common.types.DataTypes.BOOLEAN(),
                                org.apache.flink.cdc.common.types.DataTypes.TINYINT(),
                                org.apache.flink.cdc.common.types.DataTypes.SMALLINT(),
                                org.apache.flink.cdc.common.types.DataTypes.BIGINT(),
                                org.apache.flink.cdc.common.types.DataTypes.FLOAT(),
                                org.apache.flink.cdc.common.types.DataTypes.DECIMAL(10, 2),
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP()));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    1,
                                    1.0,
                                    BinaryStringData.fromString("value1"),
                                    true,
                                    (byte) 1,
                                    (short) 2,
                                    100L,
                                    1.0f,
                                    DecimalData.fromBigDecimal(new BigDecimal("10.00"), 10, 2),
                                    TimestampData.fromMillis(1633024800000L)
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    2,
                                    2.0,
                                    BinaryStringData.fromString("value2"),
                                    false,
                                    (byte) 2,
                                    (short) 3,
                                    200L,
                                    2.0f,
                                    DecimalData.fromBigDecimal(new BigDecimal("20.00"), 10, 2),
                                    TimestampData.fromMillis(1633111200000L)
                                })));
    }

    /**
     * Creates a list of test events that simulate inserting and then deleting a row from a table.
     *
     * @param tableId the identifier of the table.
     * @return a list of events simulating inserts and deletes from the table.
     */
    public static List<Event> createTestEventsWithDelete(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        org.apache.flink.cdc.common.types.RowType.of(
                                org.apache.flink.cdc.common.types.DataTypes.INT(),
                                org.apache.flink.cdc.common.types.DataTypes.DOUBLE(),
                                org.apache.flink.cdc.common.types.DataTypes.STRING()));

        BinaryRecordData insertRecord1 =
                generator.generate(new Object[] {1, 1.0, BinaryStringData.fromString("value1")});
        BinaryRecordData insertRecord2 =
                generator.generate(new Object[] {2, 2.0, BinaryStringData.fromString("value2")});
        BinaryRecordData deleteRecord =
                generator.generate(new Object[] {2, 2.0, BinaryStringData.fromString("value2")});

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(tableId, insertRecord1),
                DataChangeEvent.insertEvent(tableId, insertRecord2),
                DataChangeEvent.deleteEvent(tableId, deleteRecord));
    }

    /**
     * Creates a list of test events that simulate adding a column and then inserting rows into a
     * table.
     *
     * @param tableId the identifier of the table.
     * @return a list of events simulating a schema change and inserts into the table.
     */
    public static List<Event> createTestEventsWithAddColumn(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        org.apache.flink.cdc.common.types.RowType.of(
                                org.apache.flink.cdc.common.types.DataTypes.INT(),
                                org.apache.flink.cdc.common.types.DataTypes.DOUBLE(),
                                org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                org.apache.flink.cdc.common.types.DataTypes.BOOLEAN()));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        new PhysicalColumn(
                                                "extra_bool", DataTypes.BOOLEAN(), null)))),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    3, 3.0, BinaryStringData.fromString("value3"), true
                                })));
    }
}
