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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.testutils.operators.EventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Collections;

/** Unit tests for the {@link TransformSchemaOperator}. */
public class TransformSchemaOperatorTest {
    private static final TableId CUSTOMERS_TABLEID =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("col2", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();
    private static final Schema CUSTOMERS_LATEST_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("col2", DataTypes.STRING())
                    .physicalColumn("col3", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();
    private static final Schema EXPECT_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("col2", DataTypes.STRING())
                    .physicalColumn("col12", DataTypes.STRING())
                    .primaryKey("col2")
                    .partitionKey("col12")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();
    private static final Schema EXPECT_LATEST_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("col2", DataTypes.STRING())
                    .physicalColumn("col12", DataTypes.STRING())
                    .physicalColumn("col3", DataTypes.STRING())
                    .primaryKey("col2")
                    .partitionKey("col12")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema NULLABILITY_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema EXPECTED_NULLABILITY_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("uid", DataTypes.STRING())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("uname", DataTypes.STRING())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    @Test
    void testEventTransform() throws Exception {
        TransformSchemaOperator transform =
                TransformSchemaOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "*, concat(col1,col2) col12",
                                "col2",
                                "col12",
                                "key1=value1,key2=value2")
                        .build();
        EventOperatorTestHarness<TransformSchemaOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
        // Add column
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        CUSTOMERS_TABLEID, Collections.singletonList(columnWithPosition));
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CUSTOMERS_LATEST_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator recordDataGeneratorExpect =
                new BinaryRecordDataGenerator(((RowType) EXPECT_LATEST_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("3"),
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    null,
                                    new BinaryStringData("3")
                                }));

        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("3")
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("3"),
                                    new BinaryStringData("3")
                                }));
        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    null,
                                    new BinaryStringData("3")
                                }),
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("3"),
                                    null,
                                    new BinaryStringData("3")
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(new CreateTableEvent(CUSTOMERS_TABLEID, EXPECT_SCHEMA)));
        transform.processElement(new StreamRecord<>(addColumnEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(addColumnEvent));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
    }

    @Test
    public void testNullabilityColumn() throws Exception {
        TransformSchemaOperator transform =
                TransformSchemaOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "id, upper(id) uid, name, upper(name) uname",
                                "id",
                                "id",
                                "key1=value1,key2=value2")
                        .build();
        EventOperatorTestHarness<TransformSchemaOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, NULLABILITY_SCHEMA);
        transform.processElement(new StreamRecord<>(createTableEvent));

        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        CUSTOMERS_TABLEID, EXPECTED_NULLABILITY_SCHEMA)));
    }
}
