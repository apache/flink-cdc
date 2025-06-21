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
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Collections;

/** Unit tests for the {@link PreTransformOperator}. */
class PreTransformOperatorTest {
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
                    .primaryKey("col2")
                    .partitionKey("col12")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();
    private static final Schema EXPECT_LATEST_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("col2", DataTypes.STRING())
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
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema REFERENCED_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("ref1", DataTypes.STRING())
                    .physicalColumn("ref2", DataTypes.INT())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema EXPECTED_REFERENCED_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("ref1", DataTypes.STRING())
                    .physicalColumn("ref2", DataTypes.INT())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema WILDCARD_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema EXPECTED_WILDCARD_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final TableId METADATA_TABLEID =
            TableId.tableId("my_company", "my_branch", "metadata_table");
    private static final Schema METADATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema EXPECTED_METADATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final TableId METADATA_AS_TABLEID =
            TableId.tableId("my_company", "my_branch", "metadata_as_table");
    private static final Schema METADATA_AS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("sid", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("name_upper", DataTypes.STRING())
                    .physicalColumn("tbname", DataTypes.STRING())
                    .primaryKey("sid")
                    .build();

    private static final Schema MULTITRANSFORM_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("sex", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final Schema EXPECTED_MULTITRANSFORM_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final Schema COL_NAME_MAPPING_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("foo", DataTypes.INT())
                    .physicalColumn("bar", DataTypes.INT())
                    .physicalColumn("foo-bar", DataTypes.INT())
                    .physicalColumn("bar-foo", DataTypes.INT())
                    .physicalColumn("class", DataTypes.INT())
                    .build();

    private static final Schema EXPECTED_COL_NAME_MAPPING_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("foo", DataTypes.INT())
                    .physicalColumn("bar", DataTypes.INT())
                    .physicalColumn("foo-bar", DataTypes.INT())
                    .physicalColumn("bar-foo", DataTypes.INT())
                    .physicalColumn("class", DataTypes.INT())
                    .build();

    @Test
    void testEventTransform() throws Exception {
        PreTransformOperator transform =
                PreTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "*, concat(col1,col2) col12",
                                null,
                                "col2",
                                "col12",
                                "key1=value1,key2=value2",
                                null,
                                new SupportedMetadataColumn[0])
                        .build();
        RegularEventOperatorTestHarness<PreTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
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
                                    new BinaryStringData("3")
                                }),
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("3"),
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
                .isEqualTo(
                        new StreamRecord<>(
                                new AddColumnEvent(
                                        CUSTOMERS_TABLEID,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                "col3", DataTypes.STRING()),
                                                        AddColumnEvent.ColumnPosition.AFTER,
                                                        "col2")))));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testNullabilityColumn() throws Exception {
        PreTransformOperator transform =
                PreTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "id, upper(id) uid, name, upper(name) uname",
                                null,
                                "id",
                                "id",
                                "key1=value1,key2=value2",
                                null,
                                new SupportedMetadataColumn[0])
                        .build();
        RegularEventOperatorTestHarness<PreTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
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
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testReduceTransformColumn() throws Exception {
        PreTransformOperator transform =
                PreTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "id, upper(id) as uid, age + 1 as newage, lower(ref1) as ref1",
                                "newage > 17 and ref2 > 17",
                                "id",
                                "id",
                                "key1=value1,key2=value2",
                                null,
                                new SupportedMetadataColumn[0])
                        .build();
        RegularEventOperatorTestHarness<PreTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, REFERENCED_SCHEMA);
        transform.processElement(new StreamRecord<>(createTableEvent));

        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        CUSTOMERS_TABLEID, EXPECTED_REFERENCED_SCHEMA)));

        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) REFERENCED_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator recordDataGeneratorExpect =
                new BinaryRecordDataGenerator(
                        ((RowType) EXPECTED_REFERENCED_SCHEMA.toRowDataType()));

        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                    new BinaryStringData("Reference"),
                                    42,
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Reference"),
                                    42
                                }));

        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                    new BinaryStringData("Reference"),
                                    42,
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    18,
                                    new BinaryStringData("Arisu"),
                                    new BinaryStringData("UpdatedReference"),
                                    41,
                                }));
        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Reference"),
                                    42
                                }),
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    18,
                                    new BinaryStringData("UpdatedReference"),
                                    41
                                }));

        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));

        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testWildcardTransformColumn() throws Exception {
        PreTransformOperator transform =
                PreTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "*, age + 1 as newage",
                                "newage > 17",
                                "id",
                                "id",
                                "key1=value1,key2=value2",
                                null,
                                new SupportedMetadataColumn[0])
                        .build();
        RegularEventOperatorTestHarness<PreTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, WILDCARD_SCHEMA);
        transform.processElement(new StreamRecord<>(createTableEvent));

        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(CUSTOMERS_TABLEID, EXPECTED_WILDCARD_SCHEMA)));

        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) WILDCARD_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator recordDataGeneratorExpect =
                new BinaryRecordDataGenerator(((RowType) EXPECTED_WILDCARD_SCHEMA.toRowDataType()));

        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"), 17, new BinaryStringData("Alice")
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                }));

        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"), 17, new BinaryStringData("Alice")
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"), 18, new BinaryStringData("Arisu")
                                }));
        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                }),
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    18,
                                    new BinaryStringData("Arisu"),
                                }));

        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));

        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testMetadataTransform() throws Exception {
        PreTransformOperator transform =
                PreTransformOperator.newBuilder()
                        .addTransform(
                                METADATA_TABLEID.identifier(),
                                "*, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ identifier_name, __namespace_name__, __schema_name__, __table_name__",
                                " __table_name__ = 'metadata_table' ")
                        .build();

        RegularEventOperatorTestHarness<PreTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(METADATA_TABLEID, METADATA_SCHEMA);
        transform.processElement(new StreamRecord<>(createTableEvent));

        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(METADATA_TABLEID, EXPECTED_METADATA_SCHEMA)));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testMultiTransformWithDiffRefColumns() throws Exception {
        PreTransformOperator transform =
                PreTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "id, 'Juvenile' as roleName",
                                "age < 18",
                                "id",
                                null,
                                null,
                                null,
                                new SupportedMetadataColumn[0])
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "id, name as roleName",
                                "age >= 18",
                                "id",
                                null,
                                null,
                                null,
                                new SupportedMetadataColumn[0])
                        .build();
        RegularEventOperatorTestHarness<PreTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, MULTITRANSFORM_SCHEMA);

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        CUSTOMERS_TABLEID, EXPECTED_MULTITRANSFORM_SCHEMA)));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testMultiTransformWithAsterisk() throws Exception {
        PreTransformOperator transform =
                PreTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "*, 'Juvenile' as roleName",
                                "age < 18",
                                "id",
                                null,
                                null,
                                null,
                                new SupportedMetadataColumn[0])
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "id, age, name, sex, 'Juvenile' as roleName",
                                "age >= 18",
                                "id",
                                null,
                                null,
                                null,
                                new SupportedMetadataColumn[0])
                        .build();
        RegularEventOperatorTestHarness<PreTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, MULTITRANSFORM_SCHEMA);

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(CUSTOMERS_TABLEID, MULTITRANSFORM_SCHEMA)));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testMultiTransformMissingProjection() throws Exception {
        PreTransformOperator transform =
                PreTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                null,
                                "age < 18",
                                "id",
                                null,
                                null,
                                null,
                                new SupportedMetadataColumn[0])
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "id, age, UPPER(name) as name, sex",
                                "age >= 18",
                                "id",
                                null,
                                null,
                                null,
                                new SupportedMetadataColumn[0])
                        .build();
        RegularEventOperatorTestHarness<PreTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, MULTITRANSFORM_SCHEMA);

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(CUSTOMERS_TABLEID, MULTITRANSFORM_SCHEMA)));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testColumnNameMapping() throws Exception {
        PreTransformOperator transform =
                PreTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "foo, `foo-bar`, foo-bar AS f0, `bar-foo` AS f1, class",
                                " `foo-bar` > 1 and foo-bar > 1 and class > 1")
                        .build();

        RegularEventOperatorTestHarness<PreTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, COL_NAME_MAPPING_SCHEMA);
        transform.processElement(new StreamRecord<>(createTableEvent));

        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        CUSTOMERS_TABLEID, EXPECTED_COL_NAME_MAPPING_SCHEMA)));
        transformFunctionEventEventOperatorTestHarness.close();
    }
}
