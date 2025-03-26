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

package org.apache.flink.cdc.runtime.operators.schema.regular;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.apache.commons.collections.ListUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/** Unit tests for the {@link SchemaOperator} to handle evolved schema. */
class SchemaEvolveTest {

    private static final DataType TINYINT = DataTypes.TINYINT();
    private static final DataType SMALLINT = DataTypes.SMALLINT();
    private static final DataType INT = DataTypes.INT();
    private static final DataType BIGINT = DataTypes.BIGINT();
    private static final DataType FLOAT = DataTypes.FLOAT();
    private static final DataType DOUBLE = DataTypes.DOUBLE();
    private static final DataType STRING = DataTypes.STRING();

    private static final TableId CUSTOMERS_TABLE_ID =
            TableId.tableId("my_company", "my_branch", "customers");

    /** Tests common evolve schema changes without exceptions. */
    @Test
    void testEvolveSchema() throws Exception {
        TableId tableId = CUSTOMERS_TABLE_ID;
        Schema schemaV1 =
                Schema.newBuilder()
                        .physicalColumn("id", INT)
                        .physicalColumn("name", STRING)
                        .physicalColumn("age", SMALLINT)
                        .primaryKey("id")
                        .build();

        SchemaChangeBehavior behavior = SchemaChangeBehavior.EVOLVE;

        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(30), behavior);
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDurationAndBehavior(
                        schemaOperator, 17, Duration.ofSeconds(3), behavior);
        harness.open();

        // Test CreateTableEvent
        {
            List<Event> createAndInsertDataEvents =
                    Arrays.asList(
                            new CreateTableEvent(tableId, schemaV1),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 1, STRING, "Alice", SMALLINT, (short) 17)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 2, STRING, "Bob", SMALLINT, (short) 18)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 3, STRING, "Carol", SMALLINT, (short) 19)));

            processEvent(schemaOperator, createAndInsertDataEvents);

            Assertions.assertThat(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.CREATE_TABLE)),
                                    createAndInsertDataEvents))
                    .isEqualTo(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()));

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV1);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test AddColumnEvent
        {
            List<Event> addColumnEvents =
                    Arrays.asList(
                            new AddColumnEvent(
                                    tableId,
                                    Arrays.asList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "score", INT, "Score data")),
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "height", DOUBLE, "Height data")))),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            4,
                                            STRING,
                                            "Derrida",
                                            SMALLINT,
                                            (short) 20,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            5,
                                            STRING,
                                            "Eve",
                                            SMALLINT,
                                            (short) 21,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));
            processEvent(schemaOperator, addColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.ADD_COLUMN)),
                                    addColumnEvents));

            Schema schemaV2 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("score", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV2);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV2);

            harness.clearOutputRecords();
        }

        // Test RenameColumnEvent
        {
            List<Event> renameColumnEvents =
                    Arrays.asList(
                            new RenameColumnEvent(
                                    tableId, ImmutableMap.of("name", "namae", "age", "toshi")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            6,
                                            STRING,
                                            "Fiona",
                                            SMALLINT,
                                            (short) 22,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            7,
                                            STRING,
                                            "Gloria",
                                            SMALLINT,
                                            (short) 23,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));

            processEvent(schemaOperator, renameColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.RENAME_COLUMN)),
                                    renameColumnEvents));

            Schema schemaV3 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("toshi", SMALLINT)
                            .physicalColumn("score", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV3);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV3);

            harness.clearOutputRecords();
        }

        // Test AlterColumnTypeEvent
        {
            List<Event> alterColumnTypeEvents =
                    Arrays.asList(
                            new AlterColumnTypeEvent(
                                    tableId,
                                    ImmutableMap.of("score", BIGINT, "toshi", FLOAT),
                                    ImmutableMap.of("score", INT, "toshi", SMALLINT)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 8, STRING, "Helen", FLOAT, 22f, BIGINT, 100L,
                                            DOUBLE, 173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 9, STRING, "Iva", FLOAT, 23f, BIGINT, 97L, DOUBLE,
                                            160.)));

            processEvent(schemaOperator, alterColumnTypeEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.ALTER_COLUMN_TYPE)),
                                    alterColumnTypeEvents));

            Schema schemaV4 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("toshi", FLOAT)
                            .physicalColumn("score", BIGINT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV4);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV4);

            harness.clearOutputRecords();
        }

        // Test DropColumnEvent
        {
            List<Event> dropColumnEvents =
                    Arrays.asList(
                            new DropColumnEvent(tableId, Arrays.asList("score", "height")),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 12, STRING, "Jane", FLOAT, 11f)),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 13, STRING, "Kryo", FLOAT, 23f)));

            processEvent(schemaOperator, dropColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.DROP_COLUMN)),
                                    dropColumnEvents));

            Schema schemaV5 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("toshi", FLOAT)
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV5);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV5);

            harness.clearOutputRecords();
        }
        harness.close();
    }

    /** Tests try-evolve behavior without exceptions. */
    @Test
    void testTryEvolveSchema() throws Exception {
        TableId tableId = CUSTOMERS_TABLE_ID;
        Schema schemaV1 =
                Schema.newBuilder()
                        .physicalColumn("id", INT)
                        .physicalColumn("name", STRING)
                        .physicalColumn("age", SMALLINT)
                        .primaryKey("id")
                        .build();

        SchemaChangeBehavior behavior = SchemaChangeBehavior.TRY_EVOLVE;

        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(30), behavior);
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDurationAndBehavior(
                        schemaOperator, 17, Duration.ofSeconds(3), behavior);
        harness.open();

        // Test CreateTableEvent
        {
            List<Event> createAndInsertDataEvents =
                    Arrays.asList(
                            new CreateTableEvent(tableId, schemaV1),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 1, STRING, "Alice", SMALLINT, (short) 17)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 2, STRING, "Bob", SMALLINT, (short) 18)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 3, STRING, "Carol", SMALLINT, (short) 19)));

            processEvent(schemaOperator, createAndInsertDataEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.CREATE_TABLE)),
                                    createAndInsertDataEvents));

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV1);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test AddColumnEvent
        {
            List<Event> addColumnEvents =
                    Arrays.asList(
                            new AddColumnEvent(
                                    tableId,
                                    Arrays.asList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "score", INT, "Score data")),
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "height", DOUBLE, "Height data")))),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            4,
                                            STRING,
                                            "Derrida",
                                            SMALLINT,
                                            (short) 20,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            5,
                                            STRING,
                                            "Eve",
                                            SMALLINT,
                                            (short) 21,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));
            processEvent(schemaOperator, addColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.ADD_COLUMN)),
                                    addColumnEvents));

            Schema schemaV2 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("score", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV2);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV2);

            harness.clearOutputRecords();
        }

        // Test RenameColumnEvent
        {
            List<Event> renameColumnEvents =
                    Arrays.asList(
                            new RenameColumnEvent(
                                    tableId, ImmutableMap.of("name", "namae", "age", "toshi")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            6,
                                            STRING,
                                            "Fiona",
                                            SMALLINT,
                                            (short) 22,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            7,
                                            STRING,
                                            "Gloria",
                                            SMALLINT,
                                            (short) 23,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));

            processEvent(schemaOperator, renameColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.RENAME_COLUMN)),
                                    renameColumnEvents));

            Schema schemaV3 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("toshi", SMALLINT)
                            .physicalColumn("score", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV3);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV3);

            harness.clearOutputRecords();
        }

        // Test AlterColumnTypeEvent
        {
            List<Event> alterColumnTypeEvents =
                    Arrays.asList(
                            new AlterColumnTypeEvent(
                                    tableId,
                                    ImmutableMap.of("score", BIGINT, "toshi", FLOAT),
                                    ImmutableMap.of("score", INT, "toshi", SMALLINT)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 8, STRING, "Helen", FLOAT, 22f, BIGINT, 100L,
                                            DOUBLE, 173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 9, STRING, "Iva", FLOAT, 23f, BIGINT, 97L, DOUBLE,
                                            160.)));

            processEvent(schemaOperator, alterColumnTypeEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.ALTER_COLUMN_TYPE)),
                                    alterColumnTypeEvents));

            Schema schemaV4 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("toshi", FLOAT)
                            .physicalColumn("score", BIGINT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV4);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV4);

            harness.clearOutputRecords();
        }

        // Test DropColumnEvent
        {
            List<Event> dropColumnEvents =
                    Arrays.asList(
                            new DropColumnEvent(tableId, Arrays.asList("score", "height")),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 12, STRING, "Jane", FLOAT, 11f)),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 13, STRING, "Kryo", FLOAT, 23f)));

            processEvent(schemaOperator, dropColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.DROP_COLUMN)),
                                    dropColumnEvents));

            Schema schemaV5 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("toshi", FLOAT)
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV5);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV5);

            harness.clearOutputRecords();
        }
        harness.close();
    }

    /** Tests evolve schema changes when schema change behavior is set to EXCEPTION. */
    @Test
    void testExceptionEvolveSchema() throws Exception {
        TableId tableId = CUSTOMERS_TABLE_ID;
        Schema schemaV1 =
                Schema.newBuilder()
                        .physicalColumn("id", INT)
                        .physicalColumn("name", STRING)
                        .physicalColumn("age", SMALLINT)
                        .primaryKey("id")
                        .build();

        SchemaChangeBehavior behavior = SchemaChangeBehavior.EXCEPTION;

        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(30), behavior);
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDurationAndBehavior(
                        schemaOperator, 17, Duration.ofSeconds(3), behavior);
        harness.open();

        // Test CreateTableEvent
        {
            List<Event> createAndInsertDataEvents =
                    Arrays.asList(
                            new CreateTableEvent(tableId, schemaV1),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 1, STRING, "Alice", SMALLINT, (short) 17)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 2, STRING, "Bob", SMALLINT, (short) 18)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 3, STRING, "Carol", SMALLINT, (short) 19)));

            processEvent(schemaOperator, createAndInsertDataEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.CREATE_TABLE)),
                                    createAndInsertDataEvents));

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV1);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test AddColumnEvent (expected to fail)
        {
            List<Event> addColumnEvents =
                    Collections.singletonList(
                            new AddColumnEvent(
                                    tableId,
                                    Arrays.asList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "score", INT, "Score data")),
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "height", DOUBLE, "Height data")))));
            Assertions.assertThatThrownBy(() -> processEvent(schemaOperator, addColumnEvents))
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .cause()
                    .isExactlyInstanceOf(ExecutionException.class)
                    .cause()
                    .isExactlyInstanceOf(FlinkRuntimeException.class)
                    .hasMessage("Failed to apply schema change event.")
                    .cause()
                    .isExactlyInstanceOf(SchemaEvolveException.class)
                    .extracting("applyingEvent", "exceptionMessage")
                    .containsExactly(
                            addColumnEvents.get(0),
                            "Unexpected schema change events occurred in EXCEPTION mode. Job will fail now.");

            // No schema change events should be sent to downstream
            Assertions.assertThat(harness.getOutputRecords())
                    .map(StreamRecord::getValue)
                    .map(e -> e.getClass().getName())
                    .containsExactly("org.apache.flink.cdc.common.event.FlushEvent");
        }

        harness.close();
    }

    /** Tests evolve schema changes when schema change behavior is set to IGNORE. */
    @Test
    void testIgnoreEvolveSchema() throws Exception {
        TableId tableId = CUSTOMERS_TABLE_ID;
        Schema schemaV1 =
                Schema.newBuilder()
                        .physicalColumn("id", INT)
                        .physicalColumn("name", STRING)
                        .physicalColumn("age", SMALLINT)
                        .primaryKey("id")
                        .build();

        SchemaChangeBehavior behavior = SchemaChangeBehavior.IGNORE;

        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(30), behavior);
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDurationAndBehavior(
                        schemaOperator, 17, Duration.ofSeconds(3), behavior);
        harness.open();

        // Test CreateTableEvent
        {
            List<Event> createAndInsertDataEvents =
                    Arrays.asList(
                            new CreateTableEvent(tableId, schemaV1),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 1, STRING, "Alice", SMALLINT, (short) 17)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 2, STRING, "Bob", SMALLINT, (short) 18)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 3, STRING, "Carol", SMALLINT, (short) 19)));

            processEvent(schemaOperator, createAndInsertDataEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.CREATE_TABLE)),
                                    createAndInsertDataEvents));

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV1);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test AddColumnEvent (should be ignored)
        {
            List<Event> addColumnEvents =
                    Arrays.asList(
                            new AddColumnEvent(
                                    tableId,
                                    Arrays.asList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "score", INT, "Score data")),
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "height", DOUBLE, "Height data")))),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            4,
                                            STRING,
                                            "Derrida",
                                            SMALLINT,
                                            (short) 20,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            5,
                                            STRING,
                                            "Eve",
                                            SMALLINT,
                                            (short) 21,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));
            processEvent(schemaOperator, addColumnEvents);

            List<Event> expectedEvents =
                    Arrays.asList(
                            new FlushEvent(
                                    0,
                                    Collections.singletonList(tableId),
                                    SchemaChangeEventType.ADD_COLUMN),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 4, STRING, "Derrida", SMALLINT, (short) 20)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 5, STRING, "Eve", SMALLINT, (short) 21)));

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV2 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("score", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();

            // Downstream schema should not evolve in IGNORE mode
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV2);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test RenameColumnEvent (should be ignored)
        {
            List<Event> renameColumnEvents =
                    Arrays.asList(
                            new RenameColumnEvent(
                                    tableId, ImmutableMap.of("name", "namae", "score", "sukoa")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            6,
                                            STRING,
                                            "Fiona",
                                            SMALLINT,
                                            (short) 22,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            7,
                                            STRING,
                                            "Gloria",
                                            SMALLINT,
                                            (short) 23,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));

            processEvent(schemaOperator, renameColumnEvents);

            List<Event> expectedEvents =
                    Arrays.asList(
                            new FlushEvent(
                                    0,
                                    Collections.singletonList(tableId),
                                    SchemaChangeEventType.RENAME_COLUMN),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 6, STRING, null, SMALLINT, (short) 22)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 7, STRING, null, SMALLINT, (short) 23)));
            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV3 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("sukoa", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV3);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test AlterColumnTypeEvent (should be ignored)
        {
            List<Event> alterColumnTypeEvents =
                    Arrays.asList(
                            new AlterColumnTypeEvent(
                                    tableId, ImmutableMap.of("sukoa", BIGINT, "age", FLOAT)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 8, STRING, "Helen", FLOAT, 22f, BIGINT, 100L,
                                            DOUBLE, 173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 9, STRING, "Iva", FLOAT, 23f, BIGINT, 97L, DOUBLE,
                                            160.)));

            processEvent(schemaOperator, alterColumnTypeEvents);

            List<Event> expectedEvents =
                    Arrays.asList(
                            new FlushEvent(
                                    0,
                                    Collections.singletonList(tableId),
                                    SchemaChangeEventType.ALTER_COLUMN_TYPE),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 8, STRING, null, SMALLINT, null)),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 9, STRING, null, SMALLINT, null)));

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV4 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", FLOAT)
                            .physicalColumn("sukoa", BIGINT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV4);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test DropColumnEvent (should be ignored)
        {
            List<Event> dropColumnEvents =
                    Arrays.asList(
                            new DropColumnEvent(tableId, Arrays.asList("sukoa", "height")),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 12, STRING, "Jane", FLOAT, 11f)),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 13, STRING, "Kryo", FLOAT, 23f)));

            processEvent(schemaOperator, dropColumnEvents);

            List<Event> expectedEvents =
                    Arrays.asList(
                            new FlushEvent(
                                    0,
                                    Collections.singletonList(tableId),
                                    SchemaChangeEventType.DROP_COLUMN),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 12, STRING, null, DOUBLE, null)),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 13, STRING, null, DOUBLE, null)));
            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV5 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", FLOAT)
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV5);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }
        harness.close();
    }

    /** Tests common evolve schema changes with exceptions expected. */
    @Test
    void testEvolveSchemaWithFailure() throws Exception {
        TableId tableId = CUSTOMERS_TABLE_ID;
        Schema schemaV1 =
                Schema.newBuilder()
                        .physicalColumn("id", INT)
                        .physicalColumn("name", STRING)
                        .physicalColumn("age", SMALLINT)
                        .primaryKey("id")
                        .build();

        SchemaChangeBehavior behavior = SchemaChangeBehavior.EVOLVE;

        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(30), behavior);
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDurationAndFineGrainedBehaviorWithError(
                        schemaOperator,
                        17,
                        Duration.ofSeconds(3),
                        behavior,
                        Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet()),
                        Sets.newHashSet(
                                SchemaChangeEventType.ADD_COLUMN,
                                SchemaChangeEventType.RENAME_COLUMN));

        harness.open();

        // Test CreateTableEvent
        List<Event> createAndInsertDataEvents =
                Arrays.asList(
                        new CreateTableEvent(tableId, schemaV1),
                        DataChangeEvent.insertEvent(
                                tableId,
                                buildRecord(INT, 1, STRING, "Alice", SMALLINT, (short) 17)),
                        DataChangeEvent.insertEvent(
                                tableId, buildRecord(INT, 2, STRING, "Bob", SMALLINT, (short) 18)),
                        DataChangeEvent.insertEvent(
                                tableId,
                                buildRecord(INT, 3, STRING, "Carol", SMALLINT, (short) 19)));

        processEvent(schemaOperator, createAndInsertDataEvents);

        Assertions.assertThat(
                        harness.getOutputRecords().stream()
                                .map(StreamRecord::getValue)
                                .collect(Collectors.toList()))
                .isEqualTo(
                        ListUtils.union(
                                Collections.singletonList(
                                        new FlushEvent(
                                                0,
                                                Collections.singletonList(tableId),
                                                SchemaChangeEventType.CREATE_TABLE)),
                                createAndInsertDataEvents));

        Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV1);
        Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

        harness.clearOutputRecords();

        // Test AddColumnEvent (should fail)
        List<Event> addColumnEvents =
                Collections.singletonList(
                        new AddColumnEvent(
                                tableId,
                                Arrays.asList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("score", INT, "Score data")),
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "height", DOUBLE, "Height data")))));
        Assertions.assertThatThrownBy(() -> processEvent(schemaOperator, addColumnEvents))
                .isExactlyInstanceOf(IllegalStateException.class)
                .cause()
                .isExactlyInstanceOf(ExecutionException.class)
                .cause()
                .isExactlyInstanceOf(FlinkRuntimeException.class)
                .hasMessage("Failed to apply schema change event.")
                .cause()
                .isExactlyInstanceOf(UnsupportedSchemaChangeEventException.class)
                .extracting("applyingEvent", "exceptionMessage")
                .containsExactly(
                        addColumnEvents.get(0), "Sink doesn't support such schema change event.");

        Assertions.assertThat(harness.isJobFailed()).isTrue();
        Assertions.assertThat(harness.getJobFailureCause())
                .cause()
                .isExactlyInstanceOf(UnsupportedSchemaChangeEventException.class)
                .matches(
                        e ->
                                ((UnsupportedSchemaChangeEventException) e)
                                        .getExceptionMessage()
                                        .equals("Sink doesn't support such schema change event."));
        harness.close();
    }

    /** Tests evolve schema changes when schema change behavior is set to TRY_EVOLVE. */
    @Test
    void testTryEvolveSchemaWithFailure() throws Exception {
        TableId tableId = CUSTOMERS_TABLE_ID;
        Schema schemaV1 =
                Schema.newBuilder()
                        .physicalColumn("id", INT)
                        .physicalColumn("name", STRING)
                        .physicalColumn("age", SMALLINT)
                        .primaryKey("id")
                        .build();

        SchemaChangeBehavior behavior = SchemaChangeBehavior.TRY_EVOLVE;

        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(30), behavior);

        // All types of schema change events will be sent to the sink
        // AddColumn and RenameColumn events will always fail
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDurationAndFineGrainedBehaviorWithError(
                        schemaOperator,
                        17,
                        Duration.ofSeconds(3),
                        behavior,
                        Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet()),
                        Sets.newHashSet(
                                SchemaChangeEventType.ALTER_COLUMN_TYPE,
                                SchemaChangeEventType.DROP_COLUMN));

        harness.open();

        // Test CreateTableEvent
        {
            List<Event> createAndInsertDataEvents =
                    Arrays.asList(
                            new CreateTableEvent(tableId, schemaV1),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 1, STRING, "Alice", SMALLINT, (short) 17)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 2, STRING, "Bob", SMALLINT, (short) 18)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 3, STRING, "Carol", SMALLINT, (short) 19)));

            processEvent(schemaOperator, createAndInsertDataEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.CREATE_TABLE)),
                                    createAndInsertDataEvents));

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV1);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test AddColumnEvent
        {
            List<Event> addColumnEvents =
                    Arrays.asList(
                            new AddColumnEvent(
                                    tableId,
                                    Arrays.asList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "score", INT, "Score data")),
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "height", DOUBLE, "Height data")))),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            4,
                                            STRING,
                                            "Derrida",
                                            SMALLINT,
                                            (short) 20,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            5,
                                            STRING,
                                            "Eve",
                                            SMALLINT,
                                            (short) 21,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));
            processEvent(schemaOperator, addColumnEvents);

            List<Event> expectedEvents = new ArrayList<>();
            expectedEvents.add(
                    new FlushEvent(
                            0,
                            Collections.singletonList(tableId),
                            SchemaChangeEventType.ADD_COLUMN));
            expectedEvents.addAll(addColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV2 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("score", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();

            // Downstream schema should not evolve in IGNORE mode
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV2);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV2);

            harness.clearOutputRecords();
        }

        // Test RenameColumnEvent
        {
            List<Event> renameColumnEvents =
                    Arrays.asList(
                            new RenameColumnEvent(
                                    tableId, ImmutableMap.of("name", "namae", "score", "sukoa")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            6,
                                            STRING,
                                            "Fiona",
                                            SMALLINT,
                                            (short) 22,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            7,
                                            STRING,
                                            "Gloria",
                                            SMALLINT,
                                            (short) 23,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));

            processEvent(schemaOperator, renameColumnEvents);

            List<Event> expectedEvents = new ArrayList<>();
            expectedEvents.add(
                    new FlushEvent(
                            0,
                            Collections.singletonList(tableId),
                            SchemaChangeEventType.RENAME_COLUMN));
            expectedEvents.addAll(renameColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV3 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("sukoa", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV3);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV3);

            harness.clearOutputRecords();
        }

        // Test AlterColumnTypeEvent (should fail)
        {
            List<Event> alterColumnTypeEvents =
                    Arrays.asList(
                            new AlterColumnTypeEvent(
                                    tableId, ImmutableMap.of("sukoa", BIGINT, "age", FLOAT)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 8, STRING, "Helen", FLOAT, 22f, BIGINT, 100L,
                                            DOUBLE, 173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 9, STRING, "Iva", FLOAT, 23f, BIGINT, 97L, DOUBLE,
                                            160.)));

            processEvent(schemaOperator, alterColumnTypeEvents);

            List<Event> expectedEvents =
                    Arrays.asList(
                            new FlushEvent(
                                    0,
                                    Collections.singletonList(tableId),
                                    SchemaChangeEventType.ALTER_COLUMN_TYPE),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 8, STRING, "Helen", SMALLINT, null, INT, null,
                                            DOUBLE, 173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 9, STRING, "Iva", SMALLINT, null, INT, null,
                                            DOUBLE, 160.)));

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV4 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", FLOAT)
                            .physicalColumn("sukoa", BIGINT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();

            Schema schemaV4E =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("sukoa", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV4);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV4E);

            harness.clearOutputRecords();
        }

        // Test DropColumnEvent (should fail)
        {
            List<Event> dropColumnEvents =
                    Arrays.asList(
                            new DropColumnEvent(tableId, Arrays.asList("sukoa", "height")),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 12, STRING, "Jane", FLOAT, 11f)),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 13, STRING, "Kryo", FLOAT, 23f)));

            processEvent(schemaOperator, dropColumnEvents);

            List<Event> expectedEvents =
                    Arrays.asList(
                            new FlushEvent(
                                    0,
                                    Collections.singletonList(tableId),
                                    SchemaChangeEventType.DROP_COLUMN),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 12, STRING, "Jane", SMALLINT, null, INT, null,
                                            DOUBLE, null)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 13, STRING, "Kryo", SMALLINT, null, INT, null,
                                            DOUBLE, null)));
            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV5 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", FLOAT)
                            .primaryKey("id")
                            .build();
            Schema schemaV5E =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("sukoa", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV5);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV5E);

            harness.clearOutputRecords();
        }
        harness.close();
    }

    /** Tests fine-grained schema change configurations. */
    @Test
    void testFineGrainedSchemaEvolves() throws Exception {
        TableId tableId = CUSTOMERS_TABLE_ID;
        Schema schemaV1 =
                Schema.newBuilder()
                        .physicalColumn("id", INT)
                        .physicalColumn("name", STRING)
                        .physicalColumn("age", SMALLINT)
                        .primaryKey("id")
                        .build();

        SchemaChangeBehavior behavior = SchemaChangeBehavior.EVOLVE;

        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(30), behavior);

        // All types of schema change events will be sent to the sink
        // AddColumn and RenameColumn events will always fail
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDurationAndFineGrainedBehavior(
                        schemaOperator,
                        17,
                        Duration.ofSeconds(3),
                        behavior,
                        Sets.newHashSet(
                                SchemaChangeEventType.CREATE_TABLE,
                                SchemaChangeEventType.ADD_COLUMN,
                                SchemaChangeEventType.RENAME_COLUMN));

        harness.open();

        // Test CreateTableEvent
        {
            List<Event> createAndInsertDataEvents =
                    Arrays.asList(
                            new CreateTableEvent(tableId, schemaV1),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 1, STRING, "Alice", SMALLINT, (short) 17)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 2, STRING, "Bob", SMALLINT, (short) 18)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 3, STRING, "Carol", SMALLINT, (short) 19)));

            processEvent(schemaOperator, createAndInsertDataEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.CREATE_TABLE)),
                                    createAndInsertDataEvents));

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV1);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test AddColumnEvent
        {
            List<Event> addColumnEvents =
                    Arrays.asList(
                            new AddColumnEvent(
                                    tableId,
                                    Arrays.asList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "score", INT, "Score data")),
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "height", DOUBLE, "Height data")))),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            4,
                                            STRING,
                                            "Derrida",
                                            SMALLINT,
                                            (short) 20,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            5,
                                            STRING,
                                            "Eve",
                                            SMALLINT,
                                            (short) 21,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));
            processEvent(schemaOperator, addColumnEvents);

            List<Event> expectedEvents = new ArrayList<>();
            expectedEvents.add(
                    new FlushEvent(
                            0,
                            Collections.singletonList(tableId),
                            SchemaChangeEventType.ADD_COLUMN));

            expectedEvents.addAll(addColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV2 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("score", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();

            // Downstream schema should not evolve in IGNORE mode
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV2);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV2);

            harness.clearOutputRecords();
        }

        // Test RenameColumnEvent
        {
            List<Event> renameColumnEvents =
                    Arrays.asList(
                            new RenameColumnEvent(
                                    tableId, ImmutableMap.of("name", "namae", "score", "sukoa")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            6,
                                            STRING,
                                            "Fiona",
                                            SMALLINT,
                                            (short) 22,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            7,
                                            STRING,
                                            "Gloria",
                                            SMALLINT,
                                            (short) 23,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));

            processEvent(schemaOperator, renameColumnEvents);

            List<Event> expectedEvents = new ArrayList<>();
            expectedEvents.add(
                    new FlushEvent(
                            0,
                            Collections.singletonList(tableId),
                            SchemaChangeEventType.RENAME_COLUMN));
            expectedEvents.addAll(renameColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV3 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("sukoa", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV3);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV3);

            harness.clearOutputRecords();
        }

        // Test AlterColumnTypeEvent (should be ignored)
        {
            List<Event> alterColumnTypeEvents =
                    Arrays.asList(
                            new AlterColumnTypeEvent(
                                    tableId, ImmutableMap.of("sukoa", BIGINT, "age", FLOAT)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 8, STRING, "Helen", FLOAT, 22f, BIGINT, 100L,
                                            DOUBLE, 173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 9, STRING, "Iva", FLOAT, 23f, BIGINT, 97L, DOUBLE,
                                            160.)));

            processEvent(schemaOperator, alterColumnTypeEvents);

            List<Event> expectedEvents =
                    Arrays.asList(
                            new FlushEvent(
                                    0,
                                    Collections.singletonList(tableId),
                                    SchemaChangeEventType.ALTER_COLUMN_TYPE),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 8, STRING, "Helen", SMALLINT, null, INT, null,
                                            DOUBLE, 173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 9, STRING, "Iva", SMALLINT, null, INT, null,
                                            DOUBLE, 160.)));

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV4 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", FLOAT)
                            .physicalColumn("sukoa", BIGINT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();

            Schema schemaV4E =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("sukoa", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV4);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV4E);

            harness.clearOutputRecords();
        }

        // Test DropColumnEvent (should be ignored)
        {
            List<Event> dropColumnEvents =
                    Arrays.asList(
                            new DropColumnEvent(tableId, Arrays.asList("sukoa", "height")),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 12, STRING, "Jane", FLOAT, 11f)),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 13, STRING, "Kryo", FLOAT, 23f)));

            processEvent(schemaOperator, dropColumnEvents);

            FlushEvent result;
            result =
                    new FlushEvent(
                            0,
                            Collections.singletonList(tableId),
                            SchemaChangeEventType.DROP_COLUMN);
            List<Event> expectedEvents =
                    Arrays.asList(
                            result,
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 12, STRING, "Jane", SMALLINT, null, INT, null,
                                            DOUBLE, null)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 13, STRING, "Kryo", SMALLINT, null, INT, null,
                                            DOUBLE, null)));
            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(expectedEvents);

            Schema schemaV5 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", FLOAT)
                            .primaryKey("id")
                            .build();
            Schema schemaV5E =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("sukoa", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV5);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV5E);

            harness.clearOutputRecords();
        }
        harness.close();
    }

    /** Tests lenient schema change behavior. */
    @Test
    void testLenientSchemaEvolves() throws Exception {
        TableId tableId = CUSTOMERS_TABLE_ID;
        Schema schemaV1 =
                Schema.newBuilder()
                        .physicalColumn("id", INT)
                        .physicalColumn("name", STRING.notNull())
                        .physicalColumn("age", SMALLINT)
                        .primaryKey("id")
                        .build();

        SchemaChangeBehavior behavior = SchemaChangeBehavior.LENIENT;

        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(30), behavior);
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDurationAndBehavior(
                        schemaOperator, 17, Duration.ofSeconds(3), behavior);
        harness.open();

        // Test CreateTableEvent
        {
            List<Event> createAndInsertDataEvents =
                    Arrays.asList(
                            new CreateTableEvent(tableId, schemaV1),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 1, STRING, "Alice", SMALLINT, (short) 17)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 2, STRING, "Bob", SMALLINT, (short) 18)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(INT, 3, STRING, "Carol", SMALLINT, (short) 19)));

            processEvent(schemaOperator, createAndInsertDataEvents);

            FlushEvent result;
            TableId tableId1 = tableId;
            Event schemaChangeEvent = createAndInsertDataEvents.get(0);
            result =
                    new FlushEvent(
                            0,
                            Collections.singletonList(tableId),
                            SchemaChangeEventType.CREATE_TABLE);
            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(result), createAndInsertDataEvents));

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV1);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test AddColumnEvent
        {
            List<Event> addColumnEvents =
                    Arrays.asList(
                            new AddColumnEvent(
                                    tableId,
                                    Arrays.asList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "score", INT, "Score data")),
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "height", DOUBLE, "Height data")))),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            4,
                                            STRING,
                                            "Derrida",
                                            SMALLINT,
                                            (short) 20,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            5,
                                            STRING,
                                            "Eve",
                                            SMALLINT,
                                            (short) 21,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));
            processEvent(schemaOperator, addColumnEvents);

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.ADD_COLUMN)),
                                    addColumnEvents));

            Schema schemaV2 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("name", STRING.notNull())
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("score", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV2);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV2);

            harness.clearOutputRecords();
        }

        // Test RenameColumnEvent
        {
            List<Event> renameColumnEvents =
                    Arrays.asList(
                            new RenameColumnEvent(
                                    tableId, ImmutableMap.of("name", "namae", "age", "toshi")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            6,
                                            STRING,
                                            "Fiona",
                                            SMALLINT,
                                            (short) 22,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            7,
                                            STRING,
                                            "Gloria",
                                            SMALLINT,
                                            (short) 23,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.)));

            processEvent(schemaOperator, renameColumnEvents);

            List<Event> lenientRenameColumnEvents =
                    Arrays.asList(
                            new AddColumnEvent(
                                    tableId,
                                    Arrays.asList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn("namae", STRING, null)),
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "toshi", SMALLINT, null)))),
                            new AlterColumnTypeEvent(
                                    tableId,
                                    Collections.singletonMap("name", STRING),
                                    Collections.singletonMap("name", STRING.notNull())),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            6,
                                            STRING,
                                            null,
                                            SMALLINT,
                                            null,
                                            INT,
                                            100,
                                            DOUBLE,
                                            173.25,
                                            STRING,
                                            "Fiona",
                                            SMALLINT,
                                            (short) 22)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            7,
                                            STRING,
                                            null,
                                            SMALLINT,
                                            null,
                                            INT,
                                            97,
                                            DOUBLE,
                                            160.,
                                            STRING,
                                            "Gloria",
                                            SMALLINT,
                                            (short) 23)));

            FlushEvent result;
            int subTaskId = 0;
            TableId tableId1 = tableId;
            Event schemaChangeEvent = renameColumnEvents.get(0);
            result =
                    new FlushEvent(
                            subTaskId,
                            Collections.singletonList(tableId),
                            SchemaChangeEventType.RENAME_COLUMN);
            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(result), lenientRenameColumnEvents));

            Schema schemaV3 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING.notNull())
                            .physicalColumn("toshi", SMALLINT)
                            .physicalColumn("score", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();

            Schema schemaV3E =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("score", INT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .physicalColumn("namae", STRING)
                            .physicalColumn("toshi", SMALLINT)
                            .primaryKey("id")
                            .build();

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV3);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV3E);

            harness.clearOutputRecords();
        }

        // Test AlterColumnTypeEvent
        {
            List<Event> alterColumnTypeEvents =
                    Arrays.asList(
                            new AlterColumnTypeEvent(
                                    tableId,
                                    ImmutableMap.of("score", BIGINT, "toshi", FLOAT),
                                    ImmutableMap.of("score", INT, "toshi", SMALLINT)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 8, STRING, "Helen", FLOAT, 22f, BIGINT, 100L,
                                            DOUBLE, 173.25)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 9, STRING, "Iva", FLOAT, 23f, BIGINT, 97L, DOUBLE,
                                            160.)));

            processEvent(schemaOperator, alterColumnTypeEvents);

            List<Event> lenientAlterColumnTypeEvents =
                    Arrays.asList(
                            new AlterColumnTypeEvent(
                                    tableId,
                                    ImmutableMap.of("score", BIGINT, "toshi", FLOAT),
                                    ImmutableMap.of("score", INT, "toshi", SMALLINT)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 8, STRING, null, SMALLINT, null, BIGINT, 100L,
                                            DOUBLE, 173.25, STRING, "Helen", FLOAT, 22f)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 9, STRING, null, SMALLINT, null, BIGINT, 97L,
                                            DOUBLE, 160., STRING, "Iva", FLOAT, 23f)));

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.ALTER_COLUMN_TYPE)),
                                    lenientAlterColumnTypeEvents));

            Schema schemaV4 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING.notNull())
                            .physicalColumn("toshi", FLOAT)
                            .physicalColumn("score", BIGINT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .primaryKey("id")
                            .build();

            Schema schemaV4E =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("score", BIGINT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .physicalColumn("namae", STRING)
                            .physicalColumn("toshi", FLOAT)
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV4);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV4E);

            harness.clearOutputRecords();
        }

        // Test DropColumnEvent
        {
            List<Event> dropColumnEvents =
                    Arrays.asList(
                            new DropColumnEvent(tableId, Arrays.asList("score", "height")),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 12, STRING, "Jane", FLOAT, 11f)),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 13, STRING, "Kryo", FLOAT, 23f)));

            processEvent(schemaOperator, dropColumnEvents);

            List<Event> lenientDropColumnEvents =
                    Arrays.asList(
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 12, STRING, null, SMALLINT, null, BIGINT, null,
                                            DOUBLE, null, STRING, "Jane", FLOAT, 11f)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 13, STRING, null, SMALLINT, null, BIGINT, null,
                                            DOUBLE, null, STRING, "Kryo", FLOAT, 23f)));

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.DROP_COLUMN)),
                                    lenientDropColumnEvents));

            Schema schemaV5 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("namae", STRING.notNull())
                            .physicalColumn("toshi", FLOAT)
                            .primaryKey("id")
                            .build();

            Schema schemaV5E =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("score", BIGINT, "Score data")
                            .physicalColumn("height", DOUBLE, "Height data")
                            .physicalColumn("namae", STRING)
                            .physicalColumn("toshi", FLOAT)
                            .primaryKey("id")
                            .build();
            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV5);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV5E);

            harness.clearOutputRecords();
        }
        harness.close();
    }

    @Test
    void testLenientEvolveTweaks() throws Exception {
        TableId tableId = CUSTOMERS_TABLE_ID;
        Schema schemaV1 =
                Schema.newBuilder()
                        .physicalColumn("id", INT)
                        .physicalColumn("iina", INT.notNull())
                        .physicalColumn("name", STRING.notNull())
                        .physicalColumn("age", SMALLINT)
                        .primaryKey("id")
                        .build();

        SchemaChangeBehavior behavior = SchemaChangeBehavior.LENIENT;

        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(30), behavior);
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDurationAndBehavior(
                        schemaOperator, 17, Duration.ofSeconds(3), behavior);
        harness.open();

        // Test CreateTableEvent
        {
            List<Event> createAndInsertDataEvents =
                    Arrays.asList(
                            new CreateTableEvent(tableId, schemaV1),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 1, INT, 0, STRING, "Alice", SMALLINT, (short) 17)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 2, INT, 0, STRING, "Bob", SMALLINT, (short) 18)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            3,
                                            INT,
                                            0,
                                            STRING,
                                            "Carol",
                                            SMALLINT,
                                            (short) 19)));

            processEvent(schemaOperator, createAndInsertDataEvents);

            FlushEvent result;
            TableId tableId1 = tableId;
            Event schemaChangeEvent = createAndInsertDataEvents.get(0);
            result =
                    new FlushEvent(
                            0,
                            Collections.singletonList(tableId),
                            SchemaChangeEventType.CREATE_TABLE);
            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(result), createAndInsertDataEvents));

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV1);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV1);

            harness.clearOutputRecords();
        }

        // Test drop a non-null column
        {
            List<Event> dropColumnEvents =
                    Arrays.asList(
                            new DropColumnEvent(tableId, Collections.singletonList("name")),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 12, INT, 0, SMALLINT, (short) 11)),
                            DataChangeEvent.insertEvent(
                                    tableId, buildRecord(INT, 13, INT, 0, SMALLINT, (short) 23)));

            processEvent(schemaOperator, dropColumnEvents);

            List<Event> lenientDropColumnEvents =
                    Arrays.asList(
                            new AlterColumnTypeEvent(
                                    tableId,
                                    Collections.singletonMap("name", STRING),
                                    Collections.singletonMap("name", STRING.notNull())),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 12, INT, 0, STRING, null, SMALLINT, (short) 11)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT, 13, INT, 0, STRING, null, SMALLINT, (short) 23)));

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.DROP_COLUMN)),
                                    lenientDropColumnEvents));

            Schema schemaV2 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("iina", INT.notNull())
                            .physicalColumn("age", SMALLINT)
                            .primaryKey("id")
                            .build();

            Schema schemaV2E =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("iina", INT.notNull())
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .primaryKey("id")
                            .build();

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV2);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV2E);

            harness.clearOutputRecords();
        }

        // Test inserting non-null column and somewhere in the middle
        {
            List<Event> addColumnEvents =
                    Arrays.asList(
                            new AddColumnEvent(
                                    tableId,
                                    Arrays.asList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn("nickname", STRING),
                                                    AddColumnEvent.ColumnPosition.AFTER,
                                                    "id"),
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "extra", STRING.notNull())))),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            12,
                                            STRING,
                                            "Alice",
                                            INT,
                                            0,
                                            SMALLINT,
                                            (short) 11,
                                            STRING,
                                            "ailisi")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            13,
                                            STRING,
                                            "Bob",
                                            INT,
                                            0,
                                            SMALLINT,
                                            (short) 23,
                                            STRING,
                                            "baobo")));

            processEvent(schemaOperator, addColumnEvents);

            List<Event> lenientAddColumnEvents =
                    Arrays.asList(
                            new AddColumnEvent(
                                    tableId,
                                    Arrays.asList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn("nickname", STRING)),
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn("extra", STRING)))),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            12,
                                            INT,
                                            0,
                                            STRING,
                                            null,
                                            SMALLINT,
                                            (short) 11,
                                            STRING,
                                            "Alice",
                                            STRING,
                                            "ailisi")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            13,
                                            INT,
                                            0,
                                            STRING,
                                            null,
                                            SMALLINT,
                                            (short) 23,
                                            STRING,
                                            "Bob",
                                            STRING,
                                            "baobo")));

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.ADD_COLUMN)),
                                    lenientAddColumnEvents));

            Schema schemaV3 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("nickname", STRING)
                            .physicalColumn("iina", INT.notNull())
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("extra", STRING.notNull())
                            .primaryKey("id")
                            .build();

            Schema schemaV3E =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("iina", INT.notNull())
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("nickname", STRING)
                            .physicalColumn("extra", STRING)
                            .primaryKey("id")
                            .build();

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV3);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV3E);

            harness.clearOutputRecords();
        }

        // Test renaming a non-null column
        {
            List<Event> renameColumnEvents =
                    Arrays.asList(
                            new RenameColumnEvent(
                                    tableId, Collections.singletonMap("iina", "yina")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            41,
                                            STRING,
                                            "Carol",
                                            INT,
                                            0,
                                            SMALLINT,
                                            (short) 11,
                                            STRING,
                                            "kaluo")),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            42,
                                            STRING,
                                            "Dorothy",
                                            INT,
                                            0,
                                            SMALLINT,
                                            (short) 11,
                                            STRING,
                                            "duoluoxi")));

            processEvent(schemaOperator, renameColumnEvents);

            harness.getLatestEvolvedSchema(tableId);
            List<Event> lenientRenameColumnEvents =
                    Arrays.asList(
                            new AddColumnEvent(
                                    tableId,
                                    Collections.singletonList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn("yina", INT)))),
                            new AlterColumnTypeEvent(
                                    tableId,
                                    Collections.singletonMap("iina", INT),
                                    Collections.singletonMap("iina", INT.notNull())),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            41,
                                            INT,
                                            null,
                                            STRING,
                                            null,
                                            SMALLINT,
                                            (short) 11,
                                            STRING,
                                            "Carol",
                                            STRING,
                                            "kaluo",
                                            INT,
                                            0)),
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    buildRecord(
                                            INT,
                                            42,
                                            INT,
                                            null,
                                            STRING,
                                            null,
                                            SMALLINT,
                                            (short) 11,
                                            STRING,
                                            "Dorothy",
                                            STRING,
                                            "duoluoxi",
                                            INT,
                                            0)));

            Assertions.assertThat(
                            harness.getOutputRecords().stream()
                                    .map(StreamRecord::getValue)
                                    .collect(Collectors.toList()))
                    .isEqualTo(
                            ListUtils.union(
                                    Collections.singletonList(
                                            new FlushEvent(
                                                    0,
                                                    Collections.singletonList(tableId),
                                                    SchemaChangeEventType.RENAME_COLUMN)),
                                    lenientRenameColumnEvents));

            Schema schemaV4 =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("nickname", STRING)
                            .physicalColumn("yina", INT.notNull())
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("extra", STRING.notNull())
                            .primaryKey("id")
                            .build();

            Schema schemaV4E =
                    Schema.newBuilder()
                            .physicalColumn("id", INT)
                            .physicalColumn("iina", INT)
                            .physicalColumn("name", STRING)
                            .physicalColumn("age", SMALLINT)
                            .physicalColumn("nickname", STRING)
                            .physicalColumn("extra", STRING)
                            .physicalColumn("yina", INT)
                            .primaryKey("id")
                            .build();

            Assertions.assertThat(harness.getLatestOriginalSchema(tableId)).isEqualTo(schemaV4);
            Assertions.assertThat(harness.getLatestEvolvedSchema(tableId)).isEqualTo(schemaV4E);

            harness.clearOutputRecords();
        }
    }

    private RecordData buildRecord(final Object... args) {
        List<DataType> dataTypes = new ArrayList<>();
        List<Object> objects = new ArrayList<>();
        for (int i = 0; i < args.length; i += 2) {
            DataType dataType = (DataType) args[i];
            Object object = args[i + 1];
            dataTypes.add(dataType);
            if (dataType.equals(STRING)) {
                objects.add(BinaryStringData.fromString((String) object));
            } else {
                objects.add(object);
            }
        }
        return new BinaryRecordDataGenerator(RowType.of(dataTypes.toArray(new DataType[0])))
                .generate(objects.toArray());
    }

    private void processEvent(SchemaOperator operator, List<Event> events) throws Exception {
        for (Event event : events) {
            operator.processElement(new StreamRecord<>(event));
        }
    }
}
