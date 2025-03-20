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

package org.apache.flink.cdc.runtime.operators.schema.distributed;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaTestBase;
import org.apache.flink.cdc.runtime.testutils.operators.DistributedEventOperatorTestHarness;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.BiConsumerWithException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Supplier;

/** Unit test cases for {@link SchemaOperator}. */
public class SchemaEvolveTest extends SchemaTestBase {
    private static final TableId TABLE_ID = TableId.parse("foo.bar.baz");
    private static final Schema INITIAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("name", DataTypes.VARCHAR(128))
                    .physicalColumn("age", DataTypes.FLOAT())
                    .physicalColumn("notes", DataTypes.STRING().notNull())
                    .build();

    @Test
    void testLenientSchemaEvolution() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "id")));
        AddColumnEvent addColumnEventAtLast =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.LAST,
                                        null)));

        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("notes", "footnotes"));
        AddColumnEvent appendRenamedColumnAtLast =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("footnotes", DataTypes.STRING()),
                                        AddColumnEvent.ColumnPosition.LAST,
                                        null)));
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(
                        TABLE_ID, Collections.singletonMap("age", DataTypes.DOUBLE()));
        AlterColumnTypeEvent alterColumnTypeEventWithBackfill =
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("age", DataTypes.DOUBLE()),
                        Collections.singletonMap("age", DataTypes.FLOAT()));

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(TABLE_ID, Collections.singletonList("footnotes"));
        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(TABLE_ID);
        DropTableEvent dropTableEvent = new DropTableEvent(TABLE_ID);

        Assertions.assertThat(
                        runInHarness(
                                () ->
                                        new SchemaOperator(
                                                ROUTING_RULES,
                                                Duration.ofMinutes(3),
                                                SchemaChangeBehavior.LENIENT,
                                                "UTC"),
                                (op) ->
                                        new DistributedEventOperatorTestHarness<>(
                                                op,
                                                20,
                                                Duration.ofSeconds(3),
                                                Duration.ofMinutes(3)),
                                (operator, harness) -> {

                                    // Create a Table
                                    operator.processElement(wrap(createTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "ISFS", 1, "Alice", 17.1828f,
                                                            "Hello")));

                                    // Add a Column
                                    operator.processElement(wrap(addColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSFS",
                                                            2,
                                                            false,
                                                            "Bob",
                                                            31.415926f,
                                                            "Bye-bye")));

                                    // Rename a Column
                                    operator.processElement(wrap(renameColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSFS", 3, true, "Cicada",
                                                            123.456f, "Ok")));

                                    // Alter a Column's Type
                                    operator.processElement(wrap(alterColumnTypeEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSDS",
                                                            4,
                                                            false,
                                                            "Derrida",
                                                            7.81876754837,
                                                            "Nah")));

                                    // Drop a column
                                    operator.processElement(wrap(dropColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 5, true, "Eve",
                                                            1.414)));

                                    // Truncate a table
                                    operator.processElement(wrap(truncateTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 6, false, "Ferris",
                                                            0.001)));

                                    // Drop a table
                                    operator.processElement(wrap(dropTableEvent));
                                }))
                .map(StreamRecord::getValue)
                .containsExactly(
                        new FlushEvent(
                                0, Collections.singletonList(TABLE_ID), createTableEvent.getType()),
                        createTableEvent,
                        genInsert(TABLE_ID, "ISFS", 1, "Alice", 17.1828f, "Hello"),
                        new FlushEvent(
                                0, Collections.singletonList(TABLE_ID), addColumnEvent.getType()),
                        addColumnEventAtLast,
                        genInsert(TABLE_ID, "ISFSB", 2, "Bob", 31.415926f, "Bye-bye", false),
                        new FlushEvent(
                                0,
                                Collections.singletonList(TABLE_ID),
                                renameColumnEvent.getType()),
                        appendRenamedColumnAtLast,
                        genInsert(TABLE_ID, "ISFSBS", 3, "Cicada", 123.456f, null, true, "Ok"),
                        new FlushEvent(
                                0,
                                Collections.singletonList(TABLE_ID),
                                alterColumnTypeEvent.getType()),
                        alterColumnTypeEventWithBackfill,
                        genInsert(
                                TABLE_ID,
                                "ISDSBS",
                                4,
                                "Derrida",
                                7.81876754837,
                                null,
                                false,
                                "Nah"),
                        new FlushEvent(
                                0, Collections.singletonList(TABLE_ID), dropColumnEvent.getType()),
                        genInsert(TABLE_ID, "ISDSBS", 5, "Eve", 1.414, null, true, null),
                        new FlushEvent(
                                0,
                                Collections.singletonList(TABLE_ID),
                                truncateTableEvent.getType()),
                        genInsert(TABLE_ID, "ISDSBS", 6, "Ferris", 0.001, null, false, null),
                        new FlushEvent(
                                0, Collections.singletonList(TABLE_ID), dropTableEvent.getType()));
    }

    @Test
    void testIgnoreSchemaEvolution() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "id")));
        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("notes", "footnotes"));
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(
                        TABLE_ID, Collections.singletonMap("age", DataTypes.DOUBLE()));
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(TABLE_ID, Collections.singletonList("footnotes"));
        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(TABLE_ID);
        DropTableEvent dropTableEvent = new DropTableEvent(TABLE_ID);

        Assertions.assertThat(
                        runInHarness(
                                () ->
                                        new SchemaOperator(
                                                ROUTING_RULES,
                                                Duration.ofMinutes(3),
                                                SchemaChangeBehavior.IGNORE,
                                                "UTC"),
                                (op) ->
                                        new DistributedEventOperatorTestHarness<>(
                                                op,
                                                20,
                                                Duration.ofSeconds(3),
                                                Duration.ofMinutes(3)),
                                (operator, harness) -> {

                                    // Create a Table
                                    operator.processElement(wrap(createTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "ISFS", 1, "Alice", 17.1828f,
                                                            "Hello")));

                                    // Add a Column
                                    operator.processElement(wrap(addColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSFS",
                                                            2,
                                                            false,
                                                            "Bob",
                                                            31.415926f,
                                                            "Bye-bye")));

                                    // Rename a Column
                                    operator.processElement(wrap(renameColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSFS", 3, true, "Cicada",
                                                            123.456f, "Ok")));

                                    // Alter a Column's Type
                                    operator.processElement(wrap(alterColumnTypeEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSDS",
                                                            4,
                                                            false,
                                                            "Derrida",
                                                            7.81876754837,
                                                            "Nah")));

                                    // Drop a column
                                    operator.processElement(wrap(dropColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 5, true, "Eve",
                                                            1.414)));

                                    // Truncate a table
                                    operator.processElement(wrap(truncateTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 6, false, "Ferris",
                                                            0.001)));

                                    // Drop a table
                                    operator.processElement(wrap(dropTableEvent));
                                }))
                .map(StreamRecord::getValue)
                .containsExactly(
                        new FlushEvent(
                                0, Collections.singletonList(TABLE_ID), createTableEvent.getType()),
                        createTableEvent,
                        genInsert(TABLE_ID, "ISFS", 1, "Alice", 17.1828f, "Hello"),
                        genInsert(TABLE_ID, "ISFS", 2, "Bob", 31.415926f, "Bye-bye"),
                        genInsert(TABLE_ID, "ISFS", 3, "Cicada", 123.456f, null),
                        genInsert(TABLE_ID, "ISFS", 4, "Derrida", null, null),
                        genInsert(TABLE_ID, "ISFS", 5, "Eve", null, null),
                        genInsert(TABLE_ID, "ISFS", 6, "Ferris", null, null));
    }

    @Test
    void testExceptionSchemaEvolution() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "id")));
        Assertions.assertThatThrownBy(
                        () ->
                                runInHarness(
                                        () ->
                                                new SchemaOperator(
                                                        ROUTING_RULES,
                                                        Duration.ofMinutes(3),
                                                        SchemaChangeBehavior.EXCEPTION,
                                                        "UTC"),
                                        (op) ->
                                                new DistributedEventOperatorTestHarness<>(
                                                        op,
                                                        20,
                                                        Duration.ofSeconds(3),
                                                        Duration.ofMinutes(3)),
                                        (operator, harness) -> {

                                            // Create a Table
                                            operator.processElement(wrap(createTableEvent));
                                            operator.processElement(
                                                    wrap(
                                                            genInsert(
                                                                    TABLE_ID, "ISFS", 1, "Alice",
                                                                    17.1828f, "Hello")));

                                            // Add a Column (should fail immediately)
                                            operator.processElement(wrap(addColumnEvent));
                                        }))
                .isExactlyInstanceOf(SchemaEvolveException.class)
                .extracting("applyingEvent", "exceptionMessage")
                .containsExactly(
                        addColumnEvent,
                        "Unexpected schema change events occurred in EXCEPTION mode. Job will fail now.");
    }

    protected static <OP extends AbstractStreamOperator<E>, E extends Event, T extends Throwable>
            LinkedList<StreamRecord<E>> runInHarness(
                    Supplier<OP> opCreator,
                    Function<OP, DistributedEventOperatorTestHarness<OP, E>> harnessCreator,
                    BiConsumerWithException<OP, DistributedEventOperatorTestHarness<OP, E>, T>
                            closure)
                    throws T, Exception {
        OP operator = opCreator.get();
        try (DistributedEventOperatorTestHarness<OP, E> harness = harnessCreator.apply(operator)) {
            harness.open();
            closure.accept(operator, harness);
            return harness.getOutputRecords();
        }
    }
}
