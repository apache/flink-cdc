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
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Unit tests for the {@link PreTransformOperator} and {@link PostTransformOperator} handling schema
 * evolution events.
 */
public class TransformOperatorWithSchemaEvolveTest {

    /** Defines a unified transform test cases. */
    static class TransformWithSchemaEvolveTestCase {

        private static final Logger LOG =
                LoggerFactory.getLogger(TransformWithSchemaEvolveTestCase.class);

        private final TableId tableId;
        private final String projectionExpression;
        private final String filterExpression;

        private Schema sourceSchema;
        private Schema preTransformedSchema;
        private Schema postTransformedSchema;

        private final List<Event> sourceEvents;
        private final List<Event> preTransformedEvents;
        private final List<Event> postTransformedEvents;

        private PreTransformOperator preTransformOperator;
        private PostTransformOperator postTransformOperator;

        private RegularEventOperatorTestHarness<PreTransformOperator, Event>
                preTransformOperatorHarness;
        private RegularEventOperatorTestHarness<PostTransformOperator, Event>
                postTransformOperatorHarness;

        public static TransformWithSchemaEvolveTestCase of(
                TableId tableId,
                String projectionExpression,
                String filterExpression,
                Schema sourceSchema,
                Schema preTransformedSchema,
                Schema postTransformedSchema) {
            return new TransformWithSchemaEvolveTestCase(
                    tableId,
                    projectionExpression,
                    filterExpression,
                    sourceSchema,
                    preTransformedSchema,
                    postTransformedSchema);
        }

        private Object[] stringify(Object... objects) {
            return Arrays.stream(objects)
                    .map(o -> o instanceof String ? new BinaryStringData((String) o) : o)
                    .toArray();
        }

        public TransformWithSchemaEvolveTestCase evolveFromSource(SchemaChangeEvent event) {
            sourceEvents.add(event);
            return this;
        }

        public TransformWithSchemaEvolveTestCase expectNothingInPreTransformed() {
            preTransformedEvents.add(null);
            return this;
        }

        public TransformWithSchemaEvolveTestCase expectInPreTransformed(SchemaChangeEvent event) {
            preTransformedEvents.add(event);
            return this;
        }

        public TransformWithSchemaEvolveTestCase expectNothingInPostTransformed() {
            postTransformedEvents.add(null);
            return this;
        }

        public TransformWithSchemaEvolveTestCase expectInPostTransformed(SchemaChangeEvent event) {
            postTransformedEvents.add(event);
            return this;
        }

        private TransformWithSchemaEvolveTestCase(
                TableId tableId,
                String projectionExpression,
                String filterExpression,
                Schema sourceSchema,
                Schema preTransformedSchema,
                Schema postTransformedSchema) {
            this.tableId = tableId;
            this.projectionExpression = projectionExpression;
            this.filterExpression = filterExpression;

            this.sourceSchema = sourceSchema;
            this.preTransformedSchema = preTransformedSchema;
            this.postTransformedSchema = postTransformedSchema;

            this.sourceEvents = new ArrayList<>();
            this.preTransformedEvents = new ArrayList<>();
            this.postTransformedEvents = new ArrayList<>();

            this.sourceEvents.add(new CreateTableEvent(tableId, sourceSchema));
            this.preTransformedEvents.add(new CreateTableEvent(tableId, preTransformedSchema));
            this.postTransformedEvents.add(new CreateTableEvent(tableId, postTransformedSchema));
        }

        private TransformWithSchemaEvolveTestCase initializeHarness() throws Exception {
            preTransformOperator =
                    PreTransformOperator.newBuilder()
                            .addTransform(
                                    tableId.identifier(), projectionExpression, filterExpression)
                            .build();
            preTransformOperatorHarness =
                    RegularEventOperatorTestHarness.with(preTransformOperator, 1);
            preTransformOperatorHarness.open();

            postTransformOperator =
                    PostTransformOperator.newBuilder()
                            .addTransform(
                                    tableId.identifier(), projectionExpression, filterExpression)
                            .build();
            postTransformOperatorHarness =
                    RegularEventOperatorTestHarness.with(postTransformOperator, 1);
            postTransformOperatorHarness.open();
            return this;
        }

        private void destroyHarness() throws Exception {
            if (preTransformOperatorHarness != null) {
                preTransformOperatorHarness.close();
            }
            if (postTransformOperatorHarness != null) {
                postTransformOperatorHarness.close();
            }
        }

        public TransformWithSchemaEvolveTestCase runTests(String comment) throws Exception {
            LOG.info("Running {}#{}", getClass().getSimpleName(), comment);
            for (int i = 0; i < sourceEvents.size(); i++) {
                Event sourceEvent = sourceEvents.get(i);

                preTransformOperator.processElement(new StreamRecord<>(sourceEvent));

                Event expectedPreTransformEvent = preTransformedEvents.get(i);
                Event actualPreTransformEvent =
                        Optional.ofNullable(preTransformOperatorHarness.getOutputRecords().poll())
                                .map(StreamRecord::getValue)
                                .orElse(null);

                Assertions.assertThat(actualPreTransformEvent).isEqualTo(expectedPreTransformEvent);

                postTransformOperator.processElement(
                        new StreamRecord<>(preTransformedEvents.get(i)));
                Event expectedPostTransformEvent = postTransformedEvents.get(i);
                Event actualPostTransformEvent =
                        Optional.ofNullable(postTransformOperatorHarness.getOutputRecords().poll())
                                .map(StreamRecord::getValue)
                                .orElse(null);
                Assertions.assertThat(actualPostTransformEvent)
                        .isEqualTo(expectedPostTransformEvent);
            }

            sourceEvents.clear();
            preTransformedEvents.clear();
            postTransformedEvents.clear();
            return this;
        }

        public TransformWithSchemaEvolveTestCase runTestsAndExpect(
                String comment, Class<? extends Throwable> exceptionClass, String message) {
            try {
                Assertions.assertThatThrownBy(() -> runTests(comment))
                        .isInstanceOf(exceptionClass)
                        .hasMessageContaining(message);
            } finally {
                sourceEvents.clear();
                preTransformedEvents.clear();
                postTransformedEvents.clear();
            }
            return this;
        }
    }

    /** This case tests when schema evolution happens with unspecified columns. */
    @Test
    public void testIrrelevantSchemaChangeInExplicitTransformRules() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "data_changes");
        TransformWithSchemaEvolveTestCase.of(
                        tableId,
                        "id, age, id + age as computed",
                        "id > 100",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("age", DataTypes.INT())
                                .physicalColumn("computed", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .runTests("initializing table")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("extras", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.LAST,
                                                null))))
                .expectNothingInPreTransformed()
                .expectNothingInPostTransformed()
                .runTests("inserting unspecified columns")
                .evolveFromSource(
                        new RenameColumnEvent(tableId, Collections.singletonMap("name", "namae")))
                .expectNothingInPreTransformed()
                .expectNothingInPostTransformed()
                .runTests("renaming unspecified columns")
                .evolveFromSource(
                        new AlterColumnTypeEvent(
                                tableId, Collections.singletonMap("extras", DataTypes.DOUBLE())))
                .expectNothingInPreTransformed()
                .expectNothingInPostTransformed()
                .runTests("altering unspecified columns' type")
                .evolveFromSource(new DropColumnEvent(tableId, Arrays.asList("namae", "extras")))
                .expectNothingInPreTransformed()
                .expectNothingInPostTransformed()
                .runTests("dropping unspecified columns")
                .destroyHarness();
    }

    /** This case tests when schema evolution happens with referenced-only columns. */
    @Test
    public void testSemiRelevantSchemaChangeInExplicitTransformRules() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "data_changes");
        TransformWithSchemaEvolveTestCase.of(
                        tableId,
                        "id, age, id + age as computed",
                        "name > 100",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("age", DataTypes.INT())
                                .physicalColumn("computed", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .runTests("initializing table")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("extras", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.LAST,
                                                null))))
                .expectNothingInPreTransformed()
                .expectNothingInPostTransformed()
                .runTests("inserting unspecified columns")
                .evolveFromSource(
                        new AlterColumnTypeEvent(
                                tableId, Collections.singletonMap("extras", DataTypes.DOUBLE())))
                .expectNothingInPreTransformed()
                .expectNothingInPostTransformed()
                .runTests("altering unspecified columns' type")
                .evolveFromSource(
                        new AlterColumnTypeEvent(
                                tableId, Collections.singletonMap("name", DataTypes.VARCHAR(17))))
                .expectInPreTransformed(
                        new AlterColumnTypeEvent(
                                tableId, Collections.singletonMap("name", DataTypes.VARCHAR(17))))
                .expectNothingInPostTransformed()
                .runTests("altering referenced columns' type")
                .evolveFromSource(new DropColumnEvent(tableId, Collections.singletonList("extras")))
                .expectNothingInPreTransformed()
                .expectNothingInPostTransformed()
                .runTests("dropping unspecified columns")
                .destroyHarness();
    }

    /** This case tests when schema evolution happens with explicitly-written columns. */
    @Test
    public void testRelevantColumnSchemaInExplicitTransformRules() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "data_changes");
        TransformWithSchemaEvolveTestCase.of(
                        tableId,
                        "id, age, name, id + age as computed",
                        "name <> 'Alice'",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("age", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("computed", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .runTests("initializing table")
                .evolveFromSource(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .expectInPreTransformed(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .expectInPostTransformed(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .runTests("schema evolution on relevant columns");
    }

    /** This case tests when schema evolution happens with a wildcard character at first. */
    @Test
    public void testSchemaChangeWithPreWildcard() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "data_changes");
        TransformWithSchemaEvolveTestCase.of(
                        tableId,
                        "*, id + age as computed",
                        "name <> 'Alice'",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .physicalColumn("computed", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .runTests("initializing table")
                .evolveFromSource(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .expectInPreTransformed(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .expectInPostTransformed(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .runTests("schema evolution on relevant columns")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_first", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.FIRST,
                                                null))))
                .expectInPreTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_first", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.BEFORE,
                                                "id"))))
                .expectInPostTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_first", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.BEFORE,
                                                "id"))))
                .runTests("inserting columns at first")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_last", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.LAST,
                                                null))))
                .expectInPreTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_last", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "age"))))
                .expectInPostTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_last", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "age"))))
                .runTests("inserting columns at last")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_middle", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "name"))))
                .expectInPreTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_middle", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "name"))))
                .expectInPostTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_middle", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "name"))))
                .runTests("inserting columns at last");
    }

    /** This case tests when schema evolution happens with a wildcard character in the middle. */
    @Test
    public void testSchemaChangeWithMidWildcard() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "data_changes");
        TransformWithSchemaEvolveTestCase.of(
                        tableId,
                        "id + age as computed1, *, id * age as computed2",
                        "name <> 'Alice'",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("computed1", DataTypes.INT())
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .physicalColumn("computed2", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .runTests("initializing table")
                .evolveFromSource(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .expectInPreTransformed(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .expectInPostTransformed(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .runTests("schema evolution on relevant columns")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_first", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.FIRST,
                                                null))))
                .expectInPreTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_first", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.BEFORE,
                                                "id"))))
                .expectInPostTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_first", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.BEFORE,
                                                "id"))))
                .runTests("inserting columns at first")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_last", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.LAST,
                                                null))))
                .expectInPreTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_last", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "age"))))
                .expectInPostTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_last", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "age"))))
                .runTests("inserting columns at last")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_middle", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "name"))))
                .expectInPreTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_middle", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "name"))))
                .expectInPostTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_middle", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "name"))))
                .runTests("inserting columns at last");
    }

    /** This case tests when schema evolution happens with a wildcard character at last. */
    @Test
    public void testSchemaChangeWithPostWildcard() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "data_changes");
        TransformWithSchemaEvolveTestCase.of(
                        tableId,
                        "id + age as computed, *",
                        "name <> 'Alice'",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("computed", DataTypes.INT())
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .runTests("initializing table")
                .evolveFromSource(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .expectInPreTransformed(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .expectInPostTransformed(
                        new AlterColumnTypeEvent(
                                tableId,
                                ImmutableMap.of(
                                        "name", DataTypes.VARCHAR(17),
                                        "age", DataTypes.DOUBLE())))
                .runTests("schema evolution on relevant columns")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_first", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.FIRST,
                                                null))))
                .expectInPreTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_first", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.BEFORE,
                                                "id"))))
                .expectInPostTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_first", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.BEFORE,
                                                "id"))))
                .runTests("inserting columns at first")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_last", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.LAST,
                                                null))))
                .expectInPreTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_last", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "age"))))
                .expectInPostTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_last", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "age"))))
                .runTests("inserting columns at last")
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_middle", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "name"))))
                .expectInPreTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_middle", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "name"))))
                .expectInPostTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn(
                                                        "extras_middle", DataTypes.FLOAT()),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "name"))))
                .runTests("inserting columns at last");
    }
}
