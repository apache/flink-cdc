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
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Unit tests for the {@link PreTransformOperator} and {@link PostTransformOperator} handling schema
 * evolution events.
 */
class TransformOperatorWithSchemaEvolveTest {

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

        private BinaryRecordDataGenerator sourceRecordGenerator;
        private BinaryRecordDataGenerator preTransformedRecordGenerator;
        private BinaryRecordDataGenerator postTransformedRecordGenerator;

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
            sourceSchema = SchemaUtils.applySchemaChangeEvent(sourceSchema, event);
            sourceRecordGenerator =
                    new BinaryRecordDataGenerator((RowType) sourceSchema.toRowDataType());
            return this;
        }

        public TransformWithSchemaEvolveTestCase insertSource(Object... record) {
            sourceEvents.add(
                    DataChangeEvent.insertEvent(
                            tableId, sourceRecordGenerator.generate(stringify(record))));
            return this;
        }

        public TransformWithSchemaEvolveTestCase expectNothingInPreTransformed() {
            preTransformedEvents.add(null);
            return this;
        }

        public TransformWithSchemaEvolveTestCase expectInPreTransformed(SchemaChangeEvent event) {
            preTransformedEvents.add(event);
            preTransformedSchema = SchemaUtils.applySchemaChangeEvent(preTransformedSchema, event);
            preTransformedRecordGenerator =
                    new BinaryRecordDataGenerator((RowType) preTransformedSchema.toRowDataType());
            return this;
        }

        public TransformWithSchemaEvolveTestCase expectInPreTransformed(Object... record) {
            preTransformedEvents.add(
                    DataChangeEvent.insertEvent(
                            tableId, preTransformedRecordGenerator.generate(stringify(record))));
            return this;
        }

        public TransformWithSchemaEvolveTestCase expectNothingInPostTransformed() {
            postTransformedEvents.add(null);
            return this;
        }

        public TransformWithSchemaEvolveTestCase expectInPostTransformed(SchemaChangeEvent event) {
            postTransformedEvents.add(event);
            postTransformedSchema =
                    SchemaUtils.applySchemaChangeEvent(postTransformedSchema, event);
            postTransformedRecordGenerator =
                    new BinaryRecordDataGenerator((RowType) postTransformedSchema.toRowDataType());
            return this;
        }

        public TransformWithSchemaEvolveTestCase expectInPostTransformed(Object... event) {
            postTransformedEvents.add(
                    DataChangeEvent.insertEvent(
                            tableId, postTransformedRecordGenerator.generate(stringify(event))));
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

            this.sourceRecordGenerator =
                    new BinaryRecordDataGenerator((RowType) sourceSchema.toRowDataType());
            this.preTransformedRecordGenerator =
                    new BinaryRecordDataGenerator((RowType) preTransformedSchema.toRowDataType());
            this.postTransformedRecordGenerator =
                    new BinaryRecordDataGenerator((RowType) postTransformedSchema.toRowDataType());

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
    void testIrrelevantSchemaChangeInExplicitTransformRules() throws Exception {
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
    void testSemiRelevantSchemaChangeInExplicitTransformRules() throws Exception {
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
    void testRelevantColumnSchemaInExplicitTransformRules() throws Exception {
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
    void testSchemaChangeWithPreWildcard() throws Exception {
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
    void testSchemaChangeWithMidWildcard() throws Exception {
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
    void testSchemaChangeWithPostWildcard() throws Exception {
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

    /** This case tests column name map when schema evolution happens. */
    @Test
    public void testSchemaChangeWithColumnNameMap() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "data_changes");
        TransformWithSchemaEvolveTestCase.of(
                        tableId,
                        "*, foo-bar as computed",
                        "class <> 'class0'",
                        Schema.newBuilder()
                                .physicalColumn("foo", DataTypes.INT())
                                .physicalColumn("bar", DataTypes.INT())
                                .physicalColumn("foo-bar", DataTypes.INT())
                                .physicalColumn("class", DataTypes.STRING())
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("foo", DataTypes.INT())
                                .physicalColumn("bar", DataTypes.INT())
                                .physicalColumn("foo-bar", DataTypes.INT())
                                .physicalColumn("class", DataTypes.STRING())
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("foo", DataTypes.INT())
                                .physicalColumn("bar", DataTypes.INT())
                                .physicalColumn("foo-bar", DataTypes.INT())
                                .physicalColumn("class", DataTypes.STRING())
                                .physicalColumn("computed", DataTypes.INT())
                                .build())
                .initializeHarness()
                .runTests("initializing table")
                .insertSource(0, 0, 0, "class0")
                .expectInPreTransformed(0, 0, 0, "class0")
                .expectNothingInPostTransformed()
                .insertSource(1, 2, 3, "class1")
                .expectInPreTransformed(1, 2, 3, "class1")
                .expectInPostTransformed(1, 2, 3, "class1", -1)
                .evolveFromSource(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("bar-foo", DataTypes.INT()),
                                                AddColumnEvent.ColumnPosition.FIRST,
                                                null))))
                .expectInPreTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("bar-foo", DataTypes.INT()),
                                                AddColumnEvent.ColumnPosition.BEFORE,
                                                "foo"))))
                .expectInPostTransformed(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("bar-foo", DataTypes.INT()),
                                                AddColumnEvent.ColumnPosition.BEFORE,
                                                "foo"))))
                .insertSource(10, 2, 4, 6, "class2")
                .expectInPreTransformed(10, 2, 4, 6, "class2")
                .expectInPostTransformed(10, 2, 4, 6, "class2", -2)
                .insertSource(20, 2, 4, 6, "class0")
                .expectInPreTransformed(20, 2, 4, 6, "class0")
                .expectNothingInPostTransformed()
                .evolveFromSource(
                        new RenameColumnEvent(
                                tableId, Collections.singletonMap("bar-foo", "package")))
                .expectInPreTransformed(
                        new RenameColumnEvent(
                                tableId, Collections.singletonMap("bar-foo", "package")))
                .expectInPostTransformed(
                        new RenameColumnEvent(
                                tableId, Collections.singletonMap("bar-foo", "package")))
                .insertSource(30, 3, 6, 9, "class3")
                .expectInPreTransformed(30, 3, 6, 9, "class3")
                .expectInPostTransformed(30, 3, 6, 9, "class3", -3)
                .insertSource(40, 3, 6, 9, "class0")
                .expectInPreTransformed(40, 3, 6, 9, "class0")
                .expectNothingInPostTransformed()
                .runTests("schema evolution with mapped column names");
    }

    // ========================================================================================
    // Tests for generateSchemaChangeEvents and pending schema changes (FLINK-38828)
    // ========================================================================================

    /** Helper to invoke the private {@code generateSchemaChangeEvents} method via reflection. */
    @SuppressWarnings("unchecked")
    private static List<SchemaChangeEvent> invokeGenerateSchemaChangeEvents(
            PostTransformOperator operator, TableId tableId, Schema oldSchema, Schema newSchema)
            throws Exception {
        Method method =
                PostTransformOperator.class.getDeclaredMethod(
                        "generateSchemaChangeEvents", TableId.class, Schema.class, Schema.class);
        method.setAccessible(true);
        return (List<SchemaChangeEvent>) method.invoke(operator, tableId, oldSchema, newSchema);
    }

    /** Tests that generateSchemaChangeEvents produces correct AddColumnEvent. */
    @Test
    void testGenerateSchemaChangeEventsAddColumn() throws Exception {
        TableId tableId = TableId.tableId("ns", "sch", "tbl");
        PostTransformOperator operator =
                PostTransformOperator.newBuilder()
                        .addTransform(tableId.identifier(), "id, name", null)
                        .build();

        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Schema newSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        List<SchemaChangeEvent> events =
                invokeGenerateSchemaChangeEvents(operator, tableId, oldSchema, newSchema);

        Assertions.assertThat(events).hasSize(1);
        Assertions.assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);

        AddColumnEvent addEvent = (AddColumnEvent) events.get(0);
        Assertions.assertThat(addEvent.tableId()).isEqualTo(tableId);
        Assertions.assertThat(addEvent.getAddedColumns()).hasSize(1);

        AddColumnEvent.ColumnWithPosition colWithPos = addEvent.getAddedColumns().get(0);
        Assertions.assertThat(colWithPos.getAddColumn().getName()).isEqualTo("age");
        Assertions.assertThat(colWithPos.getAddColumn().getType()).isEqualTo(DataTypes.INT());
        // "age" is the last column, so it should be positioned LAST
        Assertions.assertThat(colWithPos.getPosition())
                .isEqualTo(AddColumnEvent.ColumnPosition.LAST);
    }

    /**
     * Tests that generateSchemaChangeEvents produces correct AddColumnEvent with AFTER position.
     */
    @Test
    void testGenerateSchemaChangeEventsAddColumnAfter() throws Exception {
        TableId tableId = TableId.tableId("ns", "sch", "tbl");
        PostTransformOperator operator =
                PostTransformOperator.newBuilder()
                        .addTransform(tableId.identifier(), "id, name", null)
                        .build();

        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("extra", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();
        // Insert "age" between "name" and "extra"
        Schema newSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .physicalColumn("extra", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();

        List<SchemaChangeEvent> events =
                invokeGenerateSchemaChangeEvents(operator, tableId, oldSchema, newSchema);

        Assertions.assertThat(events).hasSize(1);
        Assertions.assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);

        AddColumnEvent addEvent = (AddColumnEvent) events.get(0);
        AddColumnEvent.ColumnWithPosition colWithPos = addEvent.getAddedColumns().get(0);
        Assertions.assertThat(colWithPos.getAddColumn().getName()).isEqualTo("age");
        Assertions.assertThat(colWithPos.getPosition())
                .isEqualTo(AddColumnEvent.ColumnPosition.AFTER);
        Assertions.assertThat(colWithPos.getExistedColumnName()).isEqualTo("name");
    }

    /** Tests that generateSchemaChangeEvents produces correct DropColumnEvent. */
    @Test
    void testGenerateSchemaChangeEventsDropColumn() throws Exception {
        TableId tableId = TableId.tableId("ns", "sch", "tbl");
        PostTransformOperator operator =
                PostTransformOperator.newBuilder()
                        .addTransform(tableId.identifier(), "id, name", null)
                        .build();

        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        Schema newSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        List<SchemaChangeEvent> events =
                invokeGenerateSchemaChangeEvents(operator, tableId, oldSchema, newSchema);

        Assertions.assertThat(events).hasSize(1);
        Assertions.assertThat(events.get(0)).isInstanceOf(DropColumnEvent.class);

        DropColumnEvent dropEvent = (DropColumnEvent) events.get(0);
        Assertions.assertThat(dropEvent.tableId()).isEqualTo(tableId);
        Assertions.assertThat(dropEvent.getDroppedColumnNames()).containsExactly("age");
    }

    /** Tests that generateSchemaChangeEvents handles mixed add and drop changes. */
    @Test
    void testGenerateSchemaChangeEventsMixedChanges() throws Exception {
        TableId tableId = TableId.tableId("ns", "sch", "tbl");
        PostTransformOperator operator =
                PostTransformOperator.newBuilder()
                        .addTransform(tableId.identifier(), "id, name", null)
                        .build();

        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("old_col", DataTypes.FLOAT())
                        .primaryKey("id")
                        .build();
        Schema newSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("new_col", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();

        List<SchemaChangeEvent> events =
                invokeGenerateSchemaChangeEvents(operator, tableId, oldSchema, newSchema);

        // Should produce both AddColumnEvent and DropColumnEvent
        Assertions.assertThat(events).hasSize(2);

        // First event should be AddColumnEvent (added columns are processed first)
        Assertions.assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent addEvent = (AddColumnEvent) events.get(0);
        Assertions.assertThat(addEvent.getAddedColumns()).hasSize(1);
        Assertions.assertThat(addEvent.getAddedColumns().get(0).getAddColumn().getName())
                .isEqualTo("new_col");

        // Second event should be DropColumnEvent
        Assertions.assertThat(events.get(1)).isInstanceOf(DropColumnEvent.class);
        DropColumnEvent dropEvent = (DropColumnEvent) events.get(1);
        Assertions.assertThat(dropEvent.getDroppedColumnNames()).containsExactly("old_col");
    }

    /** Tests that generateSchemaChangeEvents returns empty list when schemas are identical. */
    @Test
    void testGenerateSchemaChangeEventsNoChanges() throws Exception {
        TableId tableId = TableId.tableId("ns", "sch", "tbl");
        PostTransformOperator operator =
                PostTransformOperator.newBuilder()
                        .addTransform(tableId.identifier(), "id, name", null)
                        .build();

        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        List<SchemaChangeEvent> events =
                invokeGenerateSchemaChangeEvents(operator, tableId, schema, schema);

        Assertions.assertThat(events).isEmpty();
    }

    /** Tests that generateSchemaChangeEvents positions first column correctly. */
    @Test
    void testGenerateSchemaChangeEventsAddColumnFirst() throws Exception {
        TableId tableId = TableId.tableId("ns", "sch", "tbl");
        PostTransformOperator operator =
                PostTransformOperator.newBuilder()
                        .addTransform(tableId.identifier(), "id, name", null)
                        .build();

        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        // Add "new_first" at position 0
        Schema newSchema =
                Schema.newBuilder()
                        .physicalColumn("new_first", DataTypes.BIGINT())
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        List<SchemaChangeEvent> events =
                invokeGenerateSchemaChangeEvents(operator, tableId, oldSchema, newSchema);

        Assertions.assertThat(events).hasSize(1);
        AddColumnEvent addEvent = (AddColumnEvent) events.get(0);
        AddColumnEvent.ColumnWithPosition colWithPos = addEvent.getAddedColumns().get(0);
        Assertions.assertThat(colWithPos.getAddColumn().getName()).isEqualTo("new_first");
        Assertions.assertThat(colWithPos.getPosition())
                .isEqualTo(AddColumnEvent.ColumnPosition.FIRST);
    }

    /**
     * Tests that pending schema changes are emitted before the first DataChangeEvent for a table.
     */
    @Test
    void testPendingSchemaChangesEmittedBeforeDataEvent() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "pending_test");

        // Build operator with projection "id, name, age"
        PostTransformOperator operator =
                PostTransformOperator.newBuilder()
                        .addTransform(tableId.identifier(), "id, name, age", null)
                        .build();

        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(operator, 1);
        harness.open();

        // First, send a CreateTableEvent to establish the schema
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        operator.processElement(new StreamRecord<>(new CreateTableEvent(tableId, initialSchema)));

        // Consume the CreateTableEvent output
        Event createOutput = harness.getOutputRecords().poll().getValue();
        Assertions.assertThat(createOutput).isInstanceOf(CreateTableEvent.class);

        // Now manually inject pending schema changes via reflection
        java.lang.reflect.Field pendingField =
                PostTransformOperator.class.getDeclaredField("pendingSchemaChanges");
        pendingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<TableId, List<SchemaChangeEvent>> pendingMap =
                (java.util.Map<TableId, List<SchemaChangeEvent>>) pendingField.get(operator);

        List<SchemaChangeEvent> pendingEvents = new ArrayList<>();
        pendingEvents.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("extra", DataTypes.DOUBLE()),
                                        AddColumnEvent.ColumnPosition.LAST,
                                        null))));
        pendingMap.put(tableId, pendingEvents);

        // Send a DataChangeEvent — pending changes should be emitted first
        Schema postSchema = ((CreateTableEvent) createOutput).getSchema();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator((RowType) postSchema.toRowDataType());
        DataChangeEvent dataEvent =
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(new Object[] {1, new BinaryStringData("Alice"), 30}));
        operator.processElement(new StreamRecord<>(dataEvent));

        // The first output should be the pending AddColumnEvent
        Event firstOutput = harness.getOutputRecords().poll().getValue();
        Assertions.assertThat(firstOutput).isInstanceOf(AddColumnEvent.class);
        AddColumnEvent emittedAdd = (AddColumnEvent) firstOutput;
        Assertions.assertThat(emittedAdd.getAddedColumns().get(0).getAddColumn().getName())
                .isEqualTo("extra");

        // The second output should be the DataChangeEvent
        Event secondOutput = harness.getOutputRecords().poll().getValue();
        Assertions.assertThat(secondOutput).isInstanceOf(DataChangeEvent.class);

        // Pending map should be cleared for this table
        Assertions.assertThat(pendingMap).doesNotContainKey(tableId);

        harness.close();
    }

    /** Tests that CreateTableEvent clears pending schema changes for a table. */
    @Test
    void testCreateTableEventClearsPendingChanges() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "clear_test");

        PostTransformOperator operator =
                PostTransformOperator.newBuilder()
                        .addTransform(tableId.identifier(), "id, name", null)
                        .build();

        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(operator, 1);
        harness.open();

        // First, send a CreateTableEvent to establish the schema
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        operator.processElement(new StreamRecord<>(new CreateTableEvent(tableId, initialSchema)));
        harness.getOutputRecords().poll(); // consume CreateTableEvent output

        // Inject pending schema changes via reflection
        java.lang.reflect.Field pendingField =
                PostTransformOperator.class.getDeclaredField("pendingSchemaChanges");
        pendingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<TableId, List<SchemaChangeEvent>> pendingMap =
                (java.util.Map<TableId, List<SchemaChangeEvent>>) pendingField.get(operator);

        List<SchemaChangeEvent> pendingEvents = new ArrayList<>();
        pendingEvents.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("stale_col", DataTypes.STRING()),
                                        AddColumnEvent.ColumnPosition.LAST,
                                        null))));
        pendingMap.put(tableId, pendingEvents);

        // Verify pending changes exist
        Assertions.assertThat(pendingMap).containsKey(tableId);

        // Send a new CreateTableEvent — this should clear pending changes
        Schema newSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("email", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        operator.processElement(new StreamRecord<>(new CreateTableEvent(tableId, newSchema)));

        // Pending changes should be cleared
        Assertions.assertThat(pendingMap).doesNotContainKey(tableId);

        // The output should be the new CreateTableEvent (not the stale pending AddColumnEvent)
        Event output = harness.getOutputRecords().poll().getValue();
        Assertions.assertThat(output).isInstanceOf(CreateTableEvent.class);

        // No more pending events should be in the output
        Assertions.assertThat(harness.getOutputRecords()).isEmpty();

        harness.close();
    }

    /** Tests that multiple pending schema changes are emitted in order. */
    @Test
    void testMultiplePendingSchemaChangesEmittedInOrder() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "multi_pending");

        PostTransformOperator operator =
                PostTransformOperator.newBuilder()
                        .addTransform(tableId.identifier(), "id, name", null)
                        .build();

        RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(operator, 1);
        harness.open();

        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        operator.processElement(new StreamRecord<>(new CreateTableEvent(tableId, initialSchema)));
        harness.getOutputRecords().poll(); // consume CreateTableEvent

        // Inject multiple pending schema changes
        java.lang.reflect.Field pendingField =
                PostTransformOperator.class.getDeclaredField("pendingSchemaChanges");
        pendingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<TableId, List<SchemaChangeEvent>> pendingMap =
                (java.util.Map<TableId, List<SchemaChangeEvent>>) pendingField.get(operator);

        List<SchemaChangeEvent> pendingEvents = new ArrayList<>();
        pendingEvents.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("col_a", DataTypes.INT()),
                                        AddColumnEvent.ColumnPosition.LAST,
                                        null))));
        pendingEvents.add(new DropColumnEvent(tableId, Collections.singletonList("old_col")));
        pendingMap.put(tableId, pendingEvents);

        // Send a DataChangeEvent to trigger emission
        Schema postSchema =
                ((CreateTableEvent) new CreateTableEvent(tableId, initialSchema)).getSchema();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator((RowType) postSchema.toRowDataType());
        DataChangeEvent dataEvent =
                DataChangeEvent.insertEvent(
                        tableId, generator.generate(new Object[] {1, new BinaryStringData("Bob")}));
        operator.processElement(new StreamRecord<>(dataEvent));

        // First output: AddColumnEvent
        Event first = harness.getOutputRecords().poll().getValue();
        Assertions.assertThat(first).isInstanceOf(AddColumnEvent.class);

        // Second output: DropColumnEvent
        Event second = harness.getOutputRecords().poll().getValue();
        Assertions.assertThat(second).isInstanceOf(DropColumnEvent.class);

        // Third output: DataChangeEvent
        Event third = harness.getOutputRecords().poll().getValue();
        Assertions.assertThat(third).isInstanceOf(DataChangeEvent.class);

        harness.close();
    }
}
