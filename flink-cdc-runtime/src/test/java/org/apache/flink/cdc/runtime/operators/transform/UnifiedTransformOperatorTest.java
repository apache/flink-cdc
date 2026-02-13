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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Unit tests for the {@link PreTransformOperator} and {@link PostTransformOperator}. */
class UnifiedTransformOperatorTest {

    /** Defines a unified transform test cases. */
    static class UnifiedTransformTestCase {

        private static final Logger LOG = LoggerFactory.getLogger(UnifiedTransformTestCase.class);

        private final TableId tableId;
        private final String projectionExpression;
        private final String filterExpression;

        private Schema sourceSchema;
        private Schema preTransformedSchema;
        private Schema postTransformedSchema;

        private final List<Event> sourceEvents;
        private final List<Event> preTransformedEvents;
        private final List<Event> postTransformedEvents;

        private final List<RecordData.FieldGetter> sourceFieldGetters;
        private final List<RecordData.FieldGetter> preTransformedFieldGetters;
        private final List<RecordData.FieldGetter> postTransformedFieldGetters;

        private PreTransformOperator preTransformOperator;
        private PostTransformOperator postTransformOperator;

        private final BinaryRecordDataGenerator sourceRecordGenerator;
        private final BinaryRecordDataGenerator preTransformedRecordGenerator;
        private final BinaryRecordDataGenerator postTransformedRecordGenerator;

        private RegularEventOperatorTestHarness<PreTransformOperator, Event>
                preTransformOperatorHarness;
        private RegularEventOperatorTestHarness<PostTransformOperator, Event>
                postTransformOperatorHarness;

        public static UnifiedTransformTestCase of(
                TableId tableId,
                String projectionExpression,
                String filterExpression,
                Schema sourceSchema,
                Schema preTransformedSchema,
                Schema postTransformedSchema) {
            return new UnifiedTransformTestCase(
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

        public UnifiedTransformTestCase insertSource(Object... record) {
            sourceEvents.add(
                    DataChangeEvent.insertEvent(
                            tableId, sourceRecordGenerator.generate(stringify(record))));
            return this;
        }

        public UnifiedTransformTestCase insertPreTransformed() {
            preTransformedEvents.add(null);
            return this;
        }

        public UnifiedTransformTestCase insertPreTransformed(Object... record) {
            preTransformedEvents.add(
                    DataChangeEvent.insertEvent(
                            tableId, preTransformedRecordGenerator.generate(stringify(record))));
            return this;
        }

        public UnifiedTransformTestCase insertPostTransformed() {
            postTransformedEvents.add(null);
            return this;
        }

        public UnifiedTransformTestCase insertPostTransformed(Object... record) {
            postTransformedEvents.add(
                    DataChangeEvent.insertEvent(
                            tableId, postTransformedRecordGenerator.generate(stringify(record))));
            return this;
        }

        public UnifiedTransformTestCase updateSource(Object[] beforeRecord, Object[] afterRecord) {
            sourceEvents.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            sourceRecordGenerator.generate(stringify(beforeRecord)),
                            sourceRecordGenerator.generate(stringify(afterRecord))));
            return this;
        }

        public UnifiedTransformTestCase updatePreTransformed() {
            preTransformedEvents.add(null);
            return this;
        }

        public UnifiedTransformTestCase updatePreTransformed(
                Object[] beforeRecord, Object[] afterRecord) {
            preTransformedEvents.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            preTransformedRecordGenerator.generate(stringify(beforeRecord)),
                            preTransformedRecordGenerator.generate(stringify(afterRecord))));
            return this;
        }

        public UnifiedTransformTestCase updatePostTransformed() {
            postTransformedEvents.add(null);
            return this;
        }

        public UnifiedTransformTestCase updatePostTransformed(
                Object[] beforeRecord, Object[] afterRecord) {
            postTransformedEvents.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            postTransformedRecordGenerator.generate(stringify(beforeRecord)),
                            postTransformedRecordGenerator.generate(stringify(afterRecord))));
            return this;
        }

        public UnifiedTransformTestCase deleteSource(Object... record) {
            sourceEvents.add(
                    DataChangeEvent.deleteEvent(
                            tableId, sourceRecordGenerator.generate(stringify(record))));
            return this;
        }

        public UnifiedTransformTestCase deletePreTransformed() {
            preTransformedEvents.add(null);
            return this;
        }

        public UnifiedTransformTestCase deletePreTransformed(Object... record) {
            preTransformedEvents.add(
                    DataChangeEvent.deleteEvent(
                            tableId, preTransformedRecordGenerator.generate(stringify(record))));
            return this;
        }

        public UnifiedTransformTestCase deletePostTransformed() {
            postTransformedEvents.add(null);
            return this;
        }

        public UnifiedTransformTestCase deletePostTransformed(Object... record) {
            postTransformedEvents.add(
                    DataChangeEvent.deleteEvent(
                            tableId, postTransformedRecordGenerator.generate(stringify(record))));
            return this;
        }

        private UnifiedTransformTestCase(
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
                    new BinaryRecordDataGenerator(((RowType) sourceSchema.toRowDataType()));
            this.preTransformedRecordGenerator =
                    new BinaryRecordDataGenerator(((RowType) preTransformedSchema.toRowDataType()));
            this.postTransformedRecordGenerator =
                    new BinaryRecordDataGenerator(
                            ((RowType) postTransformedSchema.toRowDataType()));

            this.sourceEvents = new ArrayList<>();
            this.preTransformedEvents = new ArrayList<>();
            this.postTransformedEvents = new ArrayList<>();

            this.sourceEvents.add(new CreateTableEvent(tableId, sourceSchema));
            this.preTransformedEvents.add(new CreateTableEvent(tableId, preTransformedSchema));
            this.postTransformedEvents.add(new CreateTableEvent(tableId, postTransformedSchema));

            this.sourceFieldGetters = SchemaUtils.createFieldGetters(sourceSchema);
            this.preTransformedFieldGetters = SchemaUtils.createFieldGetters(preTransformedSchema);
            this.postTransformedFieldGetters =
                    SchemaUtils.createFieldGetters(postTransformedSchema);
        }

        private UnifiedTransformTestCase initializeHarness() throws Exception {
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

        private void logBinaryDataContents(
                String prefix, Event event, List<RecordData.FieldGetter> fieldGetters) {
            LOG.info("{}: {}", prefix, event);
            if (event instanceof DataChangeEvent) {
                LOG.info(
                        "    Before Record Data: {}",
                        SchemaUtils.restoreOriginalData(
                                ((DataChangeEvent) event).before(), fieldGetters));
                LOG.info(
                        "    After Record Data: {}",
                        SchemaUtils.restoreOriginalData(
                                ((DataChangeEvent) event).after(), fieldGetters));
            }
        }

        public UnifiedTransformTestCase runTests() throws Exception {
            for (int i = 0; i < sourceEvents.size(); i++) {
                Event sourceEvent = sourceEvents.get(i);
                logBinaryDataContents("Source Event", sourceEvent, sourceFieldGetters);

                preTransformOperator.processElement(new StreamRecord<>(sourceEvent));

                Event expectedPreTransformEvent = preTransformedEvents.get(i);
                Event actualPreTransformEvent =
                        Optional.ofNullable(preTransformOperatorHarness.getOutputRecords().poll())
                                .map(StreamRecord::getValue)
                                .orElse(null);

                logBinaryDataContents(
                        "Expected PreTransform ",
                        expectedPreTransformEvent,
                        preTransformedFieldGetters);
                logBinaryDataContents(
                        "  Actual PreTransform ",
                        actualPreTransformEvent,
                        preTransformedFieldGetters);
                Assertions.assertThat(actualPreTransformEvent).isEqualTo(expectedPreTransformEvent);

                postTransformOperator.processElement(
                        new StreamRecord<>(preTransformedEvents.get(i)));
                Event expectedPostTransformEvent = postTransformedEvents.get(i);
                Event actualPostTransformEvent =
                        Optional.ofNullable(postTransformOperatorHarness.getOutputRecords().poll())
                                .map(StreamRecord::getValue)
                                .orElse(null);
                logBinaryDataContents(
                        "Expected PostTransform",
                        expectedPostTransformEvent,
                        postTransformedFieldGetters);
                logBinaryDataContents(
                        "  Actual PostTransform",
                        actualPostTransformEvent,
                        postTransformedFieldGetters);
                Assertions.assertThat(actualPostTransformEvent)
                        .isEqualTo(expectedPostTransformEvent);
            }

            sourceEvents.clear();
            preTransformedEvents.clear();
            postTransformedEvents.clear();
            return this;
        }
    }

    @Test
    void testDataChangeEventTransform() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "data_changes");
        UnifiedTransformTestCase.of(
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
                .insertSource(1000, "Alice", 17)
                .insertPreTransformed(1000, 17)
                .insertPostTransformed(1000, 17, 1017)
                .insertSource(2000, "Bob", 18)
                .insertPreTransformed(2000, 18)
                .insertPostTransformed(2000, 18, 2018)
                .updateSource(new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePreTransformed(new Object[] {2000, 18}, new Object[] {2000, 16})
                .updatePostTransformed(new Object[] {2000, 18, 2018}, new Object[] {2000, 16, 2016})
                // filtered out data row
                .insertSource(50, "Carol", 19)
                .insertPreTransformed(50, 19)
                .insertPostTransformed()
                .deleteSource(1000, "Alice", 17)
                .deletePreTransformed(1000, 17)
                .deletePostTransformed(1000, 17, 1017)
                .runTests()
                .destroyHarness();
    }

    @Test
    void testSchemaNullabilityTransform() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "schema_nullability");
        UnifiedTransformTestCase.of(
                        tableId,
                        "id, name, age, id + age as computed",
                        "id > 100",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING().notNull())
                                .physicalColumn("age", DataTypes.INT().notNull())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING().notNull())
                                .physicalColumn("age", DataTypes.INT().notNull())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("name", DataTypes.STRING().notNull())
                                .physicalColumn("age", DataTypes.INT().notNull())
                                .physicalColumn("computed", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource(1000, "Alice", 17)
                .insertPreTransformed(1000, "Alice", 17)
                .insertPostTransformed(1000, "Alice", 17, 1017)
                .insertSource(2000, "Bob", 18)
                .insertPreTransformed(2000, "Bob", 18)
                .insertPostTransformed(2000, "Bob", 18, 2018)
                .updateSource(new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePreTransformed(
                        new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePostTransformed(
                        new Object[] {2000, "Bob", 18, 2018},
                        new Object[] {2000, "Barcarolle", 16, 2016})
                // filtered out data row
                .insertSource(50, "Carol", 19)
                .insertPreTransformed(50, "Carol", 19)
                .insertPostTransformed()
                .deleteSource(1000, "Alice", 17)
                .deletePreTransformed(1000, "Alice", 17)
                .deletePostTransformed(1000, "Alice", 17, 1017)
                .runTests()
                .destroyHarness();
    }

    @Test
    void testReduceColumnsTransform() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "reduce_column");
        UnifiedTransformTestCase.of(
                        tableId,
                        "id, upper(id) as uid, age + 1 as newage, lower(ref1) as lowerref",
                        "newage > 17 and ref2 > 17",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.STRING().notNull())
                                .physicalColumn("name", DataTypes.STRING().notNull())
                                .physicalColumn("age", DataTypes.INT().notNull())
                                .physicalColumn("ref1", DataTypes.STRING())
                                .physicalColumn("ref2", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.STRING().notNull())
                                .physicalColumn("age", DataTypes.INT().notNull())
                                .physicalColumn("ref1", DataTypes.STRING())
                                .physicalColumn("ref2", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.STRING().notNull())
                                .physicalColumn("uid", DataTypes.STRING())
                                .physicalColumn("newage", DataTypes.INT())
                                .physicalColumn("lowerref", DataTypes.STRING())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource("id001", "Alice", 17, "Reference001", 2021)
                .insertPreTransformed("id001", 17, "Reference001", 2021)
                .insertPostTransformed("id001", "ID001", 18, "reference001")
                // this data record is filtered out since newage <= 17
                .insertSource("id002", "Bob", 15, "Reference002", 2017)
                .insertPreTransformed("id002", 15, "Reference002", 2017)
                .insertPostTransformed()
                // this data record is filtered out since ref2 <= 17
                .insertSource("id003", "Bill", 18, "Reference003", 0)
                .insertPreTransformed("id003", 18, "Reference003", 0)
                .insertPostTransformed()
                .insertSource("id004", "Carol", 18, "Reference004", 2018)
                .insertPreTransformed("id004", 18, "Reference004", 2018)
                .insertPostTransformed("id004", "ID004", 19, "reference004")
                // test update event transform
                .updateSource(
                        new Object[] {"id004", "Carol", 18, "Reference004", 2018},
                        new Object[] {"id004", "Colin", 18, "NeoReference004", 2018})
                .updatePreTransformed(
                        new Object[] {"id004", 18, "Reference004", 2018},
                        new Object[] {"id004", 18, "NeoReference004", 2018})
                .updatePostTransformed(
                        new Object[] {"id004", "ID004", 19, "reference004"},
                        new Object[] {"id004", "ID004", 19, "neoreference004"})
                // updated value to a filtered out condition
                .updateSource(
                        new Object[] {"id004", "Colin", 18, "NeoReference004", 2018},
                        new Object[] {"id004", "Colin", 10, "NeoReference004", 2018})
                .updatePreTransformed(
                        new Object[] {"id004", 18, "NeoReference004", 2018},
                        new Object[] {"id004", 10, "NeoReference004", 2018})
                .updatePostTransformed()
                .deleteSource("id001", "Alice", 17, "Reference001", 2021)
                .deletePreTransformed("id001", 17, "Reference001", 2021)
                .deletePostTransformed("id001", "ID001", 18, "reference001")
                .runTests()
                .destroyHarness();
    }

    @Test
    void testWildcardTransform() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "wildcard");
        UnifiedTransformTestCase.of(
                        tableId,
                        "*, id + age as computed",
                        "id > 100",
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
                .insertSource(1000, "Alice", 17)
                .insertPreTransformed(1000, "Alice", 17)
                .insertPostTransformed(1000, "Alice", 17, 1017)
                .insertSource(2000, "Bob", 18)
                .insertPreTransformed(2000, "Bob", 18)
                .insertPostTransformed(2000, "Bob", 18, 2018)
                .updateSource(new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePreTransformed(
                        new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePostTransformed(
                        new Object[] {2000, "Bob", 18, 2018},
                        new Object[] {2000, "Barcarolle", 16, 2016})
                // filtered out data row
                .insertSource(50, "Carol", 19)
                .insertPreTransformed(50, "Carol", 19)
                .insertPostTransformed()
                .deleteSource(1000, "Alice", 17)
                .deletePreTransformed(1000, "Alice", 17)
                .deletePostTransformed(1000, "Alice", 17, 1017)
                .runTests()
                .destroyHarness();

        UnifiedTransformTestCase.of(
                        tableId,
                        "id + age as computed, *",
                        "id > 100",
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
                .insertSource(1000, "Alice", 17)
                .insertPreTransformed(1000, "Alice", 17)
                .insertPostTransformed(1017, 1000, "Alice", 17)
                .insertSource(2000, "Bob", 18)
                .insertPreTransformed(2000, "Bob", 18)
                .insertPostTransformed(2018, 2000, "Bob", 18)
                .updateSource(new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePreTransformed(
                        new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePostTransformed(
                        new Object[] {2018, 2000, "Bob", 18},
                        new Object[] {2016, 2000, "Barcarolle", 16})
                // filtered out data row
                .insertSource(50, "Carol", 19)
                .insertPreTransformed(50, "Carol", 19)
                .insertPostTransformed()
                .deleteSource(1000, "Alice", 17)
                .deletePreTransformed(1000, "Alice", 17)
                .deletePostTransformed(1017, 1000, "Alice", 17)
                .runTests()
                .destroyHarness();
    }

    @Test
    void testMetadataTransform() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "metadata");
        UnifiedTransformTestCase.of(
                        tableId,
                        "*, __namespace_name__, __schema_name__, __table_name__",
                        "id > 100",
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
                                .physicalColumn("__namespace_name__", DataTypes.STRING().notNull())
                                .physicalColumn("__schema_name__", DataTypes.STRING().notNull())
                                .physicalColumn("__table_name__", DataTypes.STRING().notNull())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource(1000, "Alice", 17)
                .insertPreTransformed(1000, "Alice", 17)
                .insertPostTransformed(1000, "Alice", 17, "my_company", "my_branch", "metadata")
                .insertSource(2000, "Bob", 18)
                .insertPreTransformed(2000, "Bob", 18)
                .insertPostTransformed(2000, "Bob", 18, "my_company", "my_branch", "metadata")
                .updateSource(new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePreTransformed(
                        new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePostTransformed(
                        new Object[] {2000, "Bob", 18, "my_company", "my_branch", "metadata"},
                        new Object[] {
                            2000, "Barcarolle", 16, "my_company", "my_branch", "metadata"
                        })
                // filtered out data row
                .insertSource(50, "Carol", 19)
                .insertPreTransformed(50, "Carol", 19)
                .insertPostTransformed()
                .deleteSource(1000, "Alice", 17)
                .deletePreTransformed(1000, "Alice", 17)
                .deletePostTransformed(1000, "Alice", 17, "my_company", "my_branch", "metadata")
                .runTests()
                .destroyHarness();
    }

    @Test
    void testCalculatedMetadataTransform() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "metadata_transform");
        UnifiedTransformTestCase.of(
                        tableId,
                        "*, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ AS identifier_name",
                        "id > 100",
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
                                .physicalColumn("identifier_name", DataTypes.STRING())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource(1000, "Alice", 17)
                .insertPreTransformed(1000, "Alice", 17)
                .insertPostTransformed(1000, "Alice", 17, "my_company.my_branch.metadata_transform")
                .insertSource(2000, "Bob", 18)
                .insertPreTransformed(2000, "Bob", 18)
                .insertPostTransformed(2000, "Bob", 18, "my_company.my_branch.metadata_transform")
                .updateSource(new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePreTransformed(
                        new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePostTransformed(
                        new Object[] {2000, "Bob", 18, "my_company.my_branch.metadata_transform"},
                        new Object[] {
                            2000, "Barcarolle", 16, "my_company.my_branch.metadata_transform"
                        })
                // filtered out data row
                .insertSource(50, "Carol", 19)
                .insertPreTransformed(50, "Carol", 19)
                .insertPostTransformed()
                .deleteSource(1000, "Alice", 17)
                .deletePreTransformed(1000, "Alice", 17)
                .deletePostTransformed(1000, "Alice", 17, "my_company.my_branch.metadata_transform")
                .runTests()
                .destroyHarness();

        UnifiedTransformTestCase.of(
                        tableId,
                        "__namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ AS identifier_name, *",
                        "id > 100",
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
                                .physicalColumn("identifier_name", DataTypes.STRING())
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource(1000, "Alice", 17)
                .insertPreTransformed(1000, "Alice", 17)
                .insertPostTransformed("my_company.my_branch.metadata_transform", 1000, "Alice", 17)
                .insertSource(2000, "Bob", 18)
                .insertPreTransformed(2000, "Bob", 18)
                .insertPostTransformed("my_company.my_branch.metadata_transform", 2000, "Bob", 18)
                .updateSource(new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePreTransformed(
                        new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePostTransformed(
                        new Object[] {"my_company.my_branch.metadata_transform", 2000, "Bob", 18},
                        new Object[] {
                            "my_company.my_branch.metadata_transform", 2000, "Barcarolle", 16
                        })
                // filtered out data row
                .insertSource(50, "Carol", 19)
                .insertPreTransformed(50, "Carol", 19)
                .insertPostTransformed()
                .deleteSource(1000, "Alice", 17)
                .deletePreTransformed(1000, "Alice", 17)
                .deletePostTransformed("my_company.my_branch.metadata_transform", 1000, "Alice", 17)
                .runTests()
                .destroyHarness();
    }

    @Test
    void testMetadataAndCalculatedTransform() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "metadata_transform");
        UnifiedTransformTestCase.of(
                        tableId,
                        "*, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ AS identifier_name, __namespace_name__, __schema_name__, __table_name__",
                        "id > 100",
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
                                .physicalColumn("identifier_name", DataTypes.STRING())
                                .physicalColumn("__namespace_name__", DataTypes.STRING().notNull())
                                .physicalColumn("__schema_name__", DataTypes.STRING().notNull())
                                .physicalColumn("__table_name__", DataTypes.STRING().notNull())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource(1000, "Alice", 17)
                .insertPreTransformed(1000, "Alice", 17)
                .insertPostTransformed(
                        1000,
                        "Alice",
                        17,
                        "my_company.my_branch.metadata_transform",
                        "my_company",
                        "my_branch",
                        "metadata_transform")
                .insertSource(2000, "Bob", 18)
                .insertPreTransformed(2000, "Bob", 18)
                .insertPostTransformed(
                        2000,
                        "Bob",
                        18,
                        "my_company.my_branch.metadata_transform",
                        "my_company",
                        "my_branch",
                        "metadata_transform")
                .updateSource(new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePreTransformed(
                        new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePostTransformed(
                        new Object[] {
                            2000,
                            "Bob",
                            18,
                            "my_company.my_branch.metadata_transform",
                            "my_company",
                            "my_branch",
                            "metadata_transform"
                        },
                        new Object[] {
                            2000,
                            "Barcarolle",
                            16,
                            "my_company.my_branch.metadata_transform",
                            "my_company",
                            "my_branch",
                            "metadata_transform"
                        })
                // filtered out data row
                .insertSource(50, "Carol", 19)
                .insertPreTransformed(50, "Carol", 19)
                .insertPostTransformed()
                .deleteSource(1000, "Alice", 17)
                .deletePreTransformed(1000, "Alice", 17)
                .deletePostTransformed(
                        1000,
                        "Alice",
                        17,
                        "my_company.my_branch.metadata_transform",
                        "my_company",
                        "my_branch",
                        "metadata_transform")
                .runTests()
                .destroyHarness();

        UnifiedTransformTestCase.of(
                        tableId,
                        "__namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ AS identifier_name, __namespace_name__, __schema_name__, __table_name__, *",
                        "id > 100",
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
                                .physicalColumn("identifier_name", DataTypes.STRING())
                                .physicalColumn("__namespace_name__", DataTypes.STRING().notNull())
                                .physicalColumn("__schema_name__", DataTypes.STRING().notNull())
                                .physicalColumn("__table_name__", DataTypes.STRING().notNull())
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource(1000, "Alice", 17)
                .insertPreTransformed(1000, "Alice", 17)
                .insertPostTransformed(
                        "my_company.my_branch.metadata_transform",
                        "my_company",
                        "my_branch",
                        "metadata_transform",
                        1000,
                        "Alice",
                        17)
                .insertSource(2000, "Bob", 18)
                .insertPreTransformed(2000, "Bob", 18)
                .insertPostTransformed(
                        "my_company.my_branch.metadata_transform",
                        "my_company",
                        "my_branch",
                        "metadata_transform",
                        2000,
                        "Bob",
                        18)
                .updateSource(new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePreTransformed(
                        new Object[] {2000, "Bob", 18}, new Object[] {2000, "Barcarolle", 16})
                .updatePostTransformed(
                        new Object[] {
                            "my_company.my_branch.metadata_transform",
                            "my_company",
                            "my_branch",
                            "metadata_transform",
                            2000,
                            "Bob",
                            18
                        },
                        new Object[] {
                            "my_company.my_branch.metadata_transform",
                            "my_company",
                            "my_branch",
                            "metadata_transform",
                            2000,
                            "Barcarolle",
                            16
                        })
                // filtered out data row
                .insertSource(50, "Carol", 19)
                .insertPreTransformed(50, "Carol", 19)
                .insertPostTransformed()
                .deleteSource(1000, "Alice", 17)
                .deletePreTransformed(1000, "Alice", 17)
                .deletePostTransformed(
                        "my_company.my_branch.metadata_transform",
                        "my_company",
                        "my_branch",
                        "metadata_transform",
                        1000,
                        "Alice",
                        17)
                .runTests()
                .destroyHarness();
    }

    @Test
    void testMetadataTransformIncludeMetaColumnString() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "schema_nullability");
        UnifiedTransformTestCase.of(
                        tableId,
                        "id, name, age, id + age as computed, __namespace_name__ as metaColNameSpaceName,  __schema_name__ as metaColSchemaName, __table_name__ as metaColNameTableName, "
                                + "UPPER(__schema_name__) as metaColSchemaNameUpper, '__table_name__' as metaColStr1, '__namespace__name__schema__name__table__name__' as metaColStr2",
                        "id > 100",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING().notNull())
                                .physicalColumn("age", DataTypes.INT().notNull())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .physicalColumn("name", DataTypes.STRING().notNull())
                                .physicalColumn("age", DataTypes.INT().notNull())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT().notNull())
                                .physicalColumn("name", DataTypes.STRING().notNull())
                                .physicalColumn("age", DataTypes.INT().notNull())
                                .physicalColumn("computed", DataTypes.INT())
                                .physicalColumn(
                                        "metaColNameSpaceName", DataTypes.STRING().notNull())
                                .physicalColumn("metaColSchemaName", DataTypes.STRING().notNull())
                                .physicalColumn(
                                        "metaColNameTableName", DataTypes.STRING().notNull())
                                .physicalColumn("metaColSchemaNameUpper", DataTypes.STRING())
                                .physicalColumn("metaColStr1", DataTypes.STRING())
                                .physicalColumn("metaColStr2", DataTypes.STRING())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource(1000, "Alice", 17)
                .insertPreTransformed(1000, "Alice", 17)
                .insertPostTransformed(
                        1000,
                        "Alice",
                        17,
                        1017,
                        "my_company",
                        "my_branch",
                        "schema_nullability",
                        "MY_BRANCH",
                        "__table_name__",
                        "__namespace__name__schema__name__table__name__")
                .runTests()
                .destroyHarness();
    }

    @Test
    void testTransformWithCast() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "transform_with_cast");
        UnifiedTransformTestCase.of(
                        tableId,
                        "id, age + 1 as newage, CAST(CAST(id AS INT) + age AS BIGINT) as longevity, CAST(age AS VARCHAR) as string_age",
                        "newage > 17 and ref2 > 17",
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.STRING().notNull())
                                .physicalColumn("name", DataTypes.STRING().notNull())
                                .physicalColumn("age", DataTypes.INT().notNull())
                                .physicalColumn("ref1", DataTypes.STRING())
                                .physicalColumn("ref2", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.STRING().notNull())
                                .physicalColumn("age", DataTypes.INT().notNull())
                                .physicalColumn("ref2", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.STRING().notNull())
                                .physicalColumn("newage", DataTypes.INT())
                                .physicalColumn("longevity", DataTypes.BIGINT())
                                .physicalColumn("string_age", DataTypes.STRING())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource("1001", "Alice", 17, "Reference001", 2021)
                .insertPreTransformed("1001", 17, 2021)
                .insertPostTransformed("1001", 18, 1018L, "17")
                // this data record is filtered out since newage <= 17
                .insertSource("1002", "Bob", 15, "Reference002", 2017)
                .insertPreTransformed("1002", 15, 2017)
                .insertPostTransformed()
                // this data record is filtered out since ref2 <= 17
                .insertSource("1003", "Bill", 18, "Reference003", 0)
                .insertPreTransformed("1003", 18, 0)
                .insertPostTransformed()
                .insertSource("1004", "Carol", 18, "Reference004", 2018)
                .insertPreTransformed("1004", 18, 2018)
                .insertPostTransformed("1004", 19, 1022L, "18")
                // test update event transform
                .updateSource(
                        new Object[] {"1004", "Carol", 18, "Reference004", 2018},
                        new Object[] {"1004", "Colin", 19, "NeoReference004", 2018})
                .updatePreTransformed(
                        new Object[] {"1004", 18, 2018}, new Object[] {"1004", 19, 2018})
                .updatePostTransformed(
                        new Object[] {"1004", 19, 1022L, "18"},
                        new Object[] {"1004", 20, 1023L, "19"})
                // updated value to a filtered out condition
                .updateSource(
                        new Object[] {"1004", "Colin", 19, "NeoReference004", 2018},
                        new Object[] {"1004", "Colin", 10, "NeoReference004", 2018})
                .updatePreTransformed(
                        new Object[] {"1004", 19, 2018}, new Object[] {"1004", 10, 2018})
                .updatePostTransformed()
                .deleteSource("1001", "Alice", 17, "Reference001", 2021)
                .deletePreTransformed("1001", 17, 2021)
                .deletePostTransformed("1001", 18, 1018L, "17")
                .runTests()
                .destroyHarness();
    }

    @Test
    void testTransformWithCommentsAndExpressions() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "data_changes");
        UnifiedTransformTestCase.of(
                        tableId,
                        "*, id + age as computed",
                        "id > 100",
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id", DataTypes.INT(), "id column", "AUTO_INCREMENT()")
                                .physicalColumn(
                                        "name", DataTypes.STRING(), "name column", "John Smith")
                                .physicalColumn("age", DataTypes.INT(), "age column", "17")
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id", DataTypes.INT(), "id column", "AUTO_INCREMENT()")
                                .physicalColumn(
                                        "name", DataTypes.STRING(), "name column", "John Smith")
                                .physicalColumn("age", DataTypes.INT(), "age column", "17")
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        DataTypes.INT().notNull(),
                                        "id column",
                                        "AUTO_INCREMENT()")
                                .physicalColumn(
                                        "name", DataTypes.STRING(), "name column", "John Smith")
                                .physicalColumn("age", DataTypes.INT(), "age column", "17")
                                .physicalColumn("computed", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource(1001, "Alice", 17)
                .insertPreTransformed(1001, "Alice", 17)
                .insertPostTransformed(1001, "Alice", 17, 1018)
                .runTests()
                .destroyHarness();
        UnifiedTransformTestCase.of(
                        tableId,
                        "id, age, id + age as computed",
                        "id > 100",
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id", DataTypes.INT(), "id column", "AUTO_INCREMENT()")
                                .physicalColumn(
                                        "name", DataTypes.STRING(), "name column", "John Smith")
                                .physicalColumn("age", DataTypes.INT(), "age column", "17")
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id", DataTypes.INT(), "id column", "AUTO_INCREMENT()")
                                .physicalColumn("age", DataTypes.INT(), "age column", "17")
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        DataTypes.INT().notNull(),
                                        "id column",
                                        "AUTO_INCREMENT()")
                                .physicalColumn("age", DataTypes.INT(), "age column", "17")
                                .physicalColumn("computed", DataTypes.INT())
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource(1001, "Alice", 17)
                .insertPreTransformed(1001, 17)
                .insertPostTransformed(1001, 17, 1018)
                .runTests()
                .destroyHarness();

        UnifiedTransformTestCase.of(
                        tableId,
                        "'extras' AS extras, age + 1 AS new_age_incremented, age AS new_age, name AS new_name, age, name, id",
                        "id > 100",
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id", DataTypes.INT(), "id column", "AUTO_INCREMENT()")
                                .physicalColumn(
                                        "name", DataTypes.STRING(), "name column", "John Smith")
                                .physicalColumn("age", DataTypes.INT(), "age column", "17")
                                .physicalColumn(
                                        "description",
                                        DataTypes.STRING(),
                                        "description column",
                                        "nothing really important")
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id", DataTypes.INT(), "id column", "AUTO_INCREMENT()")
                                .physicalColumn(
                                        "name", DataTypes.STRING(), "name column", "John Smith")
                                .physicalColumn("age", DataTypes.INT(), "age column", "17")
                                .primaryKey("id")
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("extras", DataTypes.STRING())
                                .physicalColumn("new_age_incremented", DataTypes.INT())
                                .physicalColumn("new_age", DataTypes.INT(), "age column", "17")
                                .physicalColumn(
                                        "new_name", DataTypes.STRING(), "name column", "John Smith")
                                .physicalColumn("age", DataTypes.INT(), "age column", "17")
                                .physicalColumn(
                                        "name", DataTypes.STRING(), "name column", "John Smith")
                                .physicalColumn(
                                        "id",
                                        DataTypes.INT().notNull(),
                                        "id column",
                                        "AUTO_INCREMENT()")
                                .primaryKey("id")
                                .build())
                .initializeHarness()
                .insertSource(1001, "Alice", 17, "Whatever")
                .insertPreTransformed(1001, "Alice", 17)
                .insertPostTransformed("extras", 18, 17, "Alice", 17, "Alice", 1001)
                .runTests()
                .destroyHarness();
    }

    @Test
    public void testTransformWithColumnNameMap() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "column_name_map");
        UnifiedTransformTestCase.of(
                        tableId,
                        "foo-bar AS f0, `foo-bar`, foo-bar-`foo-bar` AS f1, class",
                        "foo-bar <> 0",
                        Schema.newBuilder()
                                .physicalColumn("foo", DataTypes.INT())
                                .physicalColumn("bar", DataTypes.INT())
                                .physicalColumn("foo-bar", DataTypes.INT())
                                .physicalColumn("bar-foo", DataTypes.INT())
                                .physicalColumn("class", DataTypes.STRING())
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("foo", DataTypes.INT())
                                .physicalColumn("bar", DataTypes.INT())
                                .physicalColumn("foo-bar", DataTypes.INT())
                                .physicalColumn("class", DataTypes.STRING())
                                .build(),
                        Schema.newBuilder()
                                .physicalColumn("f0", DataTypes.INT())
                                .physicalColumn("foo-bar", DataTypes.INT())
                                .physicalColumn("f1", DataTypes.INT())
                                .physicalColumn("class", DataTypes.STRING())
                                .build())
                .initializeHarness()
                .insertSource(1, 2, 3, 4, "class")
                .insertPreTransformed(1, 2, 3, "class")
                .insertPostTransformed(-1, 3, -4, "class")
                .runTests()
                .destroyHarness();
    }
}
