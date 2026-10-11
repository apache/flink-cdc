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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DefaultDataChangeEventHashFunctionProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.testutils.schema.TestingSchemaRegistryGateway;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for the pre-partition operators. */
class PrePartitionOperatorTest {
    private static final TableId CUSTOMERS =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("phone", DataTypes.BIGINT())
                    .primaryKey("id")
                    .build();
    private static final int DOWNSTREAM_PARALLELISM = 5;

    @Test
    void testBroadcastingSchemaChangeEvent() throws Exception {
        try (RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
                testHarness = createTestHarness()) {
            // Initialization
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);

            // CreateTableEvent
            RegularPrePartitionOperator operator = testHarness.getOperator();
            CreateTableEvent createTableEvent = new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA);
            operator.processElement(new StreamRecord<>(createTableEvent));
            assertThat(testHarness.getOutputRecords()).hasSize(DOWNSTREAM_PARALLELISM);
            for (int i = 0; i < DOWNSTREAM_PARALLELISM; i++) {
                assertThat(testHarness.getOutputRecords().poll())
                        .isEqualTo(
                                new StreamRecord<>(
                                        PartitioningEvent.ofRegular(createTableEvent, i)));
            }
        }
    }

    @Test
    void testBroadcastingFlushEvent() throws Exception {
        try (RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
                testHarness = createTestHarness()) {
            // Initialization
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);

            // FlushEvent
            RegularPrePartitionOperator operator = testHarness.getOperator();
            FlushEvent flushEvent =
                    new FlushEvent(
                            0,
                            Collections.singletonList(CUSTOMERS),
                            SchemaChangeEventType.CREATE_TABLE);
            operator.processElement(new StreamRecord<>(flushEvent));
            assertThat(testHarness.getOutputRecords()).hasSize(DOWNSTREAM_PARALLELISM);
            for (int i = 0; i < DOWNSTREAM_PARALLELISM; i++) {
                assertThat(testHarness.getOutputRecords().poll())
                        .isEqualTo(new StreamRecord<>(PartitioningEvent.ofRegular(flushEvent, i)));
            }
        }
    }

    @Test
    void testPartitioningDataChangeEvent() throws Exception {
        try (RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
                testHarness = createTestHarness()) {
            // Initialization
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);

            // DataChangeEvent
            RegularPrePartitionOperator operator = testHarness.getOperator();
            BinaryRecordDataGenerator recordDataGenerator =
                    new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SCHEMA.toRowDataType()));
            DataChangeEvent eventA =
                    DataChangeEvent.insertEvent(
                            CUSTOMERS,
                            recordDataGenerator.generate(
                                    new Object[] {1, new BinaryStringData("Alice"), 12345678L}));
            DataChangeEvent eventB =
                    DataChangeEvent.insertEvent(
                            CUSTOMERS,
                            recordDataGenerator.generate(
                                    new Object[] {2, new BinaryStringData("Bob"), 12345689L}));
            operator.processElement(new StreamRecord<>(eventA));
            operator.processElement(new StreamRecord<>(eventB));
            StreamRecord<?> recordA = testHarness.getOutputRecords().poll();
            assertThat(recordA)
                    .isEqualTo(
                            new StreamRecord<>(
                                    PartitioningEvent.ofRegular(
                                            eventA,
                                            getPartitioningTarget(CUSTOMERS_SCHEMA, eventA))));

            StreamRecord<?> recordB = testHarness.getOutputRecords().poll();
            assertThat(recordB)
                    .isEqualTo(
                            new StreamRecord<>(
                                    PartitioningEvent.ofRegular(
                                            eventB,
                                            getPartitioningTarget(CUSTOMERS_SCHEMA, eventB))));
        }
    }

    @Test
    void testRegularTopologySplitsPrimaryKeyUpdateBeforePartitioning() throws Exception {
        try (RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
                testHarness = createTestHarness(true)) {
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);
            DataChangeEvent update = customersUpdate(1, 2);

            testHarness.getOperator().processElement(new StreamRecord<>(update));

            assertSplitOutput(testHarness.getOutputRecords(), update, false, 0);
        }
    }

    @Test
    void testDistributedTopologySplitsPrimaryKeyUpdateBeforePartitioning() throws Exception {
        DistributedPrePartitionOperator operator =
                new DistributedPrePartitionOperator(
                        DOWNSTREAM_PARALLELISM,
                        new DefaultDataChangeEventHashFunctionProvider(),
                        true);
        try (RegularEventOperatorTestHarness<DistributedPrePartitionOperator, PartitioningEvent>
                testHarness =
                        RegularEventOperatorTestHarness.with(operator, DOWNSTREAM_PARALLELISM)) {
            testHarness.open();
            operator.processElement(
                    new StreamRecord<Event>(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA)));
            testHarness.clearOutputRecords();
            DataChangeEvent update = customersUpdate(1, 2);

            operator.processElement(new StreamRecord<>(update));

            assertSplitOutput(testHarness.getOutputRecords(), update, true, 0);
        }
    }

    @Test
    void testBatchTopologySplitsPrimaryKeyUpdateBeforePartitioning() throws Exception {
        BatchRegularPrePartitionOperator operator =
                new BatchRegularPrePartitionOperator(
                        DOWNSTREAM_PARALLELISM,
                        new DefaultDataChangeEventHashFunctionProvider(),
                        true);
        try (RegularEventOperatorTestHarness<BatchRegularPrePartitionOperator, PartitioningEvent>
                testHarness =
                        RegularEventOperatorTestHarness.with(operator, DOWNSTREAM_PARALLELISM)) {
            testHarness.open();
            operator.processElement(
                    new StreamRecord<Event>(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA)));
            testHarness.clearOutputRecords();
            DataChangeEvent update = customersUpdate(1, 2);

            operator.processElement(new StreamRecord<>(update));

            assertSplitOutput(testHarness.getOutputRecords(), update, false, 0);
        }
    }

    @Test
    void testSplitOptOutKeepsPrimaryKeyUpdateIntact() throws Exception {
        try (RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
                testHarness = createTestHarness(false)) {
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);
            DataChangeEvent update = customersUpdate(1, 2);

            testHarness.getOperator().processElement(new StreamRecord<>(update));

            assertThat(testHarness.getOutputRecords()).hasSize(1);
            assertThat(testHarness.getOutputRecords().getFirst().getValue().getPayload())
                    .isSameAs(update);
        }
    }

    @Test
    void testSplitterUsesDeepPrimaryKeyEqualityInsteadOfHash() {
        Schema stringKeySchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("payload", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator stringGenerator =
                new BinaryRecordDataGenerator((RowType) stringKeySchema.toRowDataType());
        DataChangeEvent collidingHashUpdate =
                DataChangeEvent.updateEvent(
                        CUSTOMERS,
                        stringGenerator.generate(
                                new Object[] {BinaryStringData.fromString("FB"), 1}),
                        stringGenerator.generate(
                                new Object[] {BinaryStringData.fromString("Ea"), 2}),
                        Collections.singletonMap("source", "collision"));

        List<DataChangeEvent> collisionResult =
                new PrimaryKeyUpdateSplitter(stringKeySchema).split(collidingHashUpdate);

        assertThat(collisionResult).hasSize(2);
        assertThat(collisionResult.get(0).op()).isEqualTo(OperationType.UPDATE_BEFORE);
        assertThat(collisionResult.get(1).op()).isEqualTo(OperationType.REPLACE);
        assertThat(collisionResult.get(0).meta()).containsEntry("source", "collision");
        assertThat(collisionResult.get(1).meta()).containsEntry("source", "collision");

        Schema binaryKeySchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BYTES())
                        .physicalColumn("payload", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator binaryGenerator =
                new BinaryRecordDataGenerator((RowType) binaryKeySchema.toRowDataType());
        DataChangeEvent equalBinaryKeyUpdate =
                DataChangeEvent.updateEvent(
                        CUSTOMERS,
                        binaryGenerator.generate(new Object[] {new byte[] {1, 2, 3}, 1}),
                        binaryGenerator.generate(new Object[] {new byte[] {1, 2, 3}, 2}));

        assertThat(new PrimaryKeyUpdateSplitter(binaryKeySchema).split(equalBinaryKeyUpdate))
                .containsExactly(equalBinaryKeyUpdate);
    }

    @Test
    void testSplitterFailsClosedForInvalidUpdate() {
        Schema noPrimaryKey = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT()));
        DataChangeEvent valid =
                DataChangeEvent.updateEvent(
                        CUSTOMERS,
                        generator.generate(new Object[] {1}),
                        generator.generate(new Object[] {2}));

        assertThatThrownBy(() -> new PrimaryKeyUpdateSplitter(noPrimaryKey).split(valid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary key");
        PrimaryKeyUpdateSplitter splitter =
                new PrimaryKeyUpdateSplitter(
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.INT())
                                .primaryKey("id")
                                .build());
        assertThatThrownBy(
                        () ->
                                splitter.split(
                                        DataChangeEvent.updateEvent(
                                                CUSTOMERS,
                                                null,
                                                generator.generate(new Object[] {2}))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("before");
        assertThatThrownBy(
                        () ->
                                splitter.split(
                                        DataChangeEvent.updateEvent(
                                                CUSTOMERS,
                                                generator.generate(new Object[] {1}),
                                                null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("after");
    }

    @Test
    void testRegularTopologyRefreshesSplitterGettersWithSchema() throws Exception {
        Schema evolvedSchema =
                Schema.newBuilder()
                        .physicalColumn("prefix", DataTypes.STRING())
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("phone", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();
        AddColumnEvent addPrefix =
                new AddColumnEvent(
                        CUSTOMERS,
                        Collections.singletonList(
                                AddColumnEvent.first(
                                        Column.physicalColumn("prefix", DataTypes.STRING()))));
        try (RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
                testHarness = createTestHarness(true)) {
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);
            testHarness.registerEvolvedSchema(CUSTOMERS, evolvedSchema);
            testHarness.getOperator().processElement(new StreamRecord<>(addPrefix));
            testHarness.clearOutputRecords();
            BinaryRecordDataGenerator generator =
                    new BinaryRecordDataGenerator((RowType) evolvedSchema.toRowDataType());
            DataChangeEvent update =
                    DataChangeEvent.updateEvent(
                            CUSTOMERS,
                            generator.generate(
                                    new Object[] {
                                        BinaryStringData.fromString("same"),
                                        1,
                                        BinaryStringData.fromString("Alice"),
                                        12345678L
                                    }),
                            generator.generate(
                                    new Object[] {
                                        BinaryStringData.fromString("same"),
                                        2,
                                        BinaryStringData.fromString("Alice"),
                                        12345678L
                                    }));

            testHarness.getOperator().processElement(new StreamRecord<>(update));

            assertThat(testHarness.getOutputRecords()).hasSize(2);
            assertThat(
                            ((DataChangeEvent)
                                            testHarness
                                                    .getOutputRecords()
                                                    .getFirst()
                                                    .getValue()
                                                    .getPayload())
                                    .op())
                    .isEqualTo(OperationType.UPDATE_BEFORE);
        }
    }

    private DataChangeEvent customersUpdate(int beforeId, int afterId) {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator((RowType) CUSTOMERS_SCHEMA.toRowDataType());
        return DataChangeEvent.updateEvent(
                CUSTOMERS,
                generator.generate(
                        new Object[] {beforeId, BinaryStringData.fromString("Alice"), 12345678L}),
                generator.generate(
                        new Object[] {afterId, BinaryStringData.fromString("Alice"), 12345678L}),
                Collections.singletonMap("source", "test"));
    }

    private void assertSplitOutput(
            List<StreamRecord<PartitioningEvent>> output,
            DataChangeEvent original,
            boolean distributed,
            int sourcePartition) {
        assertThat(output).hasSize(2);
        PartitioningEvent retract = output.get(0).getValue();
        PartitioningEvent replacement = output.get(1).getValue();
        DataChangeEvent retractPayload = (DataChangeEvent) retract.getPayload();
        DataChangeEvent replacementPayload = (DataChangeEvent) replacement.getPayload();
        assertThat(retractPayload.op()).isEqualTo(OperationType.UPDATE_BEFORE);
        assertThat(retractPayload.before()).isEqualTo(original.before());
        assertThat(retractPayload.after()).isNull();
        assertThat(replacementPayload.op()).isEqualTo(OperationType.REPLACE);
        assertThat(replacementPayload.before()).isNull();
        assertThat(replacementPayload.after()).isEqualTo(original.after());
        assertThat(retractPayload.meta()).isEqualTo(original.meta());
        assertThat(replacementPayload.meta()).isEqualTo(original.meta());
        assertThat(retract.getTargetPartition())
                .isEqualTo(getPartitioningTarget(CUSTOMERS_SCHEMA, retractPayload));
        assertThat(replacement.getTargetPartition())
                .isEqualTo(getPartitioningTarget(CUSTOMERS_SCHEMA, replacementPayload));
        assertThat(retract.getSourcePartition()).isEqualTo(distributed ? sourcePartition : -1);
        assertThat(replacement.getSourcePartition()).isEqualTo(distributed ? sourcePartition : -1);
    }

    private int getPartitioningTarget(Schema schema, DataChangeEvent dataChangeEvent) {
        return new DefaultDataChangeEventHashFunctionProvider()
                        .getHashFunction(null, schema)
                        .hashcode(dataChangeEvent)
                % DOWNSTREAM_PARALLELISM;
    }

    private RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
            createTestHarness() {
        return createTestHarness(false);
    }

    private RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
            createTestHarness(boolean requiresPrimaryKeyUpdateSplit) {
        RegularPrePartitionOperator operator =
                new RegularPrePartitionOperator(
                        TestingSchemaRegistryGateway.SCHEMA_OPERATOR_ID,
                        DOWNSTREAM_PARALLELISM,
                        new DefaultDataChangeEventHashFunctionProvider(),
                        requiresPrimaryKeyUpdateSplit);
        return RegularEventOperatorTestHarness.with(operator, DOWNSTREAM_PARALLELISM);
    }
}
