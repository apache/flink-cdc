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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DefaultDataChangeEventHashFunctionProvider;
import org.apache.flink.cdc.common.sink.TableIdHashFunctionProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.testutils.schema.TestingSchemaRegistryGateway;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for pre-partition operators. */
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
    void testBroadcastingDropTableEvent() throws Exception {
        try (RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
                testHarness = createTestHarness()) {
            // Initialization
            testHarness.open();

            RegularPrePartitionOperator operator = testHarness.getOperator();
            DropTableEvent dropTableEvent = new DropTableEvent(CUSTOMERS);
            operator.processElement(new StreamRecord<>(dropTableEvent));

            assertThat(testHarness.getOutputRecords()).hasSize(DOWNSTREAM_PARALLELISM);
            for (int i = 0; i < DOWNSTREAM_PARALLELISM; i++) {
                assertThat(testHarness.getOutputRecords().poll())
                        .isEqualTo(
                                new StreamRecord<>(PartitioningEvent.ofRegular(dropTableEvent, i)));
            }
        }
    }

    @Test
    void testDistributedDropTableEventDoesNotRecreateHashFunction() throws Exception {
        AtomicInteger hashFunctionCreations = new AtomicInteger();
        DistributedPrePartitionOperator operator =
                new DistributedPrePartitionOperator(
                        DOWNSTREAM_PARALLELISM,
                        (tableId, schema) -> {
                            hashFunctionCreations.incrementAndGet();
                            return event -> 0;
                        });
        try (RegularEventOperatorTestHarness<DistributedPrePartitionOperator, PartitioningEvent>
                testHarness =
                        RegularEventOperatorTestHarness.with(operator, DOWNSTREAM_PARALLELISM)) {
            testHarness.open();

            CreateTableEvent createTableEvent = new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA);
            operator.processElement(new StreamRecord<>(createTableEvent));
            assertThat(hashFunctionCreations).hasValue(1);
            testHarness.clearOutputRecords();

            DropTableEvent dropTableEvent = new DropTableEvent(CUSTOMERS);
            operator.processElement(new StreamRecord<>(dropTableEvent));

            assertThat(hashFunctionCreations).hasValue(1);
            assertThat(testHarness.getOutputRecords()).hasSize(DOWNSTREAM_PARALLELISM);
            for (int i = 0; i < DOWNSTREAM_PARALLELISM; i++) {
                assertThat(testHarness.getOutputRecords().poll())
                        .isEqualTo(
                                new StreamRecord<>(
                                        PartitioningEvent.ofDistributed(dropTableEvent, 0, i)));
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

    private int getPartitioningTarget(Schema schema, DataChangeEvent dataChangeEvent) {
        return new DefaultDataChangeEventHashFunctionProvider()
                        .getHashFunction(null, schema)
                        .hashcode(dataChangeEvent)
                % DOWNSTREAM_PARALLELISM;
    }

    @Test
    void testPartitioningDataChangeEventWithTableIdStrategy() throws Exception {
        try (RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
                testHarness = createTableIdStrategyTestHarness()) {
            // Initialization
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);

            // DataChangeEvent with different primary key values
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
            DataChangeEvent eventC =
                    DataChangeEvent.insertEvent(
                            CUSTOMERS,
                            recordDataGenerator.generate(
                                    new Object[] {
                                        100, new BinaryStringData("Charlie"), 99999999L
                                    }));
            operator.processElement(new StreamRecord<>(eventA));
            operator.processElement(new StreamRecord<>(eventB));
            operator.processElement(new StreamRecord<>(eventC));

            // All events from the same table should be routed to the same subtask
            StreamRecord<?> recordA = testHarness.getOutputRecords().poll();
            StreamRecord<?> recordB = testHarness.getOutputRecords().poll();
            StreamRecord<?> recordC = testHarness.getOutputRecords().poll();

            int targetA = ((PartitioningEvent) recordA.getValue()).getTargetPartition();
            int targetB = ((PartitioningEvent) recordB.getValue()).getTargetPartition();
            int targetC = ((PartitioningEvent) recordC.getValue()).getTargetPartition();

            // Verify all events land on the same subtask
            assertThat(targetA).isEqualTo(targetB);
            assertThat(targetB).isEqualTo(targetC);

            // Verify the target is calculated based on TableId only
            int expectedTarget = getTableIdPartitioningTarget(CUSTOMERS);
            assertThat(targetA).isEqualTo(expectedTarget);
        }
    }

    private int getTableIdPartitioningTarget(TableId tableId) {
        return new TableIdHashFunctionProvider()
                        .getHashFunction(tableId, CUSTOMERS_SCHEMA)
                        .hashcode(
                                DataChangeEvent.insertEvent(
                                        tableId,
                                        org.apache.flink.cdc.common.data.GenericRecordData.of()))
                % DOWNSTREAM_PARALLELISM;
    }

    private RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
            createTableIdStrategyTestHarness() {
        RegularPrePartitionOperator operator =
                new RegularPrePartitionOperator(
                        TestingSchemaRegistryGateway.SCHEMA_OPERATOR_ID,
                        DOWNSTREAM_PARALLELISM,
                        new TableIdHashFunctionProvider());
        return RegularEventOperatorTestHarness.with(operator, DOWNSTREAM_PARALLELISM);
    }

    private RegularEventOperatorTestHarness<RegularPrePartitionOperator, PartitioningEvent>
            createTestHarness() {
        RegularPrePartitionOperator operator =
                new RegularPrePartitionOperator(
                        TestingSchemaRegistryGateway.SCHEMA_OPERATOR_ID,
                        DOWNSTREAM_PARALLELISM,
                        new DefaultDataChangeEventHashFunctionProvider());
        return RegularEventOperatorTestHarness.with(operator, DOWNSTREAM_PARALLELISM);
    }
}
