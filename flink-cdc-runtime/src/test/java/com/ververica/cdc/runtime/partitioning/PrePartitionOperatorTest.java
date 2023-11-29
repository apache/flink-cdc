/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.partitioning;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.testutils.operators.EventOperatorTestHarness;
import com.ververica.cdc.runtime.testutils.schema.TestingSchemaRegistryGateway;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link PrePartitionOperator}. */
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
        try (EventOperatorTestHarness<PrePartitionOperator, PartitioningEvent> testHarness =
                createTestHarness()) {
            // Initialization
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);

            // CreateTableEvent
            PrePartitionOperator operator = testHarness.getOperator();
            CreateTableEvent createTableEvent = new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA);
            operator.processElement(new StreamRecord<>(createTableEvent));
            assertThat(testHarness.getOutputRecords()).hasSize(DOWNSTREAM_PARALLELISM);
            for (int i = 0; i < DOWNSTREAM_PARALLELISM; i++) {
                assertThat(testHarness.getOutputRecords().poll())
                        .isEqualTo(new StreamRecord<>(new PartitioningEvent(createTableEvent, i)));
            }
        }
    }

    @Test
    void testBroadcastingFlushEvent() throws Exception {
        try (EventOperatorTestHarness<PrePartitionOperator, PartitioningEvent> testHarness =
                createTestHarness()) {
            // Initialization
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);

            // FlushEvent
            PrePartitionOperator operator = testHarness.getOperator();
            FlushEvent flushEvent = new FlushEvent(CUSTOMERS);
            operator.processElement(new StreamRecord<>(flushEvent));
            assertThat(testHarness.getOutputRecords()).hasSize(DOWNSTREAM_PARALLELISM);
            for (int i = 0; i < DOWNSTREAM_PARALLELISM; i++) {
                assertThat(testHarness.getOutputRecords().poll())
                        .isEqualTo(new StreamRecord<>(new PartitioningEvent(flushEvent, i)));
            }
        }
    }

    @Test
    void testPartitioningDataChangeEvent() throws Exception {
        try (EventOperatorTestHarness<PrePartitionOperator, PartitioningEvent> testHarness =
                createTestHarness()) {
            // Initialization
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);

            // DataChangeEvent
            PrePartitionOperator operator = testHarness.getOperator();
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
            assertThat(testHarness.getOutputRecords().poll())
                    .isEqualTo(
                            new StreamRecord<>(
                                    new PartitioningEvent(
                                            eventA,
                                            getPartitioningTarget(CUSTOMERS_SCHEMA, eventA))));
            assertThat(testHarness.getOutputRecords().poll())
                    .isEqualTo(
                            new StreamRecord<>(
                                    new PartitioningEvent(
                                            eventB,
                                            getPartitioningTarget(CUSTOMERS_SCHEMA, eventB))));
        }
    }

    private int getPartitioningTarget(Schema schema, DataChangeEvent dataChangeEvent) {
        return new PrePartitionOperator.HashFunction(schema).apply(dataChangeEvent)
                % DOWNSTREAM_PARALLELISM;
    }

    private EventOperatorTestHarness<PrePartitionOperator, PartitioningEvent> createTestHarness() {
        PrePartitionOperator operator =
                new PrePartitionOperator(
                        TestingSchemaRegistryGateway.SCHEMA_OPERATOR_ID, DOWNSTREAM_PARALLELISM);
        return new EventOperatorTestHarness<>(operator, DOWNSTREAM_PARALLELISM);
    }
}
