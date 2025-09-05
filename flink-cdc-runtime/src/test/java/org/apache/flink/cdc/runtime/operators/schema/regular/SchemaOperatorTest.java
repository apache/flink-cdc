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

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/** Unit tests for the {@link SchemaOperator}. */
class SchemaOperatorTest {

    private static final TableId CUSTOMERS =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("phone", DataTypes.BIGINT())
                    .primaryKey("id")
                    .build();

    @Test
    void testProcessElement() throws Exception {
        final int maxParallelism = 4;
        final int parallelism = 2;
        final OperatorID opID = new OperatorID();
        final TableId tableId = TableId.tableId("testProcessElement");
        final RowType rowType = DataTypes.ROW(DataTypes.BIGINT(), DataTypes.STRING());
        final Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        List<OneInputStreamOperatorTestHarness<Event, Event>> testHarnesses = new ArrayList<>();
        for (int subtaskIndex = 0; subtaskIndex < parallelism; subtaskIndex++) {
            SchemaOperator operator = new SchemaOperator(new ArrayList<>());
            OneInputStreamOperatorTestHarness<Event, Event> testHarness =
                    createTestHarness(maxParallelism, parallelism, subtaskIndex, opID, operator);
            testHarnesses.add(testHarness);
            testHarness.setup(EventSerializer.INSTANCE);
            testHarness.open();
            operator.registerInitialSchema(tableId, schema);

            Map<String, String> meta = new HashMap<>();
            meta.put("subtask", String.valueOf(subtaskIndex));

            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            List<Event> testData =
                    Arrays.asList(
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    generator.generate(
                                            new Object[] {1L, BinaryStringData.fromString("1")}),
                                    generator.generate(
                                            new Object[] {2L, BinaryStringData.fromString("2")}),
                                    meta),
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    generator.generate(
                                            new Object[] {3L, BinaryStringData.fromString("3")}),
                                    generator.generate(
                                            new Object[] {4L, BinaryStringData.fromString("4")}),
                                    meta));
            for (Event event : testData) {
                testHarness.processElement(event, 0);
            }

            Collection<StreamRecord<Event>> result = testHarness.getRecordOutput();
            assertThat(result.stream().map(StreamRecord::getValue).collect(Collectors.toList()))
                    .isEqualTo(testData);
        }

        for (int subtaskIndex = 0; subtaskIndex < parallelism; subtaskIndex++) {
            testHarnesses.get(subtaskIndex).close();
        }
    }

    @Test
    void testProcessSchemaChangeEventWithTimeOut() throws Exception {
        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(1));
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDuration(
                        schemaOperator, 1, Duration.ofSeconds(3));
        harness.open();
        assertThatThrownBy(
                        () ->
                                schemaOperator.processElement(
                                        new StreamRecord<>(
                                                new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA))))
                .isExactlyInstanceOf(IllegalStateException.class)
                .cause()
                .isExactlyInstanceOf(TimeoutException.class);
        harness.close();
    }

    @Test
    void testProcessSchemaChangeEventWithOutTimeOut() throws Exception {
        SchemaOperator schemaOperator =
                new SchemaOperator(new ArrayList<>(), Duration.ofSeconds(30));
        RegularEventOperatorTestHarness<SchemaOperator, Event> harness =
                RegularEventOperatorTestHarness.withDuration(
                        schemaOperator, 1, Duration.ofSeconds(3));
        harness.open();
        assertThatCode(
                        () ->
                                schemaOperator.processElement(
                                        new StreamRecord<>(
                                                new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA))))
                .doesNotThrowAnyException();
        harness.close();
    }

    private OneInputStreamOperatorTestHarness<Event, Event> createTestHarness(
            int maxParallelism,
            int parallelism,
            int subtaskIndex,
            OperatorID opID,
            OneInputStreamOperator<Event, Event> operator)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                operator,
                maxParallelism,
                parallelism,
                subtaskIndex,
                EventSerializer.INSTANCE,
                opID);
    }
}
