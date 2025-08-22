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

package org.apache.flink.cdc.runtime.operators.sink;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Unit tests for the DataSinkOperator ({@link DataSinkWriterOperator}/{@link
 * DataSinkFunctionOperator} handling schema evolution events.
 */
class DataSinkOperatorWithSchemaEvolveTest {
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

    public RegularEventOperatorTestHarness<DataSinkOperatorAdapter, Event> setupHarness(
            DataSinkOperatorAdapter dataSinkWriterOperator) throws Exception {
        RegularEventOperatorTestHarness<DataSinkOperatorAdapter, Event> harness =
                RegularEventOperatorTestHarness.withDuration(
                        dataSinkWriterOperator, 1, Duration.ofSeconds(3));
        // Initialization
        harness.open();
        return harness;
    }

    private FlushEvent createFlushEvent(TableId tableId, SchemaChangeEvent postEvent) {
        return new FlushEvent(0, Collections.singletonList(tableId), postEvent.getType());
    }

    private void processSchemaChangeEvent(
            DataSinkOperatorAdapter dataSinkWriterOperator,
            RegularEventOperatorTestHarness<DataSinkOperatorAdapter, Event> schemaOperatorHarness,
            TableId tableId,
            SchemaChangeEvent event)
            throws Exception {
        // Create the flush event to process before the schema change event
        FlushEvent flushEvent = createFlushEvent(tableId, event);

        // Send flush event to SinkWriterOperator
        dataSinkWriterOperator.processElement(new StreamRecord<>(flushEvent));

        // Send schema change request to coordinator
        SchemaChangeResponse schemaEvolveResponse =
                schemaOperatorHarness.requestSchemaChangeEvent(tableId, event);
        List<SchemaChangeEvent> finishedSchemaChangeEvents =
                schemaEvolveResponse.getAppliedSchemaChangeEvents();

        // Send the schema change events to SinkWriterOperator
        finishedSchemaChangeEvents.forEach(
                e -> {
                    try {
                        dataSinkWriterOperator.processElement(new StreamRecord<>(e));
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });
    }

    private void processDataChangeEvent(
            DataSinkOperatorAdapter dataSinkWriterOperator, DataChangeEvent event)
            throws Exception {
        // Send the data change event to SinkWriterOperator
        dataSinkWriterOperator.processElement(new StreamRecord<>(event));
    }

    private void assertOutputEvents(
            RegularEventOperatorTestHarness<DataSinkOperatorAdapter, Event>
                    dataSinkWriterOperatorHarness,
            List<Event> expectedEvents) {
        Assertions.assertThat(
                        dataSinkWriterOperatorHarness.getOutputRecords().stream()
                                .map(StreamRecord::getValue)
                                .collect(Collectors.toList()))
                .isEqualTo(expectedEvents);
    }

    /**
     * This case tests the schema evolution process for handling schema change events by a sink
     * operator under normal conditions.
     */
    @Test
    void testSchemaChangeEvent() throws Exception {
        DataSinkOperatorAdapter dataSinkWriterOperator = new DataSinkOperatorAdapter();
        try (RegularEventOperatorTestHarness<DataSinkOperatorAdapter, Event>
                dataSinkWriterOperatorHarness = setupHarness(dataSinkWriterOperator)) {
            // Create CreateTableEvent
            CreateTableEvent createTableEvent =
                    new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
            // test processing CreateTableEvent
            processSchemaChangeEvent(
                    dataSinkWriterOperator,
                    dataSinkWriterOperatorHarness,
                    CUSTOMERS_TABLEID,
                    createTableEvent);
            assertOutputEvents(
                    dataSinkWriterOperatorHarness, Collections.singletonList(createTableEvent));
            dataSinkWriterOperatorHarness.clearOutputRecords();

            // Add column
            AddColumnEvent.ColumnWithPosition columnWithPosition =
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("col3", DataTypes.STRING()));
            AddColumnEvent addColumnEvent =
                    new AddColumnEvent(
                            CUSTOMERS_TABLEID, Collections.singletonList(columnWithPosition));

            // test processing AddColumnEvent
            processSchemaChangeEvent(
                    dataSinkWriterOperator,
                    dataSinkWriterOperatorHarness,
                    CUSTOMERS_TABLEID,
                    addColumnEvent);
            assertOutputEvents(
                    dataSinkWriterOperatorHarness, Collections.singletonList(addColumnEvent));
            dataSinkWriterOperatorHarness.clearOutputRecords();

            // Insert
            BinaryRecordDataGenerator recordDataGenerator =
                    new BinaryRecordDataGenerator(
                            ((RowType) CUSTOMERS_LATEST_SCHEMA.toRowDataType()));
            DataChangeEvent insertEvent =
                    DataChangeEvent.insertEvent(
                            CUSTOMERS_TABLEID,
                            recordDataGenerator.generate(
                                    new Object[] {
                                        new BinaryStringData("1"),
                                        new BinaryStringData("2"),
                                        new BinaryStringData("3"),
                                    }));

            // test processing DataChangeEvent
            processDataChangeEvent(dataSinkWriterOperator, insertEvent);
            assertOutputEvents(
                    dataSinkWriterOperatorHarness, Collections.singletonList(insertEvent));
            dataSinkWriterOperatorHarness.clearOutputRecords();
        }
    }

    /**
     * This case tests the schema evolution process for handling schema change events by a sink
     * operator after failover.
     */
    @Test
    void testSchemaChangeEventAfterFailover() throws Exception {
        DataSinkOperatorAdapter dataSinkWriterOperator = new DataSinkOperatorAdapter();
        try (RegularEventOperatorTestHarness<DataSinkOperatorAdapter, Event>
                dataSinkWriterOperatorHarness = setupHarness(dataSinkWriterOperator)) {
            dataSinkWriterOperatorHarness.registerOriginalSchema(
                    CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
            dataSinkWriterOperatorHarness.registerEvolvedSchema(
                    CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);

            // Add column
            AddColumnEvent.ColumnWithPosition columnWithPosition =
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("col3", DataTypes.STRING()));
            AddColumnEvent addColumnEvent =
                    new AddColumnEvent(
                            CUSTOMERS_TABLEID, Collections.singletonList(columnWithPosition));

            // test AddColumnEvent
            processSchemaChangeEvent(
                    dataSinkWriterOperator,
                    dataSinkWriterOperatorHarness,
                    CUSTOMERS_TABLEID,
                    addColumnEvent);
            assertOutputEvents(
                    dataSinkWriterOperatorHarness,
                    Arrays.asList(
                            new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA),
                            addColumnEvent));
            dataSinkWriterOperatorHarness.clearOutputRecords();

            // Insert
            BinaryRecordDataGenerator recordDataGenerator =
                    new BinaryRecordDataGenerator(
                            ((RowType) CUSTOMERS_LATEST_SCHEMA.toRowDataType()));
            DataChangeEvent insertEvent =
                    DataChangeEvent.insertEvent(
                            CUSTOMERS_TABLEID,
                            recordDataGenerator.generate(
                                    new Object[] {
                                        new BinaryStringData("1"),
                                        new BinaryStringData("2"),
                                        new BinaryStringData("3"),
                                    }));

            // test DataChangeEvent
            processDataChangeEvent(dataSinkWriterOperator, insertEvent);
            assertOutputEvents(
                    dataSinkWriterOperatorHarness, Collections.singletonList(insertEvent));
            dataSinkWriterOperatorHarness.clearOutputRecords();
        }
    }

    /**
     * This case tests the schema evolution process for handling data change events by a sink
     * operator under normal conditions.
     */
    @Test
    void testDataChangeEvent() throws Exception {
        DataSinkOperatorAdapter dataSinkWriterOperator = new DataSinkOperatorAdapter();
        try (RegularEventOperatorTestHarness<DataSinkOperatorAdapter, Event>
                dataSinkWriterOperatorHarness = setupHarness(dataSinkWriterOperator)) {
            // Create CreateTableEvent
            CreateTableEvent createTableEvent =
                    new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
            // test CreateTableEvent
            processSchemaChangeEvent(
                    dataSinkWriterOperator,
                    dataSinkWriterOperatorHarness,
                    CUSTOMERS_TABLEID,
                    createTableEvent);
            assertOutputEvents(
                    dataSinkWriterOperatorHarness, Collections.singletonList(createTableEvent));
            dataSinkWriterOperatorHarness.clearOutputRecords();

            // Insert
            BinaryRecordDataGenerator recordDataGenerator =
                    new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SCHEMA.toRowDataType()));
            DataChangeEvent insertEvent =
                    DataChangeEvent.insertEvent(
                            CUSTOMERS_TABLEID,
                            recordDataGenerator.generate(
                                    new Object[] {
                                        new BinaryStringData("1"), new BinaryStringData("2")
                                    }));

            // test DataChangeEvent
            processDataChangeEvent(dataSinkWriterOperator, insertEvent);
            assertOutputEvents(
                    dataSinkWriterOperatorHarness, Collections.singletonList(insertEvent));
            dataSinkWriterOperatorHarness.clearOutputRecords();
        }
    }

    /**
     * This case tests the data change process for handling schema change events by a sink operator
     * after failover.
     */
    @Test
    void testDataChangeEventAfterFailover() throws Exception {
        DataSinkOperatorAdapter dataSinkWriterOperator = new DataSinkOperatorAdapter();
        try (RegularEventOperatorTestHarness<DataSinkOperatorAdapter, Event>
                dataSinkWriterOperatorHarness = setupHarness(dataSinkWriterOperator)) {
            dataSinkWriterOperatorHarness.registerOriginalSchema(
                    CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
            dataSinkWriterOperatorHarness.registerEvolvedSchema(
                    CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);

            // Insert
            BinaryRecordDataGenerator recordDataGenerator =
                    new BinaryRecordDataGenerator(
                            ((RowType) CUSTOMERS_LATEST_SCHEMA.toRowDataType()));
            DataChangeEvent insertEvent =
                    DataChangeEvent.insertEvent(
                            CUSTOMERS_TABLEID,
                            recordDataGenerator.generate(
                                    new Object[] {
                                        new BinaryStringData("1"),
                                        new BinaryStringData("2"),
                                        new BinaryStringData("3"),
                                    }));

            // test DataChangeEvent
            processDataChangeEvent(dataSinkWriterOperator, insertEvent);
            assertOutputEvents(
                    dataSinkWriterOperatorHarness,
                    Arrays.asList(
                            new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA),
                            insertEvent));
            dataSinkWriterOperatorHarness.clearOutputRecords();
        }
    }
}
