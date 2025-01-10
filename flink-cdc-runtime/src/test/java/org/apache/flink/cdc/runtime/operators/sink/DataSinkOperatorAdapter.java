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

import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * The DataSinkOperatorAdapter class acts as an adapter for testing the core schema evolution
 * process in both {@link DataSinkWriterOperator} and {@link DataSinkFunctionOperator}.
 */
public class DataSinkOperatorAdapter extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, BoundedOneInput {

    private SchemaEvolutionClient schemaEvolutionClient;

    private final OperatorID schemaOperatorID;

    /** A set of {@link TableId} that already processed {@link CreateTableEvent}. */
    private final Set<TableId> processedTableIds;

    public DataSinkOperatorAdapter() {
        this.schemaOperatorID = new OperatorID();
        this.processedTableIds = new HashSet<>();
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        schemaEvolutionClient =
                new SchemaEvolutionClient(
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway(),
                        schemaOperatorID);
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        schemaEvolutionClient.registerSubtask(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void snapshotState(StateSnapshotContext context) {}

    @Override
    public void processWatermark(Watermark mark) {}

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) {}

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();

        // FlushEvent triggers flush
        if (event instanceof FlushEvent) {
            handleFlushEvent(((FlushEvent) event));
            return;
        }

        // CreateTableEvent marks the table as processed directly
        if (event instanceof CreateTableEvent) {
            processedTableIds.add(((CreateTableEvent) event).tableId());
            // replace FlinkWriterOperator/StreamSink and emit the event for testing
            output.collect(element);
            return;
        }

        // Check if the table is processed before emitting all other events, because we have to make
        // sure that sink have a view of the full schema before processing any change events,
        // including schema changes.
        ChangeEvent changeEvent = (ChangeEvent) event;
        if (!processedTableIds.contains(changeEvent.tableId())) {
            emitLatestSchema(changeEvent.tableId());
            processedTableIds.add(changeEvent.tableId());
        }
        processedTableIds.add(changeEvent.tableId());
        output.collect(element);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {}

    @Override
    public void close() throws Exception {}

    @Override
    public void endInput() {}

    // ----------------------------- Helper functions -------------------------------

    private void handleFlushEvent(FlushEvent event) throws Exception {
        // omit copySinkWriter/userFunction flush from testing
        if (event.getSchemaChangeEventType() != SchemaChangeEventType.CREATE_TABLE) {
            event.getTableIds().stream()
                    .filter(tableId -> !processedTableIds.contains(tableId))
                    .forEach(
                            tableId -> {
                                LOG.info("Table {} has not been processed", tableId);
                                try {
                                    emitLatestSchema(tableId);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                                processedTableIds.add(tableId);
                            });
        }
        schemaEvolutionClient.notifyFlushSuccess(
                getRuntimeContext().getIndexOfThisSubtask(), event.getSourceSubTaskId());
    }

    private void emitLatestSchema(TableId tableId) throws Exception {
        Optional<Schema> schema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
        if (schema.isPresent()) {
            // request and process CreateTableEvent because SinkWriter need to retrieve
            // Schema to deserialize RecordData after resuming job.
            output.collect(new StreamRecord<>(new CreateTableEvent(tableId, schema.get())));
            processedTableIds.add(tableId);
        } else {
            throw new RuntimeException(
                    "Could not find schema message from SchemaRegistry for " + tableId);
        }
    }
}
