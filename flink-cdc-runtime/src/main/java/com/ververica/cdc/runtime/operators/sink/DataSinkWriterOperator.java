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

package com.ververica.cdc.runtime.operators.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * An operator that processes records to be written into a {@link
 * org.apache.flink.api.connector.sink2.Sink}.
 *
 * <p>The operator is a proxy of SinkWriterOperator in Flink.
 *
 * <p>The operator is always part of a sink pipeline and is the first operator.
 *
 * @param <CommT> the type of the committable (to send to downstream operators)
 */
@Internal
public class DataSinkWriterOperator<CommT> extends AbstractStreamOperator<CommittableMessage<CommT>>
        implements OneInputStreamOperator<Event, CommittableMessage<CommT>>, BoundedOneInput {

    private SchemaEvolutionClient schemaEvolutionClient;

    private final OperatorID schemaOperatorID;

    private final Sink<Event> sink;

    private final ProcessingTimeService processingTimeService;

    private final MailboxExecutor mailboxExecutor;

    /** Operator that actually execute sink logic. */
    private Object flinkWriterOperator;

    /**
     * The internal {@link SinkWriter} of flinkWriterOperator, obtained it through reflection to
     * deal with {@link FlushEvent}.
     */
    private SinkWriter<Event> copySinkWriter;

    /** A set of {@link TableId} that already processed {@link CreateTableEvent}. */
    private final Set<TableId> processedTableIds;

    public DataSinkWriterOperator(
            Sink<Event> sink,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            OperatorID schemaOperatorID) {
        this.sink = sink;
        this.processingTimeService = processingTimeService;
        this.mailboxExecutor = mailboxExecutor;
        this.schemaOperatorID = schemaOperatorID;
        this.processedTableIds = new HashSet<>();
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<CommittableMessage<CommT>>> output) {
        super.setup(containingTask, config, output);
        flinkWriterOperator = createFlinkWriterOperator();
        this.<AbstractStreamOperator<CommittableMessage<CommT>>>getFlinkWriterOperator()
                .setup(containingTask, config, output);
        schemaEvolutionClient =
                new SchemaEvolutionClient(
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway(),
                        schemaOperatorID);
    }

    @Override
    public void open() throws Exception {
        this.<AbstractStreamOperator<CommittableMessage<CommT>>>getFlinkWriterOperator().open();
        copySinkWriter = getFieldValue("sinkWriter");
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        schemaEvolutionClient.registerSubtask(getRuntimeContext().getIndexOfThisSubtask());
        this.<AbstractStreamOperator<CommittableMessage<CommT>>>getFlinkWriterOperator()
                .initializeState(context);
    }

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
            this.<OneInputStreamOperator<Event, CommittableMessage<CommT>>>getFlinkWriterOperator()
                    .processElement(element);
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
        this.<OneInputStreamOperator<Event, CommittableMessage<CommT>>>getFlinkWriterOperator()
                .processElement(element);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        this.<AbstractStreamOperator<CommittableMessage<CommT>>>getFlinkWriterOperator()
                .prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void close() throws Exception {
        this.<OneInputStreamOperator<Event, CommittableMessage<CommT>>>getFlinkWriterOperator()
                .close();
    }

    @Override
    public void endInput() throws Exception {
        this.<BoundedOneInput>getFlinkWriterOperator().endInput();
    }

    // ----------------------------- Helper functions -------------------------------

    private void handleFlushEvent(FlushEvent event) throws Exception {
        copySinkWriter.flush(false);
        schemaEvolutionClient.notifyFlushSuccess(
                getRuntimeContext().getIndexOfThisSubtask(), event.getTableId());
    }

    private void emitLatestSchema(TableId tableId) throws Exception {
        Optional<Schema> schema = schemaEvolutionClient.getLatestSchema(tableId);
        if (schema.isPresent()) {
            // request and process CreateTableEvent because SinkWriter need to retrieve
            // Schema to deserialize RecordData after resuming job.
            this.<OneInputStreamOperator<Event, CommittableMessage<CommT>>>getFlinkWriterOperator()
                    .processElement(
                            new StreamRecord<>(new CreateTableEvent(tableId, schema.get())));
            processedTableIds.add(tableId);
        } else {
            throw new RuntimeException(
                    "Could not find schema message from SchemaRegistry for " + tableId);
        }
    }

    // -------------------------- Reflection helper functions --------------------------

    private Object createFlinkWriterOperator() {
        try {
            Class<?> flinkWriterClass =
                    getRuntimeContext()
                            .getUserCodeClassLoader()
                            .loadClass(
                                    "org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator");
            Constructor<?> constructor =
                    flinkWriterClass.getDeclaredConstructor(
                            Sink.class, ProcessingTimeService.class, MailboxExecutor.class);
            constructor.setAccessible(true);
            return constructor.newInstance(sink, processingTimeService, mailboxExecutor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SinkWriterOperator in Flink", e);
        }
    }

    /**
     * Finds a field by name from its declaring class. This also searches for the field in super
     * classes.
     *
     * @param fieldName the name of the field to find.
     * @return the Object value of this field.
     */
    @SuppressWarnings("unchecked")
    private <T> T getFieldValue(String fieldName) throws IllegalAccessException {
        Class<?> clazz = flinkWriterOperator.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return ((T) field.get(flinkWriterOperator));
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new RuntimeException("failed to get sinkWriter");
    }

    @SuppressWarnings("unchecked")
    private <T> T getFlinkWriterOperator() {
        return (T) flinkWriterOperator;
    }
}
