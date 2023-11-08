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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.runtime.operators.schema.event.FlushEvent;
import com.ververica.cdc.runtime.operators.sink.SchemaEvolutionClient;

import java.lang.reflect.Field;

/**
 * An operator that processes records to be written into a {@link
 * org.apache.flink.api.connector.sink2.Sink}. It also has a way to process committables with the
 * same parallelism or send them downstream to a {@link CommitterOperator} with a different
 * parallelism.
 *
 * <p>The operator is always part of a sink pipeline and is the first operator.
 *
 * @param <CommT> the type of the committable (to send to downstream operators)
 */
public class DataSinkWriterOperator<CommT> extends SinkWriterOperator<Event, CommT> {

    private SchemaEvolutionClient schemaEvolutionClient;

    private SinkWriter<Event> copySinkWriter;

    private final OperatorID schemaOperatorID;

    public DataSinkWriterOperator(
            Sink<Event> sink,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            OperatorID schemaOperatorID) {
        super(sink, processingTimeService, mailboxExecutor);
        this.schemaOperatorID = schemaOperatorID;
    }

    @Override
    public void setup(StreamTask containingTask, StreamConfig config, Output output) {
        super.setup(containingTask, config, output);
        schemaEvolutionClient =
                new SchemaEvolutionClient(
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway(),
                        schemaOperatorID);
    }

    @Override
    public void open() throws Exception {
        super.open();
        copySinkWriter = (SinkWriter) getFieldValue("sinkWriter");
    }

    /**
     * Finds a field by name from its declaring class. This also searches for the field in super
     * classes.
     *
     * @param fieldName the name of the field to find.
     * @return the Object value of this field.
     */
    private Object getFieldValue(String fieldName) throws IllegalAccessException {
        Class clazz = this.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(this);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        return null;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        schemaEvolutionClient.registerSubtask(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof FlushEvent) {
            copySinkWriter.flush(false);
            schemaEvolutionClient.notifyFlushSuccess(
                    getRuntimeContext().getIndexOfThisSubtask(), ((FlushEvent) event).getTableId());
        } else {
            super.processElement(element);
        }
    }
}
