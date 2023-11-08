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

    SchemaEvolutionClient schemaEvolutionClient;

    private SinkWriter copySinkWriter;

    private final OperatorID schemaOperatorID;

    DataSinkWriterOperator(
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
        Class<? extends SinkWriterOperator> cls = this.getClass();
        Field field = cls.getDeclaredField("sinkWriter");
        field.setAccessible(true);
        copySinkWriter = (SinkWriter) field.get(this);
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
