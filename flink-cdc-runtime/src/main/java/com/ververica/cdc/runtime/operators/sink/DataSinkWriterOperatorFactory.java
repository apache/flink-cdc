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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.Event;

/** Operator factory for {@link DataSinkWriterOperator}. */
@Internal
public class DataSinkWriterOperatorFactory<CommT>
        extends AbstractStreamOperatorFactory<CommittableMessage<CommT>>
        implements OneInputStreamOperatorFactory<Event, CommittableMessage<CommT>>,
                YieldingOperatorFactory<CommittableMessage<CommT>> {

    private final Sink<Event> sink;
    private final OperatorID schemaOperatorID;

    public DataSinkWriterOperatorFactory(Sink<Event> sink, OperatorID schemaOperatorID) {
        this.sink = sink;
        this.schemaOperatorID = schemaOperatorID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends StreamOperator<CommittableMessage<CommT>>> T createStreamOperator(
            StreamOperatorParameters<CommittableMessage<CommT>> parameters) {
        DataSinkWriterOperator<CommT> writerOperator =
                new DataSinkWriterOperator<>(
                        sink, processingTimeService, getMailboxExecutor(), schemaOperatorID);
        writerOperator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        return (T) writerOperator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return DataSinkWriterOperator.class;
    }
}
