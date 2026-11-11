/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.lang.reflect.Field;

/**
 * Compatibility adapter for the Flink 1.20 line: gives flink-cdc main code explicit access to the
 * package-private {@code SinkWriterOperator} without reflection. This class must stay in the same
 * package as {@code SinkWriterOperator}; each Flink generation carries its own copy in its common
 * module, compiled against that generation.
 */
@Internal
public class SinkWriterOperatorAdapter {

    private SinkWriterOperatorAdapter() {}

    /** Creates a {@code SinkWriterOperator} with the Flink 1.20 3-argument constructor. */
    public static <InputT, CommT>
            AbstractStreamOperator<CommittableMessage<CommT>> createWriterOperator(
                    Sink<InputT> sink,
                    ProcessingTimeService processingTimeService,
                    MailboxExecutor mailboxExecutor) {
        return new SinkWriterOperator<>(sink, processingTimeService, mailboxExecutor);
    }

    /**
     * Sets up the wrapped writer operator. {@code AbstractStreamOperator#setup} is public on Flink
     * 1.20, so this is a plain call here; the 2.x adapter overrides the visibility. The raw casts
     * bridge the operator's own type parameter, which the adapter hides from callers.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void setupWriterOperator(
            AbstractStreamOperator<?> operator,
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<?> output) {
        ((AbstractStreamOperator) operator).setup(containingTask, config, output);
    }

    /**
     * Returns the writer operator's inner {@link SinkWriter}. The field is private in {@code
     * SinkWriterOperator} with no accessor, so one localized reflection read remains inside the
     * compatibility layer; flink-cdc main code itself holds no reflection.
     */
    public static <InputT> SinkWriter<InputT> getSinkWriter(AbstractStreamOperator<?> operator) {
        try {
            Field sinkWriterField = SinkWriterOperator.class.getDeclaredField("sinkWriter");
            sinkWriterField.setAccessible(true);
            return (SinkWriter<InputT>) sinkWriterField.get(operator);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Failed to get sinkWriter from SinkWriterOperator", e);
        }
    }
}
