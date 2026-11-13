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
 * Compatibility adapter for the Flink 2.x line: gives flink-cdc main code explicit access to the
 * package-private {@code SinkWriterOperator} without reflection. This class must stay in the same
 * package as {@code SinkWriterOperator}; each Flink generation carries its own copy in its common
 * module, compiled against that generation.
 */
@Internal
public class SinkWriterOperatorAdapter {

    private SinkWriterOperatorAdapter() {}

    /**
     * Creates a {@code SinkWriterOperator} with the Flink 2.x 4-argument constructor. The {@code
     * StreamOperatorParameters} argument may be null, matching the legacy reflective fallback path
     * this adapter replaces.
     */
    public static <InputT, CommT>
            AbstractStreamOperator<CommittableMessage<CommT>> createWriterOperator(
                    Sink<InputT> sink,
                    ProcessingTimeService processingTimeService,
                    MailboxExecutor mailboxExecutor) {
        return new SinkWriterOperator<>(null, sink, processingTimeService, mailboxExecutor);
    }

    /**
     * Sets up the wrapped writer operator. On Flink 2.x {@code setup} is protected on {@code
     * SinkWriterOperator}; being in the same package allows a direct call after the cast. The raw
     * casts bridge the operator's own type parameter, which the adapter hides from callers.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void setupWriterOperator(
            AbstractStreamOperator<?> operator,
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<?> output) {
        if (!(operator instanceof SinkWriterOperator)) {
            throw new IllegalStateException(
                    "Expected a SinkWriterOperator but got: " + operator.getClass().getName());
        }
        ((SinkWriterOperator) operator).setup(containingTask, config, output);
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
