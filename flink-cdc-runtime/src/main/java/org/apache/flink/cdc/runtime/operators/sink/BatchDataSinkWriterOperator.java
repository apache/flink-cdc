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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.runtime.operators.sink.exception.SinkWrapperException;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * An operator that processes records to be written into a {@link Sink} in batch mode.
 *
 * <p>The operator is a proxy of SinkWriterOperator in Flink.
 *
 * <p>The operator is always part of a sink pipeline and is the first operator.
 *
 * @param <CommT> the type of the committable (to send to downstream operators)
 */
@Internal
public class BatchDataSinkWriterOperator<CommT>
        extends AbstractStreamOperator<CommittableMessage<CommT>>
        implements OneInputStreamOperator<Event, CommittableMessage<CommT>>, BoundedOneInput {

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

    public BatchDataSinkWriterOperator(
            Sink<Event> sink,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor) {
        this.sink = sink;
        this.processingTimeService = processingTimeService;
        this.mailboxExecutor = mailboxExecutor;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<CommittableMessage<CommT>>> output) {
        super.setup(containingTask, config, output);
        flinkWriterOperator = createFlinkWriterOperator();
        invokeFlinkWriterOperatorMethod(
                "setup",
                new Class<?>[] {StreamTask.class, StreamConfig.class, Output.class},
                containingTask,
                config,
                output);
    }

    @Override
    public void open() throws Exception {
        invokeFlinkWriterOperatorMethod("open", new Class<?>[0]);
        copySinkWriter = getFieldValue("sinkWriter");
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        invokeFlinkWriterOperatorMethod(
                "initializeState", new Class<?>[] {StateInitializationContext.class}, context);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        invokeFlinkWriterOperatorMethod(
                "snapshotState", new Class<?>[] {StateSnapshotContext.class}, context);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        invokeFlinkWriterOperatorMethod("processWatermark", new Class<?>[] {Watermark.class}, mark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        super.processWatermarkStatus(watermarkStatus);
        invokeFlinkWriterOperatorMethod(
                "processWatermarkStatus", new Class<?>[] {WatermarkStatus.class}, watermarkStatus);
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();

        try {
            this.<OneInputStreamOperator<Event, CommittableMessage<CommT>>>getFlinkWriterOperator()
                    .processElement(element);
        } catch (Exception e) {
            throw new SinkWrapperException(event, e);
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        invokeFlinkWriterOperatorMethod(
                "prepareSnapshotPreBarrier", new Class<?>[] {long.class}, checkpointId);
    }

    @Override
    public void close() throws Exception {
        invokeFlinkWriterOperatorMethod("close", new Class<?>[0]);
    }

    @Override
    public void endInput() throws Exception {
        invokeFlinkWriterOperatorMethod("endInput", new Class<?>[0]);
    }

    // ----------------------------- Helper functions -------------------------------

    private void handleFlushEvent(FlushEvent event) throws Exception {
        copySinkWriter.flush(false);
    }

    // -------------------------- Reflection helper functions --------------------------

    private Object createFlinkWriterOperator() {
        try {
            Class<?> flinkWriterClass =
                    getRuntimeContext()
                            .getUserCodeClassLoader()
                            .loadClass(
                                    "org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator");

            // Try to find a constructor whose first three parameters are compatible with
            // (Sink, ProcessingTimeService, MailboxExecutor). This makes the code resilient
            // against Flink 2.x adding extra parameters to the constructor.
            Constructor<?> target = null;
            for (Constructor<?> c : flinkWriterClass.getDeclaredConstructors()) {
                Class<?>[] p = c.getParameterTypes();
                if (p.length >= 3
                        && Sink.class.isAssignableFrom(p[0])
                        && ProcessingTimeService.class.isAssignableFrom(p[1])
                        && MailboxExecutor.class.isAssignableFrom(p[2])) {
                    target = c;
                    break;
                }
            }

            if (target == null) {
                // Fallback: use the first declared constructor and best-effort argument mapping
                // below. This covers Flink 2.2 where the constructor signature may have changed.
                Constructor<?>[] all = flinkWriterClass.getDeclaredConstructors();
                if (all.length == 0) {
                    throw new RuntimeException(
                            "No constructors found on SinkWriterOperator in Flink runtime");
                }
                target = all[0];
            }

            target.setAccessible(true);
            Class<?>[] paramTypes = target.getParameterTypes();
            Object[] args = new Object[paramTypes.length];
            for (int i = 0; i < paramTypes.length; i++) {
                Class<?> t = paramTypes[i];
                if (Sink.class.isAssignableFrom(t)) {
                    args[i] = sink;
                } else if (ProcessingTimeService.class.isAssignableFrom(t)) {
                    args[i] = processingTimeService;
                } else if (MailboxExecutor.class.isAssignableFrom(t)) {
                    args[i] = mailboxExecutor;
                } else if (t.isPrimitive()) {
                    if (t == boolean.class) {
                        args[i] = false;
                    } else if (t == char.class) {
                        args[i] = '\0';
                    } else if (t == byte.class) {
                        args[i] = (byte) 0;
                    } else if (t == short.class) {
                        args[i] = (short) 0;
                    } else if (t == int.class) {
                        args[i] = 0;
                    } else if (t == long.class) {
                        args[i] = 0L;
                    } else if (t == float.class) {
                        args[i] = 0.0f;
                    } else if (t == double.class) {
                        args[i] = 0.0d;
                    }
                } else {
                    // Best effort: pass null for any extra, unknown parameters.
                    args[i] = null;
                }
            }
            return target.newInstance(args);
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

    private void invokeFlinkWriterOperatorMethod(
            String methodName, Class<?>[] parameterTypes, Object... args) {
        try {
            Method m = flinkWriterOperator.getClass().getDeclaredMethod(methodName, parameterTypes);
            m.setAccessible(true);
            m.invoke(flinkWriterOperator, args);
        } catch (NoSuchMethodException e) {
            // Method does not exist in this Flink version (for example, open() signature changed);
            // ignore for compatibility.
            return;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to invoke method "
                            + methodName
                            + " on wrapped flink writer operator "
                            + flinkWriterOperator.getClass().getName(),
                    e);
        }
    }
}
