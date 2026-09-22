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

package org.apache.flink.cdc.composer.testsink.factory;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** A test-only sink that fails one sink subtask while schema evolution is in progress. */
public class FailingDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "schema-evolution-failing-sink";
    public static final String ORIGINAL_FAILURE_MESSAGE = "Intentional sink subtask failure";

    private static volatile CountDownLatch metadataApplying = new CountDownLatch(1);
    private static volatile CountDownLatch releaseMetadataApplier = new CountDownLatch(1);
    private static volatile CountDownLatch sinkFailureTriggered = new CountDownLatch(1);
    private static final AtomicBoolean failOnce = new AtomicBoolean();

    public static void reset() {
        metadataApplying = new CountDownLatch(1);
        releaseMetadataApplier = new CountDownLatch(1);
        sinkFailureTriggered = new CountDownLatch(1);
        failOnce.set(false);
    }

    public static boolean awaitMetadataApplying(Duration timeout) throws InterruptedException {
        return metadataApplying.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static boolean awaitSinkFailure(Duration timeout) throws InterruptedException {
        return sinkFailureTriggered.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static void releaseMetadataApplier() {
        releaseMetadataApplier.countDown();
    }

    @Override
    public DataSink createDataSink(Context context) {
        return new FailingDataSink();
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    private static class FailingDataSink implements DataSink {

        @Override
        public EventSinkProvider getEventSinkProvider() {
            return FlinkSinkProvider.of(new FailingSink());
        }

        @Override
        public MetadataApplier getMetadataApplier() {
            return new BlockingMetadataApplier();
        }
    }

    private static class FailingSink implements Sink<Event> {

        @Override
        public SinkWriter<Event> createWriter(InitContext context) {
            return new FailingSinkWriter(getTaskEnvironment(context), context.getSubtaskId());
        }

        private static Environment getTaskEnvironment(InitContext context) {
            // Sink.InitContext does not expose the task environment needed for an out-of-band
            // failure.
            try {
                Class<?> initContextBase =
                        Class.forName(
                                "org.apache.flink.streaming.runtime.operators.sink.InitContextBase");
                Object initContext = unwrapInitContext(context, initContextBase);
                Field runtimeContextField = initContextBase.getDeclaredField("runtimeContext");
                runtimeContextField.setAccessible(true);
                Object runtimeContext = runtimeContextField.get(initContext);
                if (!(runtimeContext instanceof StreamingRuntimeContext)) {
                    throw new IllegalStateException(
                            "Unexpected runtime context: "
                                    + (runtimeContext == null
                                            ? "null"
                                            : runtimeContext.getClass().getName()));
                }

                Field taskEnvironment =
                        StreamingRuntimeContext.class.getDeclaredField("taskEnvironment");
                taskEnvironment.setAccessible(true);
                return (Environment) taskEnvironment.get(runtimeContext);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Cannot access the task environment.", e);
            }
        }

        private static Object unwrapInitContext(Object context, Class<?> initContextBase)
                throws ReflectiveOperationException {
            Object current = context;
            while (!initContextBase.isInstance(current)) {
                String className = current.getClass().getName();
                String wrappedField;
                if (className.endsWith("Sink$InitContextWrapper")) {
                    wrappedField = "wrapped";
                } else if (className.equals(
                        "org.apache.flink.api.connector.sink2.InitContextAdapter")) {
                    wrappedField = "context";
                } else {
                    throw new NoSuchFieldException("Cannot unwrap init context " + className);
                }
                Field wrapped = current.getClass().getDeclaredField(wrappedField);
                wrapped.setAccessible(true);
                current = wrapped.get(current);
            }
            return current;
        }
    }

    private static class FailingSinkWriter implements SinkWriter<Event> {

        private final Thread failureThread;

        private FailingSinkWriter(Environment taskEnvironment, int subtaskId) {
            CountDownLatch metadataApplyingLatch = metadataApplying;
            failureThread =
                    new Thread(
                            () ->
                                    failTaskAfterSchemaEvolutionStarts(
                                            taskEnvironment, subtaskId, metadataApplyingLatch),
                            "schema-evolution-sink-failure");
            failureThread.setDaemon(true);
            failureThread.start();
        }

        private static void failTaskAfterSchemaEvolutionStarts(
                Environment taskEnvironment, int subtaskId, CountDownLatch metadataApplyingLatch) {
            if (subtaskId != 0 || failOnce.get()) {
                return;
            }

            try {
                metadataApplyingLatch.await();
                if (failOnce.compareAndSet(false, true)) {
                    sinkFailureTriggered.countDown();
                    // Fail the task externally so the sink remains chained with the schema
                    // operator, independent of the sink's blocked mailbox thread.
                    taskEnvironment.failExternally(new RuntimeException(ORIGINAL_FAILURE_MESSAGE));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void write(Event element, Context context) {}

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {
            failureThread.interrupt();
        }
    }

    private static class BlockingMetadataApplier implements MetadataApplier {

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
                throws SchemaEvolveException {
            metadataApplying.countDown();
            try {
                releaseMetadataApplier.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SchemaEvolveException(
                        schemaChangeEvent, "Blocking metadata applier was interrupted.", e);
            }
        }
    }
}
