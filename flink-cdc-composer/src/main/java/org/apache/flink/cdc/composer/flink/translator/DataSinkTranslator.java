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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.FlinkEnvironmentUtils;
import org.apache.flink.cdc.composer.flink.compat.FlinkPipelineBridges;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.flink.compat.FlinkPipelineBridge;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkWriterOperatorFactory;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Translator used to build {@link DataSink} for given {@link DataStream}. */
@Internal
public class DataSinkTranslator {

    private static final String SINK_WRITER_PREFIX = "Sink Writer: ";
    private static final String SINK_COMMITTER_PREFIX = "Sink Committer: ";

    public DataSink createDataSink(
            SinkDef sinkDef, Configuration pipelineConfig, StreamExecutionEnvironment env) {
        // Search the data sink factory
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sinkDef.getType(), DataSinkFactory.class);

        // Include sink connector JAR
        FactoryDiscoveryUtils.getJarPathByIdentifier(sinkFactory)
                .ifPresent(jar -> FlinkEnvironmentUtils.addJar(env, jar));

        // Create data sink
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        sinkDef.getConfig(),
                        pipelineConfig,
                        Thread.currentThread().getContextClassLoader(),
                        env.getConfiguration()));
    }

    public void translate(
            SinkDef sinkDef,
            DataStream<Event> input,
            DataSink dataSink,
            OperatorID schemaOperatorID,
            OperatorUidGenerator operatorUidGenerator) {
        translate(sinkDef, input, dataSink, false, schemaOperatorID, operatorUidGenerator);
    }

    public void translate(
            SinkDef sinkDef,
            DataStream<Event> input,
            DataSink dataSink,
            boolean isBatchMode,
            OperatorID schemaOperatorID,
            OperatorUidGenerator operatorUidGenerator) {
        EventSinkProvider eventSinkProvider = dataSink.getEventSinkProvider();
        if (eventSinkProvider == null) {
            return;
        }
        String sinkName = generateSinkName(sinkDef);
        if (eventSinkProvider instanceof FlinkSinkProvider) {
            FlinkSinkProvider sinkProvider = (FlinkSinkProvider) eventSinkProvider;
            Sink<Event> sink = sinkProvider.getSink();
            sinkTo(input, sink, sinkName, isBatchMode, schemaOperatorID, operatorUidGenerator);
        } else {
            // SinkFunction path (Flink 1.x compat)
            FlinkPipelineBridge bridge = FlinkPipelineBridges.getDefault();
            bridge.sinkTo(
                    input,
                    eventSinkProvider,
                    sinkName,
                    isBatchMode,
                    schemaOperatorID,
                    operatorUidGenerator::generateUid);
        }
    }

    @VisibleForTesting
    void sinkTo(
            DataStream<Event> input,
            Sink<Event> sink,
            String sinkName,
            boolean isBatchMode,
            OperatorID schemaOperatorID,
            OperatorUidGenerator operatorUidGenerator) {
        DataStream<Event> stream = input;
        // Pre-write topology (reflection for Flink 1.x WithPreWriteTopology vs 2.x
        // SupportsPreWriteTopology)
        stream = addPreWriteTopologyIfSupported(sink, stream);

        if (supportsTwoPhaseCommit(sink)) {
            addCommittingTopology(
                    sink, stream, sinkName, isBatchMode, schemaOperatorID, operatorUidGenerator);
        } else {
            stream.transform(
                            SINK_WRITER_PREFIX + sinkName,
                            CommittableMessageTypeInfo.noOutput(),
                            new DataSinkWriterOperatorFactory<>(
                                    sink, isBatchMode, schemaOperatorID))
                    .uid(operatorUidGenerator.generateUid("sink-writer"));
        }
    }

    /**
     * True if sink has createCommitter and getCommittableSerializer (Flink 1.x
     * TwoPhaseCommittingSink vs 2.x SupportsCommitter).
     *
     * <p>We must also consider methods declared on superclasses (for example {@code
     * PaimonEventSink} extends {@code PaimonSink} which declares {@code createCommitter}). Using
     * {@code getDeclaredMethods()} only on the concrete class would miss those and incorrectly
     * disable the committer stage.
     */
    private static boolean supportsTwoPhaseCommit(Sink<Event> sink) {
        try {
            boolean hasCreateCommitter = false;
            boolean hasGetCommittableSerializer = false;

            // getMethods() returns all public methods, including those from superclasses and
            // interfaces, without forcing us to load 1.x-only types such as Sink.InitContext.
            for (Method m : sink.getClass().getMethods()) {
                if ("createCommitter".equals(m.getName()) && m.getParameterCount() == 0) {
                    hasCreateCommitter = true;
                }
                if ("getCommittableSerializer".equals(m.getName()) && m.getParameterCount() == 0) {
                    hasGetCommittableSerializer = true;
                }
            }
            return hasCreateCommitter && hasGetCommittableSerializer;
        } catch (NoClassDefFoundError e) {
            // Sink class references 1.x-only types (e.g. Sink.InitContext); assume two-phase
            return true;
        }
    }

    /**
     * Calls addPreWriteTopology via reflection to support both Flink 1.x (WithPreWriteTopology) and
     * 2.x (SupportsPreWriteTopology).
     */
    private static DataStream<Event> addPreWriteTopologyIfSupported(
            Sink<Event> sink, DataStream<Event> stream) {
        try {
            Method m = sink.getClass().getMethod("addPreWriteTopology", DataStream.class);
            @SuppressWarnings("unchecked")
            DataStream<Event> result = (DataStream<Event>) m.invoke(sink, stream);
            return result != null ? result : stream;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return stream;
        }
    }

    private <CommT> void addCommittingTopology(
            Sink<Event> sink,
            DataStream<Event> inputStream,
            String sinkName,
            boolean isBatchMode,
            OperatorID schemaOperatorID,
            OperatorUidGenerator operatorUidGenerator) {
        TypeInformation<CommittableMessage<CommT>> typeInformation =
                CommittableMessageTypeInfo.of(() -> getCommittableSerializer(sink));
        DataStream<CommittableMessage<CommT>> written =
                inputStream
                        .transform(
                                SINK_WRITER_PREFIX + sinkName,
                                typeInformation,
                                new DataSinkWriterOperatorFactory<>(
                                        sink, isBatchMode, schemaOperatorID))
                        .uid(operatorUidGenerator.generateUid("sink-writer"));

        DataStream<CommittableMessage<CommT>> preCommitted =
                addPreCommitTopologyIfSupported(sink, written);

        // TODO: Hard coding checkpoint
        boolean isCheckpointingEnabled = true;
        DataStream<CommittableMessage<CommT>> committed =
                preCommitted
                        .transform(
                                SINK_COMMITTER_PREFIX + sinkName,
                                typeInformation,
                                getCommitterOperatorFactory(
                                        sink, isBatchMode, isCheckpointingEnabled))
                        .uid(operatorUidGenerator.generateUid("sink-committer"));

        addPostCommitTopologyIfSupported(sink, committed);
    }

    /**
     * Calls addPreCommitTopology via reflection (Flink 1.x WithPreCommitTopology vs 2.x
     * SupportsPreCommitTopology).
     */
    @SuppressWarnings("unchecked")
    private static <CommT> DataStream<CommittableMessage<CommT>> addPreCommitTopologyIfSupported(
            Sink<Event> sink, DataStream<CommittableMessage<CommT>> written) {
        try {
            Method m = sink.getClass().getMethod("addPreCommitTopology", DataStream.class);
            Object result = m.invoke(sink, written);
            return result != null ? (DataStream<CommittableMessage<CommT>>) result : written;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return written;
        }
    }

    /**
     * Calls addPostCommitTopology via reflection (Flink 1.x WithPostCommitTopology vs 2.x
     * SupportsPostCommitTopology).
     */
    private static <CommT> void addPostCommitTopologyIfSupported(
            Sink<Event> sink, DataStream<CommittableMessage<CommT>> committed) {
        try {
            Method m = sink.getClass().getMethod("addPostCommitTopology", DataStream.class);
            m.invoke(sink, committed);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            // Sink does not support post-commit topology
        }
    }

    private String generateSinkName(SinkDef sinkDef) {
        return sinkDef.getName()
                .orElse(String.format("Flink CDC Event Sink: %s", sinkDef.getType()));
    }

    private static <CommT> SimpleVersionedSerializer<CommT> getCommittableSerializer(Object sink) {
        // Uses reflection-friendly method name (works with TwoPhaseCommittingSink and
        // SupportsCommitter) during Flink 1.18 to 1.19. Remove this when Flink 1.18 is no longer
        // supported.
        try {
            return (SimpleVersionedSerializer<CommT>)
                    sink.getClass().getDeclaredMethod("getCommittableSerializer").invoke(sink);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to get CommittableSerializer", e);
        }
    }

    /**
     * Creates a {@code CommitterOperatorFactory} via reflection.
     *
     * <p>Flink 1.19+ / 2.x requires the first constructor parameter to be {@code
     * SupportsCommitter<CommT>}. Sinks like PaimonSink declare {@code createCommitter()} (no-arg)
     * and {@code getCommittableSerializer()} without implementing the {@code SupportsCommitter}
     * interface. In that case we create a dynamic proxy that adapts the sink.
     */
    @SuppressWarnings("unchecked")
    private static <CommT>
            OneInputStreamOperatorFactory<CommittableMessage<CommT>, CommittableMessage<CommT>>
                    getCommitterOperatorFactory(
                            Sink<Event> sink, boolean isBatchMode, boolean isCheckpointingEnabled) {
        try {
            Class<?> factoryClass =
                    Class.forName(
                            "org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory");
            java.lang.reflect.Constructor<?> ctor = factoryClass.getDeclaredConstructors()[0];
            Class<?> firstParamType = ctor.getParameterTypes()[0];

            Object firstArg;
            if (firstParamType.isInstance(sink)) {
                firstArg = sink;
            } else {
                firstArg = adaptToSupportsCommitter(sink, firstParamType);
            }

            ctor.setAccessible(true);
            return (OneInputStreamOperatorFactory<
                            CommittableMessage<CommT>, CommittableMessage<CommT>>)
                    ctor.newInstance(firstArg, isBatchMode, isCheckpointingEnabled);

        } catch (ClassNotFoundException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
            throw new RuntimeException("Failed to create CommitterOperatorFactory", e);
        }
    }

    /**
     * Creates a dynamic proxy implementing {@code SupportsCommitter} (or whatever the target
     * interface is) by delegating to the sink's existing {@code createCommitter()} and {@code
     * getCommittableSerializer()} methods.
     */
    private static Object adaptToSupportsCommitter(Sink<Event> sink, Class<?> targetInterface) {
        return java.lang.reflect.Proxy.newProxyInstance(
                targetInterface.getClassLoader(),
                new Class<?>[] {targetInterface},
                new SupportsCommitterInvocationHandler(sink));
    }

    /** Serializable invocation handler used to adapt sinks to SupportsCommitter. */
    private static final class SupportsCommitterInvocationHandler
            implements InvocationHandler, java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private final Sink<Event> sink;

        private SupportsCommitterInvocationHandler(Sink<Event> sink) {
            this.sink = sink;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if ("createCommitter".equals(name)) {
                // SupportsCommitter.createCommitter(CommitterInitContext) ->
                // delegate to sink.createCommitter() (no-arg)
                Method noArgMethod = sink.getClass().getMethod("createCommitter");
                noArgMethod.setAccessible(true);
                return noArgMethod.invoke(sink);
            }
            if ("getCommittableSerializer".equals(name)) {
                Method serMethod = sink.getClass().getMethod("getCommittableSerializer");
                serMethod.setAccessible(true);
                return serMethod.invoke(sink);
            }
            // Delegate any other method (toString, equals, hashCode) to sink when present
            try {
                Method sinkMethod = sink.getClass().getMethod(name, method.getParameterTypes());
                sinkMethod.setAccessible(true);
                return sinkMethod.invoke(sink, args);
            } catch (NoSuchMethodException ignore) {
                // Fall back to default behaviour for Object methods on proxy
                return method.invoke(this, args);
            }
        }
    }
}
