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
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkWriterOperatorFactory;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Translator used to build {@link DataSink} for given {@link DataStream}. */
@Internal
public class DataSinkTranslator {

    private static final Logger LOG = LoggerFactory.getLogger(DataSinkTranslator.class);

    private static final String SINK_WRITER_PREFIX = "Sink Writer: ";
    private static final String SINK_COMMITTER_PREFIX = "Sink Committer: ";

    private static final String FLINK_SINK_FUNCTION_PROVIDER_CLASS =
            "org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider";
    private static final String SINK_FUNCTION_CLASS =
            "org.apache.flink.streaming.api.functions.sink.SinkFunction";
    private static final String STREAM_SINK_CLASS =
            "org.apache.flink.streaming.api.operators.StreamSink";
    private static final String DATA_SINK_FUNCTION_OPERATOR_CLASS =
            "org.apache.flink.cdc.runtime.operators.sink.DataSinkFunctionOperator";
    private static final String BATCH_DATA_SINK_FUNCTION_OPERATOR_CLASS =
            "org.apache.flink.cdc.runtime.operators.sink.BatchDataSinkFunctionOperator";
    private static final String LEGACY_SINK_TRANSFORMATION_CLASS =
            "org.apache.flink.streaming.api.transformations.LegacySinkTransformation";
    // Sink V2 commit topology classes (not available in Flink 2.x)
    private static final String TWO_PHASE_COMMITTING_SINK_CLASS =
            "org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink";
    private static final String SUPPORTS_COMMITTER_CLASS =
            "org.apache.flink.api.connector.sink2.SupportsCommitter";
    private static final String WITH_PRE_WRITE_TOPOLOGY_CLASS =
            "org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology";
    private static final String WITH_PRE_COMMIT_TOPOLOGY_CLASS =
            "org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology";
    private static final String WITH_POST_COMMIT_TOPOLOGY_CLASS =
            "org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology";

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
        // Get sink provider
        EventSinkProvider eventSinkProvider = dataSink.getEventSinkProvider();
        if (eventSinkProvider == null) {
            return;
        }
        String sinkName = generateSinkName(sinkDef);
        if (eventSinkProvider instanceof FlinkSinkProvider) {
            // Sink V2 (works with both Flink 1.x and 2.x)
            FlinkSinkProvider sinkProvider = (FlinkSinkProvider) eventSinkProvider;
            Sink<Event> sink = sinkProvider.getSink();
            sinkTo(input, sink, sinkName, isBatchMode, schemaOperatorID, operatorUidGenerator);
        } else {
            // Try legacy SinkFunction path via reflection (Flink 1.x only)
            sinkToLegacy(
                    input,
                    eventSinkProvider,
                    sinkName,
                    isBatchMode,
                    schemaOperatorID,
                    operatorUidGenerator);
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
        // Pre-write topology (use reflection since WithPreWriteTopology is removed in Flink 2.x)
        stream = tryAddPreWriteTopology(sink, stream);

        if (isTwoPhaseCommittingSink(sink)) {
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
     * Checks if the sink supports two-phase committing. In Flink 1.x, this is indicated by
     * implementing {@code TwoPhaseCommittingSink}. In Flink 2.x, the equivalent interface is {@code
     * SupportsCommitter}.
     */
    private static boolean isTwoPhaseCommittingSink(Sink<Event> sink) {
        // Try Flink 1.x: TwoPhaseCommittingSink
        try {
            Class<?> tpcClass = Class.forName(TWO_PHASE_COMMITTING_SINK_CLASS);
            if (tpcClass.isInstance(sink)) {
                return true;
            }
        } catch (ClassNotFoundException ignored) {
            // Not available in Flink 2.x
        }
        // Try Flink 2.x: SupportsCommitter
        try {
            Class<?> scClass = Class.forName(SUPPORTS_COMMITTER_CLASS);
            if (scClass.isInstance(sink)) {
                return true;
            }
        } catch (ClassNotFoundException ignored) {
            // Not available in Flink 1.x
        }
        return false;
    }

    /**
     * Tries to add a pre-write topology via reflection. In Flink 1.x, this checks for {@code
     * WithPreWriteTopology}. In Flink 2.x, this interface does not exist.
     */
    @SuppressWarnings("unchecked")
    private static DataStream<Event> tryAddPreWriteTopology(
            Sink<Event> sink, DataStream<Event> stream) {
        try {
            Class<?> clazz = Class.forName(WITH_PRE_WRITE_TOPOLOGY_CLASS);
            if (clazz.isInstance(sink)) {
                Method method = clazz.getMethod("addPreWriteTopology", DataStream.class);
                return (DataStream<Event>) method.invoke(sink, stream);
            }
        } catch (ClassNotFoundException ignored) {
            // WithPreWriteTopology not available (Flink 2.x)
        } catch (Exception e) {
            throw new RuntimeException("Failed to add pre-write topology via reflection", e);
        }
        return stream;
    }

    /**
     * Sink to a legacy SinkFunction via reflection. This method handles the case where
     * FlinkSinkFunctionProvider, SinkFunction, StreamSink, DataSinkFunctionOperator,
     * BatchDataSinkFunctionOperator, and LegacySinkTransformation are not available in Flink 2.x.
     */
    private void sinkToLegacy(
            DataStream<Event> input,
            EventSinkProvider eventSinkProvider,
            String sinkName,
            boolean isBatchMode,
            OperatorID schemaOperatorID,
            OperatorUidGenerator operatorUidGenerator) {
        try {
            // Check if FlinkSinkFunctionProvider is available
            Class<?> sinkFuncProviderClass = Class.forName(FLINK_SINK_FUNCTION_PROVIDER_CLASS);
            if (!sinkFuncProviderClass.isInstance(eventSinkProvider)) {
                throw new IllegalStateException(
                        String.format(
                                "Unsupported EventSinkProvider type \"%s\"",
                                eventSinkProvider.getClass().getCanonicalName()));
            }

            // Get SinkFunction via reflection
            Object sinkFunction =
                    sinkFuncProviderClass.getMethod("getSinkFunction").invoke(eventSinkProvider);

            // Create the operator (StreamSink subclass)
            Class<?> sinkFunctionClass = Class.forName(SINK_FUNCTION_CLASS);
            Object sinkOperator;
            if (isBatchMode) {
                Class<?> batchOpClass = Class.forName(BATCH_DATA_SINK_FUNCTION_OPERATOR_CLASS);
                sinkOperator =
                        batchOpClass.getConstructor(sinkFunctionClass).newInstance(sinkFunction);
            } else {
                Class<?> opClass = Class.forName(DATA_SINK_FUNCTION_OPERATOR_CLASS);
                sinkOperator =
                        opClass.getConstructor(sinkFunctionClass, OperatorID.class)
                                .newInstance(sinkFunction, schemaOperatorID);
            }

            // Create LegacySinkTransformation via reflection
            Class<?> streamSinkClass = Class.forName(STREAM_SINK_CLASS);
            Class<?> legacySinkTransClass = Class.forName(LEGACY_SINK_TRANSFORMATION_CLASS);
            StreamExecutionEnvironment executionEnvironment = input.getExecutionEnvironment();

            // Find the constructor: LegacySinkTransformation(Transformation, String, StreamSink,
            // int, boolean)
            Constructor<?> constructor = null;
            for (Constructor<?> c : legacySinkTransClass.getDeclaredConstructors()) {
                Class<?>[] paramTypes = c.getParameterTypes();
                if (paramTypes.length == 5 && streamSinkClass.isAssignableFrom(paramTypes[2])) {
                    constructor = c;
                    break;
                }
            }
            if (constructor == null) {
                throw new NoSuchMethodException(
                        "Cannot find suitable constructor for LegacySinkTransformation");
            }

            Object transformation =
                    constructor.newInstance(
                            input.getTransformation(),
                            SINK_WRITER_PREFIX + sinkName,
                            sinkOperator,
                            executionEnvironment.getParallelism(),
                            false);

            // Set UID
            Method setUidMethod = transformation.getClass().getMethod("setUid", String.class);
            setUidMethod.invoke(transformation, operatorUidGenerator.generateUid("sink-writer"));

            // Add operator to environment
            executionEnvironment.addOperator(
                    (org.apache.flink.api.dag.Transformation<?>) transformation);

        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Legacy SinkFunction API is not available. This is expected in Flink 2.x. "
                            + "Please use FlinkSinkProvider (Sink V2 API) instead.",
                    e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create legacy sink via reflection", e);
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

        DataStream<CommittableMessage<CommT>> preCommitted = written;
        // Pre-commit topology (use reflection since WithPreCommitTopology is removed in Flink 2.x)
        preCommitted = tryAddPreCommitTopology(sink, written, preCommitted);

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

        // Post-commit topology (use reflection since WithPostCommitTopology is removed in Flink
        // 2.x)
        tryAddPostCommitTopology(sink, committed);
    }

    /**
     * Tries to add a pre-commit topology via reflection. In Flink 1.x, this checks for {@code
     * WithPreCommitTopology}. In Flink 2.x, this interface does not exist.
     */
    @SuppressWarnings("unchecked")
    private static <CommT> DataStream<CommittableMessage<CommT>> tryAddPreCommitTopology(
            Sink<Event> sink,
            DataStream<CommittableMessage<CommT>> written,
            DataStream<CommittableMessage<CommT>> fallback) {
        try {
            Class<?> clazz = Class.forName(WITH_PRE_COMMIT_TOPOLOGY_CLASS);
            if (clazz.isInstance(sink)) {
                Method method = clazz.getMethod("addPreCommitTopology", DataStream.class);
                return (DataStream<CommittableMessage<CommT>>) method.invoke(sink, written);
            }
        } catch (ClassNotFoundException ignored) {
            // WithPreCommitTopology not available (Flink 2.x)
        } catch (Exception e) {
            throw new RuntimeException("Failed to add pre-commit topology via reflection", e);
        }
        return fallback;
    }

    /**
     * Tries to add a post-commit topology via reflection. In Flink 1.x, this checks for {@code
     * WithPostCommitTopology}. In Flink 2.x, this interface does not exist.
     */
    private static <CommT> void tryAddPostCommitTopology(
            Sink<Event> sink, DataStream<CommittableMessage<CommT>> committed) {
        try {
            Class<?> clazz = Class.forName(WITH_POST_COMMIT_TOPOLOGY_CLASS);
            if (clazz.isInstance(sink)) {
                Method method = clazz.getMethod("addPostCommitTopology", DataStream.class);
                method.invoke(sink, committed);
            }
        } catch (ClassNotFoundException ignored) {
            // WithPostCommitTopology not available (Flink 2.x)
        } catch (Exception e) {
            throw new RuntimeException("Failed to add post-commit topology via reflection", e);
        }
    }

    private String generateSinkName(SinkDef sinkDef) {
        return sinkDef.getName()
                .orElse(String.format("Flink CDC Event Sink: %s", sinkDef.getType()));
    }

    private static <CommT> SimpleVersionedSerializer<CommT> getCommittableSerializer(Object sink) {
        // FIX ME: TwoPhaseCommittingSink has been deprecated, and its signature has changed
        // during Flink 1.18 to 1.19. Remove this when Flink 1.18 is no longer supported.
        try {
            return (SimpleVersionedSerializer<CommT>)
                    sink.getClass().getDeclaredMethod("getCommittableSerializer").invoke(sink);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to get CommittableSerializer", e);
        }
    }

    private static <CommT>
            OneInputStreamOperatorFactory<CommittableMessage<CommT>, CommittableMessage<CommT>>
                    getCommitterOperatorFactory(
                            Sink<Event> sink, boolean isBatchMode, boolean isCheckpointingEnabled) {
        // FIX ME: OneInputStreamOperatorFactory is an @Internal class, and its signature has
        // changed during Flink 1.18 to 1.19. Remove this when Flink 1.18 is no longer supported.
        try {
            return (OneInputStreamOperatorFactory<
                            CommittableMessage<CommT>, CommittableMessage<CommT>>)
                    Class.forName(
                                    "org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory")
                            .getDeclaredConstructors()[0]
                            .newInstance(sink, isBatchMode, isCheckpointingEnabled);

        } catch (ClassNotFoundException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
            throw new RuntimeException("Failed to create CommitterOperatorFactory", e);
        }
    }
}
