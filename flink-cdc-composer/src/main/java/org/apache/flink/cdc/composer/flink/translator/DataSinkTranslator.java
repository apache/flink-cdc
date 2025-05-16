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
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.FlinkEnvironmentUtils;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.runtime.operators.sink.BatchDataSinkFunctionOperator;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkFunctionOperator;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkWriterOperatorFactory;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;

import java.lang.reflect.InvocationTargetException;

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
                        Thread.currentThread().getContextClassLoader()));
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
        String sinkName = generateSinkName(sinkDef);
        if (eventSinkProvider instanceof FlinkSinkProvider) {
            // Sink V2
            FlinkSinkProvider sinkProvider = (FlinkSinkProvider) eventSinkProvider;
            Sink<Event> sink = sinkProvider.getSink();
            sinkTo(input, sink, sinkName, isBatchMode, schemaOperatorID, operatorUidGenerator);
        } else if (eventSinkProvider instanceof FlinkSinkFunctionProvider) {
            // SinkFunction
            FlinkSinkFunctionProvider sinkFunctionProvider =
                    (FlinkSinkFunctionProvider) eventSinkProvider;
            SinkFunction<Event> sinkFunction = sinkFunctionProvider.getSinkFunction();
            sinkTo(
                    input,
                    sinkFunction,
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
        // Pre-write topology
        if (sink instanceof WithPreWriteTopology) {
            stream = ((WithPreWriteTopology<Event>) sink).addPreWriteTopology(stream);
        }

        if (sink instanceof TwoPhaseCommittingSink) {
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

    private void sinkTo(
            DataStream<Event> input,
            SinkFunction<Event> sinkFunction,
            String sinkName,
            boolean isBatchMode,
            OperatorID schemaOperatorID,
            OperatorUidGenerator operatorUidGenerator) {
        StreamSink<Event> sinkOperator;
        if (isBatchMode) {
            sinkOperator = new BatchDataSinkFunctionOperator(sinkFunction);
        } else {
            sinkOperator = new DataSinkFunctionOperator(sinkFunction, schemaOperatorID);
        }
        final StreamExecutionEnvironment executionEnvironment = input.getExecutionEnvironment();
        PhysicalTransformation<Event> transformation =
                new LegacySinkTransformation<>(
                        input.getTransformation(),
                        SINK_WRITER_PREFIX + sinkName,
                        sinkOperator,
                        executionEnvironment.getParallelism(),
                        false);
        transformation.setUid(operatorUidGenerator.generateUid("sink-writer"));
        executionEnvironment.addOperator(transformation);
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
        if (sink instanceof WithPreCommitTopology) {
            preCommitted =
                    ((WithPreCommitTopology<Event, CommT>) sink).addPreCommitTopology(written);
        }

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

        if (sink instanceof WithPostCommitTopology) {
            ((WithPostCommitTopology<Event, CommT>) sink).addPostCommitTopology(committed);
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
