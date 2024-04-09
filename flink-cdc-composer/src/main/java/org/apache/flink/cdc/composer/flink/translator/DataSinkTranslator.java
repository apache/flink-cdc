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
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkFunctionOperator;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkWriterOperatorFactory;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;

/** Translator used to build {@link DataSink} for given {@link DataStream}. */
@Internal
public class DataSinkTranslator {

    private static final String SINK_WRITER_PREFIX = "Sink Writer: ";
    private static final String SINK_COMMITTER_PREFIX = "Sink Committer: ";

    public void translate(
            SinkDef sinkDef,
            DataStream<Event> input,
            DataSink dataSink,
            OperatorID schemaOperatorID) {
        // Get sink provider
        EventSinkProvider eventSinkProvider = dataSink.getEventSinkProvider();
        String sinkName = generateSinkName(sinkDef);
        if (eventSinkProvider instanceof FlinkSinkProvider) {
            // Sink V2
            FlinkSinkProvider sinkProvider = (FlinkSinkProvider) eventSinkProvider;
            Sink<Event> sink = sinkProvider.getSink();
            sinkTo(input, sink, sinkName, schemaOperatorID);
        } else if (eventSinkProvider instanceof FlinkSinkFunctionProvider) {
            // SinkFunction
            FlinkSinkFunctionProvider sinkFunctionProvider =
                    (FlinkSinkFunctionProvider) eventSinkProvider;
            SinkFunction<Event> sinkFunction = sinkFunctionProvider.getSinkFunction();
            sinkTo(input, sinkFunction, sinkName, schemaOperatorID);
        }
    }

    private void sinkTo(
            DataStream<Event> input,
            Sink<Event> sink,
            String sinkName,
            OperatorID schemaOperatorID) {
        DataStream<Event> stream = input;
        // Pre write topology
        if (sink instanceof WithPreWriteTopology) {
            stream = ((WithPreWriteTopology<Event>) sink).addPreWriteTopology(stream);
        }

        if (sink instanceof TwoPhaseCommittingSink) {
            addCommittingTopology(sink, stream, sinkName, schemaOperatorID);
        } else {
            input.transform(
                    SINK_WRITER_PREFIX + sinkName,
                    CommittableMessageTypeInfo.noOutput(),
                    new DataSinkWriterOperatorFactory<>(sink, schemaOperatorID));
        }
    }

    private void sinkTo(
            DataStream<Event> input,
            SinkFunction<Event> sinkFunction,
            String sinkName,
            OperatorID schemaOperatorID) {
        DataSinkFunctionOperator sinkOperator =
                new DataSinkFunctionOperator(sinkFunction, schemaOperatorID);
        final StreamExecutionEnvironment executionEnvironment = input.getExecutionEnvironment();
        PhysicalTransformation<Event> transformation =
                new LegacySinkTransformation<>(
                        input.getTransformation(),
                        SINK_WRITER_PREFIX + sinkName,
                        sinkOperator,
                        executionEnvironment.getParallelism(),
                        false);
        executionEnvironment.addOperator(transformation);
    }

    private <CommT> void addCommittingTopology(
            Sink<Event> sink,
            DataStream<Event> inputStream,
            String sinkName,
            OperatorID schemaOperatorID) {
        TwoPhaseCommittingSink<Event, CommT> committingSink =
                (TwoPhaseCommittingSink<Event, CommT>) sink;
        TypeInformation<CommittableMessage<CommT>> typeInformation =
                CommittableMessageTypeInfo.of(committingSink::getCommittableSerializer);
        DataStream<CommittableMessage<CommT>> written =
                inputStream.transform(
                        SINK_WRITER_PREFIX + sinkName,
                        typeInformation,
                        new DataSinkWriterOperatorFactory<>(sink, schemaOperatorID));

        DataStream<CommittableMessage<CommT>> preCommitted = written;
        if (sink instanceof WithPreCommitTopology) {
            preCommitted =
                    ((WithPreCommitTopology<Event, CommT>) sink).addPreCommitTopology(written);
        }

        // TODO: Hard coding stream mode and checkpoint
        boolean isBatchMode = false;
        boolean isCheckpointingEnabled = true;
        DataStream<CommittableMessage<CommT>> committed =
                preCommitted.transform(
                        SINK_COMMITTER_PREFIX + sinkName,
                        typeInformation,
                        new CommitterOperatorFactory<>(
                                committingSink, isBatchMode, isCheckpointingEnabled));

        if (sink instanceof WithPostCommitTopology) {
            ((WithPostCommitTopology<Event, CommT>) sink).addPostCommitTopology(committed);
        }
    }

    private String generateSinkName(SinkDef sinkDef) {
        return sinkDef.getName()
                .orElse(String.format("Flink CDC Event Sink: %s", sinkDef.getType()));
    }
}
