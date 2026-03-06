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

package org.apache.flink.cdc.flink.compat.flink1;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceFunctionProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.flink.compat.FlinkPipelineBridge;
import org.apache.flink.cdc.runtime.operators.sink.BatchDataSinkFunctionOperator;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkFunctionOperator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;

import java.util.function.Function;

/** Flink 1.x implementation of {@link FlinkPipelineBridge}. */
@Internal
public class Flink1PipelineBridge implements FlinkPipelineBridge {

    private static final String SINK_WRITER_PREFIX = "Sink Writer: ";

    @Override
    public DataStreamSource<Event> createDataStreamSource(
            StreamExecutionEnvironment env,
            EventSourceProvider eventSourceProvider,
            String sourceName) {
        if (eventSourceProvider instanceof FlinkSourceProvider) {
            FlinkSourceProvider sourceProvider = (FlinkSourceProvider) eventSourceProvider;
            return env.fromSource(
                    sourceProvider.getSource(),
                    WatermarkStrategy.noWatermarks(),
                    sourceName,
                    new EventTypeInfo());
        }
        if (eventSourceProvider instanceof FlinkSourceFunctionProvider) {
            FlinkSourceFunctionProvider sourceFunctionProvider =
                    (FlinkSourceFunctionProvider) eventSourceProvider;
            DataStreamSource<Event> stream =
                    env.addSource(sourceFunctionProvider.getSourceFunction(), new EventTypeInfo());
            stream.name(sourceName);
            return stream;
        }
        throw new IllegalStateException(
                String.format(
                        "Unsupported EventSourceProvider type \"%s\"",
                        eventSourceProvider.getClass().getCanonicalName()));
    }

    @Override
    public void sinkTo(
            DataStream<Event> input,
            EventSinkProvider eventSinkProvider,
            String sinkName,
            boolean isBatchMode,
            OperatorID schemaOperatorID,
            Function<String, String> uidGenerator) {
        if (!(eventSinkProvider instanceof FlinkSinkFunctionProvider)) {
            throw new IllegalStateException(
                    String.format(
                            "Unsupported EventSinkProvider type \"%s\" for SinkFunction path",
                            eventSinkProvider.getClass().getCanonicalName()));
        }
        FlinkSinkFunctionProvider sinkFunctionProvider =
                (FlinkSinkFunctionProvider) eventSinkProvider;
        SinkFunction<Event> sinkFunction = sinkFunctionProvider.getSinkFunction();

        StreamSink<Event> sinkOperator;
        if (isBatchMode) {
            sinkOperator = new BatchDataSinkFunctionOperator(sinkFunction);
        } else {
            sinkOperator = new DataSinkFunctionOperator(sinkFunction, schemaOperatorID);
        }
        StreamExecutionEnvironment executionEnvironment = input.getExecutionEnvironment();
        PhysicalTransformation<Event> transformation =
                new LegacySinkTransformation<>(
                        input.getTransformation(),
                        SINK_WRITER_PREFIX + sinkName,
                        sinkOperator,
                        executionEnvironment.getParallelism(),
                        false);
        String uid = (uidGenerator != null) ? uidGenerator.apply("sink-writer") : null;
        if (uid != null) {
            transformation.setUid(uid);
        }
        executionEnvironment.addOperator(transformation);
    }
}
