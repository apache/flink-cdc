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

package org.apache.flink.cdc.flink.compat.flink2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.flink.compat.FlinkPipelineBridge;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.function.Function;

/**
 * Flink 2.x implementation of {@link FlinkPipelineBridge}.
 *
 * <p>Supports only Source/Sink V2 ({@link FlinkSourceProvider}, {@link FlinkSinkProvider}).
 * SourceFunction and SinkFunction were removed in Flink 2.x; use compat-flink1 for Flink 1.x.
 */
@Internal
public class Flink2PipelineBridge implements FlinkPipelineBridge {

    private static final String UNSUPPORTED_SOURCE_MSG =
            "Flink 2.x supports only FlinkSourceProvider (Source V2). "
                    + "SourceFunction was removed in Flink 2.x. Use flink-cdc-flink-compat-flink1 for Flink 1.x.";
    private static final String UNSUPPORTED_SINK_MSG =
            "Flink 2.x supports only FlinkSinkProvider (Sink V2). "
                    + "SinkFunction was removed in Flink 2.x. Use flink-cdc-flink-compat-flink1 for Flink 1.x.";

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
        throw new UnsupportedOperationException(
                UNSUPPORTED_SOURCE_MSG
                        + " Got: "
                        + (eventSourceProvider == null
                                ? "null"
                                : eventSourceProvider.getClass().getName()));
    }

    @Override
    public void sinkTo(
            DataStream<Event> input,
            EventSinkProvider eventSinkProvider,
            String sinkName,
            boolean isBatchMode,
            OperatorID schemaOperatorID,
            Function<String, String> uidGenerator) {
        // In Flink 2.x only FlinkSinkProvider (Sink V2) is supported; the translator handles it
        // directly. This path is only hit for legacy SinkFunction-based providers, which are
        // unsupported in 2.x.
        throw new UnsupportedOperationException(
                UNSUPPORTED_SINK_MSG
                        + " Got: "
                        + (eventSinkProvider == null
                                ? "null"
                                : eventSinkProvider.getClass().getName()));
    }
}
