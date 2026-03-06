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

package org.apache.flink.cdc.flink.compat;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.function.Function;

/**
 * Version-specific bridge for binding {@link EventSourceProvider} / {@link EventSinkProvider} to
 * Flink DataStream API.
 *
 * <p>Flink 1.x implementation supports both Source/Sink V2 and legacy SourceFunction/SinkFunction.
 * Flink 2.x implementation supports only Source/Sink V2.
 *
 * <p>Implementations are loaded via {@link java.util.ServiceLoader}; exactly one compat JAR
 * (flink-cdc-flink-compat-flink1 or flink-cdc-flink-compat-flink2) must be on the classpath.
 */
@Internal
public interface FlinkPipelineBridge {

    /**
     * Creates a {@link DataStreamSource} from the given {@link EventSourceProvider}.
     *
     * @param env execution environment
     * @param eventSourceProvider provider (e.g. FlinkSourceProvider or FlinkSourceFunctionProvider
     *     on 1.x)
     * @param sourceName name for the source operator
     * @return the data stream source
     * @throws IllegalStateException if the provider type is not supported by this bridge
     */
    DataStreamSource<Event> createDataStreamSource(
            StreamExecutionEnvironment env,
            EventSourceProvider eventSourceProvider,
            String sourceName);

    /**
     * Adds the given {@link EventSinkProvider} as a sink to the input stream.
     *
     * @param input input event stream
     * @param eventSinkProvider provider (e.g. FlinkSinkProvider or FlinkSinkFunctionProvider on
     *     1.x)
     * @param sinkName name for the sink operator
     * @param isBatchMode whether the job is in batch mode
     * @param schemaOperatorID schema operator ID for coordination
     * @param uidGenerator generates operator UID from a suffix (e.g. "sink-writer" ->
     *     "prefix-sink-writer")
     */
    void sinkTo(
            DataStream<Event> input,
            EventSinkProvider eventSinkProvider,
            String sinkName,
            boolean isBatchMode,
            OperatorID schemaOperatorID,
            Function<String, String> uidGenerator);
}
