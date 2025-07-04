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

package org.apache.flink.cdc.connectors.fluss.sink.v2.metrics;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlinkSinkWriter;
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlussRecordSerializer;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;

import com.alibaba.fluss.config.Configuration;

import java.io.IOException;

/* This file is based on source code of Apache Fluss Project (https://fluss.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Flink sink for Fluss. */
public class FlinkSink<INPUT> implements Sink<INPUT> {
    private final FlussRecordSerializer<INPUT> serializer;
    private final Configuration flussConfig;

    public FlinkSink(Configuration flussConfig, FlussRecordSerializer<INPUT> serializer) {
        this.serializer = serializer;
        this.flussConfig = flussConfig;
    }

    @Deprecated
    public SinkWriter<INPUT> createWriter(InitContext context) throws IOException {
        FlinkSinkWriter<INPUT> flinkSinkWriter =
                new FlinkSinkWriter<>(flussConfig, context.getMailboxExecutor(), serializer);
        flinkSinkWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return flinkSinkWriter;
    }

    @Override
    public SinkWriter<INPUT> createWriter(WriterInitContext context) throws IOException {
        FlinkSinkWriter<INPUT> flinkSinkWriter =
                new FlinkSinkWriter<>(flussConfig, context.getMailboxExecutor(), serializer);
        flinkSinkWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return flinkSinkWriter;
    }
}
