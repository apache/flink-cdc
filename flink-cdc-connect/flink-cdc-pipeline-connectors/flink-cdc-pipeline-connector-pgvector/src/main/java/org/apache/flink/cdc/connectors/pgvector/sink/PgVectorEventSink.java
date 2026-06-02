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

package org.apache.flink.cdc.connectors.pgvector.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;

import java.io.IOException;

/** Flink sink wrapper for pgvector event writer. */
public class PgVectorEventSink implements Sink<Event> {

    private final PgVectorDataSinkConfig config;

    public PgVectorEventSink(PgVectorDataSinkConfig config) {
        this.config = config;
    }

    @Override
    public SinkWriter<Event> createWriter(InitContext context) throws IOException {
        return new PgVectorSinkWriter(
                config,
                new PgVectorSinkMetrics(InternalSinkWriterMetricGroup.wrap(context.metricGroup())));
    }

    @Override
    public SinkWriter<Event> createWriter(WriterInitContext context) throws IOException {
        return new PgVectorSinkWriter(
                config,
                new PgVectorSinkMetrics(InternalSinkWriterMetricGroup.wrap(context.metricGroup())));
    }
}
