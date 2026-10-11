/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.time.ZoneId;
import java.util.Collection;

/** A SinkV2 DWS sink backed by the official native client AUTO mode. */
public class DwsSink implements Sink<Event>, SupportsWriterState<Event, DwsWriterState> {

    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_SCHEMA = "public";

    private final DwsDataSinkConfig settings;

    public DwsSink(
            String jdbcUrl,
            String username,
            String password,
            ZoneId zoneId,
            boolean caseSensitive,
            String defaultSchema,
            boolean enableDelete) {
        this(
                DwsDataSinkConfig.builder()
                        .withUrl(jdbcUrl)
                        .withUsername(username)
                        .withPassword(password)
                        .withZoneId(zoneId)
                        .withCaseSensitive(caseSensitive)
                        .withDefaultSchema(normalizeDefaultSchema(defaultSchema))
                        .withEnableDelete(enableDelete)
                        .build());
    }

    public DwsSink(DwsDataSinkConfig settings) {
        this.settings = settings;
    }

    @Deprecated
    @Override
    public SinkWriter<Event> createWriter(Sink.InitContext context) {
        return createWriter(
                context.getMailboxExecutor(),
                context.getProcessingTimeService(),
                DwsWriterMetrics.registered(context.metricGroup()));
    }

    @Override
    public SinkWriter<Event> createWriter(WriterInitContext context) {
        return createWriter(
                context.getMailboxExecutor(),
                context.getProcessingTimeService(),
                DwsWriterMetrics.registered(context.metricGroup()));
    }

    @Override
    public StatefulSinkWriter<Event, DwsWriterState> restoreWriter(
            WriterInitContext context, Collection<DwsWriterState> writerStates) {
        return createWriter(
                context.getMailboxExecutor(),
                context.getProcessingTimeService(),
                DwsWriterMetrics.registered(context.metricGroup()));
    }

    @Override
    public SimpleVersionedSerializer<DwsWriterState> getWriterStateSerializer() {
        return new DwsWriterStateSerializer();
    }

    private DwsWriter createWriter(
            MailboxExecutor mailboxExecutor,
            ProcessingTimeService processingTimeService,
            DwsWriterMetrics metrics) {
        return new DwsWriter(
                settings,
                new DwsClientFacade.Official(settings),
                "native-client-v2",
                metrics,
                mailboxExecutor,
                processingTimeService);
    }

    private static String normalizeDefaultSchema(String defaultSchema) {
        if (defaultSchema == null || defaultSchema.trim().isEmpty()) {
            return DEFAULT_SCHEMA;
        }
        return defaultSchema.trim();
    }
}
