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

package org.apache.flink.cdc.connectors.jdbc.sink.v2;

import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.cdc.connectors.jdbc.catalog.Catalog;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterStateSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/** Implementation class of the {@link StatefulSink} interface. */
public class JdbcSink<IN> implements StatefulSink<IN, JdbcWriterState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSink.class);

    private final JdbcConnectionProvider connectionProvider;

    private final JdbcExecutionOptions executionOptions;

    private final RecordSerializationSchema<IN> serializationSchema;

    private final Catalog catalog;

    public JdbcSink(
            JdbcExecutionOptions executionOptions,
            JdbcConnectionProvider connectionProvider,
            RecordSerializationSchema<IN> serializationSchema,
            Catalog catalog) {
        this.executionOptions = executionOptions;
        this.connectionProvider = connectionProvider;
        this.serializationSchema = serializationSchema;
        this.catalog = catalog;
    }

    @Override
    public StatefulSinkWriter<IN, JdbcWriterState> createWriter(InitContext context)
            throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<IN, JdbcWriterState> restoreWriter(
            InitContext context, Collection<JdbcWriterState> collection) throws IOException {
        JdbcOutputSerializer<Object> outputSerializer =
                JdbcOutputSerializer.of(
                        context.createInputSerializer(), context.isObjectReuseEnabled());

        return new JdbcWriter<>(
                context,
                executionOptions,
                connectionProvider,
                outputSerializer,
                serializationSchema,
                catalog);
    }

    @Override
    public SimpleVersionedSerializer<JdbcWriterState> getWriterStateSerializer() {
        return new JdbcWriterStateSerializer();
    }
}
