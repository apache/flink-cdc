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

package org.apache.flink.cdc.connectors.jdbc.sink;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.jdbc.dialect.JdbcSinkDialect;
import org.apache.flink.cdc.connectors.jdbc.sink.v2.EventRecordSerializationSchema;
import org.apache.flink.cdc.connectors.jdbc.sink.v2.JdbcSink;
import org.apache.flink.cdc.connectors.jdbc.sink.v2.JdbcSinkBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

import java.io.Serializable;

/** Writing data to JDBC-like sinks. */
public class JdbcDataSink implements DataSink, Serializable {
    private final JdbcSinkDialect dialect;
    private final JdbcSinkConfig sinkConfig;

    public JdbcDataSink(JdbcSinkDialect dialect, JdbcSinkConfig sinkConfig) {
        this.dialect = dialect;
        this.sinkConfig = sinkConfig;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        JdbcConnectionOptions connectionOptions =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(sinkConfig.getConnUrl())
                        .withUsername(sinkConfig.getUsername())
                        .withPassword(sinkConfig.getPassword())
                        .withDriverName(sinkConfig.getDriverClassName())
                        .build();

        JdbcSink<Event> jdbcSink =
                new JdbcSinkBuilder<Event>()
                        .withExecutionOptions(
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(sinkConfig.getWriteBatchSize())
                                        .withBatchIntervalMs(sinkConfig.getWriteBatchIntervalMs())
                                        .withMaxRetries(sinkConfig.getWriteMaxRetries())
                                        .build())
                        .withSerializationSchema(new EventRecordSerializationSchema())
                        .withDialect(dialect)
                        .withSinkConfig(sinkConfig)
                        .buildAtLeastOnce(connectionOptions);

        return FlinkSinkProvider.of(jdbcSink);
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new JdbcMetadataApplier(dialect);
    }
}
