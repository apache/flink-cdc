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

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import org.apache.flink.cdc.connectors.sqlserver.source.offset.LsnFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.reader.SqlServerPipelineRecordEmitter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * The SQL Server CDC Source for Pipeline connector, which supports parallel reading snapshot of
 * table and then continue to capture data change from transaction log.
 *
 * <p>This source extends {@link SqlServerSourceBuilder.SqlServerIncrementalSource} and overrides
 * the record emitter to use {@link SqlServerPipelineRecordEmitter} for proper handling of schema
 * events in the CDC pipeline.
 */
@Internal
public class SqlServerPipelineSource
        extends SqlServerSourceBuilder.SqlServerIncrementalSource<Event> {

    private static final long serialVersionUID = 1L;

    public SqlServerPipelineSource(
            SqlServerSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<Event> deserializationSchema,
            LsnFactory offsetFactory,
            SqlServerDialect dataSourceDialect) {
        super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
    }

    @Override
    protected RecordEmitter<SourceRecords, Event, SourceSplitState> createRecordEmitter(
            SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
        SqlServerSourceConfig sqlServerSourceConfig = (SqlServerSourceConfig) sourceConfig;
        SqlServerDialect sqlServerDialect = (SqlServerDialect) dataSourceDialect;
        return new SqlServerPipelineRecordEmitter<>(
                deserializationSchema,
                sourceReaderMetrics,
                sqlServerSourceConfig,
                offsetFactory,
                sqlServerDialect);
    }
}
