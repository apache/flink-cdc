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
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import org.apache.flink.cdc.connectors.sqlserver.source.offset.LsnFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.reader.SqlServerPipelineRecordEmitter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/** A {@link DataSource} for Postgres cdc connector. */
@Internal
public class SqlServerDataSource implements DataSource {

    private final SqlServerSourceConfigFactory configFactory;
    private final SqlServerSourceConfig sqlServerSourceConfig;

    public SqlServerDataSource(SqlServerSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.sqlServerSourceConfig = configFactory.create(0);
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        DebeziumEventDeserializationSchema deserializer =
                new SqlServerEventDeserializer(
                        DebeziumChangelogMode.ALL, sqlServerSourceConfig.isIncludeSchemaChanges());

        LsnFactory lsnFactory = new LsnFactory();
        SqlServerDialect sqlServerDialect = new SqlServerDialect(sqlServerSourceConfig);

        SqlServerSourceBuilder.SqlServerIncrementalSource<Event> source =
                new SqlServerPipelineSource<>(
                        configFactory,
                        deserializer,
                        lsnFactory,
                        sqlServerDialect,
                        sqlServerSourceConfig);

        return FlinkSourceProvider.of(source);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new SqlServerMetadataAccessor(sqlServerSourceConfig);
    }

    @VisibleForTesting
    public SqlServerSourceConfig getSqlServerSourceConfig() {
        return sqlServerSourceConfig;
    }

    /** The {@link JdbcIncrementalSource} implementation for Postgres. */
    public static class SqlServerPipelineSource<T>
            extends SqlServerSourceBuilder.SqlServerIncrementalSource<T> {
        private final SqlServerSourceConfig sourceConfig;
        private final SqlServerDialect dataSourceDialect;

        public SqlServerPipelineSource(
                SqlServerSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                LsnFactory offsetFactory,
                SqlServerDialect dataSourceDialect,
                SqlServerSourceConfig sourceConfig) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
            this.sourceConfig = sourceConfig;
            this.dataSourceDialect = dataSourceDialect;
        }

        @Override
        protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
                SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
            return new SqlServerPipelineRecordEmitter<>(
                    deserializationSchema,
                    sourceReaderMetrics,
                    this.sourceConfig,
                    offsetFactory,
                    this.dataSourceDialect);
        }
    }
}
