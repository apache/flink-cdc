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

package org.apache.flink.cdc.connectors.postgres.source;

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
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.source.reader.PostgresPipelineRecordEmitter;
import org.apache.flink.cdc.connectors.postgres.table.PostgreSQLReadableMetadata;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import java.util.ArrayList;
import java.util.List;

/** A {@link DataSource} for Postgres cdc connector. */
@Internal
public class PostgresDataSource implements DataSource {

    private final PostgresSourceConfigFactory configFactory;
    private final PostgresSourceConfig postgresSourceConfig;

    private final List<PostgreSQLReadableMetadata> readableMetadataList;

    public PostgresDataSource(PostgresSourceConfigFactory configFactory) {
        this(configFactory, new ArrayList<>());
    }

    public PostgresDataSource(
            PostgresSourceConfigFactory configFactory,
            List<PostgreSQLReadableMetadata> readableMetadataList) {
        this.configFactory = configFactory;
        this.postgresSourceConfig = configFactory.create(0);
        this.readableMetadataList = readableMetadataList;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        DebeziumEventDeserializationSchema deserializer =
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL, readableMetadataList);

        PostgresOffsetFactory postgresOffsetFactory = new PostgresOffsetFactory();
        PostgresDialect postgresDialect = new PostgresDialect(postgresSourceConfig);

        PostgresSourceBuilder.PostgresIncrementalSource<Event> source =
                new PostgresPipelineSource<>(
                        configFactory,
                        deserializer,
                        postgresOffsetFactory,
                        postgresDialect,
                        postgresSourceConfig);

        return FlinkSourceProvider.of(source);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new PostgresMetadataAccessor(postgresSourceConfig);
    }

    @VisibleForTesting
    public PostgresSourceConfig getPostgresSourceConfig() {
        return postgresSourceConfig;
    }

    /** The {@link JdbcIncrementalSource} implementation for Postgres. */
    public static class PostgresPipelineSource<T>
            extends PostgresSourceBuilder.PostgresIncrementalSource<T> {
        private final PostgresSourceConfig sourceConfig;
        private final PostgresDialect dataSourceDialect;

        public PostgresPipelineSource(
                PostgresSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                PostgresOffsetFactory offsetFactory,
                PostgresDialect dataSourceDialect,
                PostgresSourceConfig sourceConfig) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
            this.sourceConfig = sourceConfig;
            this.dataSourceDialect = dataSourceDialect;
        }

        @Override
        protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
                SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
            return new PostgresPipelineRecordEmitter<>(
                    deserializationSchema,
                    sourceReaderMetrics,
                    this.sourceConfig,
                    offsetFactory,
                    this.dataSourceDialect);
        }
    }
}
