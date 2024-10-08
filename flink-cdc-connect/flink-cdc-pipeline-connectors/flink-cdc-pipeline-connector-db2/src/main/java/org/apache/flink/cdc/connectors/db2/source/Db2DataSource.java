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

package org.apache.flink.cdc.connectors.db2.source;

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
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfigFactory;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.source.offset.LsnFactory;
import org.apache.flink.cdc.connectors.db2.source.reader.Db2PipelineRecordEmitter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/** A {@link DataSource} for Postgres cdc connector. */
@Internal
public class Db2DataSource implements DataSource {

    private final Db2SourceConfigFactory configFactory;
    private final Db2SourceConfig db2SourceConfig;

    public Db2DataSource(Db2SourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.db2SourceConfig = configFactory.create(0);
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        DebeziumEventDeserializationSchema deserializer =
                new Db2EventDeserializer(
                        DebeziumChangelogMode.ALL, db2SourceConfig.isIncludeSchemaChanges());

        LsnFactory lsnFactory = new LsnFactory();
        Db2Dialect postgresDialect = new Db2Dialect(db2SourceConfig);

        Db2SourceBuilder.Db2IncrementalSource<Event> source =
                new Db2PipelineSource<>(
                        configFactory, deserializer, lsnFactory, postgresDialect, db2SourceConfig);

        return FlinkSourceProvider.of(source);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new Db2MetadataAccessor(db2SourceConfig);
    }

    @VisibleForTesting
    public Db2SourceConfig getDb2SourceConfig() {
        return db2SourceConfig;
    }

    /** The {@link JdbcIncrementalSource} implementation for Postgres. */
    public static class Db2PipelineSource<T> extends Db2SourceBuilder.Db2IncrementalSource<T> {
        private final Db2SourceConfig sourceConfig;
        private final Db2Dialect dataSourceDialect;

        public Db2PipelineSource(
                Db2SourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                LsnFactory offsetFactory,
                Db2Dialect dataSourceDialect,
                Db2SourceConfig sourceConfig) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
            this.sourceConfig = sourceConfig;
            this.dataSourceDialect = dataSourceDialect;
        }

        @Override
        protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
                SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
            return new Db2PipelineRecordEmitter<>(
                    deserializationSchema,
                    sourceReaderMetrics,
                    this.sourceConfig,
                    offsetFactory,
                    this.dataSourceDialect);
        }
    }
}
