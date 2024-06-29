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

package org.apache.flink.cdc.connectors.oracle.source;

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
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetFactory;
import org.apache.flink.cdc.connectors.oracle.source.reader.OraclePipelineRecordEmitter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/** A {@link DataSource} for oracle cdc connector. */
@Internal
public class OracleDataSource implements DataSource {

    private final OracleSourceConfigFactory configFactory;
    private final OracleSourceConfig oracleSourceConfig;

    public OracleDataSource(OracleSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.oracleSourceConfig = configFactory.create(0);
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        OracleEventDeserializer deserializer =
                new OracleEventDeserializer(
                        DebeziumChangelogMode.ALL, oracleSourceConfig.isIncludeSchemaChanges());

        RedoLogOffsetFactory redoLogOffsetFactory = new RedoLogOffsetFactory();
        OracleDialect oracleDialect = new OracleDialect();

        OracleSourceBuilder.OracleIncrementalSource<Event> source =
                new OraclePipelineSource<>(
                        configFactory,
                        deserializer,
                        redoLogOffsetFactory,
                        oracleDialect,
                        oracleSourceConfig);

        return FlinkSourceProvider.of(source);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new OracleMetadataAccessor(oracleSourceConfig);
    }

    @VisibleForTesting
    public OracleSourceConfig getOracleSourceConfig() {
        return oracleSourceConfig;
    }

    /** The {@link JdbcIncrementalSource} implementation for Oracle. */
    public static class OraclePipelineSource<T>
            extends OracleSourceBuilder.OracleIncrementalSource<T> {
        private final OracleSourceConfig oracleSourceConfig;

        public OraclePipelineSource(
                OracleSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                RedoLogOffsetFactory offsetFactory,
                OracleDialect dataSourceDialect,
                OracleSourceConfig oracleSourceConfig) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
            this.oracleSourceConfig = oracleSourceConfig;
        }

        @Override
        protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
                SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
            return new OraclePipelineRecordEmitter<>(
                    deserializationSchema, sourceReaderMetrics, oracleSourceConfig, offsetFactory);
        }

        public static <T> OracleSourceBuilder<T> builder() {
            return new OracleSourceBuilder<>();
        }
    }
}
