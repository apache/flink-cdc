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

package org.apache.flink.cdc.connectors.oracle.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReader;
import org.apache.flink.cdc.connectors.oracle.source.OracleDialect;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * The basic source of Incremental Snapshot framework for JDBC datasource, it is based on FLIP-27
 * and Watermark Signal Algorithm which supports parallel reading snapshot of table and then
 * continue to capture data change by streaming reading.
 */
public class OracleTableSourceReader<T> extends OracleSourceBuilder.OracleIncrementalSource<T> {

    private OracleSourceConfig sourceConfig;

    public OracleTableSourceReader(
            OracleSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            RedoLogOffsetFactory offsetFactory,
            OracleDialect dataSourceDialect) {
        super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public IncrementalSourceReader<T, JdbcSourceConfig> createReader(
            SourceReaderContext readerContext) throws Exception {
        return super.createReader(readerContext);
    }

    @Override
    protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
            SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
        return new OraclePipelineRecordEmitter(
                deserializationSchema,
                sourceReaderMetrics,
                this.sourceConfig.isIncludeSchemaChanges(),
                new RedoLogOffsetFactory(),
                this.sourceConfig);
    }
}
