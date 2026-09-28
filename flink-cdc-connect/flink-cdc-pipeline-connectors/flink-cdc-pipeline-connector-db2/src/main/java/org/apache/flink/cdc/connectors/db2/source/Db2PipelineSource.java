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
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfigFactory;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.source.offset.LsnFactory;
import org.apache.flink.cdc.connectors.db2.source.reader.Db2PipelineRecordEmitter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * The DB2 CDC Source for Pipeline connector, which supports parallel snapshot reading of tables and
 * then continues to capture data changes from the transaction log.
 *
 * <p>This source extends {@link Db2SourceBuilder.Db2IncrementalSource} and overrides the record
 * emitter to use {@link Db2PipelineRecordEmitter} for proper handling of schema events in the CDC
 * pipeline.
 */
@Internal
public class Db2PipelineSource extends Db2SourceBuilder.Db2IncrementalSource<Event> {

    private static final long serialVersionUID = 1L;

    public Db2PipelineSource(
            Db2SourceConfigFactory configFactory,
            DebeziumDeserializationSchema<Event> deserializationSchema,
            LsnFactory offsetFactory,
            Db2Dialect dataSourceDialect) {
        super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
    }

    @Override
    protected RecordEmitter<SourceRecords, Event, SourceSplitState> createRecordEmitter(
            SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
        Db2SourceConfig db2SourceConfig = (Db2SourceConfig) sourceConfig;
        Db2Dialect db2Dialect = (Db2Dialect) dataSourceDialect;
        return new Db2PipelineRecordEmitter<>(
                deserializationSchema,
                sourceReaderMetrics,
                db2SourceConfig,
                offsetFactory,
                db2Dialect);
    }
}
