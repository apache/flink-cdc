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

package org.apache.flink.cdc.connectors.mongodb.source;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.mongodb.source.reader.MongoDBPipelineRecordEmitter;

/** A {@link DataSource} for mongodb cdc connector. */
public class MongoDBDataSource implements DataSource {
    private final MongoDBSourceConfigFactory configFactory;
    private final MongoDBSourceConfig sourceConfig;

    public MongoDBDataSource(MongoDBSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        MongoDBEventDeserializer deserializer =
                new MongoDBEventDeserializer(SchemaParseMode.SCHEMA_LESS);

        MongoDBSource<Event> source =
                new MongoDBSource<>(
                        configFactory,
                        deserializer,
                        (sourceReaderMetrics, sourceConfig, offsetFactory) ->
                                new MongoDBPipelineRecordEmitter(
                                        SchemaParseMode.SCHEMA_LESS,
                                        deserializer,
                                        sourceReaderMetrics,
                                        sourceConfig,
                                        offsetFactory));

        return FlinkSourceProvider.of(source);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new MongoDBMetadataAccessor(sourceConfig, SchemaParseMode.SCHEMA_LESS);
    }

    @VisibleForTesting
    public MongoDBSourceConfig getSourceConfig() {
        return sourceConfig;
    }
}
