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
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfigFactory;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.source.offset.LsnFactory;
import org.apache.flink.cdc.connectors.db2.table.Db2ReadableMetadata;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import java.util.ArrayList;
import java.util.List;

/** A {@link DataSource} for DB2 CDC connector. */
@Internal
public class Db2DataSource implements DataSource {

    private final Db2SourceConfigFactory configFactory;
    private final Db2SourceConfig db2SourceConfig;

    private final List<Db2ReadableMetadata> readableMetadataList;

    public Db2DataSource(Db2SourceConfigFactory configFactory) {
        this(configFactory, new ArrayList<>());
    }

    public Db2DataSource(
            Db2SourceConfigFactory configFactory, List<Db2ReadableMetadata> readableMetadataList) {
        this.configFactory = configFactory;
        this.db2SourceConfig = configFactory.create(0);
        this.readableMetadataList = readableMetadataList;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        Db2EventDeserializer deserializer =
                new Db2EventDeserializer(
                        DebeziumChangelogMode.ALL,
                        db2SourceConfig.isIncludeSchemaChanges(),
                        readableMetadataList);

        LsnFactory lsnFactory = new LsnFactory();
        Db2Dialect db2Dialect = new Db2Dialect(db2SourceConfig);

        Db2PipelineSource source =
                new Db2PipelineSource(configFactory, deserializer, lsnFactory, db2Dialect);

        return FlinkSourceProvider.of(source);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new Db2MetadataAccessor(db2SourceConfig);
    }

    @Override
    public SupportedMetadataColumn[] supportedMetadataColumns() {
        return new SupportedMetadataColumn[] {
            new OpTsMetadataColumn(),
            new TableNameMetadataColumn(),
            new DatabaseNameMetadataColumn(),
            new SchemaNameMetadataColumn()
        };
    }

    @Override
    public boolean isParallelMetadataSource() {
        // During the incremental stage, DB2 never emits schema change events on different
        // partitions, since it has only one transaction log stream.
        return false;
    }

    @VisibleForTesting
    public Db2SourceConfig getDb2SourceConfig() {
        return db2SourceConfig;
    }
}
