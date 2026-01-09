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

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetFactory;
import org.apache.flink.cdc.connectors.oracle.source.reader.OracleTableSourceReader;
import org.apache.flink.cdc.connectors.oracle.table.OracleReadableMetaData;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link DynamicTableSource} that describes how to create a Oracle redo log from a logical
 * description.
 */
public class OracleDataSource implements DataSource, SupportsReadingMetadata {

    private final OracleSourceConfig sourceConfig;
    private final Configuration config;
    private final OracleSourceConfigFactory configFactory;

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    private final List<OracleReadableMetaData> readableMetadataList;

    public OracleDataSource(
            OracleSourceConfigFactory configFactory,
            Configuration config,
            List<OracleReadableMetaData> readableMetadataList) {
        this.sourceConfig = configFactory.create(0);
        this.config = config;
        this.metadataKeys = Collections.emptyList();
        this.readableMetadataList = readableMetadataList;
        this.configFactory = configFactory;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {

        OracleDialect oracleDialect = new OracleDialect();
        OracleEventDeserializer deserializer =
                new OracleEventDeserializer(
                        DebeziumChangelogMode.ALL,
                        config.get(OracleDataSourceOptions.SCHEMA_CHANGE_ENABLED),
                        readableMetadataList);

        RedoLogOffsetFactory offsetFactory = new RedoLogOffsetFactory();
        OracleTableSourceReader oracleChangeEventSource =
                new OracleTableSourceReader(
                        configFactory, deserializer, offsetFactory, oracleDialect);
        return FlinkSourceProvider.of(oracleChangeEventSource);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new OracleMetadataAccessor(sourceConfig);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(OracleReadableMetaData.values())
                .collect(
                        Collectors.toMap(
                                OracleReadableMetaData::getKey,
                                OracleReadableMetaData::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    @VisibleForTesting
    public OracleSourceConfig getSourceConfig() {
        return sourceConfig;
    }
}
