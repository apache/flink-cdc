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

package org.apache.flink.cdc.connectors.oracle.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.oracle.OracleSource;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a Oracle redo log from a logical
 * description.
 */
public class OracleTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;
    @Nullable private final String url;
    private final int port;
    private final String hostname;
    private final String database;
    private final String username;
    private final String password;
    private final String tableName;
    private final String schemaName;
    private final Properties dbzProperties;
    private final StartupOptions startupOptions;
    private final boolean enableParallelRead;
    private final int splitSize;
    private final int splitMetaGroupSize;
    private final int fetchSize;
    private final Duration connectTimeout;
    private final int connectionPoolSize;
    private final int connectMaxRetries;
    private final double distributionFactorUpper;
    private final double distributionFactorLower;
    private final String chunkKeyColumn;
    private final boolean closeIdleReaders;
    private final boolean skipSnapshotBackfill;
    private final boolean scanNewlyAddedTableEnabled;
    private final boolean assignUnboundedChunkFirst;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public OracleTableSource(
            ResolvedSchema physicalSchema,
            @Nullable String url,
            int port,
            String hostname,
            String database,
            String tableName,
            String schemaName,
            String username,
            String password,
            Properties dbzProperties,
            StartupOptions startupOptions,
            boolean enableParallelRead,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            @Nullable String chunkKeyColumn,
            boolean closeIdleReaders,
            boolean skipSnapshotBackfill,
            boolean scanNewlyAddedTableEnabled,
            boolean assignUnboundedChunkFirst) {
        this.physicalSchema = physicalSchema;
        this.url = url;
        this.port = port;
        this.hostname = checkNotNull(hostname);
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.schemaName = checkNotNull(schemaName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.dbzProperties = dbzProperties;
        this.startupOptions = startupOptions;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
        this.enableParallelRead = enableParallelRead;
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.connectTimeout = connectTimeout;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.chunkKeyColumn = chunkKeyColumn;
        this.closeIdleReaders = closeIdleReaders;
        this.skipSnapshotBackfill = skipSnapshotBackfill;
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        MetadataConverter[] metadataConverters = getMetadataConverters();
        TypeInformation<RowData> typeInfo = scanContext.createTypeInformation(producedDataType);

        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(physicalDataType)
                        .setMetadataConverters(metadataConverters)
                        .setResultTypeInfo(typeInfo)
                        .setUserDefinedConverterFactory(
                                OracleDeserializationConverterFactory.instance())
                        .build();

        if (enableParallelRead) {
            JdbcIncrementalSource<RowData> oracleChangeEventSource =
                    OracleSourceBuilder.OracleIncrementalSource.<RowData>builder()
                            .hostname(hostname)
                            .url(url)
                            .port(port)
                            .databaseList(database)
                            .schemaList(schemaName)
                            .tableList(schemaName + "." + tableName)
                            .username(username)
                            .password(password)
                            .startupOptions(startupOptions)
                            .deserializer(deserializer)
                            .debeziumProperties(dbzProperties)
                            .splitSize(splitSize)
                            .splitMetaGroupSize(splitMetaGroupSize)
                            .fetchSize(fetchSize)
                            .connectTimeout(connectTimeout)
                            .connectionPoolSize(connectionPoolSize)
                            .connectMaxRetries(connectMaxRetries)
                            .distributionFactorUpper(distributionFactorUpper)
                            .distributionFactorLower(distributionFactorLower)
                            .closeIdleReaders(closeIdleReaders)
                            .skipSnapshotBackfill(skipSnapshotBackfill)
                            .chunkKeyColumn(chunkKeyColumn)
                            .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                            .assignUnboundedChunkFirst(assignUnboundedChunkFirst)
                            .build();

            return SourceProvider.of(oracleChangeEventSource);
        } else {
            OracleSource.Builder<RowData> builder =
                    OracleSource.<RowData>builder()
                            .hostname(hostname)
                            .url(url)
                            .port(port)
                            .database(database)
                            .tableList(schemaName + "." + tableName)
                            .schemaList(schemaName)
                            .username(username)
                            .password(password)
                            .debeziumProperties(dbzProperties)
                            .startupOptions(startupOptions)
                            .deserializer(deserializer);
            DebeziumSourceFunction<RowData> sourceFunction = builder.build();

            return SourceFunctionProvider.of(sourceFunction, false);
        }
    }

    private MetadataConverter[] getMetadataConverters() {
        if (metadataKeys.isEmpty()) {
            return new MetadataConverter[0];
        }

        return metadataKeys.stream()
                .map(
                        key ->
                                Stream.of(OracleReadableMetaData.values())
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(OracleReadableMetaData::getConverter)
                .toArray(MetadataConverter[]::new);
    }

    @Override
    public DynamicTableSource copy() {
        OracleTableSource source =
                new OracleTableSource(
                        physicalSchema,
                        url,
                        port,
                        hostname,
                        database,
                        tableName,
                        schemaName,
                        username,
                        password,
                        dbzProperties,
                        startupOptions,
                        enableParallelRead,
                        splitSize,
                        splitMetaGroupSize,
                        fetchSize,
                        connectTimeout,
                        connectMaxRetries,
                        connectionPoolSize,
                        distributionFactorUpper,
                        distributionFactorLower,
                        chunkKeyColumn,
                        closeIdleReaders,
                        skipSnapshotBackfill,
                        scanNewlyAddedTableEnabled,
                        assignUnboundedChunkFirst);
        source.metadataKeys = metadataKeys;
        source.producedDataType = producedDataType;
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OracleTableSource that = (OracleTableSource) o;
        return Objects.equals(url, that.url)
                && Objects.equals(port, that.port)
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(startupOptions, that.startupOptions)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(enableParallelRead, that.enableParallelRead)
                && Objects.equals(splitSize, that.splitSize)
                && Objects.equals(splitMetaGroupSize, that.splitMetaGroupSize)
                && Objects.equals(fetchSize, that.fetchSize)
                && Objects.equals(connectTimeout, that.connectTimeout)
                && Objects.equals(connectMaxRetries, that.connectMaxRetries)
                && Objects.equals(connectionPoolSize, that.connectionPoolSize)
                && Objects.equals(distributionFactorUpper, that.distributionFactorUpper)
                && Objects.equals(distributionFactorLower, that.distributionFactorLower)
                && Objects.equals(chunkKeyColumn, that.chunkKeyColumn)
                && Objects.equals(closeIdleReaders, that.closeIdleReaders)
                && Objects.equals(skipSnapshotBackfill, that.skipSnapshotBackfill)
                && Objects.equals(scanNewlyAddedTableEnabled, that.scanNewlyAddedTableEnabled)
                && Objects.equals(assignUnboundedChunkFirst, that.assignUnboundedChunkFirst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                url,
                port,
                hostname,
                database,
                username,
                password,
                tableName,
                schemaName,
                dbzProperties,
                startupOptions,
                producedDataType,
                metadataKeys,
                enableParallelRead,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                chunkKeyColumn,
                closeIdleReaders,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);
    }

    @Override
    public String asSummaryString() {
        return "Oracle-CDC";
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
}
