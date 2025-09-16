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

package org.apache.flink.cdc.connectors.postgres.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.catalog.ObjectPath;
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
 * A {@link DynamicTableSource} that describes how to create a PostgreSQL source from a logical
 * description.
 */
public class PostgreSQLTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;
    private final int port;
    private final String hostname;
    private final String database;
    private final String schemaName;
    private final String tableName;
    private final String username;
    private final String password;
    private final String pluginName;
    private final String slotName;
    private final DebeziumChangelogMode changelogMode;
    private final Properties dbzProperties;
    private final boolean enableParallelRead;
    private final int splitSize;
    private final int splitMetaGroupSize;
    private final int fetchSize;
    private final Duration connectTimeout;
    private final int connectionPoolSize;
    private final int connectMaxRetries;
    private final double distributionFactorUpper;
    private final double distributionFactorLower;
    private final Duration heartbeatInterval;
    private final StartupOptions startupOptions;
    private final String chunkKeyColumn;
    private final boolean closeIdleReaders;
    private final boolean skipSnapshotBackfill;
    private final boolean scanNewlyAddedTableEnabled;
    private final int lsnCommitCheckpointsDelay;
    private final boolean assignUnboundedChunkFirst;
    private final boolean appendOnly;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public PostgreSQLTableSource(
            ResolvedSchema physicalSchema,
            int port,
            String hostname,
            String database,
            String schemaName,
            String tableName,
            String username,
            String password,
            String pluginName,
            String slotName,
            DebeziumChangelogMode changelogMode,
            Properties dbzProperties,
            boolean enableParallelRead,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            Duration heartbeatInterval,
            StartupOptions startupOptions,
            @Nullable String chunkKeyColumn,
            boolean closeIdleReaders,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            int lsnCommitCheckpointsDelay,
            boolean assignUnboundedChunkFirst,
            boolean appendOnly) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = checkNotNull(hostname);
        this.database = checkNotNull(database);
        this.schemaName = checkNotNull(schemaName);
        this.tableName = checkNotNull(tableName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.pluginName = checkNotNull(pluginName);
        this.slotName = slotName;
        this.changelogMode = changelogMode;
        this.dbzProperties = dbzProperties;
        this.enableParallelRead = enableParallelRead;
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.connectTimeout = connectTimeout;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.heartbeatInterval = heartbeatInterval;
        this.startupOptions = startupOptions;
        this.chunkKeyColumn = chunkKeyColumn;
        // Mutable attributes
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
        this.closeIdleReaders = closeIdleReaders;
        this.skipSnapshotBackfill = skipSnapshotBackfill;
        this.scanNewlyAddedTableEnabled = isScanNewlyAddedTableEnabled;
        this.lsnCommitCheckpointsDelay = lsnCommitCheckpointsDelay;
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
        this.appendOnly = appendOnly;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (appendOnly) {
            return ChangelogMode.insertOnly();
        }

        switch (changelogMode) {
            case UPSERT:
                return org.apache.flink.table.connector.ChangelogMode.upsert();
            case ALL:
                return org.apache.flink.table.connector.ChangelogMode.all();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported changelog mode: " + changelogMode);
        }
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
                                PostgreSQLDeserializationConverterFactory.instance())
                        .setValueValidator(new PostgresValueValidator(schemaName, tableName))
                        .setChangelogMode(changelogMode)
                        .setAppendOnly(appendOnly)
                        .build();

        if (enableParallelRead) {
            JdbcIncrementalSource<RowData> parallelSource =
                    PostgresSourceBuilder.PostgresIncrementalSource.<RowData>builder()
                            .hostname(hostname)
                            .port(port)
                            .database(database)
                            .schemaList(schemaName)
                            .tableList(schemaName + "." + tableName)
                            .username(username)
                            .password(password)
                            .decodingPluginName(pluginName)
                            .slotName(slotName)
                            .debeziumProperties(dbzProperties)
                            .deserializer(deserializer)
                            .splitSize(splitSize)
                            .splitMetaGroupSize(splitMetaGroupSize)
                            .distributionFactorUpper(distributionFactorUpper)
                            .distributionFactorLower(distributionFactorLower)
                            .fetchSize(fetchSize)
                            .connectTimeout(connectTimeout)
                            .connectMaxRetries(connectMaxRetries)
                            .connectionPoolSize(connectionPoolSize)
                            .startupOptions(startupOptions)
                            .chunkKeyColumn(new ObjectPath(schemaName, tableName), chunkKeyColumn)
                            .heartbeatInterval(heartbeatInterval)
                            .closeIdleReaders(closeIdleReaders)
                            .skipSnapshotBackfill(skipSnapshotBackfill)
                            .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                            .lsnCommitCheckpointsDelay(lsnCommitCheckpointsDelay)
                            .assignUnboundedChunkFirst(assignUnboundedChunkFirst)
                            .build();
            return SourceProvider.of(parallelSource);
        } else {
            DebeziumSourceFunction<RowData> sourceFunction =
                    PostgreSQLSource.<RowData>builder()
                            .hostname(hostname)
                            .port(port)
                            .database(database)
                            .schemaList(schemaName)
                            .tableList(schemaName + "." + tableName)
                            .username(username)
                            .password(password)
                            .decodingPluginName(pluginName)
                            .slotName(slotName)
                            .debeziumProperties(dbzProperties)
                            .deserializer(deserializer)
                            .build();
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
                                Stream.of(PostgreSQLReadableMetadata.values())
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(PostgreSQLReadableMetadata::getConverter)
                .toArray(MetadataConverter[]::new);
    }

    @Override
    public DynamicTableSource copy() {
        PostgreSQLTableSource source =
                new PostgreSQLTableSource(
                        physicalSchema,
                        port,
                        hostname,
                        database,
                        schemaName,
                        tableName,
                        username,
                        password,
                        pluginName,
                        slotName,
                        changelogMode,
                        dbzProperties,
                        enableParallelRead,
                        splitSize,
                        splitMetaGroupSize,
                        fetchSize,
                        connectTimeout,
                        connectMaxRetries,
                        connectionPoolSize,
                        distributionFactorUpper,
                        distributionFactorLower,
                        heartbeatInterval,
                        startupOptions,
                        chunkKeyColumn,
                        closeIdleReaders,
                        skipSnapshotBackfill,
                        scanNewlyAddedTableEnabled,
                        lsnCommitCheckpointsDelay,
                        assignUnboundedChunkFirst,
                        appendOnly);
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
        PostgreSQLTableSource that = (PostgreSQLTableSource) o;
        return port == that.port
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(pluginName, that.pluginName)
                && Objects.equals(slotName, that.slotName)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(changelogMode, that.changelogMode)
                && Objects.equals(enableParallelRead, that.enableParallelRead)
                && Objects.equals(splitSize, that.splitSize)
                && Objects.equals(splitMetaGroupSize, that.splitMetaGroupSize)
                && Objects.equals(fetchSize, that.fetchSize)
                && Objects.equals(connectTimeout, that.connectTimeout)
                && Objects.equals(connectMaxRetries, that.connectMaxRetries)
                && Objects.equals(connectionPoolSize, that.connectionPoolSize)
                && Objects.equals(distributionFactorUpper, that.distributionFactorUpper)
                && Objects.equals(distributionFactorLower, that.distributionFactorLower)
                && Objects.equals(heartbeatInterval, that.heartbeatInterval)
                && Objects.equals(startupOptions, that.startupOptions)
                && Objects.equals(chunkKeyColumn, that.chunkKeyColumn)
                && Objects.equals(closeIdleReaders, that.closeIdleReaders)
                && Objects.equals(skipSnapshotBackfill, that.skipSnapshotBackfill)
                && Objects.equals(scanNewlyAddedTableEnabled, that.scanNewlyAddedTableEnabled)
                && Objects.equals(assignUnboundedChunkFirst, that.assignUnboundedChunkFirst)
                && Objects.equals(appendOnly, that.appendOnly);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                port,
                hostname,
                database,
                schemaName,
                tableName,
                username,
                password,
                pluginName,
                slotName,
                dbzProperties,
                producedDataType,
                metadataKeys,
                changelogMode,
                enableParallelRead,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                heartbeatInterval,
                startupOptions,
                chunkKeyColumn,
                closeIdleReaders,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst,
                appendOnly);
    }

    @Override
    public String asSummaryString() {
        return "PostgreSQL-CDC";
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(PostgreSQLReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                PostgreSQLReadableMetadata::getKey,
                                PostgreSQLReadableMetadata::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }
}
