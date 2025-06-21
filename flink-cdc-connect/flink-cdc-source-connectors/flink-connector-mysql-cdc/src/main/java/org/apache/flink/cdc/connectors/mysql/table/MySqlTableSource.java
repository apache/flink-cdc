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

package org.apache.flink.cdc.connectors.mysql.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.debezium.config.CommonConnectorConfig.TOMBSTONES_ON_DELETE;
import static io.debezium.connector.mysql.MySqlConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.engine.DebeziumEngine.OFFSET_FLUSH_INTERVAL_MS_PROP;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a MySQL binlog source from a logical
 * description.
 */
public class MySqlTableSource implements ScanTableSource, SupportsReadingMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlTableSource.class);
    private final Set<String> exceptDbzProperties =
            Stream.of(
                            SNAPSHOT_MODE.name(),
                            OFFSET_FLUSH_INTERVAL_MS_PROP,
                            TOMBSTONES_ON_DELETE.name())
                    .collect(Collectors.toSet());

    private final ResolvedSchema physicalSchema;
    private final int port;
    private final String hostname;
    private final String database;
    private final String username;
    private final String password;
    private final String serverId;
    private final String tableName;
    private final ZoneId serverTimeZone;
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
    private final StartupOptions startupOptions;
    private final boolean scanNewlyAddedTableEnabled;
    private final boolean closeIdleReaders;
    private final Properties jdbcProperties;
    private final Duration heartbeatInterval;
    private final String chunkKeyColumn;
    final boolean skipSnapshotBackFill;
    final boolean parseOnlineSchemaChanges;
    private final boolean useLegacyJsonFormat;
    private final boolean assignUnboundedChunkFirst;

    private final boolean appendOnly;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public MySqlTableSource(
            ResolvedSchema physicalSchema,
            int port,
            String hostname,
            String database,
            String tableName,
            String username,
            String password,
            ZoneId serverTimeZone,
            Properties dbzProperties,
            @Nullable String serverId,
            boolean enableParallelRead,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            StartupOptions startupOptions,
            boolean scanNewlyAddedTableEnabled,
            boolean closeIdleReaders,
            Properties jdbcProperties,
            Duration heartbeatInterval,
            @Nullable String chunkKeyColumn,
            boolean skipSnapshotBackFill,
            boolean parseOnlineSchemaChanges,
            boolean useLegacyJsonFormat,
            boolean assignUnboundedChunkFirst,
            boolean appendOnly) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = checkNotNull(hostname);
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.serverId = serverId;
        this.serverTimeZone = serverTimeZone;
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
        this.startupOptions = startupOptions;
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
        this.closeIdleReaders = closeIdleReaders;
        this.jdbcProperties = jdbcProperties;
        this.parseOnlineSchemaChanges = parseOnlineSchemaChanges;
        // Mutable attributes
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
        this.heartbeatInterval = heartbeatInterval;
        this.chunkKeyColumn = chunkKeyColumn;
        this.skipSnapshotBackFill = skipSnapshotBackFill;
        this.useLegacyJsonFormat = useLegacyJsonFormat;
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
        this.appendOnly = appendOnly;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (appendOnly) {
            return ChangelogMode.insertOnly();
        } else {
            return ChangelogMode.all();
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        MetadataConverter[] metadataConverters = getMetadataConverters();
        final TypeInformation<RowData> typeInfo =
                scanContext.createTypeInformation(producedDataType);

        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(physicalDataType)
                        .setMetadataConverters(metadataConverters)
                        .setResultTypeInfo(typeInfo)
                        .setServerTimeZone(serverTimeZone)
                        .setUserDefinedConverterFactory(
                                MySqlDeserializationConverterFactory.instance())
                        .setAppendOnly(appendOnly)
                        .build();
        if (enableParallelRead) {
            MySqlSource<RowData> parallelSource =
                    MySqlSource.<RowData>builder()
                            .hostname(hostname)
                            .port(port)
                            .databaseList(database)
                            // MySQL debezium connector will use the regular expressions to match
                            // the fully-qualified table identifiers of tables.
                            // We need use "\\." insteadof "." .
                            .tableList(database + "\\." + tableName)
                            .username(username)
                            .password(password)
                            .serverTimeZone(serverTimeZone.toString())
                            .serverId(serverId)
                            .splitSize(splitSize)
                            .splitMetaGroupSize(splitMetaGroupSize)
                            .distributionFactorUpper(distributionFactorUpper)
                            .distributionFactorLower(distributionFactorLower)
                            .fetchSize(fetchSize)
                            .connectTimeout(connectTimeout)
                            .connectMaxRetries(connectMaxRetries)
                            .connectionPoolSize(connectionPoolSize)
                            .debeziumProperties(getParallelDbzProperties(dbzProperties))
                            .startupOptions(startupOptions)
                            .deserializer(deserializer)
                            .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                            .closeIdleReaders(closeIdleReaders)
                            .jdbcProperties(jdbcProperties)
                            .heartbeatInterval(heartbeatInterval)
                            .chunkKeyColumn(new ObjectPath(database, tableName), chunkKeyColumn)
                            .skipSnapshotBackfill(skipSnapshotBackFill)
                            .parseOnLineSchemaChanges(parseOnlineSchemaChanges)
                            .useLegacyJsonFormat(useLegacyJsonFormat)
                            .assignUnboundedChunkFirst(assignUnboundedChunkFirst)
                            .build();
            return SourceProvider.of(parallelSource);
        } else {
            org.apache.flink.cdc.connectors.mysql.MySqlSource.Builder<RowData> builder =
                    org.apache.flink.cdc.connectors.mysql.MySqlSource.<RowData>builder()
                            .hostname(hostname)
                            .port(port)
                            .databaseList(database)
                            .tableList(database + "\\." + tableName)
                            .username(username)
                            .password(password)
                            .serverTimeZone(serverTimeZone.toString())
                            .debeziumProperties(dbzProperties)
                            .startupOptions(startupOptions)
                            .deserializer(deserializer);
            Optional.ofNullable(serverId)
                    .ifPresent(serverId -> builder.serverId(Integer.parseInt(serverId)));
            DebeziumSourceFunction<RowData> sourceFunction = builder.build();
            return SourceFunctionProvider.of(sourceFunction, false);
        }
    }

    protected MetadataConverter[] getMetadataConverters() {
        if (metadataKeys.isEmpty()) {
            return new MetadataConverter[0];
        }

        return metadataKeys.stream()
                .map(
                        key ->
                                Stream.of(MySqlReadableMetadata.values())
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(MySqlReadableMetadata::getConverter)
                .toArray(MetadataConverter[]::new);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        // Return metadata in a fixed order
        return Stream.of(MySqlReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                MySqlReadableMetadata::getKey,
                                MySqlReadableMetadata::getDataType,
                                (existingValue, newValue) -> newValue,
                                LinkedHashMap::new));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        MySqlTableSource source =
                new MySqlTableSource(
                        physicalSchema,
                        port,
                        hostname,
                        database,
                        tableName,
                        username,
                        password,
                        serverTimeZone,
                        dbzProperties,
                        serverId,
                        enableParallelRead,
                        splitSize,
                        splitMetaGroupSize,
                        fetchSize,
                        connectTimeout,
                        connectMaxRetries,
                        connectionPoolSize,
                        distributionFactorUpper,
                        distributionFactorLower,
                        startupOptions,
                        scanNewlyAddedTableEnabled,
                        closeIdleReaders,
                        jdbcProperties,
                        heartbeatInterval,
                        chunkKeyColumn,
                        skipSnapshotBackFill,
                        parseOnlineSchemaChanges,
                        useLegacyJsonFormat,
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
        if (!(o instanceof MySqlTableSource)) {
            return false;
        }
        MySqlTableSource that = (MySqlTableSource) o;
        return port == that.port
                && enableParallelRead == that.enableParallelRead
                && splitSize == that.splitSize
                && splitMetaGroupSize == that.splitMetaGroupSize
                && fetchSize == that.fetchSize
                && distributionFactorUpper == that.distributionFactorUpper
                && distributionFactorLower == that.distributionFactorLower
                && scanNewlyAddedTableEnabled == that.scanNewlyAddedTableEnabled
                && closeIdleReaders == that.closeIdleReaders
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(serverTimeZone, that.serverTimeZone)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(connectTimeout, that.connectTimeout)
                && Objects.equals(connectMaxRetries, that.connectMaxRetries)
                && Objects.equals(connectionPoolSize, that.connectionPoolSize)
                && Objects.equals(startupOptions, that.startupOptions)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(jdbcProperties, that.jdbcProperties)
                && Objects.equals(heartbeatInterval, that.heartbeatInterval)
                && Objects.equals(chunkKeyColumn, that.chunkKeyColumn)
                && Objects.equals(skipSnapshotBackFill, that.skipSnapshotBackFill)
                && parseOnlineSchemaChanges == that.parseOnlineSchemaChanges
                && useLegacyJsonFormat == that.useLegacyJsonFormat
                && assignUnboundedChunkFirst == that.assignUnboundedChunkFirst
                && Objects.equals(appendOnly, that.appendOnly);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                port,
                hostname,
                database,
                username,
                password,
                serverId,
                tableName,
                serverTimeZone,
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
                startupOptions,
                producedDataType,
                metadataKeys,
                scanNewlyAddedTableEnabled,
                closeIdleReaders,
                jdbcProperties,
                heartbeatInterval,
                chunkKeyColumn,
                skipSnapshotBackFill,
                parseOnlineSchemaChanges,
                useLegacyJsonFormat,
                assignUnboundedChunkFirst,
                appendOnly);
    }

    @Override
    public String asSummaryString() {
        return "MySQL-CDC";
    }

    @VisibleForTesting
    Properties getDbzProperties() {
        return dbzProperties;
    }

    @VisibleForTesting
    Properties getParallelDbzProperties(Properties dbzProperties) {
        Properties newDbzProperties = new Properties(dbzProperties);
        for (String key : dbzProperties.stringPropertyNames()) {
            if (exceptDbzProperties.contains(key)) {
                LOG.warn("Cannot override debezium option {}.", key);
            } else {
                newDbzProperties.put(key, dbzProperties.get(key));
            }
        }
        return newDbzProperties;
    }
}
