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

package org.apache.flink.cdc.connectors.oceanbase.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oceanbase.OceanBaseSource;
import org.apache.flink.cdc.connectors.oceanbase.source.OceanBaseSourceBuilder;
import org.apache.flink.cdc.connectors.oceanbase.source.converter.OceanBaseDeserializationConverterFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
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

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link DynamicTableSource} implementation for OceanBase. */
public class OceanBaseTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;

    private final StartupOptions startupOptions;
    private final String username;
    private final String password;
    private final String tenantName;
    private final String databaseName;
    private final String tableName;
    private final String tableList;
    private final Duration connectTimeout;
    private final String serverTimeZone;

    private final String hostname;
    private final int port;
    private final String compatibleMode;
    private final String jdbcDriver;
    private final Properties jdbcProperties;

    private final String logProxyHost;
    private final Integer logProxyPort;
    private final String logProxyClientId;
    private final Long startupTimestamp;
    private final String rsList;
    private final String configUrl;
    private final String workingMode;
    private final Properties obcdcProperties;
    private final Properties debeziumProperties;
    private final boolean enableParallelRead;
    private final int splitSize;
    private final int splitMetaGroupSize;
    private final int fetchSize;
    private final int connectMaxRetries;
    private final int connectionPoolSize;
    private final double distributionFactorUpper;
    private final double distributionFactorLower;
    private final String chunkKeyColumn;
    private final boolean closeIdlerReaders;
    private final boolean scanNewlyAddedTableEnabled;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    @Deprecated
    public OceanBaseTableSource(
            ResolvedSchema physicalSchema,
            StartupOptions startupOptions,
            String username,
            String password,
            String tenantName,
            String databaseName,
            String tableName,
            String tableList,
            String serverTimeZone,
            Duration connectTimeout,
            String hostname,
            int port,
            String compatibleMode,
            String jdbcDriver,
            Properties jdbcProperties,
            String logProxyHost,
            Integer logProxyPort,
            String logProxyClientId,
            Long startupTimestamp,
            String rsList,
            String configUrl,
            String workingMode,
            Properties obcdcProperties,
            Properties debeziumProperties) {
        this.physicalSchema = physicalSchema;
        this.startupOptions = checkNotNull(startupOptions);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.tenantName = tenantName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableList = tableList;
        this.serverTimeZone = serverTimeZone;
        this.connectTimeout = connectTimeout;
        this.hostname = checkNotNull(hostname);
        this.port = port;
        this.compatibleMode = compatibleMode;
        this.jdbcDriver = jdbcDriver;
        this.jdbcProperties = jdbcProperties;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.logProxyClientId = logProxyClientId;
        this.startupTimestamp = startupTimestamp;
        this.rsList = rsList;
        this.configUrl = configUrl;
        this.workingMode = workingMode;
        this.obcdcProperties = obcdcProperties;
        this.debeziumProperties = debeziumProperties;

        this.enableParallelRead = false;
        this.splitSize = 0;
        this.splitMetaGroupSize = 0;
        this.fetchSize = 0;
        this.connectMaxRetries = 0;
        this.connectionPoolSize = 0;
        this.distributionFactorUpper = 0;
        this.distributionFactorLower = 0;
        this.chunkKeyColumn = null;
        this.closeIdlerReaders = false;
        this.scanNewlyAddedTableEnabled = false;

        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
    }

    public OceanBaseTableSource(
            ResolvedSchema physicalSchema,
            StartupOptions startupOptions,
            String username,
            String password,
            String tenantName,
            String databaseName,
            String tableName,
            String serverTimeZone,
            Duration connectTimeout,
            String hostname,
            int port,
            String compatibleMode,
            String jdbcDriver,
            String logProxyHost,
            Integer logProxyPort,
            String rsList,
            String configUrl,
            String workingMode,
            Properties obcdcProperties,
            Properties debeziumProperties,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            String chunkKeyColumn,
            boolean closeIdlerReaders,
            boolean scanNewlyAddedTableEnabled) {
        this.physicalSchema = physicalSchema;
        this.startupOptions = checkNotNull(startupOptions);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.tenantName = tenantName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.serverTimeZone = serverTimeZone;
        this.connectTimeout = connectTimeout;
        this.hostname = checkNotNull(hostname);
        this.port = port;
        this.compatibleMode = compatibleMode;
        this.jdbcDriver = jdbcDriver;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.rsList = rsList;
        this.configUrl = configUrl;
        this.workingMode = workingMode;
        this.obcdcProperties = obcdcProperties;
        this.debeziumProperties = debeziumProperties;

        this.tableList = null;
        this.jdbcProperties = null;
        this.logProxyClientId = null;
        this.startupTimestamp = null;

        this.enableParallelRead = true;
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.chunkKeyColumn = chunkKeyColumn;
        this.closeIdlerReaders = closeIdlerReaders;
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;

        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        MetadataConverter[] metadataConverters = getMetadataConverters();
        TypeInformation<RowData> resultTypeInfo = context.createTypeInformation(producedDataType);

        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(physicalDataType)
                        .setMetadataConverters(metadataConverters)
                        .setResultTypeInfo(resultTypeInfo)
                        .setServerTimeZone(
                                serverTimeZone == null
                                        ? ZoneId.systemDefault()
                                        : ZoneId.of(serverTimeZone))
                        .setUserDefinedConverterFactory(
                                OceanBaseDeserializationConverterFactory.instance())
                        .build();

        if (enableParallelRead) {
            OceanBaseSourceBuilder.OceanBaseIncrementalSource<RowData> incrementalSource =
                    OceanBaseSourceBuilder.OceanBaseIncrementalSource.<RowData>builder()
                            .startupOptions(startupOptions)
                            .hostname(hostname)
                            .port(port)
                            .compatibleMode(compatibleMode)
                            .driverClassName(jdbcDriver)
                            .tenantName(tenantName)
                            .databaseList(databaseName)
                            .tableList(databaseName + "." + tableName)
                            .username(username)
                            .password(password)
                            .logProxyHost(logProxyHost)
                            .logProxyPort(logProxyPort)
                            .rsList(rsList)
                            .configUrl(configUrl)
                            .workingMode(workingMode)
                            .obcdcProperties(obcdcProperties)
                            .debeziumProperties(debeziumProperties)
                            .serverTimeZone(serverTimeZone)
                            .connectTimeout(connectTimeout)
                            .connectionPoolSize(connectionPoolSize)
                            .connectMaxRetries(connectMaxRetries)
                            .chunkKeyColumn(chunkKeyColumn)
                            .splitSize(splitSize)
                            .fetchSize(fetchSize)
                            .splitMetaGroupSize(splitMetaGroupSize)
                            .distributionFactorUpper(distributionFactorUpper)
                            .distributionFactorLower(distributionFactorLower)
                            .closeIdleReaders(closeIdlerReaders)
                            .deserializer(deserializer)
                            .build();
            return SourceProvider.of(incrementalSource);
        }

        OceanBaseSource.Builder<RowData> builder =
                OceanBaseSource.<RowData>builder()
                        .startupOptions(startupOptions)
                        .username(username)
                        .password(password)
                        .tenantName(tenantName)
                        .databaseName(databaseName)
                        .tableName(tableName)
                        .tableList(tableList)
                        .serverTimeZone(serverTimeZone)
                        .connectTimeout(connectTimeout)
                        .hostname(hostname)
                        .port(port)
                        .compatibleMode(compatibleMode)
                        .jdbcDriver(jdbcDriver)
                        .jdbcProperties(jdbcProperties)
                        .logProxyHost(logProxyHost)
                        .logProxyPort(logProxyPort)
                        .logProxyClientId(logProxyClientId)
                        .startupTimestamp(startupTimestamp)
                        .rsList(rsList)
                        .configUrl(configUrl)
                        .workingMode(workingMode)
                        .obcdcProperties(obcdcProperties)
                        .debeziumProperties(debeziumProperties)
                        .deserializer(deserializer);
        return SourceFunctionProvider.of(builder.build(), false);
    }

    protected MetadataConverter[] getMetadataConverters() {
        if (metadataKeys.isEmpty()) {
            return new MetadataConverter[0];
        }
        return metadataKeys.stream()
                .map(
                        key ->
                                Stream.of(OceanBaseReadableMetadata.values())
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(OceanBaseReadableMetadata::getConverter)
                .toArray(MetadataConverter[]::new);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(OceanBaseReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                OceanBaseReadableMetadata::getKey,
                                OceanBaseReadableMetadata::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        if (enableParallelRead) {
            OceanBaseTableSource source =
                    new OceanBaseTableSource(
                            physicalSchema,
                            startupOptions,
                            username,
                            password,
                            tenantName,
                            databaseName,
                            tableName,
                            serverTimeZone,
                            connectTimeout,
                            hostname,
                            port,
                            compatibleMode,
                            jdbcDriver,
                            logProxyHost,
                            logProxyPort,
                            rsList,
                            configUrl,
                            workingMode,
                            obcdcProperties,
                            debeziumProperties,
                            splitSize,
                            splitMetaGroupSize,
                            fetchSize,
                            connectMaxRetries,
                            connectionPoolSize,
                            distributionFactorUpper,
                            distributionFactorLower,
                            chunkKeyColumn,
                            closeIdlerReaders,
                            scanNewlyAddedTableEnabled);
            source.metadataKeys = metadataKeys;
            source.producedDataType = producedDataType;
            return source;
        }
        OceanBaseTableSource source =
                new OceanBaseTableSource(
                        physicalSchema,
                        startupOptions,
                        username,
                        password,
                        tenantName,
                        databaseName,
                        tableName,
                        tableList,
                        serverTimeZone,
                        connectTimeout,
                        hostname,
                        port,
                        compatibleMode,
                        jdbcDriver,
                        jdbcProperties,
                        logProxyHost,
                        logProxyPort,
                        logProxyClientId,
                        startupTimestamp,
                        rsList,
                        configUrl,
                        workingMode,
                        obcdcProperties,
                        debeziumProperties);
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
        OceanBaseTableSource that = (OceanBaseTableSource) o;
        return Objects.equals(this.physicalSchema, that.physicalSchema)
                && Objects.equals(this.startupOptions, that.startupOptions)
                && Objects.equals(this.username, that.username)
                && Objects.equals(this.password, that.password)
                && Objects.equals(this.tenantName, that.tenantName)
                && Objects.equals(this.databaseName, that.databaseName)
                && Objects.equals(this.tableName, that.tableName)
                && Objects.equals(this.tableList, that.tableList)
                && Objects.equals(this.serverTimeZone, that.serverTimeZone)
                && Objects.equals(this.connectTimeout, that.connectTimeout)
                && Objects.equals(this.hostname, that.hostname)
                && Objects.equals(this.port, that.port)
                && Objects.equals(this.compatibleMode, that.compatibleMode)
                && Objects.equals(this.jdbcDriver, that.jdbcDriver)
                && Objects.equals(this.jdbcProperties, that.jdbcProperties)
                && Objects.equals(this.logProxyHost, that.logProxyHost)
                && Objects.equals(this.logProxyPort, that.logProxyPort)
                && Objects.equals(this.logProxyClientId, that.logProxyClientId)
                && Objects.equals(this.startupTimestamp, that.startupTimestamp)
                && Objects.equals(this.rsList, that.rsList)
                && Objects.equals(this.configUrl, that.configUrl)
                && Objects.equals(this.workingMode, that.workingMode)
                && Objects.equals(this.obcdcProperties, that.obcdcProperties)
                && Objects.equals(this.debeziumProperties, that.debeziumProperties)
                && Objects.equals(this.enableParallelRead, that.enableParallelRead)
                && Objects.equals(this.splitSize, that.splitSize)
                && Objects.equals(this.splitMetaGroupSize, that.splitMetaGroupSize)
                && Objects.equals(this.fetchSize, that.fetchSize)
                && Objects.equals(this.connectMaxRetries, that.connectMaxRetries)
                && Objects.equals(this.connectionPoolSize, that.connectionPoolSize)
                && Objects.equals(this.distributionFactorUpper, that.distributionFactorUpper)
                && Objects.equals(this.distributionFactorLower, that.distributionFactorLower)
                && Objects.equals(this.chunkKeyColumn, that.chunkKeyColumn)
                && Objects.equals(this.closeIdlerReaders, that.closeIdlerReaders)
                && Objects.equals(this.scanNewlyAddedTableEnabled, that.scanNewlyAddedTableEnabled)
                && Objects.equals(this.producedDataType, that.producedDataType)
                && Objects.equals(this.metadataKeys, that.metadataKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                startupOptions,
                username,
                password,
                tenantName,
                databaseName,
                tableName,
                tableList,
                serverTimeZone,
                connectTimeout,
                hostname,
                port,
                compatibleMode,
                jdbcDriver,
                jdbcProperties,
                logProxyHost,
                logProxyPort,
                logProxyClientId,
                startupTimestamp,
                rsList,
                configUrl,
                workingMode,
                obcdcProperties,
                debeziumProperties,
                enableParallelRead,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                chunkKeyColumn,
                closeIdlerReaders,
                scanNewlyAddedTableEnabled,
                producedDataType,
                metadataKeys);
    }

    @Override
    public String asSummaryString() {
        return "OceanBase-CDC";
    }
}
