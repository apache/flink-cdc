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

package org.apache.flink.cdc.connectors.tidb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.tidb.source.TiDBSourceBuilder;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.tikv.common.TiConfiguration;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class TiDBTableSource implements ScanTableSource, SupportsReadingMetadata {
    private final ResolvedSchema physicalSchema;

    private final StartupOptions startupOptions;
    private final String tableList;
    private final String tableName;
    private final Duration connectTimeout;
    private final String jdbcDriver;
    private final String serverTimeZone;

    private final String pdAddresses;
    private final String hostMapping;

    private final int port;
    private final String hostName;
    private final String database;
    private final String username;
    private final String password;
    private final Duration heartbeatInterval;

    //  incremental snapshot options
    private final int splitSize;
    private final int splitMetaGroupSize;
    private final int fetchSize;
    private final int connectionPoolSize;
    private final int connectMaxRetries;
    private final double distributionFactorUpper;
    private final double distributionFactorLower;
    private final String chunkKeyColumn;
    private final Map<ObjectPath, String> chunkKeyColumns;

    private final Properties jdbcProperties;
    private final Map<String, String> options;
    private final boolean enableParallelRead;

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public TiDBTableSource(
            ResolvedSchema physicalSchema,
            int port,
            String hostName,
            String database,
            String tableName,
            String tableList,
            String username,
            String password,
            String serverTimeZone,
            Properties jdbcProperties,
            boolean enableParallelRead,
            Duration heartbeatInterval,
            String pdAddresses,
            String hostMapping,
            Duration connectTimeout,
            Map<String, String> options,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            @Nullable String chunkKeyColumn,
            @Nullable Map<ObjectPath, String> chunkKeyColumns,
            String jdbcDriver,
            StartupOptions startupOptions) {
        this.physicalSchema = physicalSchema;
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.pdAddresses = checkNotNull(pdAddresses);
        this.port = port;
        this.username = username;
        this.password = password;
        this.serverTimeZone = serverTimeZone;
        this.jdbcProperties = jdbcProperties;
        this.hostName = hostName;
        this.options = options;

        //  incremental snapshot options
        this.enableParallelRead = enableParallelRead;
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.chunkKeyColumn = chunkKeyColumn;
        this.chunkKeyColumns = chunkKeyColumns;
        this.heartbeatInterval = heartbeatInterval;
        this.jdbcDriver = jdbcDriver;
        this.connectTimeout = connectTimeout;
        this.tableList = tableList;
        this.hostMapping = hostMapping;
        this.startupOptions = startupOptions;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // TIDB source  builder
        final TiConfiguration tiConf =
                TiDBSourceOptions.getTiConfiguration(pdAddresses, hostMapping, options);

        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();

        TypeInformation<RowData> typeInfo = scanContext.createTypeInformation(producedDataType);
        MetadataConverter[] metadataConverters = getMetadataConverters();

        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(physicalDataType)
                        .setMetadataConverters(metadataConverters)
                        .setResultTypeInfo(typeInfo)
                        .setServerTimeZone(
                                serverTimeZone == null
                                        ? ZoneId.systemDefault()
                                        : ZoneId.of(serverTimeZone))
                        .setUserDefinedConverterFactory(
                                TiDBDeserializationConverterFactory.instance())
                        .build();

        JdbcIncrementalSource<RowData> parallelSource =
                TiDBSourceBuilder.TiDBIncrementalSource.<RowData>builder()
                        .hostname(hostName)
                        .port(port)
                        .tiConfiguration(tiConf)
                        .databaseList(database)
                        .tableList(database + "\\." + tableName)
                        .username(username)
                        .password(password)
                        .serverTimeZone(serverTimeZone.toString())
                        .splitSize(splitSize)
                        .splitMetaGroupSize(splitMetaGroupSize)
                        .distributionFactorUpper(distributionFactorUpper)
                        .distributionFactorLower(distributionFactorLower)
                        .fetchSize(fetchSize)
                        .connectTimeout(connectTimeout)
                        .connectionPoolSize(connectionPoolSize)
                        .chunkKeyColumn(chunkKeyColumn)
                        .chunkKeyColumns(chunkKeyColumns)
                        .driverClassName(jdbcDriver)
                        .connectMaxRetries(connectMaxRetries)
                        .jdbcProperties(jdbcProperties)
                        .startupOptions(startupOptions)
                        .pdAddresses(pdAddresses)
                        .hostMapping(hostMapping)
                        .deserializer(deserializer)
                        .build();
        // todo  JdbcIncrementalSource<RowData> parallelSource =
        //                     TiDBSourceBuilder.TiDBIncrementalSource.builder()
        return SourceProvider.of(parallelSource);
    }

    @Override
    public DynamicTableSource copy() {
        TiDBTableSource source =
                new TiDBTableSource(
                        physicalSchema,
                        port,
                        hostName,
                        database,
                        tableName,
                        tableList,
                        username,
                        password,
                        serverTimeZone,
                        jdbcProperties,
                        enableParallelRead,
                        heartbeatInterval,
                        pdAddresses,
                        hostMapping,
                        connectTimeout,
                        options,
                        splitSize,
                        splitMetaGroupSize,
                        fetchSize,
                        connectMaxRetries,
                        connectionPoolSize,
                        distributionFactorUpper,
                        distributionFactorLower,
                        chunkKeyColumn,
                        chunkKeyColumns,
                        jdbcDriver,
                        startupOptions);
        source.producedDataType = producedDataType;
        source.metadataKeys = metadataKeys;

        return source;
    }

    @Override
    public String asSummaryString() {
        return "TiDB-CDC";
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(TiDBReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                TiDBReadableMetadata::getKey, TiDBReadableMetadata::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    // TiDBMetadataConverter to  MetadataConverter
    private MetadataConverter[] getMetadataConverters() {
        if (metadataKeys.isEmpty()) {
            return new MetadataConverter[0];
        }

        return metadataKeys.stream()
                .map(
                        key ->
                                Stream.of(TiDBReadableMetadata.values())
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(TiDBReadableMetadata::getConverter)
                .toArray(MetadataConverter[]::new);
    }
}
