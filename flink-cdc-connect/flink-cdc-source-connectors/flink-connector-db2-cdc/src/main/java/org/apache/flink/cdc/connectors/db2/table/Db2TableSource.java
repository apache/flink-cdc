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

package org.apache.flink.cdc.connectors.db2.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.db2.Db2Source;
import org.apache.flink.cdc.connectors.db2.source.Db2SourceBuilder;
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
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** TableSource for DB2 CDC connector. */
public class Db2TableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;
    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    private final int port;
    private final String hostname;
    private final String database;
    private final String tableName;
    private final String username;
    private final String password;
    private final ZoneId serverTimeZone;
    private final StartupOptions startupOptions;
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
    private final String chunkKeyColumn;
    private final boolean closeIdleReaders;
    private final boolean skipSnapshotBackfill;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public Db2TableSource(
            ResolvedSchema physicalSchema,
            int port,
            String hostname,
            String database,
            String tableName,
            String username,
            String password,
            ZoneId serverTimeZone,
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
            boolean skipSnapshotBackfill) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = hostname;
        this.database = database;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.serverTimeZone = serverTimeZone;
        this.dbzProperties = dbzProperties;
        this.startupOptions = startupOptions;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
        this.enableParallelRead = enableParallelRead;
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.connectTimeout = connectTimeout;
        this.connectionPoolSize = connectionPoolSize;
        this.connectMaxRetries = connectMaxRetries;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.chunkKeyColumn = chunkKeyColumn;
        this.closeIdleReaders = closeIdleReaders;
        this.skipSnapshotBackfill = skipSnapshotBackfill;
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
        final TypeInformation<RowData> typeInfo =
                scanContext.createTypeInformation(producedDataType);
        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(physicalDataType)
                        .setMetadataConverters(metadataConverters)
                        .setResultTypeInfo(typeInfo)
                        .setServerTimeZone(serverTimeZone)
                        .build();

        if (enableParallelRead) {
            JdbcIncrementalSource<RowData> db2ChangeEventSource =
                    Db2SourceBuilder.Db2IncrementalSource.<RowData>builder()
                            .hostname(hostname)
                            .port(port)
                            .databaseList(database)
                            .tableList(tableName)
                            .serverTimeZone(serverTimeZone.toString())
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
                            .chunkKeyColumn(chunkKeyColumn)
                            .closeIdleReaders(closeIdleReaders)
                            .skipSnapshotBackfill(skipSnapshotBackfill)
                            .build();
            return SourceProvider.of(db2ChangeEventSource);
        } else {
            Db2Source.Builder<RowData> builder =
                    Db2Source.<RowData>builder()
                            .hostname(hostname)
                            .port(port)
                            .database(database)
                            .tableList(tableName)
                            .username(username)
                            .password(password)
                            .debeziumProperties(dbzProperties)
                            .deserializer(deserializer)
                            .startupOptions(startupOptions);
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
                                Stream.of(Db2ReadableMetaData.values())
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(Db2ReadableMetaData::getConverter)
                .toArray(MetadataConverter[]::new);
    }

    @Override
    public DynamicTableSource copy() {
        Db2TableSource source =
                new Db2TableSource(
                        physicalSchema,
                        port,
                        hostname,
                        database,
                        tableName,
                        username,
                        password,
                        serverTimeZone,
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
                        skipSnapshotBackfill);
        source.metadataKeys = metadataKeys;
        source.producedDataType = producedDataType;
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Db2TableSource)) {
            return false;
        }
        Db2TableSource that = (Db2TableSource) o;
        return port == that.port
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(serverTimeZone, that.serverTimeZone)
                && Objects.equals(dbzProperties, that.dbzProperties)
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
                && Objects.equals(skipSnapshotBackfill, that.skipSnapshotBackfill);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                port,
                hostname,
                database,
                tableName,
                username,
                password,
                serverTimeZone,
                dbzProperties,
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
                skipSnapshotBackfill);
    }

    @Override
    public String asSummaryString() {
        return "DB2-CDC";
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(Db2ReadableMetaData.values())
                .collect(
                        Collectors.toMap(
                                Db2ReadableMetaData::getKey, Db2ReadableMetaData::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }
}
