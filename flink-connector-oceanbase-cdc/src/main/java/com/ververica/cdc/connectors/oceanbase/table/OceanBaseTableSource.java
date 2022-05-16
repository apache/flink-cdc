/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oceanbase.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.ververica.cdc.connectors.oceanbase.OceanBaseSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.MetadataConverter;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link DynamicTableSource} implementation for OceanBase. */
public class OceanBaseTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;

    private final StartupMode startupMode;
    private final Long startupTimestamp;

    private final String username;
    private final String password;
    private final String tenantName;
    private final String databaseName;
    private final String tableName;
    private final String hostname;
    private final Integer port;
    private final Duration connectTimeout;
    private final ZoneId serverTimeZone;

    private final String rsList;
    private final String logProxyHost;
    private final Integer logProxyPort;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public OceanBaseTableSource(
            ResolvedSchema physicalSchema,
            StartupMode startupMode,
            Long startupTimestamp,
            String username,
            String password,
            String tenantName,
            String databaseName,
            String tableName,
            String hostname,
            Integer port,
            Duration connectTimeout,
            ZoneId serverTimeZone,
            String rsList,
            String logProxyHost,
            int logProxyPort) {
        this.physicalSchema = physicalSchema;
        this.startupMode = startupMode;
        this.startupTimestamp = startupTimestamp;
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.tenantName = checkNotNull(tenantName);
        this.databaseName = checkNotNull(databaseName);
        this.tableName = checkNotNull(tableName);
        this.hostname = hostname;
        this.port = port;
        this.serverTimeZone = serverTimeZone;
        this.connectTimeout = connectTimeout;
        this.rsList = checkNotNull(rsList);
        this.logProxyHost = checkNotNull(logProxyHost);
        this.logProxyPort = logProxyPort;

        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
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
                        .setServerTimeZone(serverTimeZone)
                        .build();

        OceanBaseSource.Builder<RowData> builder =
                OceanBaseSource.<RowData>builder()
                        .startupMode(startupMode)
                        .startupTimestamp(startupTimestamp)
                        .username(username)
                        .password(password)
                        .tenantName(tenantName)
                        .databaseName(databaseName)
                        .tableName(tableName)
                        .hostname(hostname)
                        .port(port)
                        .connectTimeout(connectTimeout)
                        .rsList(rsList)
                        .logProxyHost(logProxyHost)
                        .logProxyPort(logProxyPort)
                        .serverTimeZone(serverTimeZone)
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
        OceanBaseTableSource source =
                new OceanBaseTableSource(
                        physicalSchema,
                        startupMode,
                        startupTimestamp,
                        username,
                        password,
                        tenantName,
                        databaseName,
                        tableName,
                        hostname,
                        port,
                        connectTimeout,
                        serverTimeZone,
                        rsList,
                        logProxyHost,
                        logProxyPort);
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
                && Objects.equals(this.startupMode, that.startupMode)
                && Objects.equals(this.startupTimestamp, that.startupTimestamp)
                && Objects.equals(this.username, that.username)
                && Objects.equals(this.password, that.password)
                && Objects.equals(this.tenantName, that.tenantName)
                && Objects.equals(this.databaseName, that.databaseName)
                && Objects.equals(this.tableName, that.tableName)
                && Objects.equals(this.hostname, that.hostname)
                && Objects.equals(this.port, that.port)
                && Objects.equals(this.connectTimeout, that.connectTimeout)
                && Objects.equals(this.serverTimeZone, that.serverTimeZone)
                && Objects.equals(this.rsList, that.rsList)
                && Objects.equals(this.logProxyHost, that.logProxyHost)
                && Objects.equals(this.logProxyPort, that.logProxyPort)
                && Objects.equals(this.producedDataType, that.producedDataType)
                && Objects.equals(this.metadataKeys, that.metadataKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                startupMode,
                startupTimestamp,
                username,
                password,
                tenantName,
                databaseName,
                tableName,
                hostname,
                port,
                connectTimeout,
                serverTimeZone,
                rsList,
                logProxyHost,
                logProxyPort,
                producedDataType,
                metadataKeys);
    }

    @Override
    public String asSummaryString() {
        return "OceanBase-CDC";
    }
}
