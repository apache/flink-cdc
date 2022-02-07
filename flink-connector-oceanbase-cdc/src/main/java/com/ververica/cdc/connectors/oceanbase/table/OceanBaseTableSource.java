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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.ververica.cdc.connectors.oceanbase.OceanBaseSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.MetadataConverter;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

import java.time.ZoneId;
import java.util.Objects;

/** A {@link DynamicTableSource} implementation for OceanBase. */
public class OceanBaseTableSource implements ScanTableSource {

    private final ResolvedSchema physicalSchema;

    private final OceanBaseTableSourceFactory.StartupMode startupMode;
    private final Long startupTimestamp;

    private final String username;
    private final String password;
    private final String tenantName;
    private final String databaseName;
    private final String tableName;

    private final String rsList;
    private final String logProxyHost;
    private final int logProxyPort;
    private final String jdbcUrl;

    public OceanBaseTableSource(
            ResolvedSchema physicalSchema,
            OceanBaseTableSourceFactory.StartupMode startupMode,
            Long startupTimestamp,
            String username,
            String password,
            String tenantName,
            String databaseName,
            String tableName,
            String rsList,
            String logProxyHost,
            int logProxyPort,
            String jdbcUrl) {
        this.physicalSchema = physicalSchema;
        this.startupMode = startupMode;
        this.startupTimestamp = startupTimestamp;
        this.username = username;
        this.password = password;
        this.tenantName = tenantName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.rsList = rsList;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.jdbcUrl = jdbcUrl;
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
        RowType rowType = (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> resultTypeInfo =
                context.createTypeInformation(physicalSchema.toPhysicalRowDataType());

        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(rowType)
                        .setMetadataConverters(new MetadataConverter[0])
                        .setResultTypeInfo(resultTypeInfo)
                        .setServerTimeZone(ZoneId.systemDefault())
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
                        .rsList(rsList)
                        .logProxyHost(logProxyHost)
                        .logProxyPort(logProxyPort)
                        .jdbcUrl(jdbcUrl)
                        .deserializer(deserializer);
        return SourceFunctionProvider.of(builder.build(), false);
    }

    @Override
    public DynamicTableSource copy() {
        return new OceanBaseTableSource(
                physicalSchema,
                startupMode,
                startupTimestamp,
                username,
                password,
                tenantName,
                databaseName,
                tableName,
                rsList,
                logProxyHost,
                logProxyPort,
                jdbcUrl);
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
                && Objects.equals(this.rsList, that.rsList)
                && Objects.equals(this.logProxyHost, that.logProxyHost)
                && Objects.equals(this.logProxyPort, that.logProxyPort)
                && Objects.equals(this.jdbcUrl, that.jdbcUrl);
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
                rsList,
                logProxyHost,
                logProxyPort,
                jdbcUrl);
    }

    @Override
    public String asSummaryString() {
        return "OceanBase-CDC";
    }
}
