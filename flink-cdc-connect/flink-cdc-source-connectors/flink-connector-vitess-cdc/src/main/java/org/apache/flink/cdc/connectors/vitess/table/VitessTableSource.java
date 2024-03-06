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

package org.apache.flink.cdc.connectors.vitess.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.vitess.VitessSource;
import org.apache.flink.cdc.connectors.vitess.config.SchemaAdjustmentMode;
import org.apache.flink.cdc.connectors.vitess.config.TabletType;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a Vitess source from a logical
 * description.
 */
public class VitessTableSource implements ScanTableSource {

    private final ResolvedSchema physicalSchema;
    private final String pluginName;
    private final String name;
    private final int port;
    private final String hostname;
    private final String keyspace;
    @Nullable private final String username;
    @Nullable private final String password;
    private final String tableName;
    @Nullable private String shard;
    private String gtid;
    private Boolean stopOnReshard;
    private Boolean tombstonesOnDelete;
    private SchemaAdjustmentMode schemaNameAdjustmentMode;
    private final TabletType tabletType;
    private final Properties dbzProperties;

    public VitessTableSource(
            ResolvedSchema physicalSchema,
            int port,
            String hostname,
            String keyspace,
            String tableName,
            @Nullable String username,
            @Nullable String password,
            @Nullable String shard,
            String gtid,
            Boolean stopOnReshard,
            Boolean tombstonesOnDelete,
            SchemaAdjustmentMode schemaNameAdjustmentMode,
            TabletType tabletType,
            String pluginName,
            String name,
            Properties dbzProperties) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = checkNotNull(hostname);
        this.keyspace = checkNotNull(keyspace);
        this.tableName = checkNotNull(tableName);
        this.username = username;
        this.password = password;
        this.shard = shard;
        this.gtid = gtid;
        this.stopOnReshard = stopOnReshard;
        this.tombstonesOnDelete = tombstonesOnDelete;
        this.schemaNameAdjustmentMode = checkNotNull(schemaNameAdjustmentMode);
        this.tabletType = checkNotNull(tabletType);
        this.pluginName = checkNotNull(pluginName);
        this.name = name;
        this.dbzProperties = dbzProperties;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo =
                scanContext.createTypeInformation(physicalSchema.toPhysicalRowDataType());

        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(physicalDataType)
                        .setResultTypeInfo(typeInfo)
                        .setServerTimeZone(ZoneId.of("UTC"))
                        .build();

        DebeziumSourceFunction<RowData> sourceFunction =
                VitessSource.<RowData>builder()
                        .hostname(hostname)
                        .port(port)
                        .keyspace(keyspace)
                        .tableIncludeList(tableName)
                        .username(username)
                        .password(password)
                        .tabletType(tabletType)
                        .decodingPluginName(pluginName)
                        .shard(shard)
                        .gtid(gtid)
                        .stopOnReshard(stopOnReshard)
                        .tombstonesOnDelete(tombstonesOnDelete)
                        .schemaNameAdjustmentMode(schemaNameAdjustmentMode)
                        .name(name)
                        .debeziumProperties(dbzProperties)
                        .deserializer(deserializer)
                        .build();
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new VitessTableSource(
                physicalSchema,
                port,
                hostname,
                keyspace,
                tableName,
                username,
                password,
                shard,
                gtid,
                stopOnReshard,
                tombstonesOnDelete,
                schemaNameAdjustmentMode,
                tabletType,
                pluginName,
                name,
                dbzProperties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VitessTableSource that = (VitessTableSource) o;
        return port == that.port
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(pluginName, that.pluginName)
                && Objects.equals(name, that.name)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(keyspace, that.keyspace)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(shard, that.shard)
                && Objects.equals(gtid, that.gtid)
                && Objects.equals(stopOnReshard, that.stopOnReshard)
                && Objects.equals(tombstonesOnDelete, that.tombstonesOnDelete)
                && Objects.equals(schemaNameAdjustmentMode, that.schemaNameAdjustmentMode)
                && tabletType == that.tabletType
                && Objects.equals(dbzProperties, that.dbzProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                pluginName,
                name,
                port,
                hostname,
                keyspace,
                username,
                password,
                tableName,
                shard,
                gtid,
                stopOnReshard,
                tombstonesOnDelete,
                schemaNameAdjustmentMode,
                tabletType,
                dbzProperties);
    }

    @Override
    public String toString() {
        return "VitessTableSource{"
                + "physicalSchema="
                + physicalSchema
                + ", pluginName='"
                + pluginName
                + '\''
                + ", name='"
                + name
                + '\''
                + ", port="
                + port
                + ", hostname='"
                + hostname
                + '\''
                + ", keyspace='"
                + keyspace
                + '\''
                + ", username='"
                + String.valueOf(username)
                + '\''
                + ", password='"
                + String.valueOf(password)
                + '\''
                + ", tableName='"
                + tableName
                + '\''
                + ", shard='"
                + String.valueOf(shard)
                + '\''
                + ", gtid='"
                + gtid
                + '\''
                + ", stopOnReshard="
                + stopOnReshard
                + ", tombstonesOnDelete="
                + tombstonesOnDelete
                + ", schemaNameAdjustmentMode="
                + schemaNameAdjustmentMode
                + ", tabletType="
                + tabletType
                + ", dbzProperties="
                + dbzProperties
                + '}';
    }

    @Override
    public String asSummaryString() {
        return "Vitess-CDC";
    }
}
