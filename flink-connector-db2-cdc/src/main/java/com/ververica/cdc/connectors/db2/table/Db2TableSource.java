/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.db2.table;

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

import com.ververica.cdc.connectors.db2.Db2Source;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.table.MetadataConverter;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

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
    private final String schemaName;
    private final String tableName;
    private final String username;
    private final String password;
    private final ZoneId serverTimeZone;
    private final StartupOptions startupOptions;
    private final Properties dbzProperties;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public Db2TableSource(
            ResolvedSchema physicalSchema,
            int port,
            String hostname,
            String database,
            String schemaName,
            String tableName,
            String username,
            String password,
            ZoneId serverTimeZone,
            Properties dbzProperties,
            StartupOptions startupOptions) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = hostname;
        this.database = database;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.serverTimeZone = serverTimeZone;
        this.dbzProperties = dbzProperties;
        this.startupOptions = startupOptions;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
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
        Db2Source.Builder<RowData> builder =
                Db2Source.<RowData>builder()
                        .hostname(hostname)
                        .port(port)
                        .database(database)
                        .tableList(schemaName + "." + tableName)
                        .username(username)
                        .password(password)
                        .debeziumProperties(dbzProperties)
                        .deserializer(deserializer)
                        .startupOptions(startupOptions);
        DebeziumSourceFunction<RowData> sourceFunction = builder.build();
        return SourceFunctionProvider.of(sourceFunction, false);
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
                        schemaName,
                        tableName,
                        username,
                        password,
                        serverTimeZone,
                        dbzProperties,
                        startupOptions);
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
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(serverTimeZone, that.serverTimeZone)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(metadataKeys, that.metadataKeys);
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
                serverTimeZone,
                dbzProperties,
                metadataKeys);
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
