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

package com.ververica.cdc.connectors.mysql.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.ververica.cdc.connectors.mysql.source.MySqlParallelSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema.MetadataConverter;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.DATABASE_SERVER_NAME;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SERVER_ID;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a MySQL binlog source from a logical
 * description.
 */
public class MySqlTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final TableSchema physicalSchema;
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
    private final int fetchSize;
    private final Duration connectTimeout;
    private final StartupOptions startupOptions;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public MySqlTableSource(
            TableSchema physicalSchema,
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
            int fetchSize,
            Duration connectTimeout,
            StartupOptions startupOptions) {
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
        this.fetchSize = fetchSize;
        this.connectTimeout = connectTimeout;
        this.startupOptions = startupOptions;
        // Mutable attributes
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
            Configuration configuration = getParallelSourceConf();
            MySqlParallelSource<RowData> parallelSource =
                    new MySqlParallelSource<>(deserializer, configuration);
            return SourceProvider.of(parallelSource);
        } else {
            MySqlSource.Builder<RowData> builder =
                    MySqlSource.<RowData>builder()
                            .hostname(hostname)
                            .port(port)
                            .databaseList(database)
                            .tableList(database + "." + tableName)
                            .username(username)
                            .password(password)
                            .serverTimeZone(serverTimeZone.toString())
                            .debeziumProperties(dbzProperties)
                            .startupOptions(startupOptions)
                            .deserializer(deserializer);
            Optional.ofNullable(serverId)
                    .ifPresent(
                            serverId -> builder.serverId(MySqlSourceOptions.getServerId(serverId)));
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

    private Configuration getParallelSourceConf() {
        Map<String, String> properties = new HashMap<>();
        if (dbzProperties != null) {
            dbzProperties.forEach((k, v) -> properties.put(k.toString(), v.toString()));
        }
        properties.put("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        properties.put("database.hostname", checkNotNull(hostname));
        properties.put("database.user", checkNotNull(username));
        properties.put("database.password", checkNotNull(password));
        properties.put("database.port", String.valueOf(port));
        properties.put("database.history.skip.unparseable.ddl", String.valueOf(true));
        properties.put("database.server.name", DATABASE_SERVER_NAME);

        /**
         * The server id is required, it will be replaced to 'database.server.id' when build {@link
         * MySqlSplitReader}
         */
        if (serverId != null) {
            properties.put(SERVER_ID.key(), serverId);
        }
        properties.put(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.key(), String.valueOf(splitSize));
        properties.put(SCAN_SNAPSHOT_FETCH_SIZE.key(), String.valueOf(fetchSize));
        properties.put("connect.timeout.ms", String.valueOf(connectTimeout.toMillis()));

        if (database != null) {
            properties.put("database.whitelist", database);
        }
        if (tableName != null) {
            properties.put("table.whitelist", database + "." + tableName);
        }
        if (serverTimeZone != null) {
            properties.put("database.serverTimezone", serverTimeZone.toString());
        }

        // set mode
        switch (startupOptions.startupMode) {
            case INITIAL:
                properties.put("scan.startup.mode", "initial");
                break;

            case LATEST_OFFSET:
                properties.put("scan.startup.mode", "latest-offset");
                break;

            default:
                throw new UnsupportedOperationException();
        }

        return Configuration.fromMap(properties);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(MySqlReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                MySqlReadableMetadata::getKey, MySqlReadableMetadata::getDataType));
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
                        fetchSize,
                        connectTimeout,
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
        if (!(o instanceof MySqlTableSource)) {
            return false;
        }
        MySqlTableSource that = (MySqlTableSource) o;
        return port == that.port
                && enableParallelRead == that.enableParallelRead
                && splitSize == that.splitSize
                && fetchSize == that.fetchSize
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
                && Objects.equals(startupOptions, that.startupOptions)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys);
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
                fetchSize,
                connectTimeout,
                startupOptions,
                producedDataType,
                metadataKeys);
    }

    @Override
    public String asSummaryString() {
        return "MySQL-CDC";
    }
}
