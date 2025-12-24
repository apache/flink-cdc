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

package org.apache.flink.cdc.connectors.gaussdb.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.config.Configuration;
import io.debezium.connector.gaussdb.GaussDBConnectorConfig;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The configuration for GaussDB CDC source. */
public class GaussDBSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    private static final String GAUSSDB_JDBC_DRIVER = "com.huawei.gaussdb.jdbc.Driver";

    private final List<String> schemaList;
    private final String slotName;
    private final String decodingPluginName;

    public GaussDBSourceConfig(
            StartupOptions startupOptions,
            List<String> databaseList,
            List<String> schemaList,
            @Nullable List<String> tableList,
            int splitSize,
            int splitMetaGroupSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            boolean closeIdleReaders,
            Properties dbzProperties,
            Configuration dbzConfiguration,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            @Nullable String chunkKeyColumn,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            boolean assignUnboundedChunkFirst,
            String slotName,
            String decodingPluginName) {
        super(
                startupOptions,
                databaseList,
                schemaList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                dbzProperties,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);
        this.schemaList = checkNotNull(schemaList);
        this.slotName = checkNotNull(slotName);
        this.decodingPluginName = checkNotNull(decodingPluginName);
    }

    public List<String> getSchemaList() {
        return schemaList;
    }

    public String getSlotName() {
        return slotName;
    }

    public String getDecodingPluginName() {
        return decodingPluginName;
    }

    /**
     * Returns the slot name for backfill tasks.
     * Uses a separate slot to avoid contention with the main streaming slot.
     * The backfill slot will be dropped after the backfill task completes.
     */
    public String getSlotNameForBackfillTask() {
        return slotName + "_backfill";
    }

    /** Returns the JDBC URL for config unique key. */
    public String getJdbcUrl() {
        return String.format(
                "jdbc:gaussdb://%s:%d/%s", getHostname(), getPort(), getDatabaseList().get(0));
    }

    @Override
    public GaussDBConnectorConfig getDbzConnectorConfig() {
        return new GaussDBConnectorConfig(getDbzConfiguration());
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link GaussDBSourceConfig}. */
    public static class Builder {

        private StartupOptions startupOptions = StartupOptions.initial();
        private List<String> databaseList;
        private List<String> schemaList = Collections.singletonList(GaussDBSourceOptions.SCHEMA_NAME.defaultValue());
        private List<String> tableList;
        private int splitSize = SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue();
        private int splitMetaGroupSize = SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue();
        private double distributionFactorUpper = SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND
                .defaultValue();
        private double distributionFactorLower = SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND
                .defaultValue();
        private boolean includeSchemaChanges = false;
        private boolean closeIdleReaders = false;
        private Properties dbzProperties = new Properties();
        private Configuration dbzConfiguration;
        private String driverClassName = GAUSSDB_JDBC_DRIVER;
        private String hostname;
        private int port = GaussDBSourceOptions.PORT.defaultValue();
        private String username;
        private String password;
        private int fetchSize = SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue();
        private String serverTimeZone = JdbcSourceOptions.SERVER_TIME_ZONE.defaultValue();
        private Duration connectTimeout = JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue();
        private int connectMaxRetries = JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue();
        private int connectionPoolSize = JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue();
        @Nullable
        private String chunkKeyColumn;
        private boolean skipSnapshotBackfill = JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue();
        private boolean scanNewlyAddedTableEnabled = JdbcSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue();
        private boolean assignUnboundedChunkFirst = JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED
                .defaultValue();
        private String slotName;
        private String decodingPluginName = GaussDBSourceOptions.DECODING_PLUGIN_NAME.defaultValue();

        public Builder startupOptions(StartupOptions startupOptions) {
            this.startupOptions = startupOptions;
            return this;
        }

        public Builder database(String database) {
            this.databaseList = Collections.singletonList(database);
            return this;
        }

        public Builder schema(String schema) {
            this.schemaList = Collections.singletonList(schema);
            return this;
        }

        public Builder table(@Nullable String table) {
            this.tableList = table == null ? null : Collections.singletonList(table);
            return this;
        }

        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder connectMaxRetries(int connectMaxRetries) {
            this.connectMaxRetries = connectMaxRetries;
            return this;
        }

        public Builder connectionPoolSize(int connectionPoolSize) {
            this.connectionPoolSize = connectionPoolSize;
            return this;
        }

        public Builder debeziumProperties(Properties dbzProperties) {
            this.dbzProperties = dbzProperties;
            return this;
        }

        public Builder debeziumConfiguration(Configuration dbzConfiguration) {
            this.dbzConfiguration = dbzConfiguration;
            return this;
        }

        public Builder slotName(String slotName) {
            this.slotName = slotName;
            return this;
        }

        public Builder decodingPluginName(String decodingPluginName) {
            this.decodingPluginName = decodingPluginName;
            return this;
        }

        public GaussDBSourceConfig build() {
            checkNotNull(startupOptions);
            checkNotNull(databaseList, "database-name is required");
            checkNotNull(schemaList, "schema-name is required");
            checkNotNull(hostname, "hostname is required");
            checkNotNull(username, "username is required");
            checkNotNull(password, "password is required");
            checkNotNull(slotName, "slot.name is required");
            checkNotNull(decodingPluginName, "decoding.plugin.name is required");
            checkNotNull(dbzProperties, "debezium properties is required");

            Configuration debeziumConfiguration = dbzConfiguration == null ? Configuration.from(dbzProperties)
                    : dbzConfiguration;
            return new GaussDBSourceConfig(
                    startupOptions,
                    databaseList,
                    schemaList,
                    tableList,
                    splitSize,
                    splitMetaGroupSize,
                    distributionFactorUpper,
                    distributionFactorLower,
                    includeSchemaChanges,
                    closeIdleReaders,
                    dbzProperties,
                    debeziumConfiguration,
                    driverClassName,
                    hostname,
                    port,
                    username,
                    password,
                    fetchSize,
                    serverTimeZone,
                    connectTimeout,
                    connectMaxRetries,
                    connectionPoolSize,
                    chunkKeyColumn,
                    skipSnapshotBackfill,
                    scanNewlyAddedTableEnabled,
                    assignUnboundedChunkFirst,
                    slotName,
                    decodingPluginName);
        }
    }
}
