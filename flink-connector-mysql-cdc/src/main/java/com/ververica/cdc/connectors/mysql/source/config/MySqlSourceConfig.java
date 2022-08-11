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

package com.ververica.cdc.connectors.mysql.source.config;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.RelationalTableFilters;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A MySql Source configuration which is used by {@link MySqlSource}. */
public class MySqlSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private final List<String> databaseList;
    private final List<String> tableList;
    @Nullable private final ServerIdRange serverIdRange;
    private final StartupOptions startupOptions;
    private final int splitSize;
    private final int splitMetaGroupSize;
    private final int fetchSize;
    private final String serverTimeZone;
    private final Duration connectTimeout;
    private final int connectMaxRetries;
    private final int connectionPoolSize;
    private final double distributionFactorUpper;
    private final double distributionFactorLower;
    private final boolean includeSchemaChanges;
    private final boolean scanNewlyAddedTableEnabled;
    private final Properties jdbcProperties;
    @Nullable private final String chunkKeyColumn;

    // --------------------------------------------------------------------------------------------
    // Debezium Configurations
    // --------------------------------------------------------------------------------------------
    private final Properties dbzProperties;
    private final Configuration dbzConfiguration;
    private final MySqlConnectorConfig dbzMySqlConfig;

    MySqlSourceConfig(
            String hostname,
            int port,
            String username,
            String password,
            List<String> databaseList,
            List<String> tableList,
            @Nullable ServerIdRange serverIdRange,
            StartupOptions startupOptions,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            boolean scanNewlyAddedTableEnabled,
            Properties dbzProperties,
            Properties jdbcProperties,
            @Nullable String chunkKeyColumn) {
        this.hostname = checkNotNull(hostname);
        this.port = port;
        this.username = checkNotNull(username);
        this.password = password;
        this.databaseList = checkNotNull(databaseList);
        this.tableList = checkNotNull(tableList);
        this.serverIdRange = serverIdRange;
        this.startupOptions = checkNotNull(startupOptions);
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.serverTimeZone = checkNotNull(serverTimeZone);
        this.connectTimeout = checkNotNull(connectTimeout);
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.includeSchemaChanges = includeSchemaChanges;
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
        this.dbzProperties = checkNotNull(dbzProperties);
        this.dbzConfiguration = Configuration.from(dbzProperties);
        this.dbzMySqlConfig = new MySqlConnectorConfig(dbzConfiguration);
        this.jdbcProperties = jdbcProperties;
        this.chunkKeyColumn = chunkKeyColumn;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public List<String> getDatabaseList() {
        return databaseList;
    }

    public List<String> getTableList() {
        return tableList;
    }

    @Nullable
    public ServerIdRange getServerIdRange() {
        return serverIdRange;
    }

    public StartupOptions getStartupOptions() {
        return startupOptions;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public int getSplitMetaGroupSize() {
        return splitMetaGroupSize;
    }

    public double getDistributionFactorUpper() {
        return distributionFactorUpper;
    }

    public double getDistributionFactorLower() {
        return distributionFactorLower;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public String getServerTimeZone() {
        return serverTimeZone;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public int getConnectMaxRetries() {
        return connectMaxRetries;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    public boolean isIncludeSchemaChanges() {
        return includeSchemaChanges;
    }

    public boolean isScanNewlyAddedTableEnabled() {
        return scanNewlyAddedTableEnabled;
    }

    public Properties getDbzProperties() {
        return dbzProperties;
    }

    public Configuration getDbzConfiguration() {
        return dbzConfiguration;
    }

    public MySqlConnectorConfig getMySqlConnectorConfig() {
        return dbzMySqlConfig;
    }

    public RelationalTableFilters getTableFilters() {
        return dbzMySqlConfig.getTableFilters();
    }

    public Properties getJdbcProperties() {
        return jdbcProperties;
    }

    @Nullable
    public String getChunkKeyColumn() {
        return chunkKeyColumn;
    }
}
