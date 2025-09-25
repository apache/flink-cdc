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

package org.apache.flink.cdc.connectors.base.config;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.IncrementalSource;
import org.apache.flink.table.catalog.ObjectPath;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A Source configuration which is used by {@link IncrementalSource} which used JDBC data source.
 */
public abstract class JdbcSourceConfig extends BaseSourceConfig {

    protected final String driverClassName;
    protected final String hostname;
    protected final int port;
    protected final String username;
    protected final String password;
    protected final List<String> databaseList;
    protected final List<String> schemaList;
    protected final List<String> tableList;
    protected final int fetchSize;
    protected final String serverTimeZone;
    protected final Duration connectTimeout;
    protected final int connectMaxRetries;
    protected final int connectionPoolSize;
    protected final Map<ObjectPath, String> chunkKeyColumns;

    public JdbcSourceConfig(
            StartupOptions startupOptions,
            List<String> databaseList,
            List<String> schemaList,
            List<String> tableList,
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
            Map<ObjectPath, String> chunkKeyColumns,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            boolean assignUnboundedChunkFirst) {
        super(
                startupOptions,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled,
                dbzProperties,
                dbzConfiguration,
                assignUnboundedChunkFirst);
        this.driverClassName = driverClassName;
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.databaseList = databaseList;
        this.schemaList = schemaList;
        this.tableList = tableList;
        this.fetchSize = fetchSize;
        this.serverTimeZone = serverTimeZone;
        this.connectTimeout = connectTimeout;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.chunkKeyColumns = chunkKeyColumns;
    }

    public abstract RelationalDatabaseConnectorConfig getDbzConnectorConfig();

    public String getDriverClassName() {
        return driverClassName;
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

    public Map<ObjectPath, String> getChunkKeyColumns() {
        return chunkKeyColumns;
    }

    @Override
    public boolean isScanNewlyAddedTableEnabled() {
        return isScanNewlyAddedTableEnabled;
    }

    public RelationalTableFilters getTableFilters() {
        return getDbzConnectorConfig().getTableFilters();
    }
}
