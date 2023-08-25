/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.base.config;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.IncrementalSource;
import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

import java.util.List;
import java.util.Properties;

/**
 * A Source configuration which is used by {@link IncrementalSource} which used JDBC data source.
 */
public abstract class JdbcSourceConfig extends BaseSourceConfig {

    protected final String hostname;
    protected final int port;
    protected final String username;
    protected final String password;

    protected final List<String> databaseList;
    protected final List<String> schemaList;
    protected final List<String> tableList;
    protected final int fetchSize;
    protected final String chunkKeyColumn;
    protected final DataSourcePoolConfig dataSourcePoolConfig;

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
            String hostname,
            int port,
            String username,
            String password,
            int fetchSize,
            String chunkKeyColumn,
            DataSourcePoolConfig dataSourcePoolConfig) {
        super(
                startupOptions,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                dbzProperties,
                dbzConfiguration);

        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.databaseList = databaseList;
        this.schemaList = schemaList;
        this.tableList = tableList;
        this.fetchSize = fetchSize;
        this.chunkKeyColumn = chunkKeyColumn;
        this.dataSourcePoolConfig = dataSourcePoolConfig;
    }

    public abstract RelationalDatabaseConnectorConfig getDbzConnectorConfig();

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

    public DataSourcePoolConfig getDataPoolConfig() {
        return dataSourcePoolConfig;
    }

    public String getChunkKeyColumn() {
        return chunkKeyColumn;
    }
}
