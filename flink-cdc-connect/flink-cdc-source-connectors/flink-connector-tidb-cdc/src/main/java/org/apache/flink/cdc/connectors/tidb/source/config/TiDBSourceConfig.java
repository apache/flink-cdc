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

package org.apache.flink.cdc.connectors.tidb.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.table.catalog.ObjectPath;

import io.debezium.config.Configuration;
import org.tikv.common.TiConfiguration;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** The configuration for TiDB source. */
public class TiDBSourceConfig extends JdbcSourceConfig {
    private static final long serialVersionUID = 1L;
    private final String compatibleMode;
    private final String pdAddresses;

    private final String hostMapping;
    private TiConfiguration tiConfiguration;
    private final Properties jdbcProperties;
    private Map<ObjectPath, String> chunkKeyColumns;

    public TiDBSourceConfig(
            String compatibleMode,
            StartupOptions startupOptions,
            List<String> databaseList,
            List<String> tableList,
            String pdAddresses,
            String hostMapping,
            int splitSize,
            int splitMetaGroupSize,
            TiConfiguration tiConfiguration,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            boolean closeIdleReaders,
            Properties jdbcProperties,
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
            String chunkKeyColumn,
            Map<ObjectPath, String> chunkKeyColumns,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            boolean assignUnboundedChunkFirst) {
        super(
                startupOptions,
                databaseList,
                null,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                jdbcProperties,
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
        this.compatibleMode = compatibleMode;
        this.pdAddresses = pdAddresses;
        this.hostMapping = hostMapping;
        this.jdbcProperties = jdbcProperties;
        this.tiConfiguration = tiConfiguration;
        this.chunkKeyColumns = chunkKeyColumns;
    }

    public String getCompatibleMode() {
        return compatibleMode;
    }

    public String getPdAddresses() {
        return pdAddresses;
    }

    public String getHostMapping() {
        return hostMapping;
    }

    public Properties getJdbcProperties() {
        return this.jdbcProperties;
    }

    public TiConfiguration getTiConfiguration() {
        return this.tiConfiguration;
    }

    public Map<ObjectPath, String> getChunkKeyColumns() {
        return this.chunkKeyColumns;
    }

    @Override
    public TiDBConnectorConfig getDbzConnectorConfig() {
        return new TiDBConnectorConfig(this);
    }

    public StartupOptions getStartupOptions() {
        return startupOptions;
    }
}
