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

package org.apache.flink.cdc.connectors.oceanbase.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.config.Configuration;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Describes the connection information of the OceanBase database and the configuration information
 * for performing snapshotting and streaming reading, such as splitSize.
 */
public class OceanBaseSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    private final String compatibleMode;
    private final String tenantName;
    private final String logProxyHost;
    private final Integer logProxyPort;
    private final String rsList;
    private final String configUrl;
    private final String workingMode;
    private final Properties obcdcProperties;

    public OceanBaseSourceConfig(
            String compatibleMode,
            String tenantName,
            String logProxyHost,
            Integer logProxyPort,
            String rsList,
            String configUrl,
            String workingMode,
            Properties obcdcProperties,
            StartupOptions startupOptions,
            List<String> databaseList,
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
            String chunkKeyColumn,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled) {
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
                isScanNewlyAddedTableEnabled);
        this.compatibleMode = compatibleMode;
        this.tenantName = tenantName;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.rsList = rsList;
        this.configUrl = configUrl;
        this.workingMode = workingMode;
        this.obcdcProperties = obcdcProperties;
    }

    public String getCompatibleMode() {
        return compatibleMode;
    }

    public String getTenantName() {
        return tenantName;
    }

    public String getLogProxyHost() {
        return logProxyHost;
    }

    public Integer getLogProxyPort() {
        return logProxyPort;
    }

    public String getRsList() {
        return rsList;
    }

    public String getConfigUrl() {
        return configUrl;
    }

    public String getWorkingMode() {
        return workingMode;
    }

    public Properties getObcdcProperties() {
        return obcdcProperties;
    }

    @Override
    public OceanBaseConnectorConfig getDbzConnectorConfig() {
        return new OceanBaseConnectorConfig(this);
    }
}
