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

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;

import io.debezium.config.Configuration;

import java.util.Properties;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;
import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;

/** A factory to initialize {@link OceanBaseSourceConfig}. */
@SuppressWarnings("UnusedReturnValue")
public class OceanBaseSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final long serialVersionUID = 1L;

    private String compatibleMode;
    private String driverClassName;
    private String tenantName;
    private String logProxyHost;
    private Integer logProxyPort;
    private String rsList;
    private String configUrl;
    private String workingMode;
    private Properties obcdcProperties;

    public JdbcSourceConfigFactory compatibleMode(String compatibleMode) {
        this.compatibleMode = compatibleMode;
        return this;
    }

    public JdbcSourceConfigFactory driverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
        return this;
    }

    public JdbcSourceConfigFactory tenantName(String tenantName) {
        this.tenantName = tenantName;
        return this;
    }

    public JdbcSourceConfigFactory logProxyHost(String logProxyHost) {
        this.logProxyHost = logProxyHost;
        return this;
    }

    public JdbcSourceConfigFactory logProxyPort(int logProxyPort) {
        this.logProxyPort = logProxyPort;
        return this;
    }

    public JdbcSourceConfigFactory rsList(String rsList) {
        this.rsList = rsList;
        return this;
    }

    public JdbcSourceConfigFactory configUrl(String configUrl) {
        this.configUrl = configUrl;
        return this;
    }

    public JdbcSourceConfigFactory workingMode(String workingMode) {
        this.workingMode = workingMode;
        return this;
    }

    public JdbcSourceConfigFactory obcdcProperties(Properties obcdcProperties) {
        this.obcdcProperties = obcdcProperties;
        return this;
    }

    @Override
    public OceanBaseSourceConfig create(int subtaskId) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);
        Properties props = new Properties();
        props.setProperty("database.server.name", "oceanbase_cdc");
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));
        props.setProperty("database.connect.timeout.ms", String.valueOf(connectTimeout.toMillis()));

        // table filter
        props.put("database.include.list", String.join(",", databaseList));
        if (tableList != null) {
            props.put("table.include.list", String.join(",", tableList));
        }
        // value converter
        props.put("decimal.handling.mode", "precise");
        props.put("time.precision.mode", "adaptive_time_microseconds");
        props.put("binary.handling.mode", "bytes");

        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        Configuration dbzConfiguration = Configuration.from(props);
        return new OceanBaseSourceConfig(
                compatibleMode,
                tenantName,
                logProxyHost,
                logProxyPort,
                rsList,
                configUrl,
                workingMode,
                obcdcProperties,
                startupOptions,
                databaseList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                props,
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
                true,
                scanNewlyAddedTableEnabled);
    }
}
