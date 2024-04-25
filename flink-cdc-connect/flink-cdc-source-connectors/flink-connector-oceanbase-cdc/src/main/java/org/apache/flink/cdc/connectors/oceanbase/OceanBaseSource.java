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

package org.apache.flink.cdc.connectors.oceanbase;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oceanbase.source.OceanBaseRichSourceFunction;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A builder to build a SourceFunction which can read snapshot and change events of OceanBase. */
@PublicEvolving
public class OceanBaseSource {

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link OceanBaseSource}. */
    public static class Builder<T> {

        // common config
        private StartupOptions startupOptions;
        private String username;
        private String password;
        private String tenantName;
        private String databaseName;
        private String tableName;
        private String tableList;
        private String serverTimeZone;
        private Duration connectTimeout;

        // snapshot reading config
        private String hostname;
        private Integer port;
        private String compatibleMode;
        private String jdbcDriver;
        private Properties jdbcProperties;

        // incremental reading config
        private String logProxyHost;
        private Integer logProxyPort;
        private String logProxyClientId;
        private Long startupTimestamp;
        private String rsList;
        private String configUrl;
        private String workingMode;
        private Properties obcdcProperties;
        private Properties debeziumProperties;

        private DebeziumDeserializationSchema<T> deserializer;

        public Builder<T> startupOptions(StartupOptions startupOptions) {
            this.startupOptions = startupOptions;
            return this;
        }

        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        public Builder<T> tenantName(String tenantName) {
            this.tenantName = tenantName;
            return this;
        }

        public Builder<T> databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder<T> tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder<T> tableList(String tableList) {
            this.tableList = tableList;
            return this;
        }

        public Builder<T> serverTimeZone(String serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return this;
        }

        public Builder<T> connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> compatibleMode(String compatibleMode) {
            this.compatibleMode = compatibleMode;
            return this;
        }

        public Builder<T> jdbcDriver(String jdbcDriver) {
            this.jdbcDriver = jdbcDriver;
            return this;
        }

        public Builder<T> jdbcProperties(Properties jdbcProperties) {
            this.jdbcProperties = jdbcProperties;
            return this;
        }

        public Builder<T> logProxyHost(String logProxyHost) {
            this.logProxyHost = logProxyHost;
            return this;
        }

        public Builder<T> logProxyPort(Integer logProxyPort) {
            this.logProxyPort = logProxyPort;
            return this;
        }

        public Builder<T> logProxyClientId(String logProxyClientId) {
            this.logProxyClientId = logProxyClientId;
            return this;
        }

        public Builder<T> startupTimestamp(Long startupTimestamp) {
            this.startupTimestamp = startupTimestamp;
            return this;
        }

        public Builder<T> rsList(String rsList) {
            this.rsList = rsList;
            return this;
        }

        public Builder<T> configUrl(String configUrl) {
            this.configUrl = configUrl;
            return this;
        }

        public Builder<T> workingMode(String workingMode) {
            this.workingMode = workingMode;
            return this;
        }

        public Builder<T> obcdcProperties(Properties obcdcProperties) {
            this.obcdcProperties = obcdcProperties;
            return this;
        }

        public Builder<T> debeziumProperties(Properties debeziumProperties) {
            this.debeziumProperties = debeziumProperties;
            return this;
        }

        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public SourceFunction<T> build() {
            checkNotNull(username, "username shouldn't be null");
            checkNotNull(password, "password shouldn't be null");
            checkNotNull(hostname, "hostname shouldn't be null");
            checkNotNull(port, "port shouldn't be null");

            if (startupOptions == null) {
                startupOptions = StartupOptions.initial();
            }
            if (compatibleMode == null) {
                compatibleMode = "mysql";
            }
            if (jdbcDriver == null) {
                jdbcDriver = "com.mysql.cj.jdbc.Driver";
            }

            if (connectTimeout == null) {
                connectTimeout = Duration.ofSeconds(30);
            }

            if (serverTimeZone == null) {
                serverTimeZone = ZoneId.systemDefault().getId();
            }

            switch (startupOptions.startupMode) {
                case SNAPSHOT:
                    break;
                case INITIAL:
                case LATEST_OFFSET:
                    startupTimestamp = 0L;
                    break;
                case TIMESTAMP:
                    checkNotNull(
                            startupTimestamp,
                            "startupTimestamp shouldn't be null on startup mode 'timestamp'");
                    break;
                default:
                    throw new UnsupportedOperationException(
                            startupOptions.startupMode + " mode is not supported.");
            }

            if (StringUtils.isNotEmpty(databaseName) || StringUtils.isNotEmpty(tableName)) {
                if (StringUtils.isEmpty(databaseName) || StringUtils.isEmpty(tableName)) {
                    throw new IllegalArgumentException(
                            "'database-name' and 'table-name' should be configured at the same time");
                }
            } else {
                checkNotNull(
                        tableList,
                        "'database-name', 'table-name' or 'table-list' should be configured");
            }

            ClientConf clientConf = null;
            ObReaderConfig obReaderConfig = null;

            if (!startupOptions.isSnapshotOnly()) {

                checkNotNull(logProxyHost);
                checkNotNull(logProxyPort);
                checkNotNull(tenantName);

                obReaderConfig = new ObReaderConfig();
                if (StringUtils.isNotEmpty(rsList)) {
                    obReaderConfig.setRsList(rsList);
                }
                if (StringUtils.isNotEmpty(configUrl)) {
                    obReaderConfig.setClusterUrl(configUrl);
                }
                if (StringUtils.isNotEmpty(workingMode)) {
                    obReaderConfig.setWorkingMode(workingMode);
                }
                obReaderConfig.setUsername(username);
                obReaderConfig.setPassword(password);
                obReaderConfig.setStartTimestamp(startupTimestamp);
                obReaderConfig.setTimezone(
                        DateTimeFormatter.ofPattern("xxx")
                                .format(
                                        ZoneId.of(serverTimeZone)
                                                .getRules()
                                                .getOffset(Instant.now())));

                if (obcdcProperties != null && !obcdcProperties.isEmpty()) {
                    Map<String, String> extraConfigs = new HashMap<>();
                    obcdcProperties.forEach((k, v) -> extraConfigs.put(k.toString(), v.toString()));
                    obReaderConfig.setExtraConfigs(extraConfigs);
                }
            }

            return new OceanBaseRichSourceFunction<>(
                    startupOptions,
                    username,
                    password,
                    tenantName,
                    databaseName,
                    tableName,
                    tableList,
                    serverTimeZone,
                    connectTimeout,
                    hostname,
                    port,
                    compatibleMode,
                    jdbcDriver,
                    jdbcProperties,
                    logProxyHost,
                    logProxyPort,
                    logProxyClientId,
                    obReaderConfig,
                    debeziumProperties,
                    deserializer);
        }
    }
}
