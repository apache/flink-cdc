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

package com.ververica.cdc.connectors.oceanbase;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.util.ClientIdGenerator;
import com.ververica.cdc.connectors.oceanbase.source.OceanBaseRichSourceFunction;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseDeserializationSchema;
import com.ververica.cdc.connectors.oceanbase.table.StartupMode;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;

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
        private StartupMode startupMode;
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

        // incremental reading config
        private String logProxyHost;
        private Integer logProxyPort;
        private String logProxyClientId;
        private Long startupTimestamp;
        private String rsList;
        private String configUrl;
        private String workingMode;

        private OceanBaseDeserializationSchema<T> deserializer;

        public Builder<T> startupMode(StartupMode startupMode) {
            this.startupMode = startupMode;
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

        public Builder<T> logProxyHost(String logProxyHost) {
            this.logProxyHost = logProxyHost;
            return this;
        }

        public Builder<T> logProxyPort(int logProxyPort) {
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

        public Builder<T> deserializer(OceanBaseDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public SourceFunction<T> build() {
            switch (startupMode) {
                case INITIAL:
                    checkNotNull(hostname, "hostname shouldn't be null on startup mode 'initial'");
                    checkNotNull(port, "port shouldn't be null on startup mode 'initial'");
                    startupTimestamp = 0L;
                    break;
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
                            startupMode + " mode is not supported.");
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

            if (serverTimeZone == null) {
                serverTimeZone = "+00:00";
            }

            if (connectTimeout == null) {
                connectTimeout = Duration.ofSeconds(30);
            }

            if (logProxyClientId == null) {
                logProxyClientId =
                        String.format(
                                "%s_%s_%s",
                                ClientIdGenerator.generate(),
                                Thread.currentThread().getId(),
                                checkNotNull(tenantName));
            }
            ClientConf clientConf =
                    ClientConf.builder()
                            .clientId(logProxyClientId)
                            .connectTimeoutMs((int) connectTimeout.toMillis())
                            .build();

            ObReaderConfig obReaderConfig = new ObReaderConfig();
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
            obReaderConfig.setTimezone(serverTimeZone);

            return new OceanBaseRichSourceFunction<T>(
                    StartupMode.INITIAL.equals(startupMode),
                    username,
                    password,
                    tenantName,
                    databaseName,
                    tableName,
                    tableList,
                    connectTimeout,
                    hostname,
                    port,
                    logProxyHost,
                    logProxyPort,
                    clientConf,
                    obReaderConfig,
                    deserializer);
        }
    }
}
