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

package com.ververica.cdc.connectors.oceanbase;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.ververica.cdc.connectors.oceanbase.source.OceanBaseRichSourceFunction;
import com.ververica.cdc.connectors.oceanbase.table.StartupMode;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

import java.time.Duration;
import java.time.ZoneId;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume commit log.
 */
@PublicEvolving
public class OceanBaseSource {

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link OceanBaseSource}. */
    public static class Builder<T> {

        private StartupMode startupMode;
        private Long startupTimestamp;

        private String username;
        private String password;
        private String tenantName;
        private String databaseName;
        private String tableName;
        private String hostname;
        private Integer port;
        private Duration connectTimeout;
        private String rsList;
        private String logProxyHost;
        private Integer logProxyPort;
        private ZoneId serverTimeZone = ZoneId.of("UTC");

        private DebeziumDeserializationSchema<T> deserializer;

        public Builder<T> startupMode(StartupMode startupMode) {
            this.startupMode = startupMode;
            return this;
        }

        public Builder<T> startupTimestamp(Long startupTimestamp) {
            this.startupTimestamp = startupTimestamp;
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

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder<T> rsList(String rsList) {
            this.rsList = rsList;
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

        public Builder<T> serverTimeZone(ZoneId serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return this;
        }

        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public SourceFunction<T> build() {
            switch (startupMode) {
                case INITIAL:
                case LATEST_OFFSET:
                    startupTimestamp = 0L;
                    break;
                case TIMESTAMP:
                    checkNotNull(startupTimestamp, "startupTimestamp shouldn't be null");
                    break;
                default:
                    throw new UnsupportedOperationException(
                            startupMode + " mode is not supported.");
            }

            return new OceanBaseRichSourceFunction<T>(
                    startupMode.equals(StartupMode.INITIAL),
                    checkNotNull(startupTimestamp),
                    checkNotNull(username),
                    checkNotNull(password),
                    checkNotNull(tenantName),
                    checkNotNull(databaseName),
                    checkNotNull(tableName),
                    hostname,
                    port,
                    connectTimeout,
                    checkNotNull(rsList),
                    checkNotNull(logProxyHost),
                    checkNotNull(logProxyPort),
                    checkNotNull(serverTimeZone),
                    checkNotNull(deserializer));
        }
    }
}
