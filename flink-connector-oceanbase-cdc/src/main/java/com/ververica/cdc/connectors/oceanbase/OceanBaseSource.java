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

import com.ververica.cdc.connectors.oceanbase.source.OceanBaseRichParallelSourceFunction;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseTableSourceFactory;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

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

        private OceanBaseTableSourceFactory.StartupMode startupMode;
        private Long startupTimestamp;

        private String username;
        private String password;
        private String tenantName;
        private String databaseName;
        private String tableName;

        private String jdbcUrl;
        private String rsList;
        private String logProxyHost;
        private int logProxyPort = 2983;

        private DebeziumDeserializationSchema<T> deserializer;

        public Builder<T> startupMode(OceanBaseTableSourceFactory.StartupMode startupMode) {
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

        public Builder<T> jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public SourceFunction<T> build() {
            switch (startupMode) {
                case INITIAL:
                case LATEST:
                    startupTimestamp = 0L;
                    break;
                case TIMESTAMP:
                    checkNotNull(startupTimestamp, "startupTimestamp shouldn't be null");
                    break;
                default:
                    throw new UnsupportedOperationException(
                            startupMode + " mode is not supported.");
            }

            return new OceanBaseRichParallelSourceFunction<T>(
                    startupMode.equals(OceanBaseTableSourceFactory.StartupMode.INITIAL),
                    username,
                    password,
                    tenantName,
                    databaseName,
                    tableName,
                    jdbcUrl,
                    rsList,
                    logProxyHost,
                    logProxyPort,
                    startupTimestamp,
                    deserializer);
        }
    }
}
