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

package org.apache.flink.cdc.connectors.jdbc.config;

import org.apache.flink.cdc.common.utils.Preconditions;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

/** Generic configuration class for JDBC-like sinks. */
public class JdbcSinkConfig implements Serializable {
    private final String connUrl;
    private final String username;
    private final String password;
    private final String table;
    private final String driverClassName;
    private final String serverTimeZone;
    private final Duration connectTimeout;
    private final int connectMaxRetries;
    private final int connectionPoolSize;
    private final long writeBatchIntervalMs;
    private final int writeBatchSize;
    private final int writeMaxRetries;
    private final Properties jdbcProperties;

    private final String dialect;
    private final String hostname;
    private final int port;

    protected JdbcSinkConfig(Builder<?> builder) {
        this.connUrl = builder.connUrl;
        this.username = builder.username;
        this.password = builder.password;
        this.table = builder.table;
        this.driverClassName = builder.driverClassName;
        this.serverTimeZone = builder.serverTimeZone;
        this.connectTimeout = builder.connectTimeout;
        this.connectMaxRetries = builder.connectMaxRetries;
        this.connectionPoolSize = builder.connectionPoolSize;
        this.writeBatchIntervalMs = builder.writeBatchIntervalMs;
        this.writeBatchSize = builder.writeBatchSize;
        this.writeMaxRetries = builder.writeMaxRetries;
        this.jdbcProperties = builder.jdbcProperties;

        Preconditions.checkArgument(
                connUrl.startsWith("jdbc:"), "JDBC connection string should start with `jdbc:`.");
        String cleanURI = connUrl.substring(5);

        URI uri = URI.create(cleanURI);
        this.dialect = uri.getScheme();
        this.hostname = uri.getHost();
        this.port = uri.getPort();
    }

    /** Builder class for JDBC Sink Config. */
    public static class Builder<T extends Builder<T>> {
        private String connUrl;
        private String username;
        private String password;
        private String table;
        private String driverClassName;
        private String serverTimeZone;
        private Duration connectTimeout;
        private int connectMaxRetries;
        private int connectionPoolSize;
        private long writeBatchIntervalMs;
        private int writeBatchSize;
        private int writeMaxRetries;
        private Properties jdbcProperties;

        public T connUrl(String connUrl) {
            this.connUrl = connUrl;
            return self();
        }

        public T username(String username) {
            this.username = username;
            return self();
        }

        public T password(String password) {
            this.password = password;
            return self();
        }

        public T table(String table) {
            this.table = table;
            return self();
        }

        public T driverClassName(String driverClassName) {
            this.driverClassName = driverClassName;
            return self();
        }

        public T serverTimeZone(String serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return self();
        }

        public T connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return self();
        }

        public T connectMaxRetries(int connectMaxRetries) {
            this.connectMaxRetries = connectMaxRetries;
            return self();
        }

        public T connectionPoolSize(int connectionPoolSize) {
            this.connectionPoolSize = connectionPoolSize;
            return self();
        }

        public T writeBatchIntervalMs(long writeBatchIntervalMs) {
            this.writeBatchIntervalMs = writeBatchIntervalMs;
            return self();
        }

        public T writeBatchSize(int writeBatchSize) {
            this.writeBatchSize = writeBatchSize;
            return self();
        }

        public T writeMaxRetries(int writeMaxRetries) {
            this.writeMaxRetries = writeMaxRetries;
            return self();
        }

        public T jdbcProperties(Properties jdbcProperties) {
            this.jdbcProperties = jdbcProperties;
            return self();
        }

        protected T self() {
            return (T) this;
        }

        public JdbcSinkConfig build() {
            return new JdbcSinkConfig(this);
        }
    }

    public String getConnUrl() {
        return connUrl;
    }

    public String getDialect() {
        return dialect;
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

    public String getTable() {
        return table;
    }

    public String getDriverClassName() {
        return driverClassName;
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

    public long getWriteBatchIntervalMs() {
        return writeBatchIntervalMs;
    }

    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    public int getWriteMaxRetries() {
        return writeMaxRetries;
    }

    public Properties getJdbcProperties() {
        return jdbcProperties;
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof JdbcSinkConfig)) {
            return false;
        }

        JdbcSinkConfig that = (JdbcSinkConfig) o;
        return connectMaxRetries == that.connectMaxRetries
                && connectionPoolSize == that.connectionPoolSize
                && writeBatchIntervalMs == that.writeBatchIntervalMs
                && writeBatchSize == that.writeBatchSize
                && writeMaxRetries == that.writeMaxRetries
                && port == that.port
                && connUrl.equals(that.connUrl)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(table, that.table)
                && Objects.equals(driverClassName, that.driverClassName)
                && Objects.equals(serverTimeZone, that.serverTimeZone)
                && Objects.equals(connectTimeout, that.connectTimeout)
                && Objects.equals(jdbcProperties, that.jdbcProperties)
                && Objects.equals(dialect, that.dialect)
                && Objects.equals(hostname, that.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                connUrl,
                username,
                password,
                table,
                driverClassName,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                writeBatchIntervalMs,
                writeBatchSize,
                writeMaxRetries,
                jdbcProperties,
                dialect,
                hostname,
                port);
    }
}
