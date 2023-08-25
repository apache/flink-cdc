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

import java.io.Serializable;
import java.time.Duration;

/** A config to create HikariDataSource. */
public class DataSourcePoolConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Duration connectTimeout;
    private final int connectMaxRetries;
    private final int connectionPoolSize;

    private final String driverClassName;

    private final String serverTimeZone;

    public String getDriverClassName() {
        return driverClassName;
    }

    public DataSourcePoolConfig(
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            String driverClassName,
            String serverTimeZone) {
        this.connectTimeout = connectTimeout;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.driverClassName = driverClassName;
        this.serverTimeZone = serverTimeZone;
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

    public String getServerTimeZone() {
        return serverTimeZone;
    }
}
