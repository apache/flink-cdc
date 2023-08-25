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

package com.ververica.cdc.connectors.oracle.source.config;

import com.ververica.cdc.connectors.base.config.DataSourcePoolConfig;

import javax.annotation.Nullable;

import java.time.Duration;

/**
 * An oracle config to create HikariDataSource, which provide url to use when connecting to the
 * Oracle database server.
 */
public class OracleDataSourceConfig extends DataSourcePoolConfig {
    @Nullable private String url;

    public OracleDataSourceConfig(
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            String driverClassName,
            String serverTimeZone,
            String url) {
        super(
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                driverClassName,
                serverTimeZone);
        this.url = url;
    }

    @Nullable
    public String getUrl() {
        return url;
    }
}
