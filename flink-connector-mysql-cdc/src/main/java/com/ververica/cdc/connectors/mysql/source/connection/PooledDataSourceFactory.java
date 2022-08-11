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

package com.ververica.cdc.connectors.mysql.source.connection;

import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.debezium.connector.mysql.MySqlConnectorConfig;

import java.util.Properties;

/** A connection pool factory to create pooled DataSource {@link HikariDataSource}. */
public class PooledDataSourceFactory {

    public static final String JDBC_URL_PATTERN =
            "jdbc:mysql://%s:%s/?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true";
    public static final String CONNECTION_POOL_PREFIX = "connection-pool-";
    public static final String SERVER_TIMEZONE_KEY = "serverTimezone";
    public static final int MINIMUM_POOL_SIZE = 1;
    private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();

    private PooledDataSourceFactory() {}

    public static HikariDataSource createPooledDataSource(MySqlSourceConfig sourceConfig) {
        final HikariConfig config = new HikariConfig();

        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();
        Properties jdbcProperties = sourceConfig.getJdbcProperties();

        config.setPoolName(CONNECTION_POOL_PREFIX + hostName + ":" + port);
        config.setJdbcUrl(formatJdbcUrl(hostName, port, jdbcProperties));
        config.setUsername(sourceConfig.getUsername());
        config.setPassword(sourceConfig.getPassword());
        config.setMinimumIdle(MINIMUM_POOL_SIZE);
        config.setMaximumPoolSize(sourceConfig.getConnectionPoolSize());
        config.setConnectionTimeout(sourceConfig.getConnectTimeout().toMillis());
        config.addDataSourceProperty(SERVER_TIMEZONE_KEY, sourceConfig.getServerTimeZone());
        config.setDriverClassName(
                sourceConfig.getDbzConfiguration().getString(MySqlConnectorConfig.JDBC_DRIVER));

        // optional optimization configurations for pooled DataSource
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return new HikariDataSource(config);
    }

    private static String formatJdbcUrl(String hostName, int port, Properties jdbcProperties) {
        Properties combinedProperties = new Properties();
        combinedProperties.putAll(DEFAULT_JDBC_PROPERTIES);
        combinedProperties.putAll(jdbcProperties);

        StringBuilder jdbcUrlStringBuilder =
                new StringBuilder(String.format(JDBC_URL_PATTERN, hostName, port));

        combinedProperties.forEach(
                (key, value) -> {
                    jdbcUrlStringBuilder.append("&").append(key).append("=").append(value);
                });

        return jdbcUrlStringBuilder.toString();
    }

    private static Properties initializeDefaultJdbcProperties() {
        Properties defaultJdbcProperties = new Properties();
        defaultJdbcProperties.setProperty("zeroDateTimeBehavior", "CONVERT_TO_NULL");
        defaultJdbcProperties.setProperty("characterEncoding", "UTF-8");
        defaultJdbcProperties.setProperty("characterSetResults", "UTF-8");
        return defaultJdbcProperties;
    }
}
