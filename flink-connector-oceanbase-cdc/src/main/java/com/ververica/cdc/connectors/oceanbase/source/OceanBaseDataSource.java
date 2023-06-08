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

package com.ververica.cdc.connectors.oceanbase.source;

import org.apache.flink.util.FlinkRuntimeException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

/** {@link HikariDataSource} extension to be used to maintain connections. */
public class OceanBaseDataSource extends HikariDataSource {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseDataSource.class);

    private static final String MYSQL_JDBC_URL_PREFIX = "jdbc:mysql://";
    private static final String OB_JDBC_URL_PREFIX = "jdbc:oceanbase://";

    public static final String CONNECTION_POOL_PREFIX = "connection-pool-";
    private final int connectRetryTimes;

    public OceanBaseDataSource(
            String hostname,
            int port,
            String user,
            String password,
            Duration connectTimeout,
            int connectMaxRetries,
            String jdbcDriver,
            Properties jdbcProperties,
            int connectionPoolSize) {
        super(
                config(
                        hostname,
                        port,
                        user,
                        password,
                        connectTimeout,
                        jdbcDriver,
                        jdbcProperties,
                        connectionPoolSize));
        this.connectRetryTimes = connectMaxRetries;
    }

    private static HikariConfig config(
            String hostname,
            int port,
            String username,
            String password,
            Duration connectTimeout,
            String jdbcDriver,
            Properties jdbcProperties,
            int connectionPoolSize) {
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setPoolName(CONNECTION_POOL_PREFIX + hostname + ":" + port);
        hikariConfig.setJdbcUrl(getJdbcUrl(hostname, port, jdbcDriver, jdbcProperties));
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setMaximumPoolSize(connectionPoolSize + 1);
        hikariConfig.setConnectionTimeout(connectTimeout.toMillis());
        hikariConfig.setDriverClassName(jdbcDriver);

        // optional optimization configurations for pooled DataSource
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        return hikariConfig;
    }

    private static String getJdbcUrl(
            String hostname, int port, String jdbcDriver, Properties jdbcProperties) {
        Properties properties = new Properties();
        properties.setProperty("useInformationSchema", "true");
        properties.setProperty("nullCatalogMeansCurrent", "false");
        properties.setProperty("useUnicode", "true");
        properties.setProperty("zeroDateTimeBehavior", "convertToNull");
        properties.setProperty("characterEncoding", "UTF-8");
        properties.setProperty("characterSetResults", "UTF-8");
        if (jdbcProperties != null) {
            properties.putAll(jdbcProperties);
        }

        String prefix =
                jdbcDriver.toLowerCase().contains("oceanbase")
                        ? OB_JDBC_URL_PREFIX
                        : MYSQL_JDBC_URL_PREFIX;
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(hostname).append(":").append(port).append("/?");

        properties.forEach(
                (key, value) -> {
                    sb.append("&").append(key).append("=").append(value);
                });
        return sb.toString();
    }

    @Override
    public Connection getConnection() throws SQLException {
        int i = 0;
        while (i < connectRetryTimes) {
            try {
                return super.getConnection();
            } catch (SQLException e) {
                if (i < connectRetryTimes - 1) {
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException ie) {
                        throw new FlinkRuntimeException(
                                "Failed to get connection, interrupted while doing another attempt",
                                ie);
                    }
                    LOG.warn("Get connection failed, retry times {}", i + 1);
                } else {
                    LOG.error("Get connection failed after retry {} times", i + 1);
                    throw new FlinkRuntimeException(e);
                }
            }
            i++;
        }
        return super.getConnection();
    }
}
