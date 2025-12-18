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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.relational.connection.ConnectionPoolId;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.util.FlinkRuntimeException;

import com.zaxxer.hikari.HikariDataSource;
import io.debezium.jdbc.JdbcConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;

/** A connection pool factory to create pooled GaussDB {@link HikariDataSource}. */
public class GaussDBConnectionPoolFactory extends JdbcConnectionPoolFactory {

    public static final String JDBC_URL_PATTERN = "jdbc:gaussdb://%s:%s/%s";

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBConnectionPoolFactory.class);

    private static final long RETRY_INTERVAL_MILLIS = 5_000L;

    @Override
    public String getJdbcUrl(JdbcSourceConfig sourceConfig) {
        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();
        String database = sourceConfig.getDatabaseList().get(0);
        return String.format(JDBC_URL_PATTERN, hostName, port, database);
    }

    /**
     * The reuses of connection pools are based on databases in GaussDB. Different databases in the
     * same instance cannot reuse the same connection pool to connect.
     */
    @Override
    public ConnectionPoolId getPoolId(
            JdbcConfiguration config, String dataSourcePoolFactoryIdentifier) {
        return new ConnectionPoolId(
                config.getHostname(),
                config.getPort(),
                config.getHostname(),
                config.getDatabase(),
                dataSourcePoolFactoryIdentifier);
    }

    @Override
    public HikariDataSource createPooledDataSource(JdbcSourceConfig sourceConfig) {
        final String jdbcUrl = getJdbcUrl(sourceConfig);
        final int maxRetries = Math.max(1, sourceConfig.getConnectMaxRetries());

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            HikariDataSource dataSource = null;
            try {
                dataSource = super.createPooledDataSource(sourceConfig);
                try (Connection connection = dataSource.getConnection();
                        Statement statement = connection.createStatement()) {
                    statement.execute("SELECT 1");
                }
                return dataSource;
            } catch (Exception e) {
                if (dataSource != null) {
                    dataSource.close();
                }

                if (attempt >= maxRetries) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Failed to create GaussDB connection pool for '%s' after %d attempts",
                                    jdbcUrl, attempt),
                            e);
                }

                LOG.warn(
                        "Failed to create GaussDB connection pool for '{}' (attempt {}/{}), retrying in {} ms",
                        jdbcUrl,
                        attempt,
                        maxRetries,
                        RETRY_INTERVAL_MILLIS,
                        e);

                try {
                    Thread.sleep(RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new FlinkRuntimeException(
                            "Interrupted while retrying to create GaussDB connection pool for '"
                                    + jdbcUrl
                                    + "'",
                            ie);
                }
            }
        }

        throw new FlinkRuntimeException(
                "Failed to create GaussDB connection pool for '" + jdbcUrl + "'");
    }
}
