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

package com.ververica.cdc.connectors.base.relational.connection;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/** A factory to create JDBC connection. */
public class JdbcConnectionFactory implements JdbcConnection.ConnectionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionFactory.class);

    private final JdbcSourceConfig sourceConfig;
    private final JdbcConnectionPoolFactory jdbcConnectionPoolFactory;

    public JdbcConnectionFactory(
            JdbcSourceConfig sourceConfig, JdbcConnectionPoolFactory jdbcConnectionPoolFactory) {
        this.sourceConfig = sourceConfig;
        this.jdbcConnectionPoolFactory = jdbcConnectionPoolFactory;
    }

    @Override
    public Connection connect(JdbcConfiguration config) throws SQLException {
        final int connectRetryTimes = sourceConfig.getConnectMaxRetries();

        final ConnectionPoolId connectionPoolId =
                new ConnectionPoolId(
                        sourceConfig.getHostname(),
                        sourceConfig.getPort(),
                        sourceConfig.getUsername());

        HikariDataSource dataSource =
                JdbcConnectionPools.getInstance(jdbcConnectionPoolFactory)
                        .getOrCreateConnectionPool(connectionPoolId, sourceConfig);

        int i = 0;
        while (i < connectRetryTimes) {
            try {
                return dataSource.getConnection();
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
        return dataSource.getConnection();
    }
}
