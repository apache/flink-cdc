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

package org.apache.flink.cdc.connectors.jdbc.conn;

import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.util.FlinkRuntimeException;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/** A factory to create JDBC connection. */
public class JdbcConnectionFactory implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionFactory.class);

    private final JdbcSinkConfig sinkConfig;
    private final JdbcConnectionPoolFactory jdbcConnectionPoolFactory;

    public JdbcConnectionFactory(
            JdbcSinkConfig sinkConfig, JdbcConnectionPoolFactory jdbcConnectionPoolFactory) {
        this.sinkConfig = sinkConfig;
        this.jdbcConnectionPoolFactory = jdbcConnectionPoolFactory;
    }

    public Connection connect() throws SQLException {
        final int connectRetryTimes = sinkConfig.getConnectMaxRetries();

        final ConnectionPoolId connectionPoolId =
                jdbcConnectionPoolFactory.getPoolId(
                        sinkConfig, jdbcConnectionPoolFactory.getClass().getName());

        HikariDataSource dataSource =
                JdbcConnectionPools.getInstance(jdbcConnectionPoolFactory)
                        .getOrCreateConnectionPool(connectionPoolId, sinkConfig);

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
