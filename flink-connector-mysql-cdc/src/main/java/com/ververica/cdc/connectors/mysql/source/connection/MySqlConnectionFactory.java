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

package com.ververica.cdc.connectors.mysql.source.connection;

import org.apache.flink.util.FlinkRuntimeException;

import com.zaxxer.hikari.HikariDataSource;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CONNECT_MAX_RETRIES;
import static com.ververica.cdc.connectors.mysql.source.connection.PooledDataSourceFactory.HOSTNAME;
import static com.ververica.cdc.connectors.mysql.source.connection.PooledDataSourceFactory.PORT;

/** A factory to create JDBC connection for MySql. */
public class MySqlConnectionFactory implements JdbcConnection.ConnectionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlConnectionFactory.class);

    public MySqlConnectionFactory() {}

    @Override
    public Connection connect(JdbcConfiguration config) throws SQLException {
        final int connectRetryTimes =
                config.getInteger(CONNECT_MAX_RETRIES.key(), CONNECT_MAX_RETRIES.defaultValue());

        final ConnectionPoolId connectionPoolId =
                new ConnectionPoolId(config.getString(HOSTNAME), config.getString(PORT));

        if (MySqlConnectionPools.getInstance().getConnectionPool(connectionPoolId) == null) {
            MySqlConnectionPools.getInstance().registerConnectionPool(connectionPoolId, config);
        }

        HikariDataSource dataSource =
                MySqlConnectionPools.getInstance().getConnectionPool(connectionPoolId);

        int i = 0;
        while (i < connectRetryTimes) {
            try {
                return dataSource.getConnection();
            } catch (SQLException e) {
                if (i < connectRetryTimes - 1) {
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    LOG.info("Get connection failed, retry times  {}", i + 1);
                } else {
                    LOG.info("Get connection failed after retry {} times", i + 1);
                    throw new FlinkRuntimeException(e);
                }
            }
            i++;
        }
        return dataSource.getConnection();
    }
}
