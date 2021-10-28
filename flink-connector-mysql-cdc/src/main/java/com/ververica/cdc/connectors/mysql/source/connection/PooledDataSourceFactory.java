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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.debezium.jdbc.JdbcConfiguration;

/** A connection pool factory to create pooled DataSource {@link HikariDataSource}. */
public class PooledDataSourceFactory {

    public static final String HOSTNAME = "database.hostname";
    public static final String PORT = "database.port";
    public static final String USER = "database.user";
    public static final String PASSWORD = "database.password";
    public static final String CONNECTION_POOL_SIZE = "connection.pool.size";
    public static final String CONNECT_TIMEOUT_MS = "connect.timeout.ms";
    public static final String JDBC_URL_PREFIX = "jdbc:mysql://";
    public static final String CONNECTION_POOL_PREFIX = "connection-pool-";

    private PooledDataSourceFactory() {}

    public static HikariDataSource createPooledDataSource(JdbcConfiguration jdbcConfiguration) {
        final HikariConfig config = new HikariConfig();

        String hostName = jdbcConfiguration.getString(HOSTNAME);
        String port = jdbcConfiguration.getString(PORT);

        config.setPoolName(CONNECTION_POOL_PREFIX + hostName + ":" + port);
        config.setJdbcUrl(JDBC_URL_PREFIX + hostName + ":" + port);
        config.setUsername(jdbcConfiguration.getString(USER));
        config.setPassword(jdbcConfiguration.getString(PASSWORD));
        config.setMaximumPoolSize(jdbcConfiguration.getInteger(CONNECTION_POOL_SIZE));
        config.setConnectionTimeout(
                Long.parseLong(jdbcConfiguration.getString(CONNECT_TIMEOUT_MS)));

        // optional optimization configurations for pooled DataSource
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return new HikariDataSource(config);
    }
}
