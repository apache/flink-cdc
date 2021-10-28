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

import com.zaxxer.hikari.HikariDataSource;
import io.debezium.jdbc.JdbcConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.ververica.cdc.connectors.mysql.source.connection.PooledDataSourceFactory.createPooledDataSource;

/** A MySQL Connection pool implementation. */
public class MySqlConnectionPools implements ConnectionPools {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlConnectionPools.class);
    private static final Map<ConnectionPoolId, HikariDataSource> pools = new ConcurrentHashMap<>();

    private static final MySqlConnectionPools INSTANCE = new MySqlConnectionPools();

    private MySqlConnectionPools() {}

    public static MySqlConnectionPools getInstance() {
        return INSTANCE;
    }

    @Override
    public void registerConnectionPool(ConnectionPoolId poolId, JdbcConfiguration configuration) {
        synchronized (pools) {
            if (pools.containsKey(poolId)) {
                LOG.info(
                        "Register connection pool failure due to duplicated pool existed {}",
                        poolId);
            } else {
                HikariDataSource dataSource = createPooledDataSource(configuration);
                pools.put(poolId, dataSource);
                LOG.info("Register connection pool {}", poolId);
            }
        }
    }

    public HikariDataSource getConnectionPool(ConnectionPoolId poolId) {
        synchronized (pools) {
            return pools.get(poolId);
        }
    }
}
