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
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.ververica.cdc.connectors.mysql.source.connection.PooledDataSourceFactory.createPooledDataSource;

/** A Jdbc Connection pools implementation. */
public class JdbcConnectionPools implements ConnectionPools {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionPools.class);

    private static final JdbcConnectionPools INSTANCE = new JdbcConnectionPools();
    private final Map<ConnectionPoolId, HikariDataSource> pools = new HashMap<>();

    private JdbcConnectionPools() {}

    public static JdbcConnectionPools getInstance() {
        return INSTANCE;
    }

    @Override
    public HikariDataSource getOrCreateConnectionPool(
            ConnectionPoolId poolId, MySqlSourceConfig sourceConfig) {
        synchronized (pools) {
            if (!pools.containsKey(poolId)) {
                LOG.info("Create and register connection pool {}", poolId);
                pools.put(poolId, createPooledDataSource(sourceConfig));
            }
            return pools.get(poolId);
        }
    }
}
