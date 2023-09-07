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

package com.ververica.cdc.connectors.base.relational.connection;

import org.apache.flink.annotation.VisibleForTesting;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** A Jdbc Connection pools implementation. */
public class JdbcConnectionPools implements ConnectionPools<HikariDataSource, JdbcSourceConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionPools.class);

    private static JdbcConnectionPools instance;
    private final Map<ConnectionPoolId, HikariDataSource> pools = new HashMap<>();
    private static final Map<String, JdbcConnectionPoolFactory> poolFactoryMap = new HashMap<>();

    private JdbcConnectionPools() {}

    public static synchronized JdbcConnectionPools getInstance(
            JdbcConnectionPoolFactory jdbcConnectionPoolFactory) {
        if (instance == null) {
            instance = new JdbcConnectionPools();
        }
        poolFactoryMap.put(
                jdbcConnectionPoolFactory.getClass().getName(), jdbcConnectionPoolFactory);
        return instance;
    }

    @Override
    public HikariDataSource getOrCreateConnectionPool(
            ConnectionPoolId poolId, JdbcSourceConfig sourceConfig) {
        synchronized (pools) {
            if (!pools.containsKey(poolId)) {
                LOG.info("Create and register connection pool {}", poolId);
                pools.put(
                        poolId,
                        poolFactoryMap
                                .get(poolId.getDataSourcePoolFactoryIdentifier())
                                .createPooledDataSource(sourceConfig));
            }
            return pools.get(poolId);
        }
    }

    @VisibleForTesting
    public String getJdbcUrl(
            JdbcSourceConfig sourceConfig, String dataSourcePoolFactoryIdentifier) {
        return poolFactoryMap.get(dataSourcePoolFactoryIdentifier).getJdbcUrl(sourceConfig);
    }
}
