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

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** A Jdbc Connection pools implementation. */
public class JdbcConnectionPools implements ConnectionPools<HikariDataSource, JdbcSourceConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionPools.class);

    private static JdbcConnectionPools instance;
    private final Map<ConnectionPoolId, HikariDataSource> pools = new HashMap<>();
    private static JdbcConnectionPoolFactory jdbcConnectionPoolFactory;

    private JdbcConnectionPools() {}

    public static synchronized JdbcConnectionPools getInstance(
            JdbcConnectionPoolFactory jdbcConnectionPoolFactory) {
        if (instance == null) {
            JdbcConnectionPools.jdbcConnectionPoolFactory = jdbcConnectionPoolFactory;
            instance = new JdbcConnectionPools();
        }
        return instance;
    }

    @Override
    public HikariDataSource getOrCreateConnectionPool(
            ConnectionPoolId poolId, JdbcSourceConfig sourceConfig) {
        synchronized (pools) {
            if (!pools.containsKey(poolId)) {
                LOG.info("Create and register connection pool {}", poolId);
                pools.put(poolId, jdbcConnectionPoolFactory.createPooledDataSource(sourceConfig));
            } else {
                HikariDataSource hikariDataSource = pools.get(poolId);
                String serverTimeZoneOld =
                        hikariDataSource
                                .getDataSourceProperties()
                                .getProperty(JdbcConnectionPoolFactory.SERVER_TIMEZONE_KEY);
                String serverTimeZoneNew = sourceConfig.getServerTimeZone();
                if (hikariDataSource != null
                        && StringUtils.isNotEmpty(serverTimeZoneOld)
                        && StringUtils.isNotEmpty(serverTimeZoneNew)) {
                    long connectTimeoutOld = hikariDataSource.getConnectionTimeout();
                    long connectTimeoutNew = sourceConfig.getConnectTimeout().toMillis();
                    if (!StringUtils.equals(serverTimeZoneOld, serverTimeZoneNew)
                            || connectTimeoutOld != connectTimeoutNew) {
                        HikariDataSource pooledDataSource =
                                jdbcConnectionPoolFactory.createPooledDataSource(sourceConfig);
                        pools.put(poolId, pooledDataSource);
                        return pooledDataSource;
                    }
                }
            }
            return pools.get(poolId);
        }
    }
}
