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

import org.apache.flink.annotation.Internal;

import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.zaxxer.hikari.HikariDataSource;

/** A JDBC connection pools that consists of {@link HikariDataSource}. */
@Internal
public interface ConnectionPools {

    /**
     * Gets a connection pool from pools, create a new pool if the pool does not exists in the
     * connection pools .
     */
    HikariDataSource getOrCreateConnectionPool(
            ConnectionPoolId poolId, MySqlSourceConfig sourceConfig);
}
