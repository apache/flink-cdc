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

package org.apache.flink.cdc.connectors.base;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.experimental.MysqlPooledDataSourceFactory;
import org.apache.flink.cdc.connectors.base.experimental.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.relational.connection.ConnectionPoolId;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPools;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

/** Tests for {@link JdbcConnectionPools}. */
class JdbcConnectionPoolTest {
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 3306;

    private static final String USER_NAME = "user";
    private static final String PASSWORD = "password";

    private static final String DATABASE = "test_database";
    private static final String TABLE = "test_table";

    private MysqlPooledDataSourceFactory mysqlPooledDataSourceFactory;
    private JdbcConnectionPools mysqlInstance;
    private MySqlSourceConfig mySqlSourceConfig;

    @BeforeEach
    void beforeEach() {
        mysqlPooledDataSourceFactory = new MysqlPooledDataSourceFactory();
        mysqlInstance = JdbcConnectionPools.getInstance(mysqlPooledDataSourceFactory);
        mySqlSourceConfig =
                getMockMySqlSourceConfig(HOSTNAME, PORT, USER_NAME, PASSWORD, DATABASE, TABLE);
    }

    @Test
    void testMultiConnectionPoolFactory() {
        MockConnectionPoolFactory mockConnectionPoolFactory = new MockConnectionPoolFactory();
        JdbcConnectionPools mockInstance =
                JdbcConnectionPools.getInstance(mockConnectionPoolFactory);

        assertThat(
                        mockInstance.getJdbcUrl(
                                mySqlSourceConfig, mockConnectionPoolFactory.getClass().getName()))
                .isEqualTo(mockConnectionPoolFactory.getJdbcUrl(mySqlSourceConfig));
        assertThat(
                        mysqlInstance.getJdbcUrl(
                                mySqlSourceConfig,
                                mysqlPooledDataSourceFactory.getClass().getName()))
                .isEqualTo(mysqlPooledDataSourceFactory.getJdbcUrl(mySqlSourceConfig));
        assertThat(
                        mysqlInstance.getJdbcUrl(
                                mySqlSourceConfig,
                                mysqlPooledDataSourceFactory.getClass().getName()))
                .isNotEqualTo(mockConnectionPoolFactory.getJdbcUrl(mySqlSourceConfig));
    }

    @Test
    void testNoDataSourcePoolFactoryIdentifier() {
        ConnectionPoolId poolId =
                new ConnectionPoolId(
                        HOSTNAME,
                        PORT,
                        USER_NAME,
                        DATABASE,
                        MockConnectionPoolFactory.class.getName());
        assertThrowsExactly(
                FlinkRuntimeException.class,
                () -> mysqlInstance.getOrCreateConnectionPool(poolId, mySqlSourceConfig),
                String.format(
                        "DataSourcePoolFactoryIdentifier named %s doesn't exists",
                        poolId.getDataSourcePoolFactoryIdentifier()));
    }

    private static MySqlSourceConfig getMockMySqlSourceConfig(
            String hostname,
            int port,
            String username,
            String password,
            String database,
            String table) {
        return new MySqlSourceConfig(
                StartupOptions.latest(),
                Arrays.asList(database),
                Arrays.asList(table),
                2,
                1,
                1.00,
                2.00,
                false,
                true,
                new Properties(),
                null,
                "com.mysql.cj.jdbc.Driver",
                hostname,
                port,
                username,
                password,
                20,
                "UTC",
                Duration.ofSeconds(10),
                2,
                3,
                false);
    }

    private static class MockConnectionPoolFactory extends JdbcConnectionPoolFactory {
        @Override
        public String getJdbcUrl(JdbcSourceConfig sourceConfig) {
            return "mock-url";
        }
    }
}
