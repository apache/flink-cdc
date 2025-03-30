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
import org.apache.flink.cdc.connectors.base.mocked.MockedPooledDataSourceFactory;
import org.apache.flink.cdc.connectors.base.mocked.MockedSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.relational.connection.ConnectionPoolId;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPools;
import org.apache.flink.util.FlinkRuntimeException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/** Tests for {@link JdbcConnectionPools}. */
class GenericConnectionPoolTest {
    public static final String HOSTNAME = "localhost";
    public static final int PORT = 3306;

    public static final String USER_NAME = "user";
    public static final String PASSWORD = "password";

    public static final String DATABASE = "test_database";

    public static final String TABLE = "test_table";

    @Test
    void testMultiConnectionPoolFactory() {
        MockConnectionPoolFactory mockConnectionPoolFactory = new MockConnectionPoolFactory();
        MockedPooledDataSourceFactory genericPooledDataSourceFactory =
                new MockedPooledDataSourceFactory();
        JdbcConnectionPools mockInstance =
                JdbcConnectionPools.getInstance(mockConnectionPoolFactory);
        JdbcConnectionPools mysqlInstance =
                JdbcConnectionPools.getInstance(genericPooledDataSourceFactory);
        MockedSourceConfig mySqlSourceConfig =
                getMockMySqlSourceConfig(HOSTNAME, PORT, USER_NAME, PASSWORD, DATABASE, TABLE);

        Assertions.assertThat(mockConnectionPoolFactory.getJdbcUrl(mySqlSourceConfig))
                .isEqualTo(
                        mockInstance.getJdbcUrl(
                                mySqlSourceConfig, mockConnectionPoolFactory.getClass().getName()));
        Assertions.assertThat(genericPooledDataSourceFactory.getJdbcUrl(mySqlSourceConfig))
                .isEqualTo(
                        mysqlInstance.getJdbcUrl(
                                mySqlSourceConfig,
                                genericPooledDataSourceFactory.getClass().getName()));
        Assertions.assertThat(mockConnectionPoolFactory.getJdbcUrl(mySqlSourceConfig))
                .isNotEqualTo(
                        mysqlInstance.getJdbcUrl(
                                mySqlSourceConfig,
                                genericPooledDataSourceFactory.getClass().getName()));
    }

    @Test
    void testNoDataSourcePoolFactoryIdentifier() {
        MockedPooledDataSourceFactory mysqlPooledDataSourceFactory =
                new MockedPooledDataSourceFactory();
        JdbcConnectionPools mysqlInstance =
                JdbcConnectionPools.getInstance(mysqlPooledDataSourceFactory);
        MockedSourceConfig mySqlSourceConfig =
                getMockMySqlSourceConfig(HOSTNAME, PORT, USER_NAME, PASSWORD, DATABASE, TABLE);
        ConnectionPoolId poolId =
                new ConnectionPoolId(
                        HOSTNAME,
                        PORT,
                        USER_NAME,
                        DATABASE,
                        MockConnectionPoolFactory.class.getName());
        Assertions.assertThatThrownBy(
                        () -> mysqlInstance.getOrCreateConnectionPool(poolId, mySqlSourceConfig))
                .withFailMessage(
                        String.format(
                                "DataSourcePoolFactoryIdentifier named %s doesn't exists",
                                poolId.getDataSourcePoolFactoryIdentifier()))
                .isExactlyInstanceOf(FlinkRuntimeException.class);
    }

    private static MockedSourceConfig getMockMySqlSourceConfig(
            String hostname,
            int port,
            String username,
            String password,
            String database,
            String table) {
        return new MockedSourceConfig(
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
