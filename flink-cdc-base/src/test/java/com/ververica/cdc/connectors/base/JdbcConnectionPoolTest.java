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

package com.ververica.cdc.connectors.base;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.experimental.MysqlPooledDataSourceFactory;
import com.ververica.cdc.connectors.base.experimental.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionPools;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/** Tests for JdbcConnection Pool. */
public class JdbcConnectionPoolTest {

    @Test
    public void testMultiConnectionPoolFactory() {
        MockConnectionPoolFactory mockConnectionPoolFactory = new MockConnectionPoolFactory();
        MysqlPooledDataSourceFactory mysqlPooledDataSourceFactory =
                new MysqlPooledDataSourceFactory();
        JdbcConnectionPools mockInstance =
                JdbcConnectionPools.getInstance(mockConnectionPoolFactory);
        JdbcConnectionPools mysqlInstance =
                JdbcConnectionPools.getInstance(mysqlPooledDataSourceFactory);
        MySqlSourceConfig mySqlSourceConfig =
                new MySqlSourceConfig(
                        StartupOptions.latest(),
                        Arrays.asList("database"),
                        Arrays.asList("table"),
                        2,
                        1,
                        1.00,
                        2.00,
                        false,
                        true,
                        new Properties(),
                        null,
                        "com.mysql.cj.jdbc.Driver",
                        "localhost",
                        8080,
                        "username",
                        "password",
                        20,
                        "UTC",
                        Duration.ofSeconds(10),
                        2,
                        3);
        Assert.assertEquals(
                mockInstance.getJdbcUrl(
                        mySqlSourceConfig, mockConnectionPoolFactory.getClass().getName()),
                mockConnectionPoolFactory.getJdbcUrl(mySqlSourceConfig));
        Assert.assertEquals(
                mysqlInstance.getJdbcUrl(
                        mySqlSourceConfig, mysqlPooledDataSourceFactory.getClass().getName()),
                mysqlPooledDataSourceFactory.getJdbcUrl(mySqlSourceConfig));
        Assert.assertNotEquals(
                mysqlInstance.getJdbcUrl(
                        mySqlSourceConfig, mysqlPooledDataSourceFactory.getClass().getName()),
                mockConnectionPoolFactory.getJdbcUrl(mySqlSourceConfig));
    }

    static class MockConnectionPoolFactory extends JdbcConnectionPoolFactory {

        @Override
        public String getJdbcUrl(JdbcSourceConfig sourceConfig) {
            return "mock-url";
        }
    }
}
