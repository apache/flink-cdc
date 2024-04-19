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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link MySqlDataSourceFactory}. */
public class MySqlDataSourceFactoryTest extends MySqlSourceTestBase {

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", TEST_USER, TEST_PASSWORD);

    @Test
    public void testCreateSource() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getTableList())
                .isEqualTo(Arrays.asList(inventoryDatabase.getDatabaseName() + ".products"));
    }

    @Test
    public void testNoMatchedTable() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        String tables = inventoryDatabase.getDatabaseName() + ".test";
        options.put(TABLES.key(), tables);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot find any table by the option 'tables' = " + tables);
    }

    @Test
    public void testExcludeTable() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".\\.*");
        String tableExclude = inventoryDatabase.getDatabaseName() + ".orders";
        options.put(TABLES_EXCLUDE.key(), tableExclude);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getTableList())
                .isNotEqualTo(Arrays.asList(inventoryDatabase.getDatabaseName() + ".orders"))
                .isEqualTo(
                        Arrays.asList(
                                inventoryDatabase.getDatabaseName() + ".customers",
                                inventoryDatabase.getDatabaseName() + ".products"));
    }

    @Test
    public void testExcludeAllTable() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        String tableExclude = inventoryDatabase.getDatabaseName() + ".prod\\.*";
        options.put(TABLES_EXCLUDE.key(), tableExclude);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Cannot find any table with by the option 'tables.exclude'  = "
                                + tableExclude);
    }

    class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return this.getClassLoader();
        }
    }
}
