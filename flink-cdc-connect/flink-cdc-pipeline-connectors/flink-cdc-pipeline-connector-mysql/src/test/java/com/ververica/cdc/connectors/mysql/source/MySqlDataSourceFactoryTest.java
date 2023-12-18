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

package com.ververica.cdc.connectors.mysql.source;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.Factory;
import com.ververica.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.ververica.cdc.connectors.mysql.source.MySqlDataSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.mysql.source.MySqlDataSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.mysql.source.MySqlDataSourceOptions.PORT;
import static com.ververica.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES;
import static com.ververica.cdc.connectors.mysql.source.MySqlDataSourceOptions.USERNAME;
import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static com.ververica.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
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
