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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PARSE_ONLINE_SCHEMA_CHANGES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TREAT_TINYINT1_AS_BOOLEAN_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link MySqlDataSourceFactory}. */
class MySqlDataSourceFactoryTest extends MySqlSourceTestBase {

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", TEST_USER, TEST_PASSWORD);

    @Test
    void testCreateSource() {
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
    void testCreateSourceScanBinlogNewlyAddedTableEnabled() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        options.put(SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        options.put(SCAN_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "If both scan.binlog.newly-added-table.enabled and scan.newly-added-table.enabled are true, data maybe duplicate after restore");
    }

    @Test
    void testNoMatchedTable() {
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
    void testExcludeTable() {
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
                                inventoryDatabase.getDatabaseName() + ".multi_max_table",
                                inventoryDatabase.getDatabaseName() + ".products"));
    }

    @Test
    void testExcludeAllTable() {
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

    @Test
    void testDatabaseAndTableWithTheSameName() throws SQLException {
        inventoryDatabase.createAndInitialize();
        // create a table with the same name of database
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            String createSameNameTableSql =
                    String.format(
                            "CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n"
                                    + "  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"
                                    + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                                    + "  description VARCHAR(512)\n"
                                    + ");",
                            inventoryDatabase.getDatabaseName(),
                            inventoryDatabase.getDatabaseName());

            statement.execute(createSameNameTableSql);
        }
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(
                TABLES.key(),
                inventoryDatabase.getDatabaseName() + "." + inventoryDatabase.getDatabaseName());
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getTableList())
                .isEqualTo(
                        Arrays.asList(
                                inventoryDatabase.getDatabaseName()
                                        + "."
                                        + inventoryDatabase.getDatabaseName()));
    }

    @Test
    void testLackRequireOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        List<String> requireKeys =
                factory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());
        for (String requireKey : requireKeys) {
            Map<String, String> remainingOptions = new HashMap<>(options);
            remainingOptions.remove(requireKey);
            Factory.Context context = new MockContext(Configuration.fromMap(remainingOptions));

            assertThatThrownBy(() -> factory.createDataSource(context))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            String.format(
                                    "One or more required options are missing.\n\n"
                                            + "Missing required options are:\n\n"
                                            + "%s",
                                    requireKey));
        }
    }

    @Test
    void testUnsupportedOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        options.put("unsupported_key", "unsupported_value");

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'mysql'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    public void testOptionalOption() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");

        // optional option
        options.put(TREAT_TINYINT1_AS_BOOLEAN_ENABLED.key(), "false");
        options.put(PARSE_ONLINE_SCHEMA_CHANGES.key(), "true");
        options.put(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.key(), "true");

        Factory.Context context = new MockContext(Configuration.fromMap(options));
        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        assertThat(factory.optionalOptions())
                .contains(
                        TREAT_TINYINT1_AS_BOOLEAN_ENABLED,
                        PARSE_ONLINE_SCHEMA_CHANGES,
                        SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED);

        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().isTreatTinyInt1AsBoolean()).isFalse();
        assertThat(dataSource.getSourceConfig().isParseOnLineSchemaChanges()).isTrue();
        assertThat(dataSource.getSourceConfig().isAssignUnboundedChunkFirst()).isTrue();
    }

    @Test
    void testPrefixRequireOption() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        options.put("jdbc.properties.requireSSL", "true");
        options.put("debezium.snapshot.mode", "initial");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getTableList())
                .isEqualTo(Arrays.asList(inventoryDatabase.getDatabaseName() + ".products"));
    }

    @Test
    void testAddChunkKeyColumns() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".\\.*");
        options.put(
                SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(),
                inventoryDatabase.getDatabaseName()
                        + ".multi_max_\\.*:order_id;"
                        + inventoryDatabase.getDatabaseName()
                        + ".products:id;");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        ObjectPath multiMaxTable =
                new ObjectPath(inventoryDatabase.getDatabaseName(), "multi_max_table");
        ObjectPath productsTable = new ObjectPath(inventoryDatabase.getDatabaseName(), "products");

        assertThat(dataSource.getSourceConfig().getChunkKeyColumns())
                .isNotEmpty()
                .isEqualTo(
                        new HashMap<ObjectPath, String>() {
                            {
                                put(multiMaxTable, "order_id");
                                put(productsTable, "id");
                            }
                        });
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
