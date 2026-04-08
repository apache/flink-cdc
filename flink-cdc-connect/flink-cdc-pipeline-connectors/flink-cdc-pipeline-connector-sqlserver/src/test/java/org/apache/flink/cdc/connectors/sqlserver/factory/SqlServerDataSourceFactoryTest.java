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

package org.apache.flink.cdc.connectors.sqlserver.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSource;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Tests for {@link SqlServerDataSourceFactory}. */
@Internal
public class SqlServerDataSourceFactoryTest extends SqlServerTestBase {

    private static final String DATABASE_NAME = "inventory";

    @BeforeEach
    public void before() {
        initializeSqlServerTable(DATABASE_NAME);
    }

    @Test
    public void testCreateDataSource() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.prod\\.*");
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        SqlServerDataSource dataSource = (SqlServerDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSqlServerSourceConfig().getTableList())
                .isEqualTo(Arrays.asList("dbo.products", "dbo.products_on_hand"));
    }

    @Test
    public void testNoMatchedTable() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        String tables = DATABASE_NAME + ".dbo.nonexistent";
        options.put(TABLES.key(), tables);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot find any table by the option 'tables' = " + tables);
    }

    @Test
    public void testExcludeTable() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.\\.*");
        String tableExclude = DATABASE_NAME + ".dbo.orders";
        options.put(TABLES_EXCLUDE.key(), tableExclude);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        SqlServerDataSource dataSource = (SqlServerDataSource) factory.createDataSource(context);
        List<String> actualTableList =
                new ArrayList<>(dataSource.getSqlServerSourceConfig().getTableList());
        Collections.sort(actualTableList);
        assertThat(actualTableList).doesNotContain("dbo.orders");
    }

    @Test
    public void testExcludeAllTable() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.products");
        String tableExclude = DATABASE_NAME + ".dbo.products";
        options.put(TABLES_EXCLUDE.key(), tableExclude);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Cannot find any table with by the option 'tables.exclude'  = "
                                + tableExclude);
    }

    @Test
    public void testLackRequireOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.prod\\.*");

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
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
    public void testUnsupportedOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.prod\\.*");
        options.put("unsupported_key", "unsupported_value");

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'sqlserver'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    public void testOptionalOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.prod\\.*");

        Factory.Context context = new MockContext(Configuration.fromMap(options));
        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        assertThat(factory.optionalOptions()).contains(PORT);

        SqlServerDataSource dataSource = (SqlServerDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSqlServerSourceConfig().getPort())
                .isEqualTo(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT));
    }

    @Test
    public void testChunkKeyColumnOptionIsSupported() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.products");
        options.put(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(), "id");

        Factory.Context context = new MockContext(Configuration.fromMap(options));
        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();

        assertThat(factory.optionalOptions()).contains(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        SqlServerDataSource dataSource = (SqlServerDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSqlServerSourceConfig().getChunkKeyColumn()).isEqualTo("id");
    }

    @Test
    public void testStartupFromTimestamp() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.prod\\.*");
        options.put(SCAN_STARTUP_MODE.key(), "timestamp");
        options.put(SCAN_STARTUP_TIMESTAMP_MILLIS.key(), "1667232000000");

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        SqlServerDataSource dataSource = (SqlServerDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSqlServerSourceConfig().getStartupOptions())
                .isEqualTo(StartupOptions.timestamp(1667232000000L));
    }

    @Test
    public void testTimestampStartupRequiresTimestampMillis() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.prod\\.*");
        options.put(SCAN_STARTUP_MODE.key(), "timestamp");

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("scan.startup.timestamp-millis");
    }

    @Test
    public void testPrefixRequireOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.prod\\.*");
        options.put("debezium.snapshot.mode", "initial");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        SqlServerDataSource dataSource = (SqlServerDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSqlServerSourceConfig().getTableList())
                .isEqualTo(Arrays.asList("dbo.products", "dbo.products_on_hand"));
    }

    @Test
    public void testJdbcPropertiesAreForwardedToDatabaseConfig() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo.prod\\.*");
        options.put("jdbc.properties.encrypt", "true");
        options.put("jdbc.properties.trustServerCertificate", "true");

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        SqlServerDataSource dataSource = (SqlServerDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSqlServerSourceConfig().getDbzProperties())
                .containsEntry("database.encrypt", "true")
                .containsEntry("database.trustServerCertificate", "true");
    }

    @Test
    public void testTableValidationWithDifferentDatabases() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), "db1.dbo.table1,db2.dbo.table2");

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "The value of option `tables` is `db1.dbo.table1,db2.dbo.table2`, but not all table names have the same database name");
    }

    @Test
    public void testTableValidationRequiresDatabaseSchemaTableFormat() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), DATABASE_NAME + ".dbo");

        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Table '"
                                + DATABASE_NAME
                                + ".dbo' does not match the expected 'database.schema.table' format.")
                .hasMessageContaining(TABLES.key());
    }

    static class MockContext implements Factory.Context {

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
            return this.getClass().getClassLoader();
        }
    }
}
