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

package org.apache.flink.cdc.connectors.postgres.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDataSource;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.PG_PORT;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.SLOT_NAME;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.postgres.source.PostgresDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Tests for {@link PostgresDataSourceFactory}. */
@Internal
public class PostgresDataSourceFactoryTest extends PostgresTestBase {

    private String slotName;

    @BeforeEach
    public void before() {
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");
        slotName = getSlotName();
    }

    @AfterEach
    public void after() throws SQLException {
        String sql = String.format("SELECT pg_drop_replication_slot('%s')", slotName);
        try (Connection connection =
                        PostgresTestBase.getJdbcConnection(POSTGRES_CONTAINER, "postgres");
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            LOG.error("Drop replication slot failed.");
        }
    }

    @Test
    public void testCreateDataSource() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), POSTGRES_CONTAINER.getDatabaseName() + ".inventory.prod\\.*");
        options.put(SLOT_NAME.key(), slotName);
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
        PostgresDataSource dataSource = (PostgresDataSource) factory.createDataSource(context);
        assertThat(dataSource.getPostgresSourceConfig().getTableList())
                .isEqualTo(Arrays.asList("inventory.products"));
    }

    @Test
    public void testNoMatchedTable() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        String tables = POSTGRES_CONTAINER.getDatabaseName() + ".inventory.test";
        options.put(TABLES.key(), tables);
        options.put(SLOT_NAME.key(), slotName);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot find any table by the option 'tables' = " + tables);
    }

    @Test
    public void testExcludeTable() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), POSTGRES_CONTAINER.getDatabaseName() + ".\\.*.\\.*");
        String tableExclude = POSTGRES_CONTAINER.getDatabaseName() + ".inventory.orders";
        options.put(TABLES_EXCLUDE.key(), tableExclude);
        options.put(SLOT_NAME.key(), slotName);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
        PostgresDataSource dataSource = (PostgresDataSource) factory.createDataSource(context);
        List<String> actualTableList =
                new ArrayList<>(dataSource.getPostgresSourceConfig().getTableList());
        Collections.sort(actualTableList);
        assertThat(actualTableList)
                .isNotEqualTo(Collections.singletonList("inventory.orders"))
                .isEqualTo(
                        Arrays.asList(
                                "inventory.customers",
                                "inventory.multi_max_table",
                                "inventory.products"));
    }

    @Test
    public void testExcludeAllTable() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), POSTGRES_CONTAINER.getDatabaseName() + ".inventory.prod\\.*");
        String tableExclude = POSTGRES_CONTAINER.getDatabaseName() + ".inventory.prod\\.*";
        options.put(TABLES_EXCLUDE.key(), tableExclude);
        options.put(SLOT_NAME.key(), slotName);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Cannot find any table with by the option 'tables.exclude'  = "
                                + tableExclude);
    }

    @Test
    public void testLackRequireOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), POSTGRES_CONTAINER.getDatabaseName() + ".inventory.prod\\.*");
        options.put(SLOT_NAME.key(), slotName);

        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
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
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), POSTGRES_CONTAINER.getDatabaseName() + ".inventory.prod\\.*");
        options.put(SLOT_NAME.key(), slotName);
        options.put("unsupported_key", "unsupported_value");

        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'postgres'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    public void testOptionalOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), POSTGRES_CONTAINER.getDatabaseName() + ".inventory.prod\\.*");
        options.put(SLOT_NAME.key(), slotName);
        options.put(TABLES_EXCLUDE.key(), "true");

        Factory.Context context = new MockContext(Configuration.fromMap(options));
        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
        assertThat(factory.optionalOptions()).contains(PG_PORT);

        PostgresDataSource dataSource = (PostgresDataSource) factory.createDataSource(context);
        assertThat(dataSource.getPostgresSourceConfig().getPort())
                .isEqualTo(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT));
    }

    @Test
    public void testPrefixRequireOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), POSTGRES_CONTAINER.getDatabaseName() + ".inventory.prod\\.*");
        options.put(SLOT_NAME.key(), slotName);
        options.put("jdbc.properties.requireSSL", "true");
        options.put("debezium.snapshot.mode", "initial");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
        PostgresDataSource dataSource = (PostgresDataSource) factory.createDataSource(context);
        assertThat(dataSource.getPostgresSourceConfig().getTableList())
                .isEqualTo(Arrays.asList("inventory.products"));
    }

    @Test
    public void testTableValidationWithDifferentDatabases() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(
                TABLES.key(),
                "aia_test.public.aia_t_icc_jjdb,different_db.public.aia_t_icc_jjdb_extend");
        options.put(SLOT_NAME.key(), slotName);

        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "The value of option `tables` is `aia_test.public.aia_t_icc_jjdb,different_db.public.aia_t_icc_jjdb_extend`, but not all table names have the same database name");
    }

    @Test
    public void testTableValidationWithOriginalBugScenario() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), POSTGRES_CONTAINER.getHost());
        options.put(
                PG_PORT.key(), String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        String tables =
                POSTGRES_CONTAINER.getDatabaseName()
                        + ".public.aia_t_icc_jjdb,"
                        + POSTGRES_CONTAINER.getDatabaseName()
                        + ".public.aia_t_icc_jjdb_\\\\d{6},"
                        + POSTGRES_CONTAINER.getDatabaseName()
                        + ".public.aia_t_icc_jjdb_extend";
        options.put(TABLES.key(), tables);
        options.put(SLOT_NAME.key(), slotName);

        PostgresDataSourceFactory factory = new PostgresDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot find any table by the option 'tables'");
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
