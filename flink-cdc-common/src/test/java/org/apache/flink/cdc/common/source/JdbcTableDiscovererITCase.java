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

package org.apache.flink.cdc.common.source;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.source.discover.JdbcTableDiscoverer;
import org.apache.flink.cdc.common.source.discover.TableDiscoverer;
import org.apache.flink.cdc.common.source.discover.TableDiscovererFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link JdbcTableDiscoverer} and {@link TableDiscovererFactory} SPI loading.
 * Uses a real MySQL container via Testcontainers to verify end-to-end table discovery.
 */
@Testcontainers
class JdbcTableDiscovererITCase {

    @Container
    private static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>("mysql:8.0")
                    .withDatabaseName("meta_db")
                    .withUsername("test_user")
                    .withPassword("test_password");

    @BeforeAll
    static void setupDatabase() throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
                Statement stmt = conn.createStatement()) {
            // Create the subscription table with the default column name
            stmt.execute(
                    "CREATE TABLE cdc_subscriptions ("
                            + "  subscribe_table_name VARCHAR(255) PRIMARY KEY"
                            + ")");
            // Seed some subscription entries
            stmt.execute(
                    "INSERT INTO cdc_subscriptions VALUES "
                            + "('source_db.orders'), "
                            + "('source_db.products'), "
                            + "('analytics_db.user_events')");

            // Create another subscription table with a custom column name
            stmt.execute(
                    "CREATE TABLE custom_subscriptions ("
                            + "  table_fqn VARCHAR(255) PRIMARY KEY"
                            + ")");
            stmt.execute(
                    "INSERT INTO custom_subscriptions VALUES "
                            + "('warehouse.inventory'), "
                            + "('warehouse.shipments')");
        }
    }

    @AfterAll
    static void tearDown() {
        // Container is auto-stopped by Testcontainers
    }

    // =========================================================================
    //  SPI Loading Tests
    // =========================================================================

    @Test
    void testSpiLoadsJdbcFactory() {
        // Verify that the SPI mechanism correctly discovers JdbcTableDiscovererFactory
        TableDiscoverer discoverer =
                TableDiscovererFactory.createDiscoverer(
                        "jdbc", Thread.currentThread().getContextClassLoader());
        assertThat(discoverer).isInstanceOf(JdbcTableDiscoverer.class);
    }

    @Test
    void testSpiFailsForUnknownType() {
        assertThatThrownBy(
                        () ->
                                TableDiscovererFactory.createDiscoverer(
                                        "unknown-type",
                                        Thread.currentThread().getContextClassLoader()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported 'table.discoverer.type' value: 'unknown-type'");
    }

    @Test
    void testSpiCaseInsensitive() {
        // Verify case-insensitive matching
        TableDiscoverer discoverer =
                TableDiscovererFactory.createDiscoverer(
                        "JDBC", Thread.currentThread().getContextClassLoader());
        assertThat(discoverer).isInstanceOf(JdbcTableDiscoverer.class);
    }

    // =========================================================================
    //  End-to-End Discovery Tests (SPI + MySQL read)
    // =========================================================================

    @Test
    void testDiscoverTablesWithDefaultColumn() throws Exception {
        // Load discoverer via SPI
        TableDiscoverer discoverer =
                TableDiscovererFactory.createDiscoverer(
                        "jdbc", Thread.currentThread().getContextClassLoader());

        // Build configuration
        Configuration config = buildConfig("cdc_subscriptions", null);

        // Open and discover
        discoverer.open(
                TableDiscovererFactory.createContext(
                        config, Thread.currentThread().getContextClassLoader()));
        try {
            Set<TableId> discovered = discoverer.discover();
            assertThat(discovered)
                    .containsExactlyInAnyOrder(
                            TableId.tableId("source_db", "orders"),
                            TableId.tableId("source_db", "products"),
                            TableId.tableId("analytics_db", "user_events"));
        } finally {
            discoverer.close();
        }
    }

    @Test
    void testDiscoverTablesWithCustomColumn() throws Exception {
        TableDiscoverer discoverer =
                TableDiscovererFactory.createDiscoverer(
                        "jdbc", Thread.currentThread().getContextClassLoader());

        Configuration config = buildConfig("custom_subscriptions", "table_fqn");

        discoverer.open(
                TableDiscovererFactory.createContext(
                        config, Thread.currentThread().getContextClassLoader()));
        try {
            Set<TableId> discovered = discoverer.discover();
            assertThat(discovered)
                    .containsExactlyInAnyOrder(
                            TableId.tableId("warehouse", "inventory"),
                            TableId.tableId("warehouse", "shipments"));
        } finally {
            discoverer.close();
        }
    }

    @Test
    void testDiscoverReflectsDynamicChanges() throws Exception {
        TableDiscoverer discoverer =
                TableDiscovererFactory.createDiscoverer(
                        "jdbc", Thread.currentThread().getContextClassLoader());

        Configuration config = buildConfig("cdc_subscriptions", null);
        discoverer.open(
                TableDiscovererFactory.createContext(
                        config, Thread.currentThread().getContextClassLoader()));
        try {
            // Initial discovery
            Set<TableId> initial = discoverer.discover();
            assertThat(initial).hasSize(3);

            // Dynamically add a new subscription row
            try (Connection conn =
                            DriverManager.getConnection(
                                    MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
                    Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO cdc_subscriptions VALUES ('new_db.new_table')");
            }

            // Re-discover — should include the new entry
            Set<TableId> updated = discoverer.discover();
            assertThat(updated).hasSize(4);
            assertThat(updated).contains(TableId.tableId("new_db", "new_table"));

            // Cleanup: remove the added row to not affect other tests
            try (Connection conn =
                            DriverManager.getConnection(
                                    MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
                    Statement stmt = conn.createStatement()) {
                stmt.execute(
                        "DELETE FROM cdc_subscriptions WHERE subscribe_table_name = 'new_db.new_table'");
            }
        } finally {
            discoverer.close();
        }
    }

    @Test
    void testMissingRequiredConfigThrows() {
        TableDiscoverer discoverer =
                TableDiscovererFactory.createDiscoverer(
                        "jdbc", Thread.currentThread().getContextClassLoader());

        // Missing jdbc-url
        Configuration config =
                Configuration.fromMap(
                        Map.of(
                                "table.discoverer.jdbc.table-name", "some_table",
                                "table.discoverer.jdbc.username", "user",
                                "table.discoverer.jdbc.password", "pass"));

        assertThatThrownBy(
                        () ->
                                discoverer.open(
                                        TableDiscovererFactory.createContext(
                                                config,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table.discoverer.jdbc.url");
    }

    // =========================================================================
    //  Helpers
    // =========================================================================

    private Configuration buildConfig(String tableName, String columnName) {
        Map<String, String> map = new HashMap<>();
        map.put("table.discoverer.jdbc.url", MYSQL.getJdbcUrl());
        map.put("table.discoverer.jdbc.table-name", tableName);
        map.put("table.discoverer.jdbc.username", MYSQL.getUsername());
        map.put("table.discoverer.jdbc.password", MYSQL.getPassword());
        if (columnName != null) {
            map.put("table.discoverer.jdbc.column-name", columnName);
        }
        return Configuration.fromMap(map);
    }
}
