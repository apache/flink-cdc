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
 * Uses a real MySQL container via Testcontainers to verify both the default shared-table mode and
 * the advanced custom-query escape hatch.
 */
@Testcontainers
class JdbcTableDiscovererITCase {

    private static final String SUBSCRIBE_ID_ORDERS = "orders-subscription";
    private static final String SUBSCRIBE_ID_ANALYTICS = "analytics-subscription";

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
            // Shared subscription table with default column names.
            stmt.execute(
                    "CREATE TABLE cdc_subscriptions ("
                            + "  subscribe_id VARCHAR(64) NOT NULL,"
                            + "  subscribe_table_name VARCHAR(255) NOT NULL,"
                            + "  PRIMARY KEY (subscribe_id, subscribe_table_name)"
                            + ")");
            stmt.execute(
                    "INSERT INTO cdc_subscriptions VALUES "
                            + "('orders-subscription',    'source_db.orders'),"
                            + "('orders-subscription',    'source_db.order_items'),"
                            + "('orders-subscription',    'source_db.products'),"
                            + "('analytics-subscription', 'analytics_db.user_events')");

            // Shared subscription table using non-default column names.
            stmt.execute(
                    "CREATE TABLE custom_subscriptions ("
                            + "  sub_id VARCHAR(64) NOT NULL,"
                            + "  table_fqn VARCHAR(255) NOT NULL,"
                            + "  PRIMARY KEY (sub_id, table_fqn)"
                            + ")");
            stmt.execute(
                    "INSERT INTO custom_subscriptions VALUES "
                            + "('warehouse-sub', 'warehouse.inventory'),"
                            + "('warehouse-sub', 'warehouse.shipments'),"
                            + "('hr-sub',        'hr.employees')");
        }
    }

    // =========================================================================
    //  SPI loading
    // =========================================================================

    @Test
    void testSpiLoadsJdbcFactory() {
        TableDiscoverer discoverer =
                TableDiscovererFactory.createDiscoverer(
                        "jdbc", Thread.currentThread().getContextClassLoader());
        assertThat(discoverer).isInstanceOf(JdbcTableDiscoverer.class);
    }

    @Test
    void testSpiCaseInsensitive() {
        TableDiscoverer discoverer =
                TableDiscovererFactory.createDiscoverer(
                        "JDBC", Thread.currentThread().getContextClassLoader());
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

    // =========================================================================
    //  Default mode — shared subscription table
    // =========================================================================

    @Test
    void testDefaultModeWithDefaultColumns() throws Exception {
        Configuration config =
                sharedTableConfigBuilder()
                        .table("cdc_subscriptions")
                        .subscribeId(SUBSCRIBE_ID_ORDERS)
                        .build();

        assertThat(runDiscovery(config))
                .containsExactlyInAnyOrder(
                        TableId.tableId("source_db", "orders"),
                        TableId.tableId("source_db", "order_items"),
                        TableId.tableId("source_db", "products"));
    }

    @Test
    void testDefaultModeFiltersBySubscribeId() throws Exception {
        Configuration config =
                sharedTableConfigBuilder()
                        .table("cdc_subscriptions")
                        .subscribeId(SUBSCRIBE_ID_ANALYTICS)
                        .build();

        assertThat(runDiscovery(config))
                .containsExactlyInAnyOrder(TableId.tableId("analytics_db", "user_events"));
    }

    @Test
    void testDefaultModeWithCustomColumns() throws Exception {
        Configuration config =
                sharedTableConfigBuilder()
                        .table("custom_subscriptions")
                        .columnName("table_fqn")
                        .subscribeIdColumn("sub_id")
                        .subscribeId("warehouse-sub")
                        .build();

        assertThat(runDiscovery(config))
                .containsExactlyInAnyOrder(
                        TableId.tableId("warehouse", "inventory"),
                        TableId.tableId("warehouse", "shipments"));
    }

    @Test
    void testDefaultModeReflectsDynamicChanges() throws Exception {
        Configuration config =
                sharedTableConfigBuilder()
                        .table("cdc_subscriptions")
                        .subscribeId(SUBSCRIBE_ID_ANALYTICS)
                        .build();

        TableDiscoverer discoverer =
                TableDiscovererFactory.createDiscoverer(
                        "jdbc", Thread.currentThread().getContextClassLoader());
        discoverer.open(
                TableDiscovererFactory.createContext(
                        config, Thread.currentThread().getContextClassLoader()));
        try {
            assertThat(discoverer.discover()).hasSize(1);

            executeSql(
                    "INSERT INTO cdc_subscriptions VALUES "
                            + "('analytics-subscription', 'analytics_db.sessions')");
            try {
                Set<TableId> updated = discoverer.discover();
                assertThat(updated)
                        .containsExactlyInAnyOrder(
                                TableId.tableId("analytics_db", "user_events"),
                                TableId.tableId("analytics_db", "sessions"));
            } finally {
                executeSql(
                        "DELETE FROM cdc_subscriptions WHERE subscribe_table_name "
                                + "= 'analytics_db.sessions'");
            }
        } finally {
            discoverer.close();
        }
    }

    // =========================================================================
    //  Advanced escape hatch — custom query
    // =========================================================================

    @Test
    void testCustomQueryReadsFirstColumn() throws Exception {
        Map<String, String> map = baseConnectionConfig();
        map.put(
                "table.discoverer.jdbc.subscribe-query",
                "SELECT subscribe_table_name FROM cdc_subscriptions "
                        + "WHERE subscribe_id = 'orders-subscription' "
                        + "ORDER BY subscribe_table_name");

        assertThat(runDiscovery(Configuration.fromMap(map)))
                .containsExactlyInAnyOrder(
                        TableId.tableId("source_db", "order_items"),
                        TableId.tableId("source_db", "orders"),
                        TableId.tableId("source_db", "products"));
    }

    @Test
    void testCustomQueryOverridesDefaultModeOptions() throws Exception {
        // Mix both modes: the custom-query option must win and silently ignore the default-mode
        // options below (table-name / subscribe-id).
        Map<String, String> map = baseConnectionConfig();
        map.put("table.discoverer.jdbc.table-name", "cdc_subscriptions");
        map.put("table.discoverer.jdbc.subscribe-id", SUBSCRIBE_ID_ANALYTICS);
        map.put(
                "table.discoverer.jdbc.subscribe-query",
                "SELECT subscribe_table_name FROM cdc_subscriptions "
                        + "WHERE subscribe_id = 'orders-subscription'");

        assertThat(runDiscovery(Configuration.fromMap(map)))
                .containsExactlyInAnyOrder(
                        TableId.tableId("source_db", "orders"),
                        TableId.tableId("source_db", "order_items"),
                        TableId.tableId("source_db", "products"));
    }

    // =========================================================================
    //  Validation
    // =========================================================================

    @Test
    void testMissingJdbcUrlThrows() {
        Map<String, String> map = new HashMap<>();
        map.put("table.discoverer.jdbc.username", "user");
        map.put("table.discoverer.jdbc.password", "pass");
        map.put("table.discoverer.jdbc.table-name", "cdc_subscriptions");
        map.put("table.discoverer.jdbc.subscribe-id", "x");

        assertThatThrownBy(() -> runDiscovery(Configuration.fromMap(map)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table.discoverer.jdbc.url");
    }

    @Test
    void testDefaultModeMissingTableNameThrows() {
        Map<String, String> map = baseConnectionConfig();
        map.put("table.discoverer.jdbc.subscribe-id", SUBSCRIBE_ID_ORDERS);

        assertThatThrownBy(() -> runDiscovery(Configuration.fromMap(map)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table.discoverer.jdbc.table-name");
    }

    @Test
    void testDefaultModeMissingSubscribeIdThrows() {
        Map<String, String> map = baseConnectionConfig();
        map.put("table.discoverer.jdbc.table-name", "cdc_subscriptions");

        assertThatThrownBy(() -> runDiscovery(Configuration.fromMap(map)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table.discoverer.jdbc.subscribe-id");
    }

    // =========================================================================
    //  Helpers
    // =========================================================================

    private Set<TableId> runDiscovery(Configuration config) throws Exception {
        TableDiscoverer discoverer =
                TableDiscovererFactory.createDiscoverer(
                        "jdbc", Thread.currentThread().getContextClassLoader());
        discoverer.open(
                TableDiscovererFactory.createContext(
                        config, Thread.currentThread().getContextClassLoader()));
        try {
            return discoverer.discover();
        } finally {
            discoverer.close();
        }
    }

    private static Map<String, String> baseConnectionConfig() {
        Map<String, String> map = new HashMap<>();
        map.put("table.discoverer.jdbc.url", MYSQL.getJdbcUrl());
        map.put("table.discoverer.jdbc.username", MYSQL.getUsername());
        map.put("table.discoverer.jdbc.password", MYSQL.getPassword());
        return map;
    }

    private static void executeSql(String sql) throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private SharedTableConfigBuilder sharedTableConfigBuilder() {
        return new SharedTableConfigBuilder();
    }

    private static final class SharedTableConfigBuilder {
        private String table;
        private String columnName;
        private String subscribeIdColumn;
        private String subscribeId;

        SharedTableConfigBuilder table(String table) {
            this.table = table;
            return this;
        }

        SharedTableConfigBuilder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        SharedTableConfigBuilder subscribeIdColumn(String subscribeIdColumn) {
            this.subscribeIdColumn = subscribeIdColumn;
            return this;
        }

        SharedTableConfigBuilder subscribeId(String subscribeId) {
            this.subscribeId = subscribeId;
            return this;
        }

        Configuration build() {
            Map<String, String> map = baseConnectionConfig();
            map.put("table.discoverer.jdbc.table-name", table);
            map.put("table.discoverer.jdbc.subscribe-id", subscribeId);
            if (columnName != null) {
                map.put("table.discoverer.jdbc.column-name", columnName);
            }
            if (subscribeIdColumn != null) {
                map.put("table.discoverer.jdbc.subscribe-id-column", subscribeIdColumn);
            }
            return Configuration.fromMap(map);
        }
    }
}
