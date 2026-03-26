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

package org.apache.flink.cdc.connectors.dws.utils;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.dws.factory.DwsDataSinkFactory;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** Base class for GaussDB DWS sink integration tests. */
public class DwsSinkTestBase extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(DwsSinkTestBase.class);

    protected static final int DEFAULT_PARALLELISM = 1;
    protected static final DwsContainer DWS_CONTAINER = createDwsContainer();

    private static DwsContainer createDwsContainer() {
        return new DwsContainer();
    }

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(DWS_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        DWS_CONTAINER.stop();
        LOG.info("Containers are stopped.");
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
            return Configuration.fromMap(Collections.singletonMap("local-time-zone", "UTC"));
        }

        @Override
        public ClassLoader getClassLoader() {
            return Thread.currentThread().getContextClassLoader();
        }
    }

    public static DataSink createDwsDataSink(Configuration factoryConfiguration) {
        DwsDataSinkFactory factory = new DwsDataSinkFactory();
        return factory.createDataSink(new MockContext(factoryConfiguration));
    }

    public static void createDatabase(String databaseName) {
        try {
            LOG.info("Attempting to create database: {}", databaseName);
            LOG.info("Container JDBC URL: {}", DWS_CONTAINER.getJdbcUrl(databaseName));
            LOG.info("Container Username: {}", DWS_CONTAINER.getUsername());
            LOG.info("Container Password: {}", DWS_CONTAINER.getPassword());

            try (Connection conn = createDatabaseConnection(DwsContainer.DWS_DATABASE);
                    java.sql.Statement stmt = conn.createStatement()) {

                String sql = String.format("CREATE DATABASE %s", databaseName);
                stmt.execute(sql);
                LOG.info("Database {} created successfully", databaseName);

            } catch (SQLException e) {
                if (e.getMessage().contains("already exists")) {
                    LOG.info("Database {} already exists, skipping creation", databaseName);
                } else {
                    throw new RuntimeException(
                            String.format("Failed to create database %s", databaseName), e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTable(
            String databaseName, String tableName, String primaryKey, List<String> schema) {
        try {
            try (Connection conn = createDatabaseConnection(databaseName);
                    java.sql.Statement stmt = conn.createStatement()) {

                String sql =
                        String.format(
                                "CREATE TABLE %s (%s, PRIMARY KEY (%s))",
                                tableName, String.join(", ", schema), primaryKey);
                stmt.execute(sql);
                LOG.info("Table {}.{} created successfully", databaseName, tableName);

            } catch (SQLException e) {
                if (e.getMessage().contains("already exists")) {
                    LOG.info(
                            "Table {}.{} already exists, skipping creation",
                            databaseName,
                            tableName);
                } else {
                    throw new RuntimeException("Failed to create table: " + e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create table", e);
        }
    }

    public static void dropDatabase(String databaseName) {
        try {
            try (Connection conn = createDatabaseConnection(DwsContainer.DWS_DATABASE);
                    java.sql.Statement stmt = conn.createStatement()) {

                String sql = String.format("DROP DATABASE IF EXISTS %s", databaseName);
                stmt.execute(sql);
                LOG.info("Database {} dropped successfully", databaseName);

            } catch (SQLException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static void dropTable(String databaseName, String tableName) {
        try {
            try (Connection conn = createDatabaseConnection(databaseName);
                    java.sql.Statement stmt = conn.createStatement()) {

                String sql = String.format("DROP TABLE IF EXISTS %s", tableName);
                stmt.execute(sql);
                LOG.info("Table {}.{} dropped successfully", databaseName, tableName);

            } catch (SQLException e) {
                LOG.warn("Failed to drop table {}.{}: {}", databaseName, tableName, e.getMessage());
            }
        } catch (Exception e) {
            LOG.warn("Failed to drop table using JDBC: {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    public List<String> inspectTableSchema(TableId tableId) throws SQLException {
        List<String> results = new ArrayList<>();
        try (Connection conn = createConnection(tableId);
                java.sql.Statement statement = conn.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT column_name, data_type, is_nullable, column_default "
                                                + "FROM information_schema.columns "
                                                + "WHERE table_schema='%s' AND table_name='%s' "
                                                + "ORDER BY ordinal_position",
                                        tableId.getSchemaName(), tableId.getTableName()))) {
            while (rs.next()) {
                List<String> columns = new ArrayList<>();
                for (int i = 1; i <= 4; i++) {
                    columns.add(rs.getString(i));
                }
                results.add(String.join(" | ", columns));
            }
        }
        return results;
    }

    public boolean tableExists(TableId tableId) throws SQLException {
        try (Connection conn = createConnection(tableId);
                java.sql.Statement statement = conn.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT COUNT(*) FROM information_schema.tables "
                                                + "WHERE table_schema='%s' AND table_name='%s'",
                                        tableId.getSchemaName(), tableId.getTableName()))) {
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    public List<String> fetchTableContent(TableId tableId, int columnCount) throws SQLException {
        List<String> results = new ArrayList<>();
        try (Connection conn = createConnection(tableId);
                java.sql.Statement statement = conn.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT * FROM %s.%s",
                                        tableId.getSchemaName(), tableId.getTableName()))) {
            while (rs.next()) {
                List<String> columns = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    columns.add(rs.getString(i));
                }
                results.add(String.join(" | ", columns));
            }
        }
        return results;
    }

    public static void createSchema(String databaseName, String schemaName) {
        try (Connection conn = createDatabaseConnection(databaseName);
                java.sql.Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("CREATE SCHEMA %s", schemaName));
        } catch (SQLException e) {
            if (e.getMessage().contains("already exists")) {
                LOG.info(
                        "Schema {} already exists in database {}, skipping creation",
                        schemaName,
                        databaseName);
                return;
            }
            throw new RuntimeException(
                    String.format(
                            "Failed to create schema %s in database %s", schemaName, databaseName),
                    e);
        }
    }

    public static void dropTable(TableId tableId) {
        try (Connection conn = createConnection(tableId);
                java.sql.Statement stmt = conn.createStatement()) {
            stmt.execute(
                    String.format(
                            "DROP TABLE IF EXISTS %s.%s",
                            tableId.getSchemaName(), tableId.getTableName()));
        } catch (SQLException e) {
            LOG.warn("Failed to drop table {}: {}", tableId, e.getMessage());
        }
    }

    protected static Connection createConnection(TableId tableId) throws SQLException {
        return createDatabaseConnection(resolveDatabase(tableId));
    }

    protected static Connection createDatabaseConnection(String databaseName) throws SQLException {
        return DriverManager.getConnection(
                DWS_CONTAINER.getJdbcUrl(databaseName),
                DWS_CONTAINER.getUsername(),
                DWS_CONTAINER.getPassword());
    }

    protected static String resolveDatabase(TableId tableId) {
        return tableId.getNamespace() == null || tableId.getNamespace().trim().isEmpty()
                ? DwsContainer.DWS_DATABASE
                : tableId.getNamespace();
    }

    public static <T> void assertEqualsInAnyOrder(List<T> expected, List<T> actual) {
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    public static <T> void assertEqualsInOrder(List<T> expected, List<T> actual) {
        Assertions.assertThat(actual).containsExactlyElementsOf(expected);
    }

    public static <K, V> void assertMapEquals(Map<K, V> expected, Map<K, V> actual) {
        Assertions.assertThat(actual).containsExactlyEntriesOf(expected);
    }
}
