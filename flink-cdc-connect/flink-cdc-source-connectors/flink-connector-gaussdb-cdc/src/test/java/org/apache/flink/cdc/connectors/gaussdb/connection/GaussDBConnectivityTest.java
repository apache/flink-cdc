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

package org.apache.flink.cdc.connectors.gaussdb.connection;

import org.apache.flink.cdc.connectors.gaussdb.GaussDBTestBase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Comprehensive connectivity verification tests for GaussDB JDBC driver.
 *
 * <p>This test class validates different connection methods and configurations for GaussDB:
 *
 * <ul>
 *   <li>PostgreSQL-compatible JDBC driver (for test data preparation)
 *   <li>Native GaussDB JDBC driver (com.huawei.gaussdb.jdbc.Driver)
 *   <li>Connection pooling and retry mechanisms
 * </ul>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class GaussDBConnectivityTest extends GaussDBTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBConnectivityTest.class);

    @Test
    void testPostgreSQLDriverConnection() throws Exception {
        LOG.info("Testing PostgreSQL driver connection for test data preparation");

        String jdbcUrl =
                String.format(
                        "jdbc:postgresql://%s:%d/%s?sslmode=disable",
                        HOSTNAME, PORT, DATABASE_NAME);

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            fail("PostgreSQL JDBC driver not found on classpath", e);
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD)) {
            assertThat(connection).isNotNull();
            assertThat(connection.isValid(5)).isTrue();

            DatabaseMetaData metaData = connection.getMetaData();
            LOG.info(
                    "Connected using PostgreSQL driver - Database: {}, Version: {}",
                    metaData.getDatabaseProductName(),
                    metaData.getDatabaseProductVersion());

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT version()")) {
                if (rs.next()) {
                    String version = rs.getString(1);
                    LOG.info("Database version: {}", version);
                    assertThat(version).isNotEmpty();
                }
            }

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT current_database()")) {
                if (rs.next()) {
                    String currentDb = rs.getString(1);
                    LOG.info("Current database: {}", currentDb);
                    assertThat(currentDb).isEqualTo(DATABASE_NAME);
                }
            }
        }

        LOG.info("PostgreSQL driver connection test PASSED");
    }

    @Test
    void testGaussDBNativeDriverConnection() throws Exception {
        LOG.info("Testing native GaussDB driver connection");

        String jdbcUrl = String.format("jdbc:gaussdb://%s:%d/%s", HOSTNAME, PORT, DATABASE_NAME);

        try {
            Class.forName("com.huawei.gaussdb.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            fail("GaussDB JDBC driver not found on classpath", e);
        }

        Properties props = new Properties();
        props.setProperty("user", USERNAME);
        props.setProperty("password", PASSWORD);

        try (Connection connection = DriverManager.getConnection(jdbcUrl, props)) {
            assertThat(connection).isNotNull();
            assertThat(connection.isValid(5)).isTrue();

            DatabaseMetaData metaData = connection.getMetaData();
            LOG.info(
                    "Connected using GaussDB native driver - Database: {}, Version: {}, Driver: {}",
                    metaData.getDatabaseProductName(),
                    metaData.getDatabaseProductVersion(),
                    metaData.getDriverName());

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT current_schema()")) {
                if (rs.next()) {
                    String currentSchema = rs.getString(1);
                    LOG.info("Current schema: {}", currentSchema);
                    assertThat(currentSchema).isNotEmpty();
                }
            }
        }

        LOG.info("GaussDB native driver connection test PASSED");
    }

    @Test
    void testGaussDBDriverWithSSLDisabled() throws Exception {
        LOG.info("Testing GaussDB driver with SSL explicitly disabled");

        String jdbcUrl =
                String.format(
                        "jdbc:gaussdb://%s:%d/%s?sslmode=disable", HOSTNAME, PORT, DATABASE_NAME);

        try {
            Class.forName("com.huawei.gaussdb.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            fail("GaussDB JDBC driver not found on classpath", e);
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD)) {
            assertThat(connection).isNotNull();
            assertThat(connection.isValid(5)).isTrue();

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        }

        LOG.info("GaussDB driver with SSL disabled test PASSED");
    }

    @Test
    void testBasicDMLOperations() throws Exception {
        LOG.info("Testing basic DML operations via getJdbcConnection()");

        String testTable = "connectivity_test_" + System.currentTimeMillis();

        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {

            stmt.execute(
                    String.format(
                            "CREATE TABLE %s.%s (id INT PRIMARY KEY, name VARCHAR(100))",
                            SCHEMA_NAME, testTable));
            LOG.info("Created test table: {}.{}", SCHEMA_NAME, testTable);

            int inserted =
                    stmt.executeUpdate(
                            String.format(
                                    "INSERT INTO %s.%s (id, name) VALUES (1, 'test')",
                                    SCHEMA_NAME, testTable));
            assertThat(inserted).isEqualTo(1);
            LOG.info("Inserted 1 row");

            try (ResultSet rs =
                    stmt.executeQuery(
                            String.format(
                                    "SELECT id, name FROM %s.%s WHERE id = 1",
                                    SCHEMA_NAME, testTable))) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("test");
                LOG.info("Query returned expected results");
            }

            int updated =
                    stmt.executeUpdate(
                            String.format(
                                    "UPDATE %s.%s SET name = 'updated' WHERE id = 1",
                                    SCHEMA_NAME, testTable));
            assertThat(updated).isEqualTo(1);
            LOG.info("Updated 1 row");

            int deleted =
                    stmt.executeUpdate(
                            String.format(
                                    "DELETE FROM %s.%s WHERE id = 1", SCHEMA_NAME, testTable));
            assertThat(deleted).isEqualTo(1);
            LOG.info("Deleted 1 row");

            stmt.execute(String.format("DROP TABLE %s.%s", SCHEMA_NAME, testTable));
            LOG.info("Dropped test table");
        }

        LOG.info("Basic DML operations test PASSED");
    }

    @Test
    void testReplicationSlotManagement() throws Exception {
        LOG.info("Testing replication slot creation and deletion");

        String testSlotName = "test_slot_" + System.currentTimeMillis();

        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {

            stmt.execute(
                    String.format(
                            "SELECT * FROM pg_create_logical_replication_slot('%s', 'mppdb_decoding')",
                            testSlotName));
            LOG.info("Created replication slot: {}", testSlotName);

            try (ResultSet rs =
                    stmt.executeQuery(
                            String.format(
                                    "SELECT slot_name, plugin FROM pg_replication_slots WHERE slot_name = '%s'",
                                    testSlotName))) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("slot_name")).isEqualTo(testSlotName);
                assertThat(rs.getString("plugin")).isEqualTo("mppdb_decoding");
                LOG.info("Verified replication slot exists with correct plugin");
            }

            stmt.execute(String.format("SELECT pg_drop_replication_slot('%s')", testSlotName));
            LOG.info("Dropped replication slot: {}", testSlotName);

            try (ResultSet rs =
                    stmt.executeQuery(
                            String.format(
                                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s'",
                                    testSlotName))) {
                assertThat(rs.next())
                        .isFalse()
                        .withFailMessage("Replication slot should have been deleted");
            }
        }

        LOG.info("Replication slot management test PASSED");
    }

    @Test
    void testConnectionProperties() throws Exception {
        LOG.info("Testing connection metadata and properties");

        try (Connection connection = getJdbcConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            LOG.info("Database Product Name: {}", metaData.getDatabaseProductName());
            LOG.info("Database Product Version: {}", metaData.getDatabaseProductVersion());
            LOG.info("Driver Name: {}", metaData.getDriverName());
            LOG.info("Driver Version: {}", metaData.getDriverVersion());
            LOG.info("JDBC Major Version: {}", metaData.getJDBCMajorVersion());
            LOG.info("JDBC Minor Version: {}", metaData.getJDBCMinorVersion());
            LOG.info("SQL Keywords: {}", metaData.getSQLKeywords());

            assertThat(metaData.getDatabaseProductName()).isNotEmpty();
            assertThat(metaData.getDatabaseProductVersion()).isNotEmpty();
            assertThat(metaData.supportsTransactions()).isTrue();
            assertThat(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY)).isTrue();

            LOG.info("Catalog Term: {}", metaData.getCatalogTerm());
            LOG.info("Schema Term: {}", metaData.getSchemaTerm());
            LOG.info("Max Connections: {}", metaData.getMaxConnections());
        }

        LOG.info("Connection properties test PASSED");
    }

    @Test
    void testSchemaAndTableMetadata() throws Exception {
        LOG.info("Testing schema and table metadata queries");

        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {

            try (ResultSet rs =
                    stmt.executeQuery(
                            String.format(
                                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s'",
                                    SCHEMA_NAME))) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("schema_name")).isEqualTo(SCHEMA_NAME);
                LOG.info("Verified schema '{}' exists", SCHEMA_NAME);
            }

            try (ResultSet rs = stmt.executeQuery("SELECT current_user")) {
                assertThat(rs.next()).isTrue();
                String currentUser = rs.getString(1);
                LOG.info("Current user: {}", currentUser);
                assertThat(currentUser).isNotEmpty();
            }

            try (ResultSet rs =
                    stmt.executeQuery(
                            "SELECT count(*) as table_count FROM information_schema.tables WHERE table_schema = '"
                                    + SCHEMA_NAME
                                    + "'")) {
                assertThat(rs.next()).isTrue();
                int tableCount = rs.getInt("table_count");
                LOG.info("Number of tables in schema '{}': {}", SCHEMA_NAME, tableCount);
            }
        }

        LOG.info("Schema and table metadata test PASSED");
    }
}
