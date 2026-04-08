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

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.sqlserver.factory.SqlServerDataSourceFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Test cases for matching SQL Server source tables. */
public class SqlServerTablePatternMatchingTest extends SqlServerTestBase {

    private static final String DATABASE_NAME = "pattern_test";

    @BeforeEach
    public void before() {
        initializePatternTestDatabase();
    }

    private void initializePatternTestDatabase() {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            // Drop database if exists
            statement.execute(
                    String.format(
                            "IF EXISTS(select 1 from sys.databases where name = '%s') "
                                    + "BEGIN ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                                    + "DROP DATABASE [%s]; END",
                            DATABASE_NAME, DATABASE_NAME, DATABASE_NAME));

            // Create database
            statement.execute(String.format("CREATE DATABASE %s;", DATABASE_NAME));
            statement.execute(String.format("USE %s;", DATABASE_NAME));

            // Wait for SQL Server Agent
            statement.execute("WAITFOR DELAY '00:00:03';");
            statement.execute("EXEC sys.sp_cdc_enable_db;");

            // Create test tables
            String[] tableNames = {"tbl1", "tbl2", "tbl3", "tbl_special"};
            for (String tableName : tableNames) {
                statement.execute(
                        String.format(
                                "CREATE TABLE dbo.%s (id INT NOT NULL PRIMARY KEY);", tableName));
                statement.execute(String.format("INSERT INTO dbo.%s VALUES (1);", tableName));
                statement.execute(
                        String.format(
                                "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', "
                                        + "@source_name = '%s', @role_name = NULL, @supports_net_changes = 0;",
                                tableName));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize pattern test database", e);
        }
    }

    @Test
    public void testWildcardMatchingAllTables() {
        List<String> tables = testTableMatching(DATABASE_NAME + ".dbo.\\.*", null);
        assertThat(tables)
                .containsExactlyInAnyOrder("dbo.tbl1", "dbo.tbl2", "dbo.tbl3", "dbo.tbl_special");
    }

    @Test
    public void testWildcardMatchingPartialTables() {
        List<String> tables = testTableMatching(DATABASE_NAME + ".dbo.tbl[1-2]", null);
        assertThat(tables).containsExactlyInAnyOrder("dbo.tbl1", "dbo.tbl2");
    }

    @Test
    public void testExactTableMatching() {
        List<String> tables = testTableMatching(DATABASE_NAME + ".dbo.tbl1", null);
        assertThat(tables).containsExactly("dbo.tbl1");
    }

    @Test
    public void testMultipleTableMatching() {
        List<String> tables =
                testTableMatching(DATABASE_NAME + ".dbo.tbl1," + DATABASE_NAME + ".dbo.tbl3", null);
        assertThat(tables).containsExactlyInAnyOrder("dbo.tbl1", "dbo.tbl3");
    }

    @Test
    public void testWildcardMatchingWithExclusion() {
        List<String> tables =
                testTableMatching(DATABASE_NAME + ".dbo.\\.*", DATABASE_NAME + ".dbo.tbl1");
        assertThat(tables).containsExactlyInAnyOrder("dbo.tbl2", "dbo.tbl3", "dbo.tbl_special");
    }

    @Test
    public void testWildcardMatchingWithPatternExclusion() {
        List<String> tables =
                testTableMatching(DATABASE_NAME + ".dbo.\\.*", DATABASE_NAME + ".dbo.tbl[1-2]");
        assertThat(tables).containsExactlyInAnyOrder("dbo.tbl3", "dbo.tbl_special");
    }

    @Test
    public void testWildcardMatchingWithMultipleExclusions() {
        List<String> tables =
                testTableMatching(
                        DATABASE_NAME + ".dbo.\\.*",
                        DATABASE_NAME + ".dbo.tbl1," + DATABASE_NAME + ".dbo.tbl_special");
        assertThat(tables).containsExactlyInAnyOrder("dbo.tbl2", "dbo.tbl3");
    }

    @Test
    public void testPatternMatchingWithUnderscore() {
        List<String> tables = testTableMatching(DATABASE_NAME + ".dbo.tbl_\\.*", null);
        assertThat(tables).containsExactly("dbo.tbl_special");
    }

    @Test
    public void testMatchingWithSpacedRules() {
        // Test with spaces around commas
        List<String> tables =
                testTableMatching(
                        DATABASE_NAME + ".dbo.tbl1 , " + DATABASE_NAME + ".dbo.tbl2", null);
        assertThat(tables).containsExactlyInAnyOrder("dbo.tbl1", "dbo.tbl2");
    }

    private List<String> testTableMatching(String tablesConfig, @Nullable String tablesExclude) {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MSSQL_SERVER_CONTAINER.getHost());
        options.put(
                PORT.key(),
                String.valueOf(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT)));
        options.put(USERNAME.key(), MSSQL_SERVER_CONTAINER.getUsername());
        options.put(PASSWORD.key(), MSSQL_SERVER_CONTAINER.getPassword());
        options.put(TABLES.key(), tablesConfig);
        if (tablesExclude != null) {
            options.put(TABLES_EXCLUDE.key(), tablesExclude);
        }

        Factory.Context context = new MockContext(Configuration.fromMap(options));
        SqlServerDataSourceFactory factory = new SqlServerDataSourceFactory();
        SqlServerDataSource dataSource = (SqlServerDataSource) factory.createDataSource(context);
        return dataSource.getSqlServerSourceConfig().getTableList();
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
