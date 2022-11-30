/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.sqlserver;

import org.apache.flink.test.util.AbstractTestBase;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;

/** Utility class for sqlserver tests. */
public class SqlServerTestBase extends AbstractTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerTestBase.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private static final String STATEMENTS_PLACEHOLDER = "#";

    private static final String DISABLE_DB_CDC =
            "IF EXISTS(select 1 from sys.databases where name='#' AND is_cdc_enabled=1)\n"
                    + "EXEC sys.sp_cdc_disable_db";

    public static final MSSQLServerContainer MSSQL_SERVER_CONTAINER =
            new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2019-latest")
                    .withPassword("Password!")
                    .withEnv("MSSQL_AGENT_ENABLED", "true")
                    .withEnv("MSSQL_PID", "Standard")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MSSQL_SERVER_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MSSQL_SERVER_CONTAINER.getJdbcUrl(),
                MSSQL_SERVER_CONTAINER.getUsername(),
                MSSQL_SERVER_CONTAINER.getPassword());
    }

    private static void dropTestDatabase(Connection connection, String databaseName)
            throws SQLException {
        try {
            Awaitility.await("Disabling CDC")
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    connection
                                            .createStatement()
                                            .execute(String.format("USE [%s]", databaseName));
                                } catch (SQLException e) {
                                    // if the database doesn't yet exist, there is no need to
                                    // disable CDC
                                    return true;
                                }
                                try {
                                    disableDbCdc(connection, databaseName);
                                    return true;
                                } catch (SQLException e) {
                                    return false;
                                }
                            });
        } catch (ConditionTimeoutException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to disable CDC on %s", databaseName), e);
        }

        connection.createStatement().execute("USE master");

        try {
            Awaitility.await(String.format("Dropping database %s", databaseName))
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    String sql =
                                            String.format(
                                                    "IF EXISTS(select 1 from sys.databases where name = '%s') DROP DATABASE [%s]",
                                                    databaseName, databaseName);
                                    connection.createStatement().execute(sql);
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn(
                                            String.format(
                                                    "DROP DATABASE %s failed (will be retried): {}",
                                                    databaseName),
                                            e.getMessage());
                                    try {
                                        connection
                                                .createStatement()
                                                .execute(
                                                        String.format(
                                                                "ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;",
                                                                databaseName));
                                    } catch (SQLException e2) {
                                        LOG.error("Failed to rollbackimmediately", e2);
                                    }
                                    return false;
                                }
                            });
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Failed to drop test database", e);
        }
    }

    /**
     * Disables CDC for a given database, if not already disabled.
     *
     * @param name the name of the DB, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    protected static void disableDbCdc(Connection connection, String name) throws SQLException {
        Objects.requireNonNull(name);
        connection.createStatement().execute(DISABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, name));
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializeSqlServerTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = SqlServerTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            dropTestDatabase(connection, sqlFile);
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
