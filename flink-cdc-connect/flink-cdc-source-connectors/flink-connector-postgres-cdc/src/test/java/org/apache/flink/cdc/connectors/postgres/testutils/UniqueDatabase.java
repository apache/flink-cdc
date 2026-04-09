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

package org.apache.flink.cdc.connectors.postgres.testutils;

import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;

import org.assertj.core.api.Assertions;
import org.testcontainers.containers.PostgreSQLContainer;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Create and populate a unique instance of a PostgreSQL database for each run of JUnit test. A user
 * of class needs to provide a logical name for Debezium and database name. It is expected that
 * there is a init file in <code>src/test/resources/ddl/&lt;database_name&gt;.sql</code>. The
 * database name is enriched with a unique suffix that guarantees complete isolation between runs
 * <code>
 * &lt;database_name&gt_&lt;suffix&gt</code>
 *
 * <p>This class is inspired from Debezium project.
 */
public class UniqueDatabase {
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private static final String DROP_DATABASE_DDL = "DROP SCHEMA IF EXISTS $DBNAME$;";

    private final PostgreSQLContainer container;
    private final String databaseName;

    private final String schemaName;
    private final String templateName;
    private final String username;
    private final String password;

    public UniqueDatabase(
            PostgreSQLContainer container,
            String databaseName,
            String schemaName,
            String username,
            String password) {
        this(
                container,
                databaseName,
                schemaName,
                Integer.toUnsignedString(new Random().nextInt(), 36),
                username,
                password);
    }

    private UniqueDatabase(
            PostgreSQLContainer container,
            String databaseName,
            String schemaName,
            final String identifier,
            String username,
            String password) {
        this.container = container;
        this.databaseName = databaseName + "_" + identifier;
        this.schemaName = schemaName;
        this.templateName = schemaName;
        this.username = username;
        this.password = password;
    }

    public String getHost() {
        return container.getHost();
    }

    public int getDatabasePort() {
        return container.getMappedPort(5432);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    private void createDatabase(String databaseName) throws SQLException {
        try (Connection connection =
                PostgresTestBase.getJdbcConnection(container, PostgresTestBase.DEFAULT_DB)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE DATABASE " + databaseName);
            }
        }
    }

    /** Creates the database and populates it with initialization SQL script. */
    public void createAndInitialize() {
        final String ddlFile = String.format("ddl/%s.sql", templateName);
        final URL ddlTestFile = UniqueDatabase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();

        try {
            createDatabase(databaseName);
            try (Connection connection =
                            PostgresTestBase.getJdbcConnection(container, databaseName);
                    Statement statement = connection.createStatement()) {
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
                                                .map(this::convertSQL)
                                                .collect(Collectors.joining("\n"))
                                                .split(";"))
                                .map(x -> x.replace("$$", ";"))
                                .collect(Collectors.toList());
                for (String stmt : statements) {
                    statement.execute(stmt);
                }

                // run an analyze to collect the statics about tables, used in estimating
                // row count in chunk splitter (for auto-vacuum tables, we don't need to do it)
                statement.execute("analyze");
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Drop the database if it is existing. */
    public void dropDatabase() {
        try {
            try (Connection connection =
                            PostgresTestBase.getJdbcConnection(container, databaseName);
                    Statement statement = connection.createStatement()) {
                final String dropDatabaseStatement = convertSQL(DROP_DATABASE_DDL);
                statement.execute(dropDatabaseStatement);
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Drop slot from database. */
    public boolean removeSlot(String slotName) {
        String sql = String.format("SELECT pg_drop_replication_slot('%s')", slotName);
        try (Connection connection = PostgresTestBase.getJdbcConnection(container, databaseName);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
            return true;
        } catch (Exception exception) {
            return false;
        }
    }

    /** Drop slot from database. */
    public String checkSlot(String slotName) {
        String sql =
                String.format(
                        "SELECT slot_name from pg_replication_slots where slot_name = '%s'",
                        slotName);
        try (Connection connection = PostgresTestBase.getJdbcConnection(container, databaseName);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("slot_name");
                }
                return String.format("Replication slot \"%s\" does not exist", slotName);
            }
        } catch (Exception exception) {
            return exception.getMessage();
        }
    }

    private String convertSQL(final String sql) {
        return sql.replace("$DBNAME$", schemaName);
    }
}
