/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.base.testutils;

import com.ververica.cdc.connectors.utils.TestEnvUtils;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;

/**
 * Create and populate a unique instance of a MySQL database for each run of JUnit test. A user of
 * class needs to provide a logical name for Debezium and database name. It is expected that there
 * is a init file in <code>src/test/resources/ddl/&lt;database_name&gt;.sql</code>. The database
 * name is enriched with a unique suffix that guarantees complete isolation between runs <code>
 * &lt;database_name&gt_&lt;suffix&gt</code>
 *
 * <p>This class is inspired from Debezium project.
 */
public class UniqueDatabase {

    private static final String[] CREATE_DATABASE_DDL =
            new String[] {"CREATE DATABASE $DBNAME$;", "USE $DBNAME$;"};
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    private final MySqlContainer container;
    private final String databaseName;
    private final String templateName;

    public UniqueDatabase(MySqlContainer container, String databaseName) {
        this(container, databaseName, Integer.toUnsignedString(new Random().nextInt(), 36));
    }

    private UniqueDatabase(MySqlContainer container, String databaseName, final String identifier) {
        this.container = container;
        this.databaseName = databaseName + "_" + identifier;
        this.templateName = databaseName;
    }

    public String getHost() {
        if (TestEnvUtils.useContainer()) {
            return container.getHost();
        }
        return TestEnvUtils.getHost();
    }

    public int getDatabasePort() {
        if (TestEnvUtils.useContainer()) {
            return container.getDatabasePort();
        }
        return TestEnvUtils.getPort();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getUsername() {
        if (TestEnvUtils.useContainer()) {
            return TEST_USER;
        }
        return TestEnvUtils.getUser();
    }

    public String getPassword() {
        if (TestEnvUtils.useContainer()) {
            return TEST_PASSWORD;
        }
        return TestEnvUtils.getPassword();
    }

    protected String getJdbcUrl(String databaseName) {
        if (TestEnvUtils.useContainer()) {
            return container.getJdbcUrl(databaseName);
        }
        return "jdbc:mysql://" + getHost() + ":" + getDatabasePort() + "/" + databaseName;
    }

    /** @return Fully qualified table name <code>&lt;databaseName&gt;.&lt;tableName&gt;</code> */
    public String qualifiedTableName(final String tableName) {
        return String.format("%s.%s", databaseName, tableName);
    }

    /** Creates the database and populates it with initialization SQL script. */
    public void createAndInitialize() {
        final String ddlFile = String.format("ddl/%s.sql", templateName);
        final URL ddlTestFile = UniqueDatabase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try {
            try (Connection connection =
                            DriverManager.getConnection(
                                    getJdbcUrl(""), getUsername(), getPassword());
                    Statement statement = connection.createStatement()) {
                final List<String> statements =
                        Arrays.stream(
                                        Stream.concat(
                                                        Arrays.stream(CREATE_DATABASE_DDL),
                                                        Files.readAllLines(
                                                                Paths.get(ddlTestFile.toURI()))
                                                                .stream())
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
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(databaseName), getUsername(), getPassword());
    }

    private String convertSQL(final String sql) {
        return sql.replace("$DBNAME$", databaseName);
    }
}
