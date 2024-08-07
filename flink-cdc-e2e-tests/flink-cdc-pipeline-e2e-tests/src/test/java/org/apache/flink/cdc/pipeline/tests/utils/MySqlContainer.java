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

package org.apache.flink.cdc.pipeline.tests.utils;

import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;

/**
 * Docker container for MySQL. The difference between this class and {@link
 * org.testcontainers.containers.MySQLContainer} is that TC MySQLContainer has problems when
 * overriding mysql conf file, i.e. my.cnf.
 */
public class MySqlContainer extends JdbcDatabaseContainer<MySqlContainer> {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlContainer.class);
    public static final String IMAGE = "mysql";
    public static final Integer MYSQL_PORT = 3306;
    private static final String MY_CNF_CONFIG_OVERRIDE_PARAM_NAME = "MY_CNF";
    private static final String SETUP_SQL_PARAM_NAME = "SETUP_SQL";
    public static final String TEST_USERNAME = "test";
    public static final String TEST_PASSWORD = "test";
    private final String databaseName;
    private final Network network;
    private final String networkAlias;

    private static final String[] CREATE_DATABASE_DDL =
            new String[] {"CREATE DATABASE IF NOT EXISTS $DBNAME$;", "USE $DBNAME$;"};
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    public MySqlContainer(
            MySqlVersion version, Network network, String networkAlias, String databaseName) {
        super(DockerImageName.parse(IMAGE + ":" + version.getVersion()));
        this.databaseName = databaseName;
        this.network = network;
        this.networkAlias = networkAlias;
        addExposedPort(MYSQL_PORT);
    }

    @Override
    protected void configure() {
        addEnv("MYSQL_DATABASE", databaseName);
        addEnv("MYSQL_USER", TEST_USERNAME);
        addEnv("MYSQL_PASSWORD", TEST_PASSWORD);
        addEnv("MYSQL_ROOT_PASSWORD", TEST_PASSWORD);
        this.withNetwork(network);
        this.withNetworkAliases(networkAlias);
        setStartupAttempts(3);
    }

    public void waitForLog(String regex, int count, int timeoutSeconds) {
        new LogMessageWaitStrategy()
                .withRegEx(regex)
                .withTimes(count)
                .withStartupTimeout(Duration.of(timeoutSeconds, ChronoUnit.SECONDS))
                .waitUntilReady(this);
    }

    @Override
    public String getDriverClassName() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return "com.mysql.cj.jdbc.Driver";
        } catch (ClassNotFoundException e) {
            return "com.mysql.jdbc.Driver";
        }
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getDatabasePort()
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    public boolean checkMySqlAvailability() {
        try {
            Container.ExecResult rs =
                    this.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-u" + TEST_USERNAME,
                            "-P" + MYSQL_PORT,
                            "-p" + TEST_PASSWORD,
                            "-h127.0.0.1",
                            "-e SELECT 1");

            if (rs.getExitCode() != 0) {
                return false;
            }
            String output = rs.getStdout();
            LOG.info("MySQL backend status:\n" + output);
            return output.contains("1");
        } catch (Exception e) {
            LOG.warn("Failed to check backend status: " + e.getMessage());
            return false;
        }
    }

    public void executeSql(String sql) {
        try {
            Container.ExecResult rs =
                    this.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-u" + TEST_USERNAME,
                            "-P" + MYSQL_PORT,
                            "-p" + TEST_PASSWORD,
                            "-h127.0.0.1",
                            "-e " + sql);

            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to execute SQL." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute SQL.", e);
        }
    }

    public void createDatabase(String database) {
        executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", database));
    }

    public void dropDatabase(String database) {
        executeSql(String.format("DROP DATABASE IF EXISTS %s", database));
    }

    public void createAndInitialize(String templateName) {
        final String ddlFile = String.format("ddl/%s.sql", templateName);
        final URL ddlTestFile = MySqlContainer.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try {
            try (Connection connection =
                            DriverManager.getConnection(
                                    this.getJdbcUrl(), TEST_USERNAME, TEST_PASSWORD);
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
                                                .collect(Collectors.joining("\n"))
                                                .split(";"))
                                .collect(Collectors.toList());

                for (String stmt : statements) {
                    statement.execute(stmt);
                }
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private String convertSQL(final String sql) {
        return sql.replace("$DBNAME$", databaseName);
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(databaseName);
    }

    public int getDatabasePort() {
        return getMappedPort(MYSQL_PORT);
    }

    @Override
    protected String constructUrlForConnection(String queryString) {
        String url = super.constructUrlForConnection(queryString);

        if (!url.contains("useSSL=")) {
            String separator = url.contains("?") ? "&" : "?";
            url = url + separator + "useSSL=false";
        }

        if (!url.contains("allowPublicKeyRetrieval=")) {
            url = url + "&allowPublicKeyRetrieval=true";
        }

        return url;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return TEST_USERNAME;
    }

    @Override
    public String getPassword() {
        return TEST_PASSWORD;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    public MySqlContainer withConfigurationOverride(String s) {
        parameters.put(MY_CNF_CONFIG_OVERRIDE_PARAM_NAME, s);
        return this;
    }

    public MySqlContainer withSetupSQL(String sqlPath) {
        parameters.put(SETUP_SQL_PARAM_NAME, sqlPath);
        return this;
    }
}
