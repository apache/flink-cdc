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

package com.ververica.cdc.connectors.oceanbase;

import org.apache.flink.util.TestLogger;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;

/** Basic class for testing OceanBase source. */
public class OceanBaseTestBase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseTestBase.class);

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final Duration CONTAINER_STARTUP_TIMEOUT = Duration.ofMinutes(4);

    public static final String OB_SYS_PASSWORD = "pswd";
    public static final String OB_TEST_PASSWORD = "test";

    public static final String NETWORK_MODE = "host";

    // --------------------------------------------------------------------------------------------
    // Attributes about host and port when network is on 'host' mode.
    // --------------------------------------------------------------------------------------------

    protected static int getObServerSqlPort() {
        return 2881;
    }

    protected static int getLogProxyPort() {
        return 2983;
    }

    public static String getRsList() {
        return "127.0.0.1:2882:2881";
    }

    // --------------------------------------------------------------------------------------------
    // Attributes about user.
    // From OceanBase 4.0.0.0 CE, we can only fetch the commit log of non-sys tenant.
    // --------------------------------------------------------------------------------------------

    protected static String getTenant() {
        return "test";
    }

    protected static String getUsername() {
        return "root@" + getTenant();
    }

    protected static String getPassword() {
        return OB_TEST_PASSWORD;
    }

    @ClassRule
    public static final GenericContainer<?> OB_SERVER =
            new GenericContainer<>("whhe/oceanbase-ce:4.2.0.0")
                    .withNetworkMode(NETWORK_MODE)
                    .withEnv("MODE", "slim")
                    .withEnv("OB_ROOT_PASSWORD", OB_SYS_PASSWORD)
                    .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                    .withStartupTimeout(CONTAINER_STARTUP_TIMEOUT)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final GenericContainer<?> LOG_PROXY =
            new GenericContainer<>("whhe/oblogproxy:1.1.3_4x")
                    .withNetworkMode(NETWORK_MODE)
                    .withEnv("OB_SYS_PASSWORD", OB_SYS_PASSWORD)
                    .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                    .withStartupTimeout(CONTAINER_STARTUP_TIMEOUT)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(OB_SERVER, LOG_PROXY)).join();
        LOG.info("Containers are started.");

        try (Connection connection =
                        DriverManager.getConnection(getJdbcUrl(""), getUsername(), "");
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("ALTER USER root IDENTIFIED BY '%s'", getPassword()));
        } catch (SQLException e) {
            LOG.error("Set test user password failed.", e);
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        Stream.of(OB_SERVER, LOG_PROXY).forEach(GenericContainer::stop);
        LOG.info("Containers are stopped.");
    }

    public static String getJdbcUrl(String databaseName) {
        return "jdbc:mysql://"
                + OB_SERVER.getHost()
                + ":"
                + getObServerSqlPort()
                + "/"
                + databaseName
                + "?useSSL=false";
    }

    protected static Connection getJdbcConnection(String databaseName) throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(databaseName), getUsername(), getPassword());
    }

    private static void dropTestDatabase(Connection connection, String databaseName) {
        try {
            Awaitility.await(String.format("Dropping database %s", databaseName))
                    .atMost(120, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    String sql =
                                            String.format(
                                                    "DROP DATABASE IF EXISTS %s", databaseName);
                                    connection.createStatement().execute(sql);
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn(
                                            String.format(
                                                    "DROP DATABASE %s failed: {}", databaseName),
                                            e.getMessage());
                                    return false;
                                }
                            });
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Failed to drop test database", e);
        }
    }

    protected void initializeTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = getClass().getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection("");
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
