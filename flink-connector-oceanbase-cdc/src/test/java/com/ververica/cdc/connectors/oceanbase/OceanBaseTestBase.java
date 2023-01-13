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

    public static final int OB_SERVER_SQL_PORT = 2881;
    public static final int OB_SERVER_RPC_PORT = 2882;
    public static final int LOG_PROXY_PORT = 2983;

    public static final String OB_SYS_USERNAME = "root";
    public static final String OB_SYS_PASSWORD = "pswd";

    public static final String NETWORK_MODE = "host";

    // --------------------------------------------------------------------------------------------
    // Attributes about host and port when network is on 'host' mode.
    // --------------------------------------------------------------------------------------------

    protected static String getObServerHost() {
        return "127.0.0.1";
    }

    protected static String getLogProxyHost() {
        return "127.0.0.1";
    }

    protected static int getObServerSqlPort() {
        return OB_SERVER_SQL_PORT;
    }

    protected static int getObServerRpcPort() {
        return OB_SERVER_RPC_PORT;
    }

    protected static int getLogProxyPort() {
        return LOG_PROXY_PORT;
    }

    // --------------------------------------------------------------------------------------------
    // Attributes about user.
    // Here we use the root user of 'sys' tenant, which is not recommended for production.
    // --------------------------------------------------------------------------------------------

    protected static String getTenant() {
        return "sys";
    }

    protected static String getUsername() {
        return OB_SYS_USERNAME;
    }

    protected static String getPassword() {
        return OB_SYS_PASSWORD;
    }

    @ClassRule
    public static final GenericContainer<?> OB_SERVER =
            new GenericContainer<>("oceanbase/oceanbase-ce:3.1.4")
                    .withNetworkMode(NETWORK_MODE)
                    .withExposedPorts(OB_SERVER_SQL_PORT, OB_SERVER_RPC_PORT)
                    .withEnv("OB_ROOT_PASSWORD", OB_SYS_PASSWORD)
                    .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                    .withStartupTimeout(CONTAINER_STARTUP_TIMEOUT)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final GenericContainer<?> LOG_PROXY =
            new GenericContainer<>("whhe/oblogproxy:1.0.3")
                    .withNetworkMode(NETWORK_MODE)
                    .withExposedPorts(LOG_PROXY_PORT)
                    .withEnv("OB_SYS_USERNAME", OB_SYS_USERNAME)
                    .withEnv("OB_SYS_PASSWORD", OB_SYS_PASSWORD)
                    .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                    .withStartupTimeout(CONTAINER_STARTUP_TIMEOUT)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(OB_SERVER, LOG_PROXY)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        Stream.of(OB_SERVER, LOG_PROXY).forEach(GenericContainer::stop);
        LOG.info("Containers are stopped.");
    }

    public static String getJdbcUrl(String databaseName) {
        return "jdbc:mysql://"
                + getObServerHost()
                + ":"
                + getObServerSqlPort()
                + "/"
                + databaseName
                + "?useSSL=false";
    }

    public static String getRsList() {
        return String.format(
                "%s:%s:%s", getObServerHost(), getObServerRpcPort(), getObServerSqlPort());
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
