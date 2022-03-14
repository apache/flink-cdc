/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.test.util.AbstractTestBase;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
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
public class OceanBaseTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseTestBase.class);

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private static final String SERVICE_ALIAS_OB_LOG_PROXY = "logproxy";
    private static final String SERVICE_ALIAS_OB_SERVER = "observer";

    private static final String NETWORK_MODE = "host";

    public static final int OB_LOG_PROXY_PORT = 2983;
    public static final int OB_SERVER_SQL_PORT = 2881;
    public static final int OB_SERVER_RPC_PORT = 2882;

    public static final String OB_SYS_USERNAME = "user";
    public static final String OB_SYS_USERNAME_ENCRYPTED = "441AB4AC40CD3C2DAC19873CD04CC72C";
    public static final String OB_SYS_PASSWORD = "pswd";
    public static final String OB_SYS_PASSWORD_ENCRYPTED = "B5D4F8E0DBC9F0E57A13296DCD307B5D";

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    @ClassRule
    public static final GenericContainer<?> OB_SERVER =
            new GenericContainer<>("whhe/obce-mini")
                    .withNetwork(NETWORK)
                    .withNetworkMode(NETWORK_MODE)
                    .withNetworkAliases(SERVICE_ALIAS_OB_SERVER)
                    .withExposedPorts(OB_SERVER_SQL_PORT, OB_SERVER_RPC_PORT)
                    .withStartupTimeout(Duration.ofSeconds(120))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final GenericContainer<?> OB_LOG_PROXY =
            new GenericContainer<>("whhe/oblogproxy")
                    .withNetwork(NETWORK)
                    .withNetworkMode(NETWORK_MODE)
                    .withNetworkAliases(SERVICE_ALIAS_OB_LOG_PROXY)
                    .withExposedPorts(OB_LOG_PROXY_PORT)
                    .withEnv("OB_SYS_USERNAME", OB_SYS_USERNAME_ENCRYPTED)
                    .withEnv("OB_SYS_PASSWORD", OB_SYS_PASSWORD_ENCRYPTED)
                    .dependsOn(OB_SERVER)
                    .withStartupTimeout(Duration.ofSeconds(120))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(OB_SERVER, OB_LOG_PROXY)).join();
        LOG.info("Containers are started.");
        createSysUser();
        LOG.info("Sys user {} created.", OB_SYS_USERNAME);
    }

    @AfterClass
    public static void stopContainers() {
        Stream.of(OB_SERVER, OB_LOG_PROXY).forEach(GenericContainer::stop);
    }

    public static String getJdbcUrl(String databaseName) {
        return "jdbc:mysql://"
                + OB_SERVER.getContainerIpAddress()
                + ":"
                + OB_SERVER.getMappedPort(OB_SERVER_SQL_PORT)
                + "/"
                + databaseName;
    }

    protected static Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(""), "root", "");
    }

    protected static Connection getJdbcConnection(String databaseName) throws SQLException {
        return DriverManager.getConnection(
                getJdbcUrl(databaseName), OB_SYS_USERNAME, OB_SYS_PASSWORD);
    }

    /** Create a user with password in sys tenant. */
    protected static void createSysUser() {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE USER '%s' IDENTIFIED BY '%s'",
                            OB_SYS_USERNAME, OB_SYS_PASSWORD));
            statement.execute(
                    String.format(
                            "GRANT ALL PRIVILEGES ON *.* TO '%s' WITH GRANT OPTION",
                            OB_SYS_USERNAME));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    protected void initializeTidbTable(String sqlFile) {
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
