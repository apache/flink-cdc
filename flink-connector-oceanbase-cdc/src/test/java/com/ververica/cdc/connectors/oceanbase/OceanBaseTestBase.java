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

import org.apache.flink.util.TestLogger;

import com.alibaba.dcm.DnsCacheManipulator;
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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
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

    public static final String OB_LOG_PROXY_SERVICE_NAME = "oblogproxy";

    // Should be deprecated after official images are released.
    public static final String DOCKER_IMAGE_NAME = "whhe/oblogproxy:obce_3.1.1";

    // For details about config, see https://github.com/whhe/dockerfiles/tree/master/oblogproxy
    public static final int OB_LOG_PROXY_PORT = 2983;
    public static final int OB_SERVER_SQL_PORT = 2881;
    public static final int OB_SERVER_RPC_PORT = 2882;

    // Here we use root user of system tenant for testing, as the log proxy service needs
    // a user of system tenant for authentication. It is not recommended for production.
    public static final String OB_SYS_USERNAME = "root";
    public static final String OB_SYS_PASSWORD = "pswd";

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    @ClassRule
    public static final GenericContainer<?> OB_WITH_LOG_PROXY =
            new GenericContainer<>(DOCKER_IMAGE_NAME)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(OB_LOG_PROXY_SERVICE_NAME)
                    .withExposedPorts(OB_SERVER_SQL_PORT, OB_SERVER_RPC_PORT, OB_LOG_PROXY_PORT)
                    .withEnv("OB_ROOT_PASSWORD", OB_SYS_PASSWORD)
                    .waitingFor(
                            new WaitAllStrategy()
                                    .withStrategy(Wait.forListeningPort())
                                    .withStrategy(Wait.forLogMessage(".*boot success!.*", 1)))
                    .withStartupTimeout(Duration.ofSeconds(120))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        // Add jvm dns cache for flink to invoke ob interface.
        DnsCacheManipulator.setDnsCache(OB_LOG_PROXY_SERVICE_NAME, "127.0.0.1");
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(OB_WITH_LOG_PROXY)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        DnsCacheManipulator.removeDnsCache(OB_LOG_PROXY_SERVICE_NAME);
        LOG.info("Stopping containers...");
        Stream.of(OB_WITH_LOG_PROXY).forEach(GenericContainer::stop);
        LOG.info("Containers are stopped.");
    }

    public static String getJdbcUrl(String databaseName) {
        return "jdbc:mysql://"
                + OB_WITH_LOG_PROXY.getContainerIpAddress()
                + ":"
                + OB_WITH_LOG_PROXY.getMappedPort(OB_SERVER_SQL_PORT)
                + "/"
                + databaseName
                + "?useSSL=false";
    }

    protected static Connection getJdbcConnection(String databaseName) throws SQLException {
        return DriverManager.getConnection(
                getJdbcUrl(databaseName), OB_SYS_USERNAME, OB_SYS_PASSWORD);
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
