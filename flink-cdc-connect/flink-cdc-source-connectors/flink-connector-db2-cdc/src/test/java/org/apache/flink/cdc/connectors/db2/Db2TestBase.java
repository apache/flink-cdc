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

package org.apache.flink.cdc.connectors.db2;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.lang3.StringUtils;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Basic class for testing DB2 source, this contains a DB2 container which enables redo logs. */
public class Db2TestBase {

    private static final Logger LOG = LoggerFactory.getLogger(Db2TestBase.class);

    private static final DockerImageName DEBEZIUM_DOCKER_IMAGE_NAME =
            DockerImageName.parse(
                            new ImageFromDockerfile("custom/db2-cdc:1.4")
                                    .withDockerfile(getFilePath("db2_server/Dockerfile"))
                                    .get())
                    .asCompatibleSubstituteFor("ibmcom/db2");
    private static boolean db2AsnAgentRunning = false;
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    protected static final Db2Container DB2_CONTAINER =
            new Db2Container(DEBEZIUM_DOCKER_IMAGE_NAME)
                    .withDatabaseName("testdb")
                    .withUsername("db2inst1")
                    .withPassword("flinkpw")
                    .withEnv("AUTOCONFIG", "false")
                    .withEnv("ARCHIVE_LOGS", "true")
                    .acceptLicense()
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withLogConsumer(
                            outputFrame -> {
                                if (outputFrame
                                        .getUtf8String()
                                        .contains("The asncdc program enable finished")) {
                                    db2AsnAgentRunning = true;
                                }
                            });

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(DB2_CONTAINER)).join();
        LOG.info("Containers are started.");

        LOG.info("Waiting db2 asn agent start...");
        while (!db2AsnAgentRunning) {
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                LOG.error("unexpected interrupted exception", e);
            }
        }
        LOG.info("Db2 asn agent are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        if (DB2_CONTAINER != null) {
            DB2_CONTAINER.stop();
        }
        LOG.info("Containers are stopped.");
    }

    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                DB2_CONTAINER.getJdbcUrl(),
                DB2_CONTAINER.getUsername(),
                DB2_CONTAINER.getPassword());
    }

    private static Path getFilePath(String resourceFilePath) {
        Path path = null;
        try {
            URL filePath = Db2TestBase.class.getClassLoader().getResource(resourceFilePath);
            assertNotNull("Cannot locate " + resourceFilePath, filePath);
            path = Paths.get(filePath.toURI());
        } catch (URISyntaxException e) {
            LOG.error("Cannot get path from URI.", e);
        }
        return path;
    }

    private static void dropTestTable(Connection connection, String tableName) {

        try {
            Awaitility.await(String.format("cdc remove table %s", tableName))
                    .atMost(30, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    String removeSql =
                                            String.format(
                                                    "CALL ASNCDC.REMOVETABLE('DB2INST1', '%s')",
                                                    tableName);
                                    connection.createStatement().execute(removeSql);
                                    String reinitSql =
                                            String.format(
                                                    "VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc');",
                                                    tableName);
                                    connection.createStatement().execute(reinitSql);
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn(
                                            String.format(
                                                    "cdc remove TABLE %s failed (will be retried): {}",
                                                    tableName),
                                            e.getMessage());
                                    return false;
                                }
                            });
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to remove cdc table " + tableName, e);
        }

        try {
            Awaitility.await(String.format("Dropping table %s", tableName))
                    .atMost(30, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    String sql = String.format("DROP TABLE DB2INST1.%s", tableName);
                                    connection.createStatement().execute(sql);
                                    connection.commit();
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn(
                                            String.format(
                                                    "DROP TABLE %s failed (will be retried): {}",
                                                    tableName),
                                            e.getMessage());
                                    return false;
                                }
                            });
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to drop table", e);
        }
    }

    private static boolean checkTableExists(Connection connection, String tableName) {
        AtomicBoolean tableExists = new AtomicBoolean(false);
        try {
            Awaitility.await(String.format("check table %s exists or not", tableName))
                    .atMost(30, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    String tableExistSql =
                                            String.format(
                                                    "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABNAME = '%s' AND "
                                                            + "TABSCHEMA = 'DB2INST1';",
                                                    tableName);
                                    ResultSet resultSet =
                                            connection
                                                    .createStatement()
                                                    .executeQuery(tableExistSql);
                                    if (resultSet.next()) {
                                        if (resultSet.getInt(1) == 1) {
                                            tableExists.set(true);
                                        }
                                    }
                                    return true;
                                } catch (SQLException e) {
                                    LOG.warn(
                                            String.format(
                                                    "check table %s exists failed", tableName),
                                            e.getMessage());
                                    return false;
                                }
                            });
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to check table " + tableName + " exists", e);
        }
        return tableExists.get();
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializeDb2Table(String sqlFile, String tableName) {
        final String ddlFile = String.format("db2_server/%s.sql", sqlFile);
        final URL ddlTestFile = Db2TestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            if (checkTableExists(connection, tableName)) {
                LOG.info("{} table exist", tableName);
                dropTestTable(connection, tableName.toUpperCase(Locale.ROOT));
                // sleep 10 seconds to make sure ASN replication agent has been notified
                Thread.sleep(10_000);
            }
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
                Thread.sleep(500);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }

    public void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return format("(%s)", StringUtils.join(captureCustomerTables, ","));
        }
    }
}
