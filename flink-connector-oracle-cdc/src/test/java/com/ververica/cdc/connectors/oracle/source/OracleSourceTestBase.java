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

package com.ververica.cdc.connectors.oracle.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Basic class for testing {@link OracleSourceBuilder.OracleIncrementalSource}. */
public class OracleSourceTestBase extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(OracleSourceTestBase.class);
    protected static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    protected static final int DEFAULT_PARALLELISM = 4;
    protected static final String ORACLE_SYSTEM_USER = "system";
    protected static final String ORACLE_SYSTEM_PASSWORD = "oracle";
    protected static final String ORACLE_DATABASE = "XE";
    protected static final String ORACLE_SCHEMA = "DEBEZIUM";

    public static final String CONNECTOR_USER = "dbzuser";

    public static final String CONNECTOR_PWD = "dbz";

    public static final String SCHEMA_USER = "debezium";

    public static final String SCHEMA_PWD = "dbz";
    protected static final OracleContainer ORACLE_CONTAINER =
            new OracleContainer("goodboy008/oracle-19.3.0-ee:latest")
                    .withEnv("ORACLE_SID", ORACLE_DATABASE)
                    .withEnv("ORACLE_PWD", ORACLE_SYSTEM_PASSWORD)
                    .withEnv("ENABLE_ARCHIVELOG", "true")
                    .withFileSystemBind(
                            "scripts/setup/setup-logminer-noncdb.sh",
                            "/docker-entrypoint-initdb.d/setup")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .setConfiguration(new Configuration())
                            .withHaLeadershipControl()
                            .build());

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        if (ORACLE_CONTAINER != null) {
            ORACLE_CONTAINER.stop();
        }
        LOG.info("Containers are stopped.");
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

    public static Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(ORACLE_CONTAINER.getJdbcUrl(), "dbzuser", "dbz");
    }

    public static void createAndInitialize(String sqlFile) throws Exception {
        final String ddlFile = String.format("ddl/%s", sqlFile);
        final URL ddlTestFile = OracleSourceITCase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection();
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
    }
}
