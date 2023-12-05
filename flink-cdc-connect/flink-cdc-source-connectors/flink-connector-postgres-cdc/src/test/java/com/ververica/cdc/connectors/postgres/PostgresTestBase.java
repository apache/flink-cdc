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

package com.ververica.cdc.connectors.postgres;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;

import com.ververica.cdc.connectors.postgres.source.PostgresConnectionPoolFactory;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import com.ververica.cdc.connectors.postgres.testutils.UniqueDatabase;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Basic class for testing PostgreSQL source, this contains a PostgreSQL container which enables
 * binlog.
 */
public abstract class PostgresTestBase extends AbstractTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresTestBase.class);
    public static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    public static final String DEFAULT_DB = "postgres";

    // use newer version of postgresql image to support pgoutput plugin
    // when testing postgres 13, only 13-alpine supports both amd64 and arm64
    protected static final DockerImageName PG_IMAGE =
            DockerImageName.parse("debezium/postgres:9.6").asCompatibleSubstituteFor("postgres");

    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withDatabaseName(DEFAULT_DB)
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20");

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(POSTGRES_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        POSTGRES_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    protected Connection getJdbcConnection(PostgreSQLContainer container) throws SQLException {
        return DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
    }

    public static Connection getJdbcConnection(PostgreSQLContainer container, String databaseName)
            throws SQLException {
        String jdbcUrl =
                String.format(
                        PostgresConnectionPoolFactory.JDBC_URL_PATTERN,
                        container.getHost(),
                        container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                        databaseName);
        return DriverManager.getConnection(
                jdbcUrl, container.getUsername(), container.getPassword());
    }

    public static String getSlotName() {
        final Random random = new Random();
        int id = random.nextInt(10000);
        return "flink_" + id;
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializePostgresTable(PostgreSQLContainer container, String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = PostgresTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection(container);
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
                                            .split(";\n"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected PostgresConnection createConnection(Map<String, String> properties) {
        Configuration config = Configuration.from(properties);
        return new PostgresConnection(JdbcConfiguration.adapt(config), "test-connection");
    }

    protected void waitForSnapshotStarted(String sinkName) throws InterruptedException {
        while (sinkSize(sinkName) == 0) {
            Thread.sleep(300);
        }
    }

    protected void waitForSinkResult(String sinkName, List<String> expected)
            throws InterruptedException {
        List<String> actual = TestValuesTableFactory.getResults(sinkName);
        actual = actual.stream().sorted().collect(Collectors.toList());
        while (actual.size() != expected.size() || !actual.equals(expected)) {
            actual =
                    TestValuesTableFactory.getResults(sinkName).stream()
                            .sorted()
                            .collect(Collectors.toList());
            Thread.sleep(1000);
        }
    }

    protected void waitForSinkSize(String sinkName, int expectedSize) throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    protected int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    protected PostgresSourceConfigFactory getMockPostgresSourceConfigFactory(
            UniqueDatabase database, String schemaName, String tableName, int splitSize) {
        return getMockPostgresSourceConfigFactory(
                database, schemaName, tableName, splitSize, false);
    }

    protected PostgresSourceConfigFactory getMockPostgresSourceConfigFactory(
            UniqueDatabase database,
            String schemaName,
            String tableName,
            int splitSize,
            boolean skipSnapshotBackfill) {

        PostgresSourceConfigFactory postgresSourceConfigFactory = new PostgresSourceConfigFactory();
        postgresSourceConfigFactory.hostname(database.getHost());
        postgresSourceConfigFactory.port(database.getDatabasePort());
        postgresSourceConfigFactory.username(database.getUsername());
        postgresSourceConfigFactory.password(database.getPassword());
        postgresSourceConfigFactory.database(database.getDatabaseName());
        postgresSourceConfigFactory.schemaList(new String[] {schemaName});
        postgresSourceConfigFactory.tableList(schemaName + "." + tableName);
        postgresSourceConfigFactory.splitSize(splitSize);
        postgresSourceConfigFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        return postgresSourceConfigFactory;
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
}
