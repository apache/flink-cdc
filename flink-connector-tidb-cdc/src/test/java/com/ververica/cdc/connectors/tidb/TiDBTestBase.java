package com.ververica.cdc.connectors.tidb;

import org.apache.flink.test.util.AbstractTestBase;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
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

/** Utility class for tidb tests. */
public class TiDBTestBase extends AbstractTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TiDBTestBase.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    public static final String TIDB_USER = "root";
    public static final String TIDB_PASSWORD = "";
    public static final int TIDB_PORT = 4000;
    public static final int PD_PORT = 2379;

    public static final String TIDB_SERVICE_NAME = "tidb";
    public static final String TIKV_SERVICE_NAME = "tikv";
    public static final String PD_SERVICE_NAME = "pd";

    public static final DockerComposeContainer environment =
            new DockerComposeContainer(new File("src/test/resources/docker/docker-compose.yml"))
                    .withExposedService(
                            TIDB_SERVICE_NAME + "_1",
                            TIDB_PORT,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
                    .withExposedService(
                            PD_SERVICE_NAME + "_1",
                            PD_PORT,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
                    .withLocalCompose(true);

    @BeforeClass
    public static void startContainers() throws Exception {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(environment)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        if (environment != null) {
            environment.stop();
        }
    }

    public String getTIDBHost() {
        return environment.getServiceHost(TIDB_SERVICE_NAME, TIDB_PORT);
    }

    public Integer getTIDBPort() {
        return environment.getServicePort(TIDB_SERVICE_NAME, TIDB_PORT);
    }

    public String getPDHost() {
        return environment.getServiceHost(PD_SERVICE_NAME, PD_PORT);
    }

    public Integer getPDPort() {
        return environment.getServicePort(PD_SERVICE_NAME, PD_PORT);
    }

    public String getJdbcUrl(String databaseName) {
        return "jdbc:mysql://" + getTIDBHost() + ":" + getTIDBPort() + "/" + databaseName;
    }

    protected Connection getJdbcConnection(String databaseName) throws SQLException {

        return DriverManager.getConnection(getJdbcUrl(databaseName), TIDB_USER, TIDB_PASSWORD);
    }

    private static void dropTestDatabase(Connection connection, String databaseName)
            throws SQLException {
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

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializeTidbTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = TiDBTestBase.class.getClassLoader().getResource(ddlFile);
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
