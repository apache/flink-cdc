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

package org.apache.flink.cdc.connectors.postgres;

import org.apache.flink.cdc.connectors.postgres.source.PostgresConnectionPoolFactory;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

/**
 * Basic class for testing PostgreSQL source, this contains a PostgreSQL container which enables wal
 * log.
 */
public abstract class PostgresTestBase extends AbstractTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(PostgresTestBase.class);
    public static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    public static final String DEFAULT_DB = "postgres";
    public static final String TEST_USER = "postgres";
    public static final String TEST_PASSWORD = "postgres";

    private static final String DEFAULT_POSTGRES_IMAGE = "postgres:14";
    private static final String POSTGRES_IMAGE_PROPERTY_KEY = "flink.cdc.postgres.image";

    // use official postgresql image to support pgoutput plugin
    protected static final DockerImageName PG_IMAGE =
            DockerImageName.parse(getPostgresImage()).asCompatibleSubstituteFor("postgres");

    // PostgreSQL 10 image for testing partition tables without publish_via_partition_root
    protected static final DockerImageName PG10_IMAGE =
            DockerImageName.parse("postgres:10").asCompatibleSubstituteFor("postgres");

    public static final Network NETWORK = Network.newNetwork();
    public static final String INTER_CONTAINER_POSTGRES_ALIAS = "postgres";

    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withDatabaseName(DEFAULT_DB)
                    .withUsername(TEST_USER)
                    .withPassword(TEST_PASSWORD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_POSTGRES_ALIAS)
                    .withReuse(false)
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "wal_level=logical");

    /**
     * PostgreSQL 10 container for testing PG10-specific behavior. Lazily initialized to avoid
     * starting the container when not needed.
     */
    private static volatile PostgreSQLContainer<?> pg10Container;

    static {
        // Singleton Container pattern: start container once and reuse across all tests
        POSTGRES_CONTAINER.start();
    }

    /**
     * Gets the PostgreSQL 10 container, starting it lazily if needed. Use this for tests that
     * specifically need PG10 behavior (e.g., partition tables without publish_via_partition_root).
     */
    public static PostgreSQLContainer<?> getPg10Container() {
        if (pg10Container == null) {
            synchronized (PostgresTestBase.class) {
                if (pg10Container == null) {
                    pg10Container =
                            new PostgreSQLContainer<>(PG10_IMAGE)
                                    .withDatabaseName(DEFAULT_DB)
                                    .withUsername(TEST_USER)
                                    .withPassword(TEST_PASSWORD)
                                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                                    .withReuse(false)
                                    .withCommand(
                                            "postgres",
                                            "-c",
                                            "fsync=off",
                                            "-c",
                                            "max_replication_slots=20",
                                            "-c",
                                            "wal_level=logical");
                    pg10Container.start();
                    LOG.info("PostgreSQL 10 container started lazily for PG10-specific tests.");
                }
            }
        }
        return pg10Container;
    }

    private static String getPostgresImage() {
        return System.getProperty(POSTGRES_IMAGE_PROPERTY_KEY, DEFAULT_POSTGRES_IMAGE);
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
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();
        try (Connection connection = getJdbcConnection(container);
                Statement statement = connection.createStatement()) {
            final String ddl =
                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                            .map(String::trim)
                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                            .map(
                                    x -> {
                                        final Matcher m = COMMENT_PATTERN.matcher(x);
                                        return m.matches() ? m.group(1) : x;
                                    })
                            .collect(Collectors.joining("\n"));

            final List<String> statements = splitSqlStatements(ddl);
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> splitSqlStatements(String ddl) {
        List<String> statements = new ArrayList<>();
        if (ddl == null || ddl.isEmpty()) {
            return statements;
        }

        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        String dollarQuoteTag = null;

        for (int i = 0; i < ddl.length(); ) {
            if (dollarQuoteTag != null) {
                if (ddl.startsWith(dollarQuoteTag, i)) {
                    current.append(dollarQuoteTag);
                    i += dollarQuoteTag.length();
                    dollarQuoteTag = null;
                } else {
                    current.append(ddl.charAt(i));
                    i++;
                }
                continue;
            }

            char ch = ddl.charAt(i);
            if (inSingleQuote) {
                current.append(ch);
                if (ch == '\'') {
                    if (i + 1 < ddl.length() && ddl.charAt(i + 1) == '\'') {
                        current.append('\'');
                        i += 2;
                        continue;
                    }
                    inSingleQuote = false;
                }
                i++;
                continue;
            }

            if (ch == '\'') {
                inSingleQuote = true;
                current.append(ch);
                i++;
                continue;
            }

            if (ch == '$') {
                String tag = tryReadDollarQuoteTag(ddl, i);
                if (tag != null) {
                    dollarQuoteTag = tag;
                    current.append(tag);
                    i += tag.length();
                    continue;
                }
            }

            if (ch == ';') {
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) {
                    statements.add(stmt);
                }
                current.setLength(0);
                i++;
                continue;
            }

            current.append(ch);
            i++;
        }

        String last = current.toString().trim();
        if (!last.isEmpty()) {
            statements.add(last);
        }
        return statements;
    }

    private static String tryReadDollarQuoteTag(String ddl, int start) {
        int end = ddl.indexOf('$', start + 1);
        if (end < 0) {
            return null;
        }
        String tag = ddl.substring(start, end + 1);
        for (int i = 1; i < tag.length() - 1; i++) {
            char c = tag.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_')) {
                return null;
            }
        }
        return tag;
    }

    protected PostgresConnection createConnection(Map<String, String> properties) {
        Configuration config = Configuration.from(properties);
        return new PostgresConnection(JdbcConfiguration.adapt(config), "test-connection");
    }

    protected void waitForSnapshotStarted(String sinkName) throws InterruptedException {
        while (sinkSize(sinkName) == 0) {
            sleep(300);
        }
    }

    protected void waitForSinkResult(String sinkName, List<String> expected)
            throws InterruptedException {
        List<String> actual = TestValuesTableFactory.getResultsAsStrings(sinkName);
        actual = actual.stream().sorted().collect(Collectors.toList());
        while (actual.size() != expected.size() || !actual.equals(expected)) {
            actual =
                    TestValuesTableFactory.getResultsAsStrings(sinkName).stream()
                            .sorted()
                            .collect(Collectors.toList());
            sleep(1000);
        }
    }

    protected void waitForSinkSize(String sinkName, int expectedSize) throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            sleep(100);
        }
    }

    protected int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResultsAsStrings(sinkName).size();
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
        postgresSourceConfigFactory.setLsnCommitCheckpointsDelay(1);
        postgresSourceConfigFactory.decodingPluginName("pgoutput");
        return postgresSourceConfigFactory;
    }

    public static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }
}
