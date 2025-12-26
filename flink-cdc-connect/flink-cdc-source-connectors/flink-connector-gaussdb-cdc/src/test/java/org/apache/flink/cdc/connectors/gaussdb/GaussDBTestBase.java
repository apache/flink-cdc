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

package org.apache.flink.cdc.connectors.gaussdb;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBSource;
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import io.debezium.config.Configuration;
import io.debezium.connector.gaussdb.connection.GaussDBConnection;
import io.debezium.jdbc.JdbcConfiguration;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Basic test utilities for GaussDB CDC connector integration tests. */
public abstract class GaussDBTestBase extends AbstractTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(GaussDBTestBase.class);
    protected static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    // Remote GaussDB test instance.
    protected static final String HOSTNAME = "10.250.0.51";
    protected static final int PORT = 8000;
    protected static final String USERNAME = "tom";
    protected static final String PASSWORD = "Gauss_235";
    protected static final String DATABASE_NAME = "db1";
    protected static final String SCHEMA_NAME = "public";

    /**
     * Get JDBC connection for test data preparation. Uses GaussDB JDBC driver with extended timeout
     * settings to handle high network latency.
     */
    protected Connection getJdbcConnection() throws SQLException {
        // Use GaussDB native URL format
        String jdbcUrl =
                String.format(
                        "jdbc:gaussdb://%s:%d/%s?sslmode=disable", HOSTNAME, PORT, DATABASE_NAME);

        LOG.info("Attempting connection to GaussDB with URL: {}", jdbcUrl);
        LOG.info("Username: {}, Database: {}", USERNAME, DATABASE_NAME);

        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", USERNAME);
        props.setProperty("password", PASSWORD);
        // Extended timeouts for high-latency network (in seconds)
        props.setProperty("connectTimeout", "60");
        // Set socketTimeout to 0 (infinite) to prevent timeout during long-running CDC operations
        props.setProperty("socketTimeout", "0");
        props.setProperty("loginTimeout", "60");
        props.setProperty("tcpKeepAlive", "true");

        try {
            Class.forName("com.huawei.gaussdb.jdbc.Driver");
            long startTime = System.currentTimeMillis();
            LOG.info("Using GaussDB JDBC driver");
            Connection connection = DriverManager.getConnection(jdbcUrl, props);
            long endTime = System.currentTimeMillis();
            LOG.info("Successfully connected to GaussDB in {} ms", (endTime - startTime));
            return connection;
        } catch (ClassNotFoundException e) {
            throw new SQLException("GaussDB JDBC driver not found.", e);
        }
    }

    public static String getSlotName() {
        final Random random = new Random();
        int id = random.nextInt(10000);
        return "flink_" + id;
    }

    protected GaussDBConnection createConnection(Map<String, String> properties) {
        Configuration config = Configuration.from(properties);
        return new GaussDBConnection(JdbcConfiguration.adapt(config), "test-connection");
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

    // ----------------------------------------------------------------------------------------
    // Utilities for integration tests
    // ----------------------------------------------------------------------------------------

    /** Execute a SQL script from classpath resource or filesystem path. */
    protected void executeSqlScript(String scriptPath) throws Exception {
        Path path = resolveScriptPath(scriptPath);
        List<String> statements =
                Arrays.stream(
                                Files.readAllLines(path, StandardCharsets.UTF_8).stream()
                                        .map(String::trim)
                                        .filter(line -> !line.isEmpty() && !line.startsWith("--"))
                                        .map(
                                                line -> {
                                                    final Matcher m = COMMENT_PATTERN.matcher(line);
                                                    return m.matches() ? m.group(1) : line;
                                                })
                                        .collect(Collectors.joining("\n"))
                                        .split(";"))
                        .map(String::trim)
                        .filter(stmt -> !stmt.isEmpty())
                        .collect(Collectors.toList());

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        }
    }

    /**
     * Blocks until the iterator yields {@code expectedCount} rows or the timeout elapses.
     *
     * <p>Note: this method consumes the iterator.
     */
    protected void waitForData(Iterator<Row> iter, int expectedCount, Duration timeout)
            throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future =
                executor.submit(
                        () -> {
                            int remaining = expectedCount;
                            while (remaining > 0) {
                                if (!iter.hasNext()) {
                                    throw new IllegalStateException(
                                            "Iterator closed before expected data is available");
                                }
                                iter.next();
                                remaining--;
                            }
                        });
        try {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new AssertionError(
                    "Timed out waiting for " + expectedCount + " rows within " + timeout, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new RuntimeException("Failed while waiting for data", cause);
        } finally {
            executor.shutdownNow();
        }
    }

    protected String createReplicationSlot(String slotName) throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            try {
                statement.execute(
                        String.format(
                                "CREATE_REPLICATION_SLOT \"%s\" LOGICAL mppdb_decoding", slotName));
                return slotName;
            } catch (SQLException first) {
                // Fallback to PostgreSQL-compatible function if supported.
                try (ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT * FROM pg_create_logical_replication_slot('%s', 'mppdb_decoding')",
                                        slotName))) {
                    if (rs.next()) {
                        return slotName;
                    }
                } catch (SQLException second) {
                    first.addSuppressed(second);
                    throw first;
                }
                return slotName;
            }
        }
    }

    protected void dropReplicationSlot(String slotName) throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            try {
                statement.execute(String.format("DROP_REPLICATION_SLOT \"%s\"", slotName));
            } catch (SQLException first) {
                // Fallback to PostgreSQL-compatible function if supported.
                try {
                    statement.execute(
                            String.format("SELECT pg_drop_replication_slot('%s')", slotName));
                } catch (SQLException second) {
                    first.addSuppressed(second);
                    throw first;
                }
            }
        }
    }

    protected void waitForLsnAdvance(String slotName, long minLsn) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        while (System.currentTimeMillis() < deadline) {
            Long current = readConfirmedFlushLsn(slotName);
            if (current != null && current >= minLsn) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError(
                "Timed out waiting for slot '"
                        + slotName
                        + "' LSN to advance to at least "
                        + minLsn);
    }

    protected <T> GaussDBSourceBuilder.GaussDBIncrementalSource<T> buildGaussDBSource(
            String[] tableList, String slotName, DebeziumDeserializationSchema<T> deserializer)
            throws Exception {
        return buildGaussDBSource(
                tableList, slotName, deserializer, StartupOptions.initial(), 1000);
    }

    protected <T> GaussDBSourceBuilder.GaussDBIncrementalSource<T> buildGaussDBSource(
            String[] tableList,
            String slotName,
            DebeziumDeserializationSchema<T> deserializer,
            StartupOptions startupOptions,
            int splitSize)
            throws Exception {
        Set<String> schemas = new HashSet<>();
        for (String tableId : tableList) {
            int idx = tableId.indexOf('.');
            if (idx > 0) {
                schemas.add(tableId.substring(0, idx));
            }
        }

        return GaussDBSource.<T>incrementalBuilder()
                .hostname(HOSTNAME)
                .port(PORT)
                .database(DATABASE_NAME)
                .username(USERNAME)
                .password(PASSWORD)
                .schemaList(schemas.toArray(new String[0]))
                .tableList(tableList)
                .slotName(slotName)
                .decodingPluginName("mppdb_decoding")
                .startupOptions(startupOptions)
                .splitSize(splitSize)
                .deserializer(deserializer)
                .build();
    }

    protected void assertChangeEventOrder(List<String> events, String... expectedPatterns) {
        int pos = 0;
        for (String pattern : expectedPatterns) {
            boolean matched = false;
            Pattern compiled = Pattern.compile(pattern, Pattern.DOTALL);
            while (pos < events.size()) {
                if (compiled.matcher(events.get(pos)).find()) {
                    matched = true;
                    pos++;
                    break;
                }
                pos++;
            }
            Assertions.assertThat(matched)
                    .withFailMessage(
                            "Cannot find expected event pattern '%s' in remaining events. Events=%s",
                            pattern, events)
                    .isTrue();
        }
    }

    private static Path resolveScriptPath(String scriptPath) throws Exception {
        String normalized = scriptPath.startsWith("/") ? scriptPath.substring(1) : scriptPath;
        URL resource = GaussDBTestBase.class.getClassLoader().getResource(normalized);
        if (resource != null) {
            return Paths.get(resource.toURI());
        }
        return Paths.get(scriptPath);
    }

    private Long readConfirmedFlushLsn(String slotName) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'",
                                        slotName))) {
            if (rs.next()) {
                String lsn = rs.getString(1);
                if (lsn == null) {
                    return null;
                }
                return parseLsnToLong(lsn);
            }
        } catch (Exception e) {
            LOG.debug("Failed to query confirmed_flush_lsn for slot {}", slotName, e);
        }
        return null;
    }

    private static long parseLsnToLong(String lsn) {
        String[] parts = lsn.split("/");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid LSN format: " + lsn);
        }
        long high = Long.parseUnsignedLong(parts[0], 16);
        long low = Long.parseUnsignedLong(parts[1], 16);
        return (high << 32) + low;
    }
}
