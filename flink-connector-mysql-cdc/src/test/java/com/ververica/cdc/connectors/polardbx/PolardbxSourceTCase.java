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

package com.ververica.cdc.connectors.polardbx;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** tests for {@link MySqlSource}. */
public class PolardbxSourceTCase extends AbstractTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PolardbxSourceTCase.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final Integer PORT = 8527;
    private static final String HOST_NAME = "127.0.0.1";
    private static final String USER_NAME = "polardbx_root";
    private static final String PASSWORD = "123456";
    private static final DockerImageName POLARDBX_IMAGE =
            DockerImageName.parse("polardbx/polardb-x:latest");

    @Rule
    public GenericContainer POLARDBX_CONTAINER =
            new GenericContainer<>(POLARDBX_IMAGE)
                    .withExposedPorts(PORT)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withStartupTimeout(Duration.ofMinutes(3))
                    .withCreateContainerCmdModifier(
                            c ->
                                    c.withPortBindings(
                                            new PortBinding(
                                                    Ports.Binding.bindPort(PORT),
                                                    new ExposedPort(PORT))));

    @Before
    public void startContainers() {
        // no need to start container when the port 8527 is listening
        if (!checkConnection()) {
            LOG.info("Polardbx connection is not valid, so try to start containers...");
            Startables.deepStart(Stream.of(POLARDBX_CONTAINER)).join();
            LOG.info("Containers are started.");
        }
    }

    private static String getJdbcUrl() {
        return String.format("jdbc:mysql://%s:%s", HOST_NAME, PORT);
    }

    protected static Connection getJdbcConnection() throws SQLException {
        String jdbcUrl = getJdbcUrl();
        LOG.info("jdbcUrl is :" + jdbcUrl);
        return DriverManager.getConnection(jdbcUrl, USER_NAME, PASSWORD);
    }

    private static Boolean checkConnection() {
        LOG.info("check polardbx connection validation...");
        try {
            Connection connection = getJdbcConnection();
            return connection.isValid(3);
        } catch (SQLException e) {
            LOG.warn("polardbx connection is not valid... caused by:" + e.getMessage());
            return false;
        }
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializePolarxTable(String databaseName) throws InterruptedException {
        final String ddlFile = String.format("ddl/%s.sql", databaseName);
        final URL ddlTestFile = PolardbxSourceTCase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        // need to sleep 1s, make sure the jdbc connection can be created
        Thread.sleep(1000);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + databaseName);
            statement.execute("create database if not exists " + databaseName);
            statement.execute("use " + databaseName + ";");
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

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testPolardbxParallelSource(1, new String[] {"orders"});
    }

    private void testPolardbxParallelSource(int parallelism, String[] captureCustomerTables)
            throws Exception {
        String databaseName = "pxd_inventory";
        initializePolarxTable(databaseName);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        String sourceDDL =
                format(
                        "CREATE TABLE orders_source ("
                                + " id BIGINT NOT NULL,"
                                + " seller_id STRING,"
                                + " order_id STRING,"
                                + " buyer_id STRING,"
                                + " create_time TIMESTAMP,"
                                + " primary key (id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'server-id' = '%s'"
                                + ")",
                        HOST_NAME,
                        PORT,
                        USER_NAME,
                        PASSWORD,
                        databaseName,
                        getTableNameRegex(captureCustomerTables),
                        getServerId());

        // first step: check the snapshot data
        String[] snapshotForSingleTable =
                new String[] {
                    "+I[4, 1002, 2, 106, 2022-01-16T00:00]",
                    "+I[5, 1003, 1, 107, 2022-01-16T00:00]",
                    "+I[2, 1002, 2, 105, 2022-01-16T00:00]",
                    "+I[3, 1004, 3, 109, 2022-01-16T00:00]",
                    "+I[1, 1001, 1, 102, 2022-01-16T00:00]",
                };
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from orders_source");
        CloseableIterator<Row> iterator = tableResult.collect();
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        List<String> realSnapshotData = fetchRows(iterator, expectedSnapshotData.size());
        assertEqualsInAnyOrder(expectedSnapshotData, realSnapshotData);

        // second step: check the sink data
        tEnv.executeSql(
                "CREATE TABLE sink ("
                        + " id BIGINT NOT NULL,"
                        + " seller_id STRING,"
                        + " order_id STRING,"
                        + " buyer_id STRING,"
                        + " create_time TIMESTAMP,"
                        + " primary key (id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")");
        TableResult sinkResult = tEnv.executeSql("insert into sink select * from orders_source");

        waitForSinkSize("sink", realSnapshotData.size());
        assertEqualsInAnyOrder(expectedSnapshotData, TestValuesTableFactory.getRawResults("sink"));
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    private String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return format("(%s)", StringUtils.join(captureCustomerTables, "|"));
        }
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + 4);
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    private static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    private static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }
}
