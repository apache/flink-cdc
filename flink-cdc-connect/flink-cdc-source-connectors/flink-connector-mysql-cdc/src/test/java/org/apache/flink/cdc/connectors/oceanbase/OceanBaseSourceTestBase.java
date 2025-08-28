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

package org.apache.flink.cdc.connectors.oceanbase;

import org.apache.flink.api.common.JobID;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import io.debezium.connector.mysql.MySqlConnection;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;

/** Basic class for testing Database OceanBase. */
@Testcontainers
public abstract class OceanBaseSourceTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseSourceTestBase.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    protected static final Integer INNER_PORT = 2883;
    protected static final String USER_NAME = "root@test";
    protected static final String PASSWORD = "123456";
    protected static final Duration WAITING_TIMEOUT = Duration.ofMinutes(5);

    public static final Network NETWORK = Network.newNetwork();

    @SuppressWarnings("resource")
    @Container
    public static final GenericContainer<?> OB_BINLOG_CONTAINER =
            new GenericContainer<>("quay.io/oceanbase/obbinlog-ce:4.2.5-test")
                    .withNetwork(NETWORK)
                    .withStartupTimeout(WAITING_TIMEOUT)
                    .withExposedPorts(2881, 2883)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .waitingFor(
                            new LogMessageWaitStrategy()
                                    .withRegEx(".*OBBinlog is ready!.*")
                                    .withTimes(1)
                                    .withStartupTimeout(Duration.ofMinutes(6)));

    protected static String getHost() {
        return OB_BINLOG_CONTAINER.getHost();
    }

    protected static int getPort() {
        return OB_BINLOG_CONTAINER.getMappedPort(INNER_PORT);
    }

    protected static String getUserName() {
        return USER_NAME;
    }

    protected static String getPassword() {
        return PASSWORD;
    }

    protected static String getJdbcUrl() {
        return String.format("jdbc:mysql://%s:%s", getHost(), getPort());
    }

    protected static Connection getJdbcConnection() throws SQLException {
        String jdbcUrl = getJdbcUrl();
        LOG.info("jdbcUrl is :" + jdbcUrl);
        return DriverManager.getConnection(jdbcUrl, USER_NAME, PASSWORD);
    }

    /** initialize database and tables with ${databaseName}.sql for testing. */
    protected static void initializeOceanBaseTables(
            String ddlName, String dbName, Function<String, Boolean> filter)
            throws InterruptedException {
        final String ddlFile = String.format("ddl/%s.sql", ddlName);
        final URL ddlTestFile = OceanBaseSourceTestBase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();
        // need to sleep 1s, make sure the jdbc connection can be created
        Thread.sleep(1000);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + dbName);
            statement.execute("create database if not exists " + dbName);
            statement.execute("use " + dbName + ";");
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
                            .filter(sql -> filter == null || filter.apply(sql))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static void dropDatabase(String dbName) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + dbName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Row row = iter.next();
            rows.add(row.toString());
        }
        return rows;
    }

    protected String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return format("(%s)", StringUtils.join(captureCustomerTables, "|"));
        }
    }

    protected String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + 4);
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    protected static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    protected static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    protected static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    protected static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        Assertions.assertThat(actual).containsExactlyElementsOf(expected);
    }

    protected static String getRandomSuffix() {
        String base = UUID.randomUUID().toString().replaceAll("-", "");
        if (base.length() > 10) {
            return base.substring(0, 11);
        }
        return base;
    }

    /** The type of failover. */
    protected enum FailoverType {
        TM,
        JM,
        NONE
    }

    /** The phase of failover. */
    protected enum FailoverPhase {
        SNAPSHOT,
        BINLOG,
        NEVER
    }

    protected static void triggerFailover(
            FailoverType type, JobID jobId, MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        switch (type) {
            case TM:
                restartTaskManager(miniCluster, afterFailAction);
                break;
            case JM:
                triggerJobManagerFailover(jobId, miniCluster, afterFailAction);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    protected static void triggerJobManagerFailover(
            JobID jobId, MiniCluster miniCluster, Runnable afterFailAction) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    protected static void restartTaskManager(MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    protected MySqlConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", getHost());
        properties.put("database.port", String.valueOf(getPort()));
        properties.put("database.user", getUserName());
        properties.put("database.password", getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        return DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }
}
