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

package org.apache.flink.cdc.connectors.mysql.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSinkFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.lifecycle.Startables;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Mysql sink test base. */
public class MySqlSinkTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(MySqlSinkTestBase.class);
    protected static final int DEFAULT_PARALLELISM = 1;
    public static final Duration DEFAULT_STARTUP_TIMEOUT_SECONDS = Duration.ofSeconds(240);
    protected static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer();

    private static MySqlContainer createMySqlContainer() {
        return new MySqlContainer(MySqlVersion.V8_0);
    }

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @BeforeClass
    public static void startContainers() throws Exception {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Waiting for backends to be available");
        waitForMySqlContainerToBeReady(MYSQL_CONTAINER);
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        MYSQL_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    public static void executeSql(String sql) {
        try {
            Container.ExecResult rs =
                    MYSQL_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-h127.0.0.1",
                            "-P" + MySqlContainer.MYSQL_PORT,
                            "-u" + MYSQL_CONTAINER.getUsername(),
                            "-p" + MYSQL_CONTAINER.getPassword(),
                            "-e " + sql);

            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to execute SQL." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute SQL.", e);
        }
    }

    public static boolean waitForMySqlContainerToBeReady(MySqlContainer container)
            throws InterruptedException, TimeoutException {
        long deadline = System.currentTimeMillis() + DEFAULT_STARTUP_TIMEOUT_SECONDS.toMillis();

        while (System.currentTimeMillis() < deadline) {
            try {
                Container.ExecResult rs =
                        container.execInContainer(
                                "mysql",
                                "--protocol=TCP",
                                "-P" + MySqlContainer.MYSQL_PORT,
                                "-u" + container.getUsername(),
                                "-p" + container.getPassword(),
                                "-h127.0.0.1",
                                "-e SELECT 72067");

                if (rs.getExitCode() != 0) {
                    return false;
                }
                String output = rs.getStdout();
                LOG.info("MySQL backend status: {}", output);
                if (output.contains("72067")) {
                    return true;
                }
            } catch (Exception e) {
                LOG.warn("Failed to check backend status.", e);
            }
            Thread.sleep(1000);
        }
        throw new TimeoutException("Failed to wait for container to start.");
    }

    public List<String> inspectTableSchema(TableId tableId) throws SQLException {
        List<String> results = new ArrayList<>();
        ResultSet rs =
                MYSQL_CONTAINER
                        .createConnection("")
                        .createStatement()
                        .executeQuery(
                                String.format(
                                        "DESCRIBE `%s`.`%s`",
                                        tableId.getSchemaName(), tableId.getTableName()));

        while (rs.next()) {
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                columns.add(rs.getString(i));
            }
            results.add(String.join(" | ", columns));
        }
        return results;
    }

    public List<String> inspectTableContent(TableId tableId, int columnCount) throws SQLException {
        List<String> results = new ArrayList<>();
        ResultSet rs =
                MYSQL_CONTAINER
                        .createConnection("")
                        .createStatement()
                        .executeQuery(
                                String.format(
                                        "SELECT * FROM `%s`.`%s`",
                                        tableId.getSchemaName(), tableId.getTableName()));

        while (rs.next()) {
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                columns.add(rs.getString(i));
            }
            results.add(String.join(" | ", columns));
        }
        return results;
    }

    public static DataSink createMySqlDataSink(Configuration factoryConfiguration) {
        MySqlDataSinkFactory factory = new MySqlDataSinkFactory();
        return factory.createDataSink(new MockContext(factoryConfiguration));
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertThat(expected != null && actual != null).isTrue();
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertThat(expected != null && actual != null).isTrue();
        assertThat(actual).hasSameSizeAs(expected);
        assertThat(actual.toArray(new String[0])).isEqualTo(expected.toArray(new String[0]));
    }

    static class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return Configuration.fromMap(Collections.singletonMap("local-time-zone", "UTC"));
        }

        @Override
        public ClassLoader getClassLoader() {
            return this.getClassLoader();
        }
    }
}
