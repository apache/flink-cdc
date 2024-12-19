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

package org.apache.flink.cdc.connectors.starrocks.sink.utils;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSink;
import org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkFactory;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** Basic class for testing {@link StarRocksDataSink}. */
public class StarRocksSinkTestBase extends TestLogger {
    protected static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkTestBase.class);

    protected static final int DEFAULT_PARALLELISM = 1;

    protected static final StarRocksContainer STARROCKS_CONTAINER = createStarRocksContainer();

    public static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;

    private static StarRocksContainer createStarRocksContainer() {
        return new StarRocksContainer();
    }

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(STARROCKS_CONTAINER)).join();
        LOG.info("Waiting for StarRocks to launch");

        long startWaitingTimestamp = System.currentTimeMillis();

        new LogMessageWaitStrategy()
                .withRegEx(".*Enjoy the journal to StarRocks blazing-fast lake-house engine!.*\\s")
                .withTimes(1)
                .withStartupTimeout(
                        Duration.of(DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS))
                .waitUntilReady(STARROCKS_CONTAINER);

        while (!checkBackendAvailability()) {
            try {
                if (System.currentTimeMillis() - startWaitingTimestamp
                        > DEFAULT_STARTUP_TIMEOUT_SECONDS * 1000) {
                    throw new RuntimeException("StarRocks backend startup timed out.");
                }
                LOG.info("Waiting for backends to be available");
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                // ignore and check next round
            }
        }
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        STARROCKS_CONTAINER.stop();
        LOG.info("Containers are stopped.");
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
            return null;
        }
    }

    public static DataSink createStarRocksDataSink(Configuration factoryConfiguration) {
        StarRocksDataSinkFactory factory = new StarRocksDataSinkFactory();
        return factory.createDataSink(new MockContext(factoryConfiguration));
    }

    public static void executeSql(String sql) {
        try {
            Container.ExecResult rs =
                    STARROCKS_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            "-e " + sql);

            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to execute SQL." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute SQL.", e);
        }
    }

    public static boolean checkBackendAvailability() {
        try {
            Container.ExecResult rs =
                    STARROCKS_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            "-e SHOW BACKENDS\\G");

            if (rs.getExitCode() != 0) {
                return false;
            }
            return rs.getStdout()
                    .contains("*************************** 1. row ***************************");
        } catch (Exception e) {
            LOG.info("Failed to check backend status.", e);
            return false;
        }
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    public List<String> inspectTableSchema(TableId tableId) throws SQLException {
        List<String> results = new ArrayList<>();
        ResultSet rs =
                STARROCKS_CONTAINER
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

    public List<String> fetchTableContent(TableId tableId, int columnCount) throws SQLException {
        List<String> results = new ArrayList<>();
        ResultSet rs =
                STARROCKS_CONTAINER
                        .createConnection("")
                        .createStatement()
                        .executeQuery(
                                String.format(
                                        "SELECT * FROM %s.%s",
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

    public static <T> void assertEqualsInAnyOrder(List<T> expected, List<T> actual) {
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    public static <T> void assertEqualsInOrder(List<T> expected, List<T> actual) {
        Assertions.assertThat(actual).containsExactlyElementsOf(expected);
    }

    public static <K, V> void assertMapEquals(Map<K, V> expected, Map<K, V> actual) {
        Assertions.assertThat(actual).containsExactlyEntriesOf(expected);
    }
}
