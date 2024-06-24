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
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.lifecycle.Startables;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Mysql sink test base. */
public class MySqlSinkTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(MySqlSinkTestBase.class);
    protected static final int DEFAULT_PARALLELISM = 1;
    public static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;
    protected static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer();

    private static MySqlContainer createMySqlContainer() {
        return new MySqlContainer();
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
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Waiting for backends to be available");
        long startWaitingTimestamp = System.currentTimeMillis();

        while (!checkMySqlAvailability()) {
            try {
                if (System.currentTimeMillis() - startWaitingTimestamp
                        > DEFAULT_STARTUP_TIMEOUT_SECONDS * 1000) {
                    throw new RuntimeException("MySql startup timed out.");
                }
                LOG.info("Waiting for mysql to be available");
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                // ignore and check next round
            }
        }

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
                            "-uroot",
                            "-P" + MySqlContainer.MYSQL_PORT,
                            "-p" + MySqlContainer.PASSWORD,
                            "-h127.0.0.1",
                            "-e " + sql);

            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to execute SQL." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute SQL.", e);
        }
    }

    public static boolean checkMySqlAvailability() {
        try {
            Container.ExecResult rs =
                    MYSQL_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P" + MySqlContainer.MYSQL_PORT,
                            "-p" + MySqlContainer.PASSWORD,
                            "-h127.0.0.1",
                            "-e SELECT 1");

            if (rs.getExitCode() != 0) {
                return false;
            }
            String output = rs.getStdout();
            LOG.info("MySQL backend status:\n" + output);
            return output.contains("1");
        } catch (Exception e) {
            LOG.warn("Failed to check backend status: " + e.getMessage());
            return false;
        }
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

    public static DataSink createMySqlDataSink(Configuration factoryConfiguration) {
        MySqlDataSinkFactory factory = new MySqlDataSinkFactory();
        return factory.createDataSink(new MockContext(factoryConfiguration));
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        Assertions.assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        Assertions.assertTrue(expected != null && actual != null);
        Assertions.assertEquals(expected.size(), actual.size());
        Assertions.assertArrayEquals(
                expected.toArray(new String[0]), actual.toArray(new String[0]));
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
