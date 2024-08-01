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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

/** End-to-end tests for mysql cdc pipeline job for timestamp data. */
@RunWith(Parameterized.class)
public class MySqlTimestampE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlTimestampE2eITCase.class);

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    protected static final String INTER_CONTAINER_MYSQL_ALIAS = "mysql";

    @ClassRule
    public static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer(
                                    MySqlVersion.V8_0) // v8 support both ARM and AMD architectures
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases(INTER_CONTAINER_MYSQL_ALIAS)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_timestamp", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Before
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();
    }

    @After
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
    }

    @Test
    public void testSyncWholeDatabase() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName());
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s.timestamps, schema=columns={`id` INT NOT NULL,`ts0` TIMESTAMP_LTZ(0),`ts1` TIMESTAMP_LTZ(1),`ts2` TIMESTAMP_LTZ(2),`ts3` TIMESTAMP_LTZ(3),`ts4` TIMESTAMP_LTZ(4),`ts5` TIMESTAMP_LTZ(5),`ts6` TIMESTAMP_LTZ(6),`dt0` TIMESTAMP(0),`dt1` TIMESTAMP(1),`dt2` TIMESTAMP(2),`dt3` TIMESTAMP(3),`dt4` TIMESTAMP(4),`dt5` TIMESTAMP(5),`dt6` TIMESTAMP(6)}, primaryKeys=id, options=()}",
                        mysqlInventoryDatabase.getDatabaseName()),
                60000L);

        List<String> expectedEvents =
                Arrays.asList(
                        String.format(
                                "DataChangeEvent{tableId=%s.timestamps, before=[], after=[1, 2019-01-01T01:01:01, 2019-01-01T01:01:01.100, 2019-01-01T01:01:01.120, 2019-01-01T01:01:01.123, 2019-01-01T01:01:01.123400, 2019-01-01T01:01:01.123450, 2019-01-01T01:01:01.123456, 2019-01-01T01:01:01, 2019-01-01T01:01:01.100, 2019-01-01T01:01:01.120, 2019-01-01T01:01:01.123, 2019-01-01T01:01:01.123400, 2019-01-01T01:01:01.123450, 2019-01-01T01:01:01.123456], op=INSERT, meta=()}\n",
                                mysqlInventoryDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.timestamps, before=[], after=[2, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01, 2019-01-01T01:01:01], op=INSERT, meta=()}",
                                mysqlInventoryDatabase.getDatabaseName()));
        validateResult(expectedEvents);
        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "INSERT INTO timestamps\n"
                            + "VALUES (\n"
                            + "           3,\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59',\n"
                            + "           '2019-12-31 23:59:59'\n"
                            + "       );");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.timestamps, before=[], after=[3, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59, 2019-12-31T23:59:59], op=INSERT, meta=()}",
                        mysqlInventoryDatabase.getDatabaseName()),
                6000L);
    }

    private void validateResult(List<String> expectedEvents) throws Exception {
        for (String event : expectedEvents) {
            waitUntilSpecificEvent(event, 6000L);
        }
    }

    private void waitUntilSpecificEvent(String event, long timeout) throws Exception {
        boolean result = false;
        long endTimeout = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < endTimeout) {
            String stdout = taskManagerConsumer.toUtf8String();
            if (stdout.contains(event)) {
                result = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (!result) {
            throw new TimeoutException(
                    "failed to get specific event: "
                            + event
                            + " from stdout: "
                            + taskManagerConsumer.toUtf8String());
        }
    }
}
