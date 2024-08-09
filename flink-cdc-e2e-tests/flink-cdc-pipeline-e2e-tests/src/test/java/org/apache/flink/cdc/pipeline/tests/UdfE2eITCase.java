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

/** E2e tests for User-defined functions. */
@RunWith(Parameterized.class)
public class UdfE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(TransformE2eITCase.class);

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

    protected final UniqueDatabase transformRenameDatabase =
            new UniqueDatabase(MYSQL, "transform_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Before
    public void before() throws Exception {
        super.before();
        transformRenameDatabase.createAndInitialize();
    }

    @After
    public void after() {
        super.after();
        transformRenameDatabase.dropDatabase();
    }

    @Test
    public void testUserDefinedFunctionsInJava() throws Exception {
        testUserDefinedFunctions("java");
    }

    @Test
    public void testUserDefinedFunctionsInScala() throws Exception {
        testUserDefinedFunctions("scala");
    }

    @Test
    public void testFlinkCompatibleScalarFunctionsInJava() throws Exception {
        testFlinkCompatibleScalarFunctions("java");
    }

    @Test
    public void testFlinkCompatibleScalarFunctionsInScala() throws Exception {
        testFlinkCompatibleScalarFunctions("scala");
    }

    private void testUserDefinedFunctions(String language) throws Exception {
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
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: ID, VERSION, addone(addone(ID)) AS INC_ID, format('<%%s>', VERSION) AS FMT_VER\n"
                                + "    filter: addone(ID) <> '1009'\n"
                                + "  - source-table: %s.TABLEBETA\n"
                                + "    projection: ID, VERSION, answer() AS ANS, typeof(ID) AS TYP\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1\n"
                                + "  user-defined-function:\n"
                                + "    - name: addone\n"
                                + "      classpath: org.apache.flink.cdc.udf.examples.%s.AddOneFunctionClass\n"
                                + "    - name: format\n"
                                + "      classpath: org.apache.flink.cdc.udf.examples.%s.FormatFunctionClass\n"
                                + "    - name: lifecycle\n"
                                + "      classpath: org.apache.flink.cdc.udf.examples.%s.LifecycleFunctionClass\n"
                                + "    - name: typeof\n"
                                + "      classpath: org.apache.flink.cdc.udf.examples.%s.TypeOfFunctionClass\n"
                                + "    - name: answer\n"
                                + "      classpath: org.apache.flink.cdc.udf.examples.%s.TypeHintFunctionClass\n",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        transformRenameDatabase.getDatabaseName(),
                        transformRenameDatabase.getDatabaseName(),
                        transformRenameDatabase.getDatabaseName(),
                        language,
                        language,
                        language,
                        language,
                        language);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        Path udfJar = TestUtils.getResource("udf-examples.jar");
        Path scalaLibJar = TestUtils.getResource("scala-library.jar");
        submitPipelineJob(
                pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar, udfJar, scalaLibJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitUntilSpecificEvent("[ LifecycleFunction ] opened.", 60000L);

        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 1013, <11>], op=INSERT, meta=()}",
                        transformRenameDatabase.getDatabaseName()),
                60000L);

        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 14, Forty-two, Integer: 2014], op=INSERT, meta=()}",
                        transformRenameDatabase.getDatabaseName()),
                60000L);

        List<String> expectedEvents =
                Arrays.asList(
                        String.format(
                                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`INC_ID` STRING,`FMT_VER` STRING}, primaryKeys=ID, options=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1, 1011, <8.1>], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10, 1012, <10>], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 1013, <11>], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`ANS` STRING,`TYP` STRING}, primaryKeys=ID, options=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2011, 11, Forty-two, Integer: 2011], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2012, 12, Forty-two, Integer: 2012], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2013, 13, Forty-two, Integer: 2013], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 14, Forty-two, Integer: 2014], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()));
        validateResult(expectedEvents);
        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformRenameDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 0, 0);");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, 8.1, 1011, <8.1>], after=[1009, 100, 1011, <100>], op=UPDATE, meta=()}",
                        transformRenameDatabase.getDatabaseName()),
                20000L);

        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7, 3009, <7>], op=INSERT, meta=()}",
                        transformRenameDatabase.getDatabaseName()),
                20000L);

        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.TABLEBETA, before=[2011, 11, Forty-two, Integer: 2011], after=[], op=DELETE, meta=()}",
                        transformRenameDatabase.getDatabaseName()),
                20000L);
    }

    private void testFlinkCompatibleScalarFunctions(String language) throws Exception {
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
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: ID, VERSION, addone(addone(ID)) AS INC_ID, format('<%%s>', VERSION) AS FMT_VER\n"
                                + "    filter: addone(ID) <> '1009'\n"
                                + "  - source-table: %s.TABLEBETA\n"
                                + "    projection: ID, VERSION, typeof(ID) AS TYP\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1\n"
                                + "  user-defined-function:\n"
                                + "    - name: addone\n"
                                + "      classpath: org.apache.flink.udf.examples.%s.AddOneFunctionClass\n"
                                + "    - name: format\n"
                                + "      classpath: org.apache.flink.udf.examples.%s.FormatFunctionClass\n"
                                + "    - name: typeof\n"
                                + "      classpath: org.apache.flink.udf.examples.%s.TypeOfFunctionClass\n",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        transformRenameDatabase.getDatabaseName(),
                        transformRenameDatabase.getDatabaseName(),
                        transformRenameDatabase.getDatabaseName(),
                        language,
                        language,
                        language);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        Path udfJar = TestUtils.getResource("udf-examples.jar");
        Path scalaLibJar = TestUtils.getResource("scala-library.jar");
        submitPipelineJob(
                pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar, udfJar, scalaLibJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 1013, <11>], op=INSERT, meta=()}",
                        transformRenameDatabase.getDatabaseName()),
                60000L);

        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 14, Integer: 2014], op=INSERT, meta=()}",
                        transformRenameDatabase.getDatabaseName()),
                60000L);

        List<String> expectedEvents =
                Arrays.asList(
                        String.format(
                                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`INC_ID` STRING,`FMT_VER` STRING}, primaryKeys=ID, options=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1, 1011, <8.1>], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10, 1012, <10>], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 1013, <11>], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`TYP` STRING}, primaryKeys=ID, options=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2011, 11, Integer: 2011], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2012, 12, Integer: 2012], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2013, 13, Integer: 2013], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()),
                        String.format(
                                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 14, Integer: 2014], op=INSERT, meta=()}",
                                transformRenameDatabase.getDatabaseName()));
        validateResult(expectedEvents);
        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformRenameDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 0, 0);");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, 8.1, 1011, <8.1>], after=[1009, 100, 1011, <100>], op=UPDATE, meta=()}",
                        transformRenameDatabase.getDatabaseName()),
                20000L);

        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7, 3009, <7>], op=INSERT, meta=()}",
                        transformRenameDatabase.getDatabaseName()),
                20000L);

        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.TABLEBETA, before=[2011, 11, Integer: 2011], after=[], op=DELETE, meta=()}",
                        transformRenameDatabase.getDatabaseName()),
                20000L);
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
