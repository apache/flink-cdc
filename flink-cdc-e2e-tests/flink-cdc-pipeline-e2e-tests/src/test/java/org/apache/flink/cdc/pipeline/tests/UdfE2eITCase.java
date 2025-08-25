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
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.function.Function;
import java.util.stream.Stream;

/** E2e tests for User-defined functions. */
class UdfE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(TransformE2eITCase.class);

    protected final UniqueDatabase udfTestDatabase =
            new UniqueDatabase(MYSQL, "transform_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private final Function<String, String> dbNameFormatter =
            (s) -> String.format(s, udfTestDatabase.getDatabaseName());

    @BeforeEach
    public void before() throws Exception {
        super.before();
        udfTestDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        udfTestDatabase.dropDatabase();
    }

    private static Stream<Arguments> variants() {
        return Stream.of(
                Arguments.of("java", true),
                Arguments.of("java", false),
                Arguments.of("scala", true),
                Arguments.of("scala", false));
    }

    @ParameterizedTest(name = "language: {0}, batchMode: {1}")
    @MethodSource(value = "variants")
    void testUserDefinedFunctions(String language, boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
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
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: %s\n"
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
                        startupMode,
                        udfTestDatabase.getDatabaseName(),
                        udfTestDatabase.getDatabaseName(),
                        udfTestDatabase.getDatabaseName(),
                        parallelism,
                        runtimeMode,
                        language,
                        language,
                        language,
                        language,
                        language);
        Path udfJar = TestUtils.getResource("udf-examples.jar");
        Path scalaLibJar = TestUtils.getResource("scala-library.jar");
        submitPipelineJob(pipelineJob, udfJar, scalaLibJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        waitUntilSpecificEvent("[ LifecycleFunction ] opened.");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`INC_ID` STRING,`FMT_VER` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1, 1011, <8.1>], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10, 1012, <10>], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 1013, <11>], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`ANS` STRING,`TYP` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2011, 11, Forty-two, Integer: 2011], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2012, 12, Forty-two, Integer: 2012], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2013, 13, Forty-two, Integer: 2013], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 14, Forty-two, Integer: 2014], op=INSERT, meta=()}");

        if (batchMode) {
            return;
        }

        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        udfTestDatabase.getDatabaseName());
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

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, 8.1, 1011, <8.1>], after=[1009, 100, 1011, <100>], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7, 3009, <7>], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[2011, 11, Forty-two, Integer: 2011], after=[], op=DELETE, meta=()}");
    }

    @ParameterizedTest(name = "language: {0}, batchMode: {1}")
    @MethodSource(value = "variants")
    void testFlinkCompatibleScalarFunctions(String language, boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
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
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: %s\n"
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
                        startupMode,
                        udfTestDatabase.getDatabaseName(),
                        udfTestDatabase.getDatabaseName(),
                        udfTestDatabase.getDatabaseName(),
                        parallelism,
                        runtimeMode,
                        language,
                        language,
                        language);
        Path udfJar = TestUtils.getResource("udf-examples.jar");
        Path scalaLibJar = TestUtils.getResource("scala-library.jar");
        submitPipelineJob(pipelineJob, udfJar, scalaLibJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`INC_ID` STRING,`FMT_VER` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1, 1011, <8.1>], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10, 1012, <10>], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 1013, <11>], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`TYP` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2011, 11, Integer: 2011], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2012, 12, Integer: 2012], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2013, 13, Integer: 2013], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 14, Integer: 2014], op=INSERT, meta=()}");

        if (batchMode) {
            return;
        }

        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        udfTestDatabase.getDatabaseName());
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

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, 8.1, 1011, <8.1>], after=[1009, 100, 1011, <100>], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7, 3009, <7>], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[2011, 11, Integer: 2011], after=[], op=DELETE, meta=()}");
    }
}
