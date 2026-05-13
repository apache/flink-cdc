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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ExecConfig;
import org.testcontainers.containers.GenericContainer;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/** E2e tests for pipelines that use Python UDFs. */
class PythonUdfE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(PythonUdfE2eITCase.class);

    private static final String PYTHON_UDF_CLASSPATH = "org.apache.flink.cdc.python.PythonUdf";
    private static final String CONTAINER_PYTHON_EXECUTABLE = "/usr/bin/python3";
    private static final String PEMJA_VERSION = "0.5.5";
    private static final Path pythonUdfJar =
            TestUtils.getResource("flink-cdc-pipeline-udf-python.jar").toAbsolutePath();

    protected final UniqueDatabase pyUdfTestDatabase =
            new UniqueDatabase(MYSQL, "python_udf_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private final Function<String, String> dbNameFormatter =
            s -> String.format(s, pyUdfTestDatabase.getDatabaseName());

    @BeforeEach
    public void initializeDatabaseAndPython() throws Exception {
        pyUdfTestDatabase.createAndInitialize();
        // Only TM requires Python at runtime. Neither JM nor job submitter requires that.
        installPythonAndPemja(taskManager);
    }

    @AfterEach
    public void destroyDatabase() {
        pyUdfTestDatabase.dropDatabase();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testNormalizeEmail(int parallelism) throws Exception {
        runGenericTest(
                parallelism,
                "ID, py_normalize(EMAIL) AS EMAIL_NORM",
                null,
                List.of(
                        udf(
                                "py_normalize",
                                "def eval(s: str) -> str:",
                                "    if s is None:",
                                "        return None",
                                "    return s.strip().lower()")),
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`EMAIL_NORM` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[1, alice@example.com], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[2, bob@test.io], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, carol@example.com], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, dave@example.org], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[5, null], op=INSERT, meta=()}");
    }

    @Test
    void testEmailDomain() throws Exception {
        runGenericTest(
                1,
                "ID, py_domain(EMAIL) AS DOMAIN",
                null,
                List.of(
                        udf(
                                "py_domain",
                                "def eval(s: str) -> str:",
                                "    if s is None:",
                                "        return None",
                                "    return s.strip().lower().split('@')[-1]")),
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`DOMAIN` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[1, example.com], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[2, test.io], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, example.com], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, example.org], op=INSERT, meta=()}");
    }

    @Test
    void testIntDoubling() throws Exception {
        runGenericTest(
                1,
                "ID, py_double(AGE) AS DOUBLED",
                null,
                List.of(
                        udf(
                                "py_double",
                                "def eval(x: int) -> int:",
                                "    if x is None:",
                                "        return None",
                                "    return x * 2")),
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`DOUBLED` BIGINT}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[1, 56], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[2, 50], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, 70], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, 84], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[5, null], op=INSERT, meta=()}");
    }

    @Test
    void testBytesToHex() throws Exception {
        runGenericTest(
                1,
                "ID, py_to_hex(AVATAR) AS HEX",
                null,
                List.of(
                        udf(
                                "py_to_hex",
                                "def eval(b: bytes) -> str:",
                                "    if b is None:",
                                "        return None",
                                "    return b.hex()")),
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`HEX` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[1, 48656c6c6f], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[2, 576f726c64], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, 00ff7f], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, cafebabe], op=INSERT, meta=()}");
    }

    @Test
    void testBoolReturnUsedInFilter() throws Exception {
        runGenericTest(
                1,
                "ID, EMAIL",
                "py_age_ge_30(AGE)",
                List.of(
                        udf(
                                "py_age_ge_30",
                                "def eval(age: int) -> bool:",
                                "    return age is not None and age >= 30")),
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`EMAIL` VARCHAR(128)}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, carol@example.com], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, DAVE@example.org], op=INSERT, meta=()}");
    }

    @Test
    void testFloatReturnFromScore() throws Exception {
        runGenericTest(
                1,
                "ID, py_twice_score(SCORE) AS TWICE",
                null,
                List.of(
                        udf(
                                "py_twice_score",
                                "def eval(x: float) -> float:",
                                "    if x is None:",
                                "        return None",
                                "    return x * 2.0")),
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`TWICE` DOUBLE}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[1, 175.0], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[2, 128.0], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, 185.0], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, 156.0], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[5, null], op=INSERT, meta=()}");
    }

    @Test
    void testBytesReturnReversedAvatar() throws Exception {
        runGenericTest(
                1,
                "ID, AVATAR, py_reverse_bytes(AVATAR) AS REV",
                null,
                List.of(
                        udf(
                                "py_reverse_bytes",
                                "def eval(b: bytes) -> bytes:",
                                "    if b is None:",
                                "        return None",
                                "    return bytes(reversed(b))")),
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`AVATAR` VARBINARY(64),`REV` VARBINARY(65536)}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[1, SGVsbG8=, b2xsZUg=], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[2, V29ybGQ=, ZGxyb1c=], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, AP9/, f/8A], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, yv66vg==, vrr+yg==], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[5, null, null], op=INSERT, meta=()}");
    }

    @Test
    void testStatefulCounter() throws Exception {
        // Each PythonUdf instance has its own lifecycle.
        runGenericTest(
                1,
                "ID, py_counter(ID) AS SEQ",
                null,
                List.of(
                        udf(
                                "py_counter",
                                "_counter = 0",
                                "def eval(_id: int) -> int:",
                                "    global _counter",
                                "    _counter += 7",
                                "    return _counter")),
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`SEQ` BIGINT}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[1, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[2, 14], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, 21], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, 28], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[5, 35], op=INSERT, meta=()}");
    }

    @Test
    void testStatefulRunningTotal() throws Exception {
        runGenericTest(
                1,
                "ID, py_running_sum(AGE) AS TOTAL_AGE",
                null,
                List.of(
                        udf(
                                "py_running_sum",
                                "_total = 0",
                                "def eval(age: int) -> int:",
                                "    global _total",
                                "    if age is not None:",
                                "        _total += age",
                                "    return _total")),
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`TOTAL_AGE` BIGINT}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[1, 28], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[2, 53], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, 88], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, 130], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[5, 130], op=INSERT, meta=()}");
    }

    @Test
    void testMultiplePythonUdfs() throws Exception {
        runGenericTest(
                1,
                "ID, py_normalize(EMAIL) AS EMAIL_NORM, py_double(AGE) AS DOUBLED",
                null,
                List.of(
                        udf(
                                "py_normalize",
                                "def eval(s: str) -> str:",
                                "    if s is None:",
                                "        return None",
                                "    return s.strip().lower() if s else s"),
                        udf(
                                "py_double",
                                "def eval(x: int) -> int:",
                                "    if x is None:",
                                "        return None",
                                "    return x * 2 if x else x")),
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`EMAIL_NORM` STRING,`DOUBLED` BIGINT}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[1, alice@example.com, 56], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[2, bob@test.io, 50], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, carol@example.com, 70], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, dave@example.org, 84], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[5, null, null], op=INSERT, meta=()}");
    }

    @Test
    void testEvalRaisesAtRuntime() throws Exception {
        submitPipelineJob(
                buildJobYaml(
                        1,
                        "ID, py_boom(ID) AS X",
                        null,
                        List.of(
                                udf(
                                        "py_boom",
                                        "def eval(_id: int) -> int:",
                                        "    raise ValueError('boom')"))),
                pythonUdfJar);
        validateResult(
                dbNameFormatter,
                "Failed to evaluate projection expression `PY_BOOM(`TB`.`ID`)` for column `X` in table `%s.USERS`.",
                "<class 'ValueError'>: boom");
    }

    @Test
    void testEvalReturnAnnotationMissing() throws Exception {
        submitPipelineJob(
                buildJobYaml(
                        1,
                        "ID, py_no_ret(ID) AS X",
                        null,
                        List.of(udf("py_no_ret", "def eval(_id: int):", "    return _id"))),
                pythonUdfJar);
        waitUntilSpecificEvent("Function 'eval' has no return type annotation.");
    }

    @Test
    void testEvalReturnAnnotationUnsupported() throws Exception {
        submitPipelineJob(
                buildJobYaml(
                        1,
                        "ID, py_unsupported(ID) AS X",
                        null,
                        List.of(
                                udf(
                                        "py_unsupported",
                                        "def eval(_id: int) -> bytearray:",
                                        "    return bytearray(_id)"))),
                pythonUdfJar);
        waitUntilSpecificEvent(
                "Unsupported Python UDF return type 'bytearray'. Supported annotations: [bool, bytes, float, int, str]");
    }

    @Test
    void testEvalMissing() throws Exception {
        submitPipelineJob(
                buildJobYaml(
                        1,
                        "ID, py_no_eval(ID) AS X",
                        null,
                        List.of(
                                udf(
                                        "py_no_eval",
                                        "def other(_id: int) -> int:",
                                        "    return _id"))),
                pythonUdfJar);
        waitUntilSpecificEvent("Python UDF source does not define a top-level 'eval' function.");
    }

    private static UdfEntry udf(String name, String... code) {
        return new UdfEntry(name, String.join("\n", code));
    }

    private void runGenericTest(
            int parallelism,
            String projection,
            @Nullable String filter,
            List<UdfEntry> udfs,
            String... expectedEvents)
            throws Exception {
        submitPipelineJob(buildJobYaml(parallelism, projection, filter, udfs), pythonUdfJar);
        waitUntilJobRunning(Duration.ofSeconds(60));
        validateResult(dbNameFormatter, expectedEvents);
    }

    private String buildJobYaml(
            int parallelism, String projection, @Nullable String filter, List<UdfEntry> udfs) {
        StringBuilder transform =
                new StringBuilder("transform:\n")
                        .append("  - source-table: ")
                        .append(pyUdfTestDatabase.getDatabaseName())
                        .append(".USERS\n")
                        .append("    projection: ")
                        .append(projection)
                        .append('\n');
        if (filter != null) {
            transform.append("    filter: ").append(filter).append('\n');
        }

        StringBuilder udfsYaml = new StringBuilder();
        for (UdfEntry udf : udfs) {
            String indentedSource =
                    Arrays.stream(udf.source.split("\n", -1))
                            .map(line -> line.isEmpty() ? "" : "          " + line)
                            .collect(Collectors.joining("\n"));
            udfsYaml.append("    - name: ").append(udf.name).append('\n');
            udfsYaml.append("      classpath: ").append(PYTHON_UDF_CLASSPATH).append('\n');
            udfsYaml.append("      options:\n");
            udfsYaml.append("        python-executable: ")
                    .append(CONTAINER_PYTHON_EXECUTABLE)
                    .append('\n');
            udfsYaml.append("        source: |\n");
            udfsYaml.append(indentedSource).append('\n');
        }

        return String.format(
                "source:\n"
                        + "  type: mysql\n"
                        + "  hostname: %s\n"
                        + "  port: 3306\n"
                        + "  username: %s\n"
                        + "  password: %s\n"
                        + "  scan.startup.mode: earliest-offset\n"
                        + "  tables: %s.USERS\n"
                        + "  server-id: 5400-5404\n"
                        + "  server-time-zone: UTC\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: values\n"
                        + "%s"
                        + "\n"
                        + "pipeline:\n"
                        + "  parallelism: %d\n"
                        + "  user-defined-function:\n"
                        + "%s",
                INTER_CONTAINER_MYSQL_ALIAS,
                MYSQL_TEST_USER,
                MYSQL_TEST_PASSWORD,
                pyUdfTestDatabase.getDatabaseName(),
                transform,
                parallelism,
                udfsYaml);
    }

    private void installPythonAndPemja(GenericContainer<?> container) throws Exception {
        LOG.info(
                "Installing Python + Pemja {} into {}",
                PEMJA_VERSION,
                container.getDockerImageName());
        boolean isFlink2 = flinkVersion.startsWith("2");
        String script =
                "set -euo pipefail; "
                        + "apt-get update && "
                        + "apt-get install -y --no-install-recommends python3 python3-pip python3-dev && "
                        + "rm -rf /var/lib/apt/lists/* && "
                        + "pip3 install "
                        + (isFlink2 ? "--break-system-packages" : "")
                        + " --no-cache-dir pemja=="
                        + PEMJA_VERSION
                        + " && "
                        + "test -x "
                        + CONTAINER_PYTHON_EXECUTABLE
                        + " && "
                        + CONTAINER_PYTHON_EXECUTABLE
                        + " -c 'import pemja'";
        Container.ExecResult result =
                container.execInContainer(
                        ExecConfig.builder()
                                .user("root")
                                .command(new String[] {"bash", "-c", script})
                                .build());
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                    "Failed to install Pemja into "
                            + container.getDockerImageName()
                            + " (exit="
                            + result.getExitCode()
                            + ").\nstdout:\n"
                            + result.getStdout()
                            + "\nstderr:\n"
                            + result.getStderr());
        }
    }

    private static final class UdfEntry {
        final String name;
        final String source;

        UdfEntry(String name, String source) {
            this.name = name;
            this.source = source;
        }
    }
}
