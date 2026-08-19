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

import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ExecConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/** E2e tests for pipelines that use Python UDFs. */
class PythonUdfE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(PythonUdfE2eITCase.class);

    private static final String CONTAINER_PYTHON_EXECUTABLE = "/usr/bin/python3";
    private static final String PYTHON_FILES_DIRECTORY = "/opt/flink/python-deps";
    private static final String PEMJA_VERSION = "0.5.7";

    private final UniqueDatabase pythonUdfTestDatabase =
            new UniqueDatabase(MYSQL, "python_udf_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private final Function<String, String> databaseNameFormatter =
            event -> String.format(event, pythonUdfTestDatabase.getDatabaseName());

    @BeforeEach
    void initializeDatabaseAndPython() throws Exception {
        pythonUdfTestDatabase.createAndInitialize();
        installPythonAndPemja(taskManager);
        preparePythonFiles(taskManager);
    }

    @AfterEach
    void destroyDatabase() {
        pythonUdfTestDatabase.dropDatabase();
    }

    @Test
    void testMultiplePythonUdfsWithFilterAndPythonFiles() throws Exception {
        String pipelineJob =
                buildJobYaml(
                        "ID, py_normalize(EMAIL) AS EMAIL_NORM, py_double(AGE) AS DOUBLED",
                        "py_age_ge_30(AGE)",
                        Arrays.asList(
                                new UdfEntry(
                                        "py_normalize",
                                        "from python_udf_helpers import normalize_email\n"
                                                + "def eval(value: str) -> str:\n"
                                                + "    return normalize_email(value)",
                                        PYTHON_FILES_DIRECTORY),
                                new UdfEntry(
                                        "py_double",
                                        "def eval(value: int) -> int:\n"
                                                + "    return None if value is None else value * 2",
                                        null),
                                new UdfEntry(
                                        "py_age_ge_30",
                                        "def eval(value: int) -> bool:\n"
                                                + "    return value is not None and value >= 30",
                                        null)));

        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(60));
        validateResult(
                databaseNameFormatter,
                "CreateTableEvent{tableId=%s.USERS, schema=columns={`ID` INT NOT NULL,`EMAIL_NORM` STRING,`DOUBLED` BIGINT}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[3, carol@example.com, 70], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.USERS, before=[], after=[4, dave@example.org, 84], op=INSERT, meta=()}");
    }

    private String buildJobYaml(
            String projection, @Nullable String filter, List<UdfEntry> userDefinedFunctions) {
        StringBuilder transform =
                new StringBuilder("transform:\n")
                        .append("  - source-table: ")
                        .append(pythonUdfTestDatabase.getDatabaseName())
                        .append(".USERS\n")
                        .append("    projection: ")
                        .append(projection)
                        .append('\n');
        if (filter != null) {
            transform.append("    filter: ").append(filter).append('\n');
        }

        StringBuilder udfYaml = new StringBuilder();
        for (UdfEntry udf : userDefinedFunctions) {
            String indentedSource =
                    Arrays.stream(udf.source.split("\n", -1))
                            .map(line -> line.isEmpty() ? "" : "        " + line)
                            .collect(Collectors.joining("\n"));
            udfYaml.append("    - name: ").append(udf.name).append('\n');
            udfYaml.append("      python-code: |\n");
            udfYaml.append(indentedSource).append('\n');
            udfYaml.append("      python-executable: ")
                    .append(CONTAINER_PYTHON_EXECUTABLE)
                    .append('\n');
            if (udf.pythonFiles != null) {
                udfYaml.append("      python-files:\n");
                udfYaml.append("        - ").append(udf.pythonFiles).append('\n');
            }
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
                        + "\n"
                        + "%s"
                        + "\n"
                        + "pipeline:\n"
                        + "  parallelism: %d\n"
                        + "  user-defined-function:\n"
                        + "%s",
                INTER_CONTAINER_MYSQL_ALIAS,
                MYSQL_TEST_USER,
                MYSQL_TEST_PASSWORD,
                pythonUdfTestDatabase.getDatabaseName(),
                transform,
                parallelism,
                udfYaml);
    }

    private void installPythonAndPemja(GenericContainer<?> container) throws Exception {
        LOG.info(
                "Installing Python and Pemja {} into {}",
                PEMJA_VERSION,
                container.getDockerImageName());
        String externallyManagedOption =
                flinkVersion.startsWith("2") ? "--break-system-packages " : "";
        String script =
                "set -euo pipefail; "
                        + "apt-get update && "
                        + "apt-get install -y --no-install-recommends python3 python3-pip python3-dev && "
                        + "rm -rf /var/lib/apt/lists/* && "
                        + "python3 -m pip install "
                        + externallyManagedOption
                        + "--disable-pip-version-check --no-cache-dir pemja=="
                        + PEMJA_VERSION
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

    private void preparePythonFiles(GenericContainer<?> container) throws Exception {
        runInContainerAsRoot(container, "mkdir", "-p", PYTHON_FILES_DIRECTORY);
        container.copyFileToContainer(
                Transferable.of(
                        "def normalize_email(value):\n"
                                + "    return None if value is None else value.strip().lower()\n"),
                PYTHON_FILES_DIRECTORY + "/python_udf_helpers.py");
    }

    private static final class UdfEntry {
        private final String name;
        private final String source;
        @Nullable private final String pythonFiles;

        private UdfEntry(String name, String source, @Nullable String pythonFiles) {
            this.name = name;
            this.source = source;
            this.pythonFiles = pythonFiles;
        }
    }
}
