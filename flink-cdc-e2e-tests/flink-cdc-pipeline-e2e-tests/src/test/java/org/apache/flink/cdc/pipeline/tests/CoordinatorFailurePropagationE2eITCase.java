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
import org.apache.flink.cdc.pipeline.tests.utils.JobManagerOnlyException;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * E2e test for FLINK-40040: a failure raised on the SchemaRegistry coordinator must survive the
 * operator-coordinator RPC boundary even if the receiving side cannot load its exception class.
 *
 * <p>To reproduce the classpath asymmetry seen in production (e.g. a JDBC driver installed in the
 * JobManager's {@code lib/} directory but absent from the TaskManager), the test packages {@link
 * JobManagerOnlyException} into a jar that is copied into the JobManager container only. The values
 * sink's metadata applier then raises it on the coordinator as the cause of a rejected schema
 * change.
 *
 * <p>Without wrapping the failure into a {@code SerializedThrowable}, the exceptionally completed
 * coordination response cannot be deserialized by the TaskManager's RPC layer (which cannot load
 * user classes). The deserialization failure tears down the whole JobManager <-> TaskManager Pekko
 * association ("Association with remote system ... has failed, address is now gated"), so
 * cancellation stalls for a full RPC timeout cycle (around a minute) before the job manages to
 * reach FAILED, and the original cause never surfaces on the TaskManager — with a restart strategy
 * configured, every restart attempt repeats this stall, turning a transient coordinator error into
 * a restart loop with only misleading timeout errors.
 */
class CoordinatorFailurePropagationE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG =
            LoggerFactory.getLogger(CoordinatorFailurePropagationE2eITCase.class);

    private static final String JM_ONLY_EXCEPTION_CLASS = JobManagerOnlyException.class.getName();

    private static final String JM_ONLY_EXCEPTION_JAR = "jm-only-exception.jar";

    // Keep in sync with ValuesDatabase.ErrorOnChangeMetadataApplier#SIMULATED_CAUSE_MESSAGE.
    private static final String SIMULATED_CAUSE_MESSAGE =
            "Simulated cause for rejected schema change events.";

    protected final UniqueDatabase schemaEvolveDatabase =
            new UniqueDatabase(MYSQL, "schema_evolve", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Override
    protected List<String> copyJarToFlinkLib() {
        // Installs the jar into the JobManager container's /opt/flink/lib only; the TaskManager
        // container never gets a copy, and the jar is not submitted with the pipeline either.
        generateJobManagerOnlyExceptionJar();
        return Collections.singletonList(JM_ONLY_EXCEPTION_JAR);
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        schemaEvolveDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        schemaEvolveDatabase.dropDatabase();
    }

    @Test
    void testCoordinatorFailureWithJobManagerOnlyClassReachesTaskManager() throws Exception {
        String dbName = schemaEvolveDatabase.getDatabaseName();

        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.members\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "  error.on.schema.change: true\n"
                                + "  error.on.schema.change.exception.class: %s\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        dbName,
                        JM_ONLY_EXCEPTION_CLASS,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        validateSnapshotData(dbName);

        LOG.info("Triggering a schema change to make the coordinator fail");
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s", MYSQL.getHost(), MYSQL.getDatabasePort(), dbName);

        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stmt = conn.createStatement()) {
            waitForIncrementalStage(dbName, stmt);

            // Triggers an AddColumnEvent, which the error.on.schema.change applier rejects on the
            // coordinator with a JobManagerOnlyException cause.
            stmt.execute("ALTER TABLE members ADD COLUMN gender TINYINT AFTER age;");
        }

        // The coordinator loads JobManagerOnlyException from the JobManager's lib directory and
        // fails the job with it.
        waitUntilLogContains(
                jobManagerConsumer,
                "An exception was triggered from Schema change applying task. Job will fail now.");
        waitUntilLogContains(
                jobManagerConsumer, JM_ONLY_EXCEPTION_CLASS + ": " + SIMULATED_CAUSE_MESSAGE);
        waitUntilLogContains(
                jobManagerConsumer,
                "org.apache.flink.runtime.JobException: Recovery is suppressed by NoRestartBackoffTimeStrategy");

        // The job must fail cleanly: terminal state reached on the JobManager and all tasks
        // reporting a final execution state on the TaskManager. Without the SerializedThrowable
        // wrapping this takes a full RPC timeout cycle instead of a few seconds, because the
        // undeliverable failure tears down the JobManager <-> TaskManager Pekko association and
        // cancellation stalls until it recovers.
        waitUntilLogContains(jobManagerConsumer, "reached terminal state FAILED");
        waitUntilLogContains(
                taskManagerConsumer, "Un-registering task and sending final execution state");

        // The regression assertion: the TaskManager's RPC layer must never choke on the
        // coordinator failure. Without the SerializedThrowable wrapping, deserializing the
        // exceptionally completed coordination response fails on the TaskManager and Pekko logs
        // "Association with remote system [...] has failed, address is now gated ...
        // Reason: [org.apache.flink.cdc...Exception]" here.
        Assertions.assertThat(taskManagerConsumer.toUtf8String())
                .doesNotContain("has failed, address is now gated");
    }

    private void validateSnapshotData(String dbName) throws Exception {
        validateResult(
                s -> String.format(s, dbName),
                "CreateTableEvent{tableId=%s.members, schema=columns={`id` INT NOT NULL,`name` VARCHAR(17),`age` INT}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.members, before=[], after=[1008, Alice, 21], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.members, before=[], after=[1009, Bob, 20], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.members, before=[], after=[1010, Carol, 19], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.members, before=[], after=[1011, Derrida, 18], op=INSERT, meta=()}");
    }

    private void waitForIncrementalStage(String dbName, Statement stmt) throws Exception {
        stmt.execute("INSERT INTO members VALUES (0, '__fence__', 0);");

        // Ensure we change schema after incremental stage
        waitUntilSpecificEvent(
                taskManagerConsumer,
                String.format(
                        "DataChangeEvent{tableId=%s.members, before=[], after=[0, __fence__, 0], op=INSERT, meta=()}",
                        dbName));
    }

    /**
     * Packages the compiled {@link JobManagerOnlyException} class from the test classpath into a
     * standalone jar under the module's {@code target} directory, where {@code
     * TestUtils#getResource} can find it.
     */
    private static void generateJobManagerOnlyExceptionJar() {
        String classEntry = JM_ONLY_EXCEPTION_CLASS.replace('.', '/') + ".class";
        Path moduleDir =
                Paths.get(
                        System.getProperty("moduleDir", Paths.get("").toAbsolutePath().toString()));
        Path jarPath =
                moduleDir
                        .resolve("target")
                        .resolve("generated-e2e-jars")
                        .resolve(JM_ONLY_EXCEPTION_JAR);
        try {
            Files.createDirectories(jarPath.getParent());
            try (InputStream classStream =
                            CoordinatorFailurePropagationE2eITCase.class
                                    .getClassLoader()
                                    .getResourceAsStream(classEntry);
                    JarOutputStream jarStream =
                            new JarOutputStream(Files.newOutputStream(jarPath))) {
                if (classStream == null) {
                    throw new IllegalStateException(
                            "Could not find " + classEntry + " on the test classpath.");
                }
                jarStream.putNextEntry(new JarEntry(classEntry));
                byte[] buffer = new byte[4096];
                int read;
                while ((read = classStream.read(buffer)) != -1) {
                    jarStream.write(buffer, 0, read);
                }
                jarStream.closeEntry();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to generate " + JM_ONLY_EXCEPTION_JAR, e);
        }
    }
}
