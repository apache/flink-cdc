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

package org.apache.flink.cdc.pipeline.tests.stage2;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.cdc.pipeline.tests.recovery.RecoveryE2eFixtureFactory;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.Container.ExecResult;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;

/** End-to-end recovery coverage for regular and distributed schema coordinators. */
@ParameterizedClass
@ValueSource(ints = {1, 4})
class SchemaCoordinatorRecoveryE2eITCase extends PipelineTestEnvironment {

    SchemaCoordinatorRecoveryE2eITCase(int parallelism) {
        super(parallelism);
    }

    private static final Duration JOB_TIMEOUT = Duration.ofMinutes(2);

    @TempDir Path tempDir;

    @Override
    protected String getFlinkProperties() {
        return super.getFlinkProperties()
                .replace(
                        "restart-strategy.type: off",
                        "restart-strategy.type: fixed-delay\n"
                                + "restart-strategy.fixed-delay.attempts: 1\n"
                                + "restart-strategy.fixed-delay.delay: 0 s\n"
                                + "execution.checkpointing.min-pause: 2 s");
    }

    @ParameterizedTest(name = "parallel metadata source: {0}")
    @ValueSource(booleans = {false, true})
    void recoversFailedSchemaChangeAndContinues(boolean distributed) throws Exception {
        String evidencePath = "/tmp/shared/schema-coordinator-recovery-" + UUID.randomUUID();
        Path fixtureJar = createFixtureJar();
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: %s\n"
                                + "  evidence-path: %s\n"
                                + "  parallel-metadata-source: %s\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: %s\n"
                                + "  evidence-path: %s\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: lenient\n",
                        RecoveryE2eFixtureFactory.IDENTIFIER,
                        evidencePath,
                        distributed,
                        RecoveryE2eFixtureFactory.IDENTIFIER,
                        evidencePath,
                        distributed ? 2 : 1);

        JobID jobId = submitPipelineJob(pipelineJob, fixtureJar);
        Assertions.assertThat(jobId).as("pipeline job ID").isNotNull();
        waitUntilJobFinished(JOB_TIMEOUT);
        Assertions.assertThat(getRestClusterClient().getJobStatus(jobId).get(10, TimeUnit.SECONDS))
                .as("pipeline job completion")
                .isEqualTo(JobStatus.FINISHED);

        List<String> evidence =
                Arrays.asList(readContainerFile(evidencePath + ".events").split("\\n"));
        Assertions.assertThat(occurrences(evidence, "SOURCE\tattempt"))
                .as("job was restarted after the metadata failure")
                .isGreaterThanOrEqualTo(2);
        Assertions.assertThat(occurrences(evidence, "SOURCE\textra_v1-emitted"))
                .as("the failed schema event was replayed")
                .isEqualTo(2);
        assertRestoredFromCompletedCheckpoint(evidence);
        Assertions.assertThat(evidence)
                .contains(
                        "SOURCE\tcreate-replayed",
                        "METADATA\textra_v1-applied",
                        "METADATA\textra_v1-replayed",
                        "METADATA\textra_v2-applied");
        Assertions.assertThat(occurrences(evidence, "METADATA\textra_v1-applied"))
                .as("the no-rollback metadata effect happened once")
                .isEqualTo(1);
        Assertions.assertThat(occurrences(evidence, "METADATA\textra_v1-replayed"))
                .as("the metadata failure was replayed once")
                .isEqualTo(1);
        Assertions.assertThat(occurrences(evidence, "METADATA\textra_v2-applied"))
                .as("the follow-up schema change was applied once")
                .isEqualTo(1);
        String schema = readContainerFile(evidencePath + ".schema").trim();
        Assertions.assertThat(schema)
                .as("follow-up schema change completed after recovery")
                .isEqualTo("id,value,extra_v1,extra_v2");
        Assertions.assertThat(dataEvents(evidence))
                .as("complete data-change multiset")
                .containsExactlyInAnyOrder(
                        "DATA\tINSERT\t\t=>\t1,before",
                        "DATA\tUPDATE\t1,before,\t=>\t1,after,v1",
                        "DATA\tUPDATE\t1,after,v1,\t=>\t1,after,v1,v2",
                        "DATA\tINSERT\t\t=>\t2,deleted,v1,v2",
                        "DATA\tDELETE\t2,deleted,v1,v2\t=>\t");
        Assertions.assertThat(terminalRows(evidence, schema))
                .as("PK upsert and deletion materialized from every emitted data event")
                .containsExactly(Map.entry("1", "1,after,v1,v2"));
    }

    private Path createFixtureJar() throws IOException {
        Path fixtureJar = tempDir.resolve("schema-coordinator-recovery-fixture.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(fixtureJar))) {
            for (Class<?> fixtureClass : RecoveryE2eFixtureFactory.fixtureClasses()) {
                addClassToJar(fixtureClass, output);
            }
            output.putNextEntry(
                    new JarEntry(
                            "META-INF/services/org.apache.flink.cdc.common.factories.Factory"));
            output.write(
                    (RecoveryE2eFixtureFactory.class.getName() + "\n")
                            .getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
        return fixtureJar;
    }

    private static void addClassToJar(Class<?> clazz, JarOutputStream output) throws IOException {
        String resourceName = clazz.getName().replace('.', '/') + ".class";
        try (InputStream input = clazz.getClassLoader().getResourceAsStream(resourceName)) {
            Assertions.assertThat(input).as("class resource %s", resourceName).isNotNull();
            output.putNextEntry(new JarEntry(resourceName));
            input.transferTo(output);
            output.closeEntry();
        }
    }

    private String readContainerFile(String path) throws IOException, InterruptedException {
        ExecResult result = jobManager.execInContainer("cat", path);
        Assertions.assertThat(result.getExitCode()).as("read evidence file %s", path).isZero();
        return result.getStdout();
    }

    private static int occurrences(List<String> evidence, String expected) {
        return (int) evidence.stream().filter(expected::equals).count();
    }

    private static List<String> dataEvents(List<String> evidence) {
        return evidence.stream()
                .filter(line -> line.startsWith("DATA\t"))
                .collect(Collectors.toList());
    }

    private static void assertRestoredFromCompletedCheckpoint(List<String> evidence) {
        int firstColumnEmission = evidence.indexOf("SOURCE\textra_v1-emitted");
        int restoredCheckpoint = indexOfPrefix(evidence, "SOURCE\tcheckpoint-restored\t");
        Assertions.assertThat(firstColumnEmission).as("initial extra_v1 emission").isGreaterThan(0);
        Assertions.assertThat(restoredCheckpoint)
                .as("source restore after the failure")
                .isGreaterThan(firstColumnEmission);

        int completedCheckpointIndex = -1;
        for (int index = 0; index < firstColumnEmission; index++) {
            if (evidence.get(index).startsWith("SOURCE\tcheckpoint-completed\t")) {
                completedCheckpointIndex = index;
            }
        }
        Assertions.assertThat(completedCheckpointIndex)
                .as("checkpoint completed before initial extra_v1 emission")
                .isGreaterThanOrEqualTo(0);

        String completedCheckpoint = evidence.get(completedCheckpointIndex);
        String checkpointId = completedCheckpoint.split("\\t", -1)[2];
        String snapshot = "SOURCE\tcheckpoint-snapshot\t" + checkpointId + "\tcursor=2";
        int snapshotIndex = evidence.indexOf(snapshot);
        Assertions.assertThat(snapshotIndex)
                .as("selected checkpoint captured the pre-failure cursor")
                .isGreaterThanOrEqualTo(0);
        Assertions.assertThat(snapshotIndex)
                .as("checkpoint snapshot precedes its completion")
                .isLessThan(completedCheckpointIndex);
        Assertions.assertThat(evidence)
                .contains(snapshot, "SOURCE\tcheckpoint-restored\t" + checkpointId + "\tcursor=2");
        for (int index = snapshotIndex; index < restoredCheckpoint; index++) {
            String line = evidence.get(index);
            if (line.startsWith("SOURCE\tcheckpoint-snapshot\t")
                    || line.startsWith("SOURCE\tcheckpoint-completed\t")) {
                Assertions.assertThat(line)
                        .as("no checkpoint may advance past the replay cursor before restore")
                        .endsWith("\tcursor=2");
            }
        }
    }

    private static int indexOfPrefix(List<String> evidence, String prefix) {
        for (int index = 0; index < evidence.size(); index++) {
            if (evidence.get(index).startsWith(prefix)) {
                return index;
            }
        }
        return -1;
    }

    private static Map<String, String> terminalRows(List<String> evidence, String schema) {
        Map<String, String> rows = new LinkedHashMap<>();
        for (String line : evidence) {
            if (!line.startsWith("DATA\t")) {
                continue;
            }
            String[] parts = line.split("\\t", -1);
            String operation = parts[1];
            String row = "DELETE".equals(operation) ? parts[2] : parts[4];
            String id = row.split(",", -1)[0];
            if ("DELETE".equals(operation)) {
                rows.remove(id);
            } else {
                rows.put(id, row);
            }
        }
        int columnCount = schema.split(",", -1).length;
        rows.replaceAll((id, row) -> padToColumnCount(row, columnCount));
        return rows;
    }

    private static String padToColumnCount(String row, int columnCount) {
        StringBuilder paddedRow = new StringBuilder(row);
        for (int index = row.split(",", -1).length; index < columnCount; index++) {
            paddedRow.append(',');
        }
        return paddedRow.toString();
    }
}
