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

package org.apache.flink.cdc.cli;

import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.core.execution.RestoreModeAdapter;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.guava31.com.google.common.io.Resources;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTING_TIMEOUT;
import static org.apache.flink.configuration.CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS;
import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.JobManagerOptions.ADDRESS;
import static org.apache.flink.configuration.JobManagerOptions.PORT;
import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE;
import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH;
import static org.apache.flink.configuration.TaskManagerOptions.NUM_TASK_SLOTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CliFrontendTest {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    @BeforeEach
    void takeoverOutput() {
        System.setOut(new PrintStream(out));
        System.setErr(new PrintStream(err));
    }

    @Test
    void testNoArgument() throws Exception {
        CliFrontend.main(new String[] {});
        assertThat(out).hasToString(HELP_MESSAGE);
        assertThat(err.toString()).isEmpty();
    }

    @Test
    void testGeneratingHelpMessage() throws Exception {
        CliFrontend.main(new String[] {"--help"});
        assertThat(out).hasToString(HELP_MESSAGE);
        assertThat(err.toString()).isEmpty();
    }

    @Test
    void testMissingFlinkHome() {
        assertThatThrownBy(() -> CliFrontend.main(new String[] {pipelineDef()}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Cannot find Flink home from either command line arguments \"--flink-home\" "
                                + "or the environment variable \"FLINK_HOME\". "
                                + "Please make sure Flink home is properly set. ");
    }

    @Test
    void testGlobalPipelineConfigParsing() throws Exception {
        CliExecutor executor =
                createExecutor(
                        pipelineDef(),
                        "--flink-home",
                        flinkHome(),
                        "--global-config",
                        globalPipelineConfig());
        assertThat(executor.getGlobalPipelineConfig().toMap())
                .containsEntry("parallelism", "1")
                .containsEntry("schema.change.behavior", "ignore");
    }

    @Test
    void testSavePointConfiguration() throws Exception {
        CliExecutor executor =
                createExecutor(
                        pipelineDef(),
                        "--flink-home",
                        flinkHome(),
                        "-s",
                        flinkHome() + "/savepoints/savepoint-1",
                        "-cm",
                        "no_claim",
                        "-n");
        assertThat(executor.getFlinkConfig().get(SAVEPOINT_PATH))
                .isEqualTo(flinkHome() + "/savepoints/savepoint-1");
        assertThat(RestoreModeAdapter.getRestoreMode(executor.getFlinkConfig()).toString())
                .isEqualTo("NO_CLAIM");
        assertThat(executor.getFlinkConfig().get(SAVEPOINT_IGNORE_UNCLAIMED_STATE)).isTrue();
    }

    @Test
    void testFlinkConfigurationWithPriority() throws Exception {
        // 1. Command-line options have higher priority than pipeline definition options
        CliExecutor executorWithCliOverride =
                createExecutor(
                        pipelineDef(),
                        "--flink-home",
                        flinkHome(),
                        "-D",
                        "execution.checkpointing.timeout=11min");
        assertThat(executorWithCliOverride.getFlinkConfig().get(CHECKPOINTING_TIMEOUT))
                .isEqualTo(Duration.ofMinutes(11));
        assertThat(executorWithCliOverride.getFlinkConfig().get(DEFAULT_PARALLELISM)).isEqualTo(1);

        // 2. Pipeline definition options have higher priority than cluster config.yaml options
        CliExecutor executorWithoutCliOverride =
                createExecutor(pipelineDef(), "--flink-home", flinkHome());
        assertThat(executorWithoutCliOverride.getFlinkConfig().get(CHECKPOINTING_TIMEOUT))
                .isEqualTo(Duration.ofMinutes(12));
        assertThat(executorWithoutCliOverride.getFlinkConfig().get(DEFAULT_PARALLELISM))
                .isEqualTo(1);
    }

    @Test
    void testFlinkConfigurationMutualOverridePriority() throws Exception {
        // Verify all 3 configuration provision methods (CLI args, pipeline.yaml, config.yaml)
        // interacting simultaneously with different override relationships:
        // 1. CLI overrides both pipeline.yaml and config.yaml (parallelism.default: 3 vs 2 vs 1)
        // 2. pipeline.yaml overrides config.yaml when CLI does not specify
        // (taskmanager.numberOfTaskSlots: 4 vs 1)
        // 3. CLI overrides config.yaml directly when pipeline.yaml does not specify
        // (jobmanager.rpc.port: 9999 vs 6123)
        // 4. CLI overrides pipeline.yaml directly (execution.checkpointing.timeout: 11min vs 12min)
        // 5. pipeline.yaml value used when not overridden (execution.checkpointing.interval: 3min)
        // 6. CLI value used for option only set via CLI
        // (execution.checkpointing.max-concurrent-checkpoints: 5)
        // 7. config.yaml value preserved for option only set in cluster config
        // (jobmanager.rpc.address: localhost)
        CliExecutor executorWithCli =
                createExecutor(
                        pipelineDefWithPriority(),
                        "--flink-home",
                        flinkHome(),
                        "-D",
                        "parallelism.default=3",
                        "-D",
                        "jobmanager.rpc.port=9999",
                        "-D",
                        "execution.checkpointing.timeout=11min",
                        "-D",
                        "execution.checkpointing.max-concurrent-checkpoints=5");
        org.apache.flink.configuration.Configuration flinkConfigWithCli =
                executorWithCli.getFlinkConfig();

        assertThat(flinkConfigWithCli.get(DEFAULT_PARALLELISM)).isEqualTo(3);
        assertThat(flinkConfigWithCli.get(NUM_TASK_SLOTS)).isEqualTo(4);
        assertThat(flinkConfigWithCli.get(PORT)).isEqualTo(9999);
        assertThat(flinkConfigWithCli.get(ADDRESS)).isEqualTo("localhost");
        assertThat(flinkConfigWithCli.get(CHECKPOINTING_TIMEOUT)).isEqualTo(Duration.ofMinutes(11));
        assertThat(flinkConfigWithCli.get(CHECKPOINTING_INTERVAL)).isEqualTo(Duration.ofMinutes(3));
        assertThat(flinkConfigWithCli.get(MAX_CONCURRENT_CHECKPOINTS)).isEqualTo(5);

        // When no CLI arguments are provided, verify pipeline.yaml overrides config.yaml
        // while non-overridden cluster configs are safely preserved
        CliExecutor executorWithoutCli =
                createExecutor(pipelineDefWithPriority(), "--flink-home", flinkHome());
        org.apache.flink.configuration.Configuration flinkConfigWithoutCli =
                executorWithoutCli.getFlinkConfig();

        assertThat(flinkConfigWithoutCli.get(DEFAULT_PARALLELISM)).isEqualTo(2);
        assertThat(flinkConfigWithoutCli.get(NUM_TASK_SLOTS)).isEqualTo(4);
        assertThat(flinkConfigWithoutCli.get(PORT)).isEqualTo(6123);
        assertThat(flinkConfigWithoutCli.get(ADDRESS)).isEqualTo("localhost");
        assertThat(flinkConfigWithoutCli.get(CHECKPOINTING_TIMEOUT))
                .isEqualTo(Duration.ofMinutes(12));
        assertThat(flinkConfigWithoutCli.get(CHECKPOINTING_INTERVAL))
                .isEqualTo(Duration.ofMinutes(3));
    }

    @Test
    void testFlinkConfigurationWithNestedYaml() throws Exception {
        CliExecutor executor =
                createExecutor(
                        pipelineDefWithNestedFlinkConf(),
                        "--flink-home",
                        flinkHome(),
                        "-D",
                        "execution.checkpointing.interval=5min");
        org.apache.flink.configuration.Configuration flinkConfig = executor.getFlinkConfig();

        // Command-line override takes precedence over nested YAML
        assertThat(flinkConfig.get(CHECKPOINTING_INTERVAL)).isEqualTo(Duration.ofMinutes(5));
        // Nested YAML takes effect
        assertThat(flinkConfig.get(CHECKPOINTING_TIMEOUT)).isEqualTo(Duration.ofMinutes(15));
        // Nested YAML overrides config.yaml default parallelism
        assertThat(flinkConfig.get(DEFAULT_PARALLELISM)).isEqualTo(4);
    }

    @Test
    void testDeploymentTargetConfiguration() throws Exception {
        CliExecutor executor =
                createExecutor(
                        pipelineDef(),
                        "--flink-home",
                        flinkHome(),
                        "-t",
                        "kubernetes-application",
                        "-n");
        assertThat(executor.getDeploymentTarget()).isEqualTo("kubernetes-application");

        executor =
                createExecutor(
                        pipelineDef(), "--flink-home", flinkHome(), "-t", "yarn-application", "-n");
        assertThat(executor.getDeploymentTarget()).isEqualTo("yarn-application");
    }

    @Test
    void testAdditionalJar() throws Exception {
        String aJar = "/foo/jar/a.jar";
        String bJar = "/foo/jar/b.jar";
        CliExecutor executor =
                createExecutor(
                        pipelineDef(), "--flink-home", flinkHome(), "--jar", aJar, "--jar", bJar);
        assertThat(executor.getAdditionalJars()).contains(new Path(aJar), new Path(bJar));
    }

    @Test
    void testPipelineExecuting() throws Exception {
        CliExecutor executor =
                createExecutor(
                        pipelineDef(),
                        "--flink-home",
                        flinkHome(),
                        "--global-config",
                        globalPipelineConfig());
        NoOpComposer composer = new NoOpComposer();
        executor.setComposer(composer);
        PipelineExecution.ExecutionInfo executionInfo = executor.deployWithNoOpComposer();
        assertThat(executionInfo.getId()).isEqualTo("fake-id");
        assertThat(executionInfo.getDescription()).isEqualTo("fake-description");
    }

    @Test
    void testPipelineExecutingWithFlinkConfig() throws Exception {
        // the command line arguments to submit job to exists remote host on yarn session
        CliExecutor executor =
                createExecutor(
                        pipelineDef(),
                        "--flink-home",
                        flinkHome(),
                        "--global-config",
                        globalPipelineConfig(),
                        "-D",
                        "execution.target= yarn-session",
                        "-D",
                        "rest.bind-port =42689",
                        "-D",
                        "yarn.application.id=application_1714009558476_3563",
                        "-D",
                        "rest.bind-address=10.1.140.140");
        Map<String, String> configMap = executor.getFlinkConfig().toMap();
        assertThat(configMap)
                .containsEntry("execution.target", "yarn-session")
                .containsEntry("rest.bind-port", "42689")
                .containsEntry("yarn.application.id", "application_1714009558476_3563")
                .containsEntry("rest.bind-address", "10.1.140.140");
    }

    @Test
    void testPipelineExecutingWithInvalidFlinkConfig() throws Exception {
        assertThatThrownBy(
                        () ->
                                createExecutor(
                                        pipelineDef(),
                                        "--flink-home",
                                        flinkHome(),
                                        "-D",
                                        "=execution.target"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        String.format(
                                "null or white space argument for key or value: %s=%s",
                                "", "execution.target"));

        assertThatThrownBy(
                        () ->
                                createExecutor(
                                        pipelineDef(),
                                        "--flink-home",
                                        flinkHome(),
                                        "-D",
                                        "execution.target="))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        String.format(
                                "null or white space argument for key or value: %s=%s",
                                "execution.target", ""));

        assertThatThrownBy(
                        () -> createExecutor(pipelineDef(), "--flink-home", flinkHome(), "-D", "="))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        String.format(
                                "null or white space argument for key or value: %s=%s", "", ""));
    }

    private CliExecutor createExecutor(String... args) throws Exception {
        Options cliOptions = CliFrontendOptions.initializeOptions();
        CommandLineParser parser = new DefaultParser();
        return CliFrontend.createExecutor(parser.parse(cliOptions, args));
    }

    private String pipelineDef() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-full.yaml");
        return Paths.get(resource.toURI()).toString();
    }

    private String pipelineDefWithPriority() throws Exception {
        URL resource =
                Resources.getResource(
                        "definitions/pipeline-definition-with-flink-conf-priority.yaml");
        return Paths.get(resource.toURI()).toString();
    }

    private String pipelineDefWithNestedFlinkConf() throws Exception {
        URL resource =
                Resources.getResource(
                        "definitions/pipeline-definition-with-nested-flink-conf.yaml");
        return Paths.get(resource.toURI()).toString();
    }

    private String flinkHome() throws Exception {
        URL resource = Resources.getResource("flink-home");
        return Paths.get(resource.toURI()).toString();
    }

    private String globalPipelineConfig() throws Exception {
        URL resource = Resources.getResource("global-config/global-config.yaml");
        return Paths.get(resource.toURI()).toString();
    }

    private static final String HELP_MESSAGE =
            "usage:\n"
                    + "    -cm,--claim-mode <arg>                      Defines how should we restore\n"
                    + "                                                from the given savepoint.\n"
                    + "                                                Supported options: [claim -\n"
                    + "                                                claim ownership of the savepoint\n"
                    + "                                                and delete once it is subsumed,\n"
                    + "                                                no_claim (default) - do not\n"
                    + "                                                claim ownership, the first\n"
                    + "                                                checkpoint will not reuse any\n"
                    + "                                                files from the restored one,\n"
                    + "                                                legacy - the old behaviour, do\n"
                    + "                                                not assume ownership of the\n"
                    + "                                                savepoint files, but can reuse\n"
                    + "                                                some shared files\n"
                    + "    -D <Session dynamic flink config key=val>   Allows specifying multiple flink\n"
                    + "                                                generic configuration options.\n"
                    + "                                                The availableoptions can be\n"
                    + "                                                found at\n"
                    + "                                                https://nightlies.apache.org/fli\n"
                    + "                                                nk/flink-docs-stable/ops/config.\n"
                    + "                                                html\n"
                    + "       --flink-home <arg>                       Path of Flink home directory\n"
                    + "       --global-config <arg>                    Path of the global configuration\n"
                    + "                                                file for Flink CDC pipelines\n"
                    + "    -h,--help                                   Display help message\n"
                    + "       --jar <arg>                              JARs to be submitted together\n"
                    + "                                                with the pipeline\n"
                    + "    -n,--allow-nonRestored-state                Allow to skip savepoint state\n"
                    + "                                                that cannot be restored. You\n"
                    + "                                                need to allow this if you\n"
                    + "                                                removed an operator from your\n"
                    + "                                                program that was part of the\n"
                    + "                                                program when the savepoint was\n"
                    + "                                                triggered.\n"
                    + "    -s,--from-savepoint <arg>                   Path to a savepoint to restore\n"
                    + "                                                the job from (for example\n"
                    + "                                                hdfs:///flink/savepoint-1537\n"
                    + "    -t,--target <arg>                           The deployment target for the\n"
                    + "                                                execution. This can take one of\n"
                    + "                                                the following values:\n"
                    + "                                                - local\n"
                    + "                                                - remote\n"
                    + "                                                - yarn-session\n"
                    + "                                                - yarn-application\n"
                    + "                                                - kubernetes-application\n"
                    + "       --use-mini-cluster                       Use Flink MiniCluster to run the\n"
                    + "                                                pipeline\n";

    private static class NoOpComposer implements PipelineComposer {

        @Override
        public PipelineExecution compose(PipelineDef pipelineDef) {
            return () -> new PipelineExecution.ExecutionInfo("fake-id", "fake-description");
        }
    }
}
