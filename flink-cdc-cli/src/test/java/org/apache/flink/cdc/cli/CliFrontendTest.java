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
import org.apache.flink.core.execution.RestoreMode;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.guava31.com.google.common.io.Resources;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;

import static org.apache.flink.configuration.StateRecoveryOptions.RESTORE_MODE;
import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE;
import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH;
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
        assertThat(executor.getFlinkConfig().get(RESTORE_MODE)).isEqualTo(RestoreMode.NO_CLAIM);
        assertThat(executor.getFlinkConfig().get(SAVEPOINT_IGNORE_UNCLAIMED_STATE)).isTrue();
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
    void testPipelineExecutingWithUnValidFlinkConfig() throws Exception {
        Assertions.assertThatThrownBy(
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

        Assertions.assertThatThrownBy(
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

        Assertions.assertThatThrownBy(
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
                    + "                                                the following values\n"
                    + "                                                local/remote/yarn-session/yarn-a\n"
                    + "                                                pplication/kubernetes-session/ku\n"
                    + "                                                bernetes-application\n"
                    + "       --use-mini-cluster                       Use Flink MiniCluster to run the\n"
                    + "                                                pipeline\n";

    private static class NoOpComposer implements PipelineComposer {

        @Override
        public PipelineExecution compose(PipelineDef pipelineDef) {
            return () -> new PipelineExecution.ExecutionInfo("fake-id", "fake-description");
        }
    }
}
