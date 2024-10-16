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

import org.apache.flink.cdc.cli.parser.PipelineDefinitionParser;
import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.cli.utils.ConfigurationUtils;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineDeploymentExecutor;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.composer.flink.deployment.K8SApplicationDeploymentExecutor;
import org.apache.flink.cdc.composer.flink.deployment.YarnApplicationDeploymentExecutor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.cli.CommandLine;

import java.util.List;

import static org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment.KUBERNETES_APPLICATION;
import static org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment.LOCAL;
import static org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment.REMOTE;
import static org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment.YARN_APPLICATION;
import static org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment.YARN_SESSION;

/** Executor for doing the composing and submitting logic for {@link CliFrontend}. */
public class CliExecutor {

    private final Path pipelineDefPath;
    private final Configuration flinkConfig;
    private final Configuration globalPipelineConfig;
    private final List<Path> additionalJars;
    private final Path flinkHome;
    private final CommandLine commandLine;
    private PipelineComposer composer = null;
    private final SavepointRestoreSettings savepointSettings;

    public CliExecutor(
            CommandLine commandLine,
            Path pipelineDefPath,
            Configuration flinkConfig,
            Configuration globalPipelineConfig,
            List<Path> additionalJars,
            SavepointRestoreSettings savepointSettings,
            Path flinkHome) {
        this.commandLine = commandLine;
        this.pipelineDefPath = pipelineDefPath;
        this.flinkConfig = flinkConfig;
        this.globalPipelineConfig = globalPipelineConfig;
        this.additionalJars = additionalJars;
        this.savepointSettings = savepointSettings;
        this.flinkHome = flinkHome;
    }

    public PipelineExecution.ExecutionInfo run() throws Exception {
        // Create Submit Executor to deployment flink cdc job Or Run Flink CDC Job
        String deploymentTarget = ConfigurationUtils.getDeploymentMode(commandLine);
        switch (deploymentTarget) {
            case KUBERNETES_APPLICATION:
                return deployWithApplicationComposer(new K8SApplicationDeploymentExecutor());
            case YARN_APPLICATION:
                return deployWithApplicationComposer(new YarnApplicationDeploymentExecutor());
            case LOCAL:
                return deployWithLocalExecutor();
            case REMOTE:
            case YARN_SESSION:
                return deployWithRemoteExecutor();
            default:
                throw new IllegalArgumentException(
                        String.format("Deployment target %s is not supported", deploymentTarget));
        }
    }

    private PipelineExecution.ExecutionInfo deployWithApplicationComposer(
            PipelineDeploymentExecutor composeExecutor) throws Exception {
        return composeExecutor.deploy(
                commandLine,
                org.apache.flink.configuration.Configuration.fromMap(flinkConfig.toMap()),
                additionalJars,
                flinkHome);
    }

    private PipelineExecution.ExecutionInfo deployWithLocalExecutor() throws Exception {
        return executePipeline(FlinkPipelineComposer.ofMiniCluster());
    }

    private PipelineExecution.ExecutionInfo deployWithRemoteExecutor() throws Exception {
        org.apache.flink.configuration.Configuration configuration =
                org.apache.flink.configuration.Configuration.fromMap(flinkConfig.toMap());
        SavepointRestoreSettings.toConfiguration(savepointSettings, configuration);
        return executePipeline(
                FlinkPipelineComposer.ofRemoteCluster(configuration, additionalJars));
    }

    private PipelineExecution.ExecutionInfo executePipeline(PipelineComposer composer)
            throws Exception {
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(pipelineDefPath, globalPipelineConfig);
        PipelineExecution execution = composer.compose(pipelineDef);
        return execution.execute();
    }

    @VisibleForTesting
    public PipelineExecution.ExecutionInfo deployWithNoOpComposer() throws Exception {
        return executePipeline(this.composer);
    }

    // The main class for running application mode
    public static void main(String[] args) throws Exception {
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        org.apache.flink.core.fs.Path pipelineDefPath = new org.apache.flink.core.fs.Path(args[0]);
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(pipelineDefPath, new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPipelineComposer flinkPipelineComposer =
                FlinkPipelineComposer.ofApplicationCluster(env);
        PipelineExecution execution = flinkPipelineComposer.compose(pipelineDef);
        execution.execute();
    }

    @VisibleForTesting
    void setComposer(PipelineComposer composer) {
        this.composer = composer;
    }

    @VisibleForTesting
    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    @VisibleForTesting
    public Configuration getGlobalPipelineConfig() {
        return globalPipelineConfig;
    }

    @VisibleForTesting
    public List<Path> getAdditionalJars() {
        return additionalJars;
    }

    @VisibleForTesting
    public String getDeploymentTarget() {
        return commandLine.getOptionValue("target");
    }

    public SavepointRestoreSettings getSavepointSettings() {
        return savepointSettings;
    }
}
