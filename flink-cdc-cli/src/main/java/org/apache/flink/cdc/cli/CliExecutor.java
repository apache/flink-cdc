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
import org.apache.flink.cdc.cli.utils.FlinkEnvironmentUtils;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineDeploymentExecutor;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.deployment.ComposeDeploymentFactory;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.commons.cli.CommandLine;

import java.nio.file.Path;
import java.util.List;

/** Executor for doing the composing and submitting logic for {@link CliFrontend}. */
public class CliExecutor {

    private final Path pipelineDefPath;
    private final Configuration flinkConfig;
    private final Configuration globalPipelineConfig;
    private final boolean useMiniCluster;
    private final List<Path> additionalJars;

    private final CommandLine commandLine;

    private PipelineComposer composer = null;

    private final SavepointRestoreSettings savepointSettings;

    public CliExecutor(
            CommandLine commandLine,
            Path pipelineDefPath,
            Configuration flinkConfig,
            Configuration globalPipelineConfig,
            boolean useMiniCluster,
            List<Path> additionalJars,
            SavepointRestoreSettings savepointSettings) {
        this.commandLine = commandLine;
        this.pipelineDefPath = pipelineDefPath;
        this.flinkConfig = flinkConfig;
        this.globalPipelineConfig = globalPipelineConfig;
        this.useMiniCluster = useMiniCluster;
        this.additionalJars = additionalJars;
        this.savepointSettings = savepointSettings;
    }

    public PipelineExecution.ExecutionInfo run() throws Exception {
        // Create Submit Executor to deployment flink cdc job Or Run Flink CDC Job
        boolean isDeploymentMode = ConfigurationUtils.isDeploymentMode(commandLine);
        if (isDeploymentMode) {
            ComposeDeploymentFactory composeDeploymentFactory = new ComposeDeploymentFactory();
            PipelineDeploymentExecutor composeExecutor =
                    composeDeploymentFactory.getFlinkComposeExecutor(commandLine);
            return composeExecutor.deploy(
                    commandLine,
                    org.apache.flink.configuration.Configuration.fromMap(flinkConfig.toMap()),
                    additionalJars);
        } else {
            // Run CDC Job And Parse pipeline definition file
            PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
            PipelineDef pipelineDef =
                    pipelineDefinitionParser.parse(pipelineDefPath, globalPipelineConfig);
            // Create composer
            PipelineComposer composer = getComposer();
            // Compose pipeline
            PipelineExecution execution = composer.compose(pipelineDef);
            // Execute or submit the pipeline
            return execution.execute();
        }
    }

    private PipelineComposer getComposer() throws Exception {
        if (composer == null) {
            return FlinkEnvironmentUtils.createComposer(
                    useMiniCluster, flinkConfig, additionalJars, savepointSettings);
        }
        return composer;
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
