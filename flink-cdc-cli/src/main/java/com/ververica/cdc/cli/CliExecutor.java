/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.cli;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.cli.parser.PipelineDefinitionParser;
import com.ververica.cdc.cli.parser.YamlPipelineDefinitionParser;
import com.ververica.cdc.cli.utils.FlinkEnvironmentUtils;
import com.ververica.cdc.composer.PipelineComposer;
import com.ververica.cdc.composer.PipelineExecution;
import com.ververica.cdc.composer.definition.PipelineDef;

import java.nio.file.Path;
import java.util.List;

/** Executor for doing the composing and submitting logic for {@link CliFrontend}. */
public class CliExecutor {

    private final Path pipelineDefPath;
    private final Configuration flinkConfig;
    private final Configuration globalPipelineConfig;
    private final boolean useMiniCluster;
    private final List<Path> additionalJars;

    private PipelineComposer composer = null;

    public CliExecutor(
            Path pipelineDefPath,
            Configuration flinkConfig,
            Configuration globalPipelineConfig,
            boolean useMiniCluster,
            List<Path> additionalJars) {
        this.pipelineDefPath = pipelineDefPath;
        this.flinkConfig = flinkConfig;
        this.globalPipelineConfig = globalPipelineConfig;
        this.useMiniCluster = useMiniCluster;
        this.additionalJars = additionalJars;
    }

    public PipelineExecution.ExecutionInfo run() throws Exception {
        // Parse pipeline definition file
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(pipelineDefPath, globalPipelineConfig);

        // Create composer
        PipelineComposer composer = getComposer(flinkConfig);

        // Compose pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        // Execute the pipeline
        return execution.execute();
    }

    private PipelineComposer getComposer(Configuration flinkConfig) {
        if (composer == null) {
            return FlinkEnvironmentUtils.createComposer(
                    useMiniCluster, flinkConfig, additionalJars);
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
}
