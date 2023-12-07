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
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;

import java.nio.file.Path;

/** Executor for doing the composing and submitting logic for {@link CliFrontend}. */
public class CliExecutor {

    private final Path pipelineDefPath;
    private final Configuration globalPipelineConfig;

    private PipelineComposer composer;

    public CliExecutor(
            Path pipelineDefPath, Configuration globalPipelineConfig, PipelineComposer composer) {
        this.pipelineDefPath = pipelineDefPath;
        this.globalPipelineConfig = globalPipelineConfig;
        this.composer = composer;
    }

    public PipelineExecution.ExecutionInfo run() throws Exception {
        // Parse pipeline definition file
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(pipelineDefPath, globalPipelineConfig);

        // Compose pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        // Execute the pipeline
        return execution.execute();
    }

    @VisibleForTesting
    void setComposer(PipelineComposer composer) {
        this.composer = composer;
    }

    @VisibleForTesting
    PipelineComposer getComposer() {
        return composer;
    }

    @VisibleForTesting
    public Configuration getGlobalPipelineConfig() {
        return globalPipelineConfig;
    }
}
