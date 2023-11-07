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

package com.ververica.cdc.cli.parser;

import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.composer.definition.PipelineDef;

import java.nio.file.Path;

/** Parsing pipeline definition files and generate {@link PipelineDef}. */
public interface PipelineDefinitionParser {

    /**
     * Parse the specified pipeline definition file path, merge global configurations, then generate
     * the {@link PipelineDef}.
     */
    PipelineDef parse(Path pipelineDefPath, Configuration globalPipelineConfig) throws Exception;
}
