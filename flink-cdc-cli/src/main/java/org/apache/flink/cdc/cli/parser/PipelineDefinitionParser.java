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

package org.apache.flink.cdc.cli.parser;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.core.fs.Path;

/** Parsing pipeline definition files and generate {@link PipelineDef}. */
public interface PipelineDefinitionParser {

    /**
     * Parse the specified pipeline definition file path, merge global configurations, then generate
     * the {@link PipelineDef}.
     */
    PipelineDef parse(Path pipelineDefPath, Configuration globalPipelineConfig) throws Exception;

    /**
     * Parse the specified pipeline definition string, merge global configurations, then generate
     * the {@link PipelineDef}.
     */
    PipelineDef parse(String pipelineDefText, Configuration globalPipelineConfig) throws Exception;
}
