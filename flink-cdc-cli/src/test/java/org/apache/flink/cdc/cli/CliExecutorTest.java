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

import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.definition.PipelineDef;

import org.apache.flink.shaded.guava31.com.google.common.io.Resources;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests covering how {@link CliExecutor#main(String[])} (the application-mode entrypoint) loads the
 * pipeline definition through {@link CliExecutor#resolvePipelineDef(String)}.
 *
 * <p>The two native application deployment executors share this single entrypoint but pass {@code
 * args[0]} with different semantics, so {@code resolvePipelineDef} must handle both:
 *
 * <ul>
 *   <li><b>Kubernetes</b> ({@code K8SApplicationDeploymentExecutor}) passes the pipeline definition
 *       FILE PATH (shipped into the JobManager container, e.g. mounted by a ConfigMap), so it must
 *       be read from the file system.
 *   <li><b>YARN</b> ({@code YarnApplicationDeploymentExecutor}) reads the file on the client side
 *       and passes the pipeline definition CONTENT, which must be used verbatim.
 * </ul>
 */
class CliExecutorTest {

    /**
     * Kubernetes application mode: {@code args[0]} is a file path, so {@code resolvePipelineDef}
     * reads the file content, which then parses into a valid pipeline definition.
     */
    @Test
    void testResolvePipelineDefFromFilePath() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        String pipelineDefPath = Paths.get(resource.toURI()).toString();

        String content = CliExecutor.resolvePipelineDef(pipelineDefPath);
        assertThat(content).contains("source:").contains("type: mysql");

        PipelineDef pipelineDef =
                new YamlPipelineDefinitionParser().parse(content, new Configuration());
        assertThat(pipelineDef.getSource()).isNotNull();
    }

    /**
     * Explicit-scheme path (here {@code file://}, the same code path as {@code s3://}, {@code
     * hdfs://}, {@code oss://}): read through Flink's FileSystem so the matching plugin resolves
     * it.
     */
    @Test
    void testResolvePipelineDefFromSchemePath() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        String schemePath = resource.toURI().toString();
        assertThat(new URI(schemePath).getScheme()).isNotNull();

        String content = CliExecutor.resolvePipelineDef(schemePath);
        assertThat(content).contains("source:").contains("type: mysql");

        PipelineDef pipelineDef =
                new YamlPipelineDefinitionParser().parse(content, new Configuration());
        assertThat(pipelineDef.getSource()).isNotNull();
    }

    /**
     * YARN application mode: {@code args[0]} is already the pipeline definition content (read on
     * the client side), so {@code resolvePipelineDef} returns it verbatim and it parses correctly.
     */
    @Test
    void testResolvePipelineDefFromInlineContent() throws Exception {
        String pipelineDefContent = "source:\n  type: mysql\n\nsink:\n  type: kafka\n";

        String resolved = CliExecutor.resolvePipelineDef(pipelineDefContent);
        assertThat(resolved).isEqualTo(pipelineDefContent);

        PipelineDef pipelineDef =
                new YamlPipelineDefinitionParser().parse(resolved, new Configuration());
        assertThat(pipelineDef.getSource()).isNotNull();
    }

    /**
     * The FLINK-40005 root cause: passing a file PATH straight to the String (content) overload
     * makes the parser treat the path as YAML content, yielding a scalar node without a {@code
     * source}. {@link CliExecutor#resolvePipelineDef(String)} avoids this for the Kubernetes path
     * by reading the file first.
     */
    @Test
    void testParsingFilePathAsYamlContentFails() {
        String pipelineDefPath = "/opt/flink/config/pipeline.yaml";
        assertThatThrownBy(
                        () ->
                                new YamlPipelineDefinitionParser()
                                        .parse(pipelineDefPath, new Configuration()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing required field \"source\"");
    }
}
