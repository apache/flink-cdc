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
import org.apache.flink.cdc.composer.PipelineDeploymentExecutor;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment;
import org.apache.flink.cdc.composer.flink.deployment.K8SApplicationDeploymentExecutor;
import org.apache.flink.cdc.composer.flink.deployment.YarnApplicationDeploymentExecutor;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.cli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.List;

/** Executor for doing the composing and submitting logic for {@link CliFrontend}. */
public class CliExecutor {

    private final Path pipelineDefPath;
    private final org.apache.flink.configuration.Configuration flinkConfig;
    private final Configuration globalPipelineConfig;
    private final List<Path> additionalJars;
    private final Path flinkHome;
    private final CommandLine commandLine;
    private PipelineComposer composer = null;

    public CliExecutor(
            CommandLine commandLine,
            Path pipelineDefPath,
            org.apache.flink.configuration.Configuration flinkConfig,
            Configuration globalPipelineConfig,
            List<Path> additionalJars,
            Path flinkHome) {
        this.commandLine = commandLine;
        this.pipelineDefPath = pipelineDefPath;
        this.flinkConfig = flinkConfig;
        this.globalPipelineConfig = globalPipelineConfig;
        this.additionalJars = additionalJars;
        this.flinkHome = flinkHome;
    }

    public PipelineExecution.ExecutionInfo run() throws Exception {
        // Create Submit Executor to deployment flink cdc job Or Run Flink CDC Job
        String deploymentTargetStr = getDeploymentTarget();
        ComposeDeployment deploymentTarget =
                ComposeDeployment.getDeploymentFromName(deploymentTargetStr);
        switch (deploymentTarget) {
            case KUBERNETES_APPLICATION:
                return deployWithApplicationComposer(new K8SApplicationDeploymentExecutor());
            case YARN_APPLICATION:
                return deployWithApplicationComposer(new YarnApplicationDeploymentExecutor());
            case LOCAL:
                return deployWithComposer(FlinkPipelineComposer.ofMiniCluster());
            case REMOTE:
            case YARN_SESSION:
                return deployWithComposer(
                        FlinkPipelineComposer.ofRemoteCluster(flinkConfig, additionalJars));
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Deployment target %s is not supported", deploymentTargetStr));
        }
    }

    private PipelineExecution.ExecutionInfo deployWithApplicationComposer(
            PipelineDeploymentExecutor composeExecutor) throws Exception {
        return composeExecutor.deploy(commandLine, flinkConfig, additionalJars, flinkHome);
    }

    private PipelineExecution.ExecutionInfo deployWithComposer(PipelineComposer composer)
            throws Exception {
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(pipelineDefPath, globalPipelineConfig);
        PipelineExecution execution = composer.compose(pipelineDef);
        return execution.execute();
    }

    @VisibleForTesting
    public PipelineExecution.ExecutionInfo deployWithNoOpComposer() throws Exception {
        return deployWithComposer(this.composer);
    }

    // The main class for running application mode
    public static void main(String[] args) throws Exception {
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(resolvePipelineDef(args[0]), new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPipelineComposer flinkPipelineComposer =
                FlinkPipelineComposer.ofApplicationCluster(env);
        PipelineExecution execution = flinkPipelineComposer.compose(pipelineDef);
        execution.execute();
    }

    /**
     * Resolves the application-mode entrypoint argument into the pipeline definition YAML content.
     *
     * <p>The two native application deployment executors share this single entrypoint but pass
     * {@code args[0]} with different semantics:
     *
     * <ul>
     *   <li>{@link org.apache.flink.cdc.composer.flink.deployment.K8SApplicationDeploymentExecutor}
     *       sets {@code APPLICATION_ARGS = commandLine.getArgList()}, so {@code args[0]} is the
     *       pipeline definition FILE PATH shipped into the JobManager container (e.g. mounted via a
     *       ConfigMap by the Flink Kubernetes Operator).
     *   <li>{@link
     *       org.apache.flink.cdc.composer.flink.deployment.YarnApplicationDeploymentExecutor} reads
     *       the file on the client side and sets {@code APPLICATION_ARGS} to the pipeline
     *       definition CONTENT, because it does not ship the file into the YARN container.
     * </ul>
     *
     * <p>Three cases are handled, in order:
     *
     * <ol>
     *   <li>An explicit-scheme path (e.g. {@code s3://}, {@code hdfs://}, {@code oss://}, {@code
     *       file://}) is read through Flink's FileSystem so the matching plugin resolves it. The
     *       scheme is explicit, so — unlike a bare local path — it is not at risk of being hijacked
     *       by the cluster default FileSystem.
     *   <li>A bare local file path (e.g. shipped into the JobManager container / mounted by a
     *       ConfigMap by the Flink Kubernetes Operator) is read with the local JVM file API rather
     *       than Flink's FileSystem, whose cluster default may be S3/HDFS and would not resolve a
     *       local path.
     *   <li>Otherwise the value is already the pipeline definition CONTENT (the YARN application
     *       executor reads the file on the client side and passes the content) and is used
     *       verbatim. Without distinguishing these, the parser's String overload would treat a file
     *       path as YAML content and fail with: Missing required field "source".
     * </ol>
     */
    @VisibleForTesting
    static String resolvePipelineDef(String pipelineDefPathOrContent) throws Exception {
        // Case 1: explicit-scheme path -> read through Flink's FileSystem (plugin-aware).
        URI uri = tryParseUri(pipelineDefPathOrContent);
        if (uri != null && uri.getScheme() != null) {
            Path remotePath = new Path(pipelineDefPathOrContent);
            FileSystem fileSystem = remotePath.getFileSystem();
            try (FSDataInputStream in = fileSystem.open(remotePath);
                    ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
                return new String(out.toByteArray(), StandardCharsets.UTF_8);
            }
        }

        // Case 2: bare local file path -> read with the local JVM file API to avoid the cluster
        // default FileSystem (which may be S3/HDFS) hijacking a local path.
        try {
            java.nio.file.Path localPath = Paths.get(pipelineDefPathOrContent);
            if (Files.isRegularFile(localPath)) {
                return new String(Files.readAllBytes(localPath), StandardCharsets.UTF_8);
            }
        } catch (InvalidPathException ignored) {
            // Not a valid local path; fall through.
        }

        // Case 3: not a path -> the YARN application executor already passes the CONTENT.
        return pipelineDefPathOrContent;
    }

    private static URI tryParseUri(String value) {
        try {
            return new URI(value);
        } catch (URISyntaxException e) {
            return null;
        }
    }

    @VisibleForTesting
    void setComposer(PipelineComposer composer) {
        this.composer = composer;
    }

    @VisibleForTesting
    public org.apache.flink.configuration.Configuration getFlinkConfig() {
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

    public String getDeploymentTarget() {
        return flinkConfig.get(DeploymentOptions.TARGET);
    }
}
