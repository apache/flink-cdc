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

package org.apache.flink.cdc.composer.flink.deployment;

import org.apache.flink.cdc.composer.PipelineDeploymentExecutor;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Deploy flink cdc job by yarn application mode. */
public class YarnApplicationDeploymentExecutor implements PipelineDeploymentExecutor {
    private static final Logger LOG =
            LoggerFactory.getLogger(YarnApplicationDeploymentExecutor.class);
    private static final String FLINK_CDC_HOME_ENV_VAR = "FLINK_CDC_HOME";

    @Override
    public PipelineExecution.ExecutionInfo deploy(
            CommandLine commandLine, Configuration flinkConfig, List<Path> additionalJars)
            throws Exception {
        LOG.info("Submitting application in 'Flink Yarn Application Mode'.");
        flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
        if (flinkConfig.get(PipelineOptions.JARS) == null) {
            // Set the SHIP_FILES option in the Flink configuration to include additional JAR files
            getFlinkCDCDistJarFromEnv()
                    .ifPresent(
                            distJar -> flinkConfig.set(YarnConfigOptions.FLINK_DIST_JAR, distJar));
        }
        flinkConfig.set(
                YarnConfigOptions.SHIP_FILES,
                additionalJars.stream().map(Path::toString).collect(Collectors.toList()));

        flinkConfig.set(ApplicationConfiguration.APPLICATION_ARGS, commandLine.getArgList());
        flinkConfig.set(
                ApplicationConfiguration.APPLICATION_MAIN_CLASS,
                "org.apache.flink.cdc.cli.CliFrontend");
        final YarnClusterClientFactory yarnClusterClientFactory = new YarnClusterClientFactory();
        final YarnClusterDescriptor descriptor =
                yarnClusterClientFactory.createClusterDescriptor(flinkConfig);
        ClusterSpecification specification =
                yarnClusterClientFactory.getClusterSpecification(flinkConfig);
        ApplicationConfiguration applicationConfiguration =
                ApplicationConfiguration.fromConfiguration(flinkConfig);

        ClusterClient<ApplicationId> client = null;
        try {
            ClusterClientProvider<ApplicationId> clusterClientProvider =
                    descriptor.deployApplicationCluster(specification, applicationConfiguration);
            client = clusterClientProvider.getClusterClient();
            ApplicationId clusterId = client.getClusterId();
            LOG.info("Deployment Flink CDC From Cluster ID {}", clusterId);
            return new PipelineExecution.ExecutionInfo(
                    clusterId.toString(), "submit job successful");
        } catch (Exception e) {
            if (client != null) {
                client.shutDownCluster();
            }
            throw new RuntimeException("Failed to deploy Flink CDC job", e);
        } finally {
            descriptor.close();
            if (client != null) {
                client.close();
            }
        }
    }

    private Optional<String> getFlinkCDCDistJarFromEnv() {
        String flinkCDCHomeFromEnvVar = System.getenv(FLINK_CDC_HOME_ENV_VAR);
        Path flinkCDCLibPath = Paths.get(flinkCDCHomeFromEnvVar).resolve("lib");
        if (!Files.exists(flinkCDCLibPath) || !Files.isDirectory(flinkCDCLibPath)) {
            LOG.error(
                    "Flink cdc home lib is not file or not directory: {}",
                    flinkCDCLibPath.toAbsolutePath());
            return Optional.empty();
        }
        try (Stream<Path> paths = Files.walk(flinkCDCLibPath)) {
            List<String> distJars = new ArrayList<>();
            paths.filter(Files::isRegularFile)
                    .filter(
                            path ->
                                    path.getFileName()
                                            .toString()
                                            .matches("flink-cdc-dist-.*-.*\\.jar"))
                    .forEach(path -> distJars.add(String.valueOf(path.toAbsolutePath())));
            return Optional.ofNullable(distJars.get(0));
        } catch (IOException e) {
            LOG.error(
                    "Get  flink-cdc-dist.jar from Flink cdc home lib is : {} failed",
                    flinkCDCLibPath.toAbsolutePath(),
                    e);
            return Optional.empty();
        }
    }
}
