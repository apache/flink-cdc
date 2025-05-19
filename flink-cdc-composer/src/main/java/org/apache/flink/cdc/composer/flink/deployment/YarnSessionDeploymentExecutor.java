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

import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.flink.shaded.guava31.com.google.common.base.Joiner;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Deploy flink cdc job by yarn session mode. */
public class YarnSessionDeploymentExecutor extends AbstractDeploymentExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(YarnSessionDeploymentExecutor.class);

    @Override
    public PipelineExecution.ExecutionInfo deploy(
            CommandLine commandLine,
            Configuration flinkConfig,
            List<Path> additionalJars,
            Path flinkHome)
            throws Exception {
        LOG.info("Submitting flink job in 'Flink Yarn Session Mode'.");
        if (flinkConfig.get(PipelineOptions.JARS) == null) {
            flinkConfig.set(
                    PipelineOptions.JARS, Collections.singletonList(getFlinkCDCDistJarFromEnv()));
        }
        flinkConfig.set(
                YarnConfigOptions.SHIP_FILES,
                additionalJars.stream().map(Path::toString).collect(Collectors.toList()));

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        Path pipelinePath = new Path(commandLine.getArgList().get(0));
        FileSystem fileSystem = FileSystem.get(pipelinePath.toUri());
        FSDataInputStream pipelineInStream = fileSystem.open(pipelinePath);

        flinkConfig.set(
                ApplicationConfiguration.APPLICATION_ARGS,
                Collections.singletonList(mapper.readTree(pipelineInStream).toString()));
        YarnLogConfigUtil.setLogConfigFileInConfig(
                flinkConfig, Joiner.on(File.separator).join(flinkHome, "conf"));

        flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, getCDCMainClass());
        final YarnClusterClientFactory yarnClusterClientFactory = new YarnClusterClientFactory();
        final YarnClusterDescriptor descriptor =
                yarnClusterClientFactory.createClusterDescriptor(flinkConfig);
        ClusterSpecification specification =
                yarnClusterClientFactory.getClusterSpecification(flinkConfig);

        ClusterClient<ApplicationId> client = null;
        try {
            ClusterClientProvider<ApplicationId> clusterClientProvider =
                    descriptor.deploySessionCluster(specification);
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
}
