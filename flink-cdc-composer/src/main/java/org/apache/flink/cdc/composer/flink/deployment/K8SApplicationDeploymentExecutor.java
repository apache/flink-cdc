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
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** deploy flink cdc job by native k8s application mode. */
public class K8SApplicationDeploymentExecutor implements PipelineDeploymentExecutor {
    private static final Logger LOG =
            LoggerFactory.getLogger(K8SApplicationDeploymentExecutor.class);

    private static final String APPLICATION_MAIN_CLASS = "org.apache.flink.cdc.cli.CliExecutor";

    @Override
    public PipelineExecution.ExecutionInfo deploy(
            CommandLine commandLine,
            Configuration flinkConfig,
            List<Path> additionalJars,
            Path flinkHome) {
        LOG.info("Submitting application in 'Flink K8S Application Mode'.");
        List<String> jars = new ArrayList<>();
        if (flinkConfig.get(PipelineOptions.JARS) == null) {
            // must be added cdc dist jar by default docker container path
            jars.add("local:///opt/flink-cdc/lib/flink-cdc-dist.jar");
            flinkConfig.set(PipelineOptions.JARS, jars);
        }
        // set the default cdc latest docker image
        flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, "flink/flink-cdc:latest");
        flinkConfig.set(ApplicationConfiguration.APPLICATION_ARGS, commandLine.getArgList());
        flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, APPLICATION_MAIN_CLASS);
        KubernetesClusterClientFactory kubernetesClusterClientFactory =
                new KubernetesClusterClientFactory();
        KubernetesClusterDescriptor descriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(flinkConfig);
        ClusterSpecification specification =
                kubernetesClusterClientFactory.getClusterSpecification(flinkConfig);
        ApplicationConfiguration applicationConfiguration =
                ApplicationConfiguration.fromConfiguration(flinkConfig);
        ClusterClient<String> client = null;
        try {
            ClusterClientProvider<String> clusterClientProvider =
                    descriptor.deployApplicationCluster(specification, applicationConfiguration);
            client = clusterClientProvider.getClusterClient();
            String clusterId = client.getClusterId();
            LOG.info("Deployment Flink CDC From Cluster ID {}", clusterId);
            return new PipelineExecution.ExecutionInfo(clusterId, "submit job successful");
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
