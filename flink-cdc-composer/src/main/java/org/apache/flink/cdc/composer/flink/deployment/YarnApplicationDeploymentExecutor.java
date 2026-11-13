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

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.composer.PipelineDeploymentExecutor;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.shaded.guava.com.google.common.base.Joiner;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.CodeSource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Deploy flink cdc job by yarn application mode. */
public class YarnApplicationDeploymentExecutor implements PipelineDeploymentExecutor {
    private static final Logger LOG =
            LoggerFactory.getLogger(YarnApplicationDeploymentExecutor.class);

    private static final String FLINK_CDC_HOME_ENV_VAR = "FLINK_CDC_HOME";
    private static final String FLINK_CDC_DIST_JAR_PREFIX = "flink-cdc-dist-";
    private static final String FLINK_CDC_DIST_JAR_VERSIONED_PREFIX = "flink-cdc-dist-flink-";
    private static final String JAR_SUFFIX = ".jar";
    private static final String APPLICATION_MAIN_CLASS = "org.apache.flink.cdc.cli.CliExecutor";

    @Override
    public PipelineExecution.ExecutionInfo deploy(
            CommandLine commandLine,
            Configuration flinkConfig,
            List<Path> additionalJars,
            Path flinkHome)
            throws Exception {
        LOG.info("Submitting application in 'Flink Yarn Application Mode'.");
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

        flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, APPLICATION_MAIN_CLASS);
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

    private String getFlinkCDCDistJarFromEnv() throws IOException {
        String flinkCDCHomeFromEnvVar = System.getenv(FLINK_CDC_HOME_ENV_VAR);
        Preconditions.checkNotNull(
                flinkCDCHomeFromEnvVar,
                "FLINK_CDC_HOME is not correctly set in environment variable, current FLINK_CDC_HOME is: "
                        + flinkCDCHomeFromEnvVar);
        return getFlinkCDCDistJar(new Path(flinkCDCHomeFromEnvVar, "lib"));
    }

    /**
     * Finds the Flink CDC dist jar under the given lib directory, matching the Flink version this
     * flink-cdc build targets. The version is derived at runtime from the identity of the jar the
     * executor itself is running from.
     */
    static String getFlinkCDCDistJar(Path flinkCDCLibPath) throws IOException {
        return getFlinkCDCDistJar(flinkCDCLibPath, resolveRuntimeFlinkMajorVersion());
    }

    /**
     * Finds the Flink CDC dist jar under the given lib directory for the given Flink major version.
     * Versioned jar names ({@code flink-cdc-dist-flink-<ver>-<revision>.jar}) are matched exactly
     * against {@code flinkMajorVersion}, so a lib directory holding dist jars of several Flink
     * lines resolves to the right one, and a lib holding only dist jars of other Flink lines fails
     * loudly instead of silently picking the wrong jar. Dist jars predating the versioned naming,
     * e.g. {@code flink-cdc-dist-3.6.0-1.20.jar}, are matched by name prefix as before.
     */
    @VisibleForTesting
    static String getFlinkCDCDistJar(Path flinkCDCLibPath, String flinkMajorVersion)
            throws IOException {
        if (!flinkCDCLibPath.getFileSystem().exists(flinkCDCLibPath)
                || !flinkCDCLibPath.getFileSystem().getFileStatus(flinkCDCLibPath).isDir()) {
            throw new RuntimeException(
                    "Flink cdc home lib is not file or not directory: "
                            + flinkCDCLibPath.makeQualified(flinkCDCLibPath.getFileSystem()));
        }

        FileStatus[] fileStatuses = flinkCDCLibPath.getFileSystem().listStatus(flinkCDCLibPath);
        List<Path> distJars =
                Arrays.stream(fileStatuses)
                        .filter(status -> !status.isDir())
                        .map(FileStatus::getPath)
                        .filter(
                                path ->
                                        path.getName().startsWith(FLINK_CDC_DIST_JAR_PREFIX)
                                                && path.getName().endsWith(JAR_SUFFIX))
                        .collect(Collectors.toList());
        List<Path> matchingVersion =
                distJars.stream()
                        .filter(
                                path ->
                                        path.getName()
                                                .startsWith(
                                                        FLINK_CDC_DIST_JAR_VERSIONED_PREFIX
                                                                + flinkMajorVersion
                                                                + "-"))
                        .collect(Collectors.toList());
        if (matchingVersion.size() > 1) {
            throw new RuntimeException(
                    "Multiple Flink CDC dist jars for Flink "
                            + flinkMajorVersion
                            + " found in path: "
                            + flinkCDCLibPath
                            + ", found: "
                            + jarNames(matchingVersion));
        }
        if (matchingVersion.size() == 1) {
            return qualified(matchingVersion.get(0));
        }
        List<Path> otherVersionedJars =
                distJars.stream()
                        .filter(
                                path ->
                                        path.getName()
                                                .startsWith(FLINK_CDC_DIST_JAR_VERSIONED_PREFIX))
                        .collect(Collectors.toList());
        if (!otherVersionedJars.isEmpty()) {
            throw new RuntimeException(
                    "Flink CDC dist jars found in path "
                            + flinkCDCLibPath
                            + " are built for other Flink versions, found: "
                            + jarNames(otherVersionedJars)
                            + ", but this flink-cdc build targets Flink "
                            + flinkMajorVersion);
        }
        Optional<Path> legacyJar = distJars.stream().findFirst();
        if (legacyJar.isPresent()) {
            return qualified(legacyJar.get());
        }
        throw new FileNotFoundException(
                "Failed to fetch Flink CDC dist jar from path: " + flinkCDCLibPath);
    }

    private static String qualified(Path path) throws IOException {
        return path.makeQualified(path.getFileSystem()).toString();
    }

    private static String jarNames(List<Path> paths) {
        return paths.stream().map(path -> path.getName()).collect(Collectors.joining(", "));
    }

    /**
     * Resolves the Flink major version this build targets, derived from the identity of the jar the
     * executor itself is running from: the per-version dist jars carry the Flink line in their
     * artifactId ({@code flink-cdc-dist-flink-<ver>-...}), so a dist jar can always answer for
     * itself. Classpath shapes without a versioned identity (thin jars, class directories,
     * third-party fat jars) carry no generation information and fail loudly.
     */
    private static String resolveRuntimeFlinkMajorVersion() {
        CodeSource codeSource =
                YarnApplicationDeploymentExecutor.class.getProtectionDomain().getCodeSource();
        Preconditions.checkNotNull(
                codeSource, "Cannot determine the running jar: code source is unavailable");
        String location = codeSource.getLocation().getPath();
        int lastSlash = location.lastIndexOf('/');
        String jarName = lastSlash >= 0 ? location.substring(lastSlash + 1) : location;
        return parseFlinkMajorVersionFromJarName(jarName);
    }

    /**
     * Extracts the Flink major version from a dist jar file name. Understands both the per-version
     * naming ({@code flink-cdc-dist-flink-<ver>-<revision>.jar}) and the legacy version-suffix
     * naming ({@code flink-cdc-dist-<revision>-<ver>.jar}); anything else fails loudly.
     */
    @VisibleForTesting
    static String parseFlinkMajorVersionFromJarName(String jarName) {
        String name = jarName;
        if (name.endsWith(JAR_SUFFIX)) {
            name = name.substring(0, name.length() - JAR_SUFFIX.length());
        }
        if (name.startsWith(FLINK_CDC_DIST_JAR_VERSIONED_PREFIX)) {
            String remainder = name.substring(FLINK_CDC_DIST_JAR_VERSIONED_PREFIX.length());
            int dash = remainder.indexOf('-');
            String version = dash < 0 ? remainder : remainder.substring(0, dash);
            if (!version.isEmpty()) {
                return version;
            }
        } else if (name.startsWith(FLINK_CDC_DIST_JAR_PREFIX)) {
            // Legacy naming, e.g. flink-cdc-dist-3.6.0-1.20.jar: the Flink line is the last
            // dash-separated segment.
            String remainder = name.substring(FLINK_CDC_DIST_JAR_PREFIX.length());
            int dash = remainder.lastIndexOf('-');
            if (dash > 0) {
                String version = remainder.substring(dash + 1);
                if (!version.isEmpty()) {
                    return version;
                }
            }
        }
        throw new RuntimeException(
                "Cannot determine the Flink version of the running flink-cdc build from jar name '"
                        + jarName
                        + "': Yarn application deployment requires the per-version dist jar "
                        + "(flink-cdc-dist-flink-<ver>-*.jar) on the classpath");
    }
}
