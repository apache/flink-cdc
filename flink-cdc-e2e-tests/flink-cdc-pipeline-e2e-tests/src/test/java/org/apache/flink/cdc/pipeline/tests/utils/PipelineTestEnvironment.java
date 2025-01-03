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

package org.apache.flink.cdc.pipeline.tests.utils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.TestLogger;

import com.fasterxml.jackson.core.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkState;

/** Test environment running pipeline job on Flink containers. */
@RunWith(Parameterized.class)
public abstract class PipelineTestEnvironment extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineTestEnvironment.class);

    @Parameterized.Parameter public String flinkVersion;

    public Integer parallelism = getParallelism();

    private int getParallelism() {
        try {
            return Integer.parseInt(System.getProperty("specifiedParallelism"));
        } catch (NumberFormatException ex) {
            LOG.warn(
                    "Unable to parse specified parallelism configuration ({} provided). Use 4 by default.",
                    System.getProperty("specifiedParallelism"));
            return 4;
        }
    }

    // ------------------------------------------------------------------------------------------
    // Flink Variables
    // ------------------------------------------------------------------------------------------
    public static final int JOB_MANAGER_REST_PORT = 8081;
    public static final String INTER_CONTAINER_JM_ALIAS = "jobmanager";
    public static final String INTER_CONTAINER_TM_ALIAS = "taskmanager";

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Nullable protected RestClusterClient<StandaloneClusterId> restClusterClient;
    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;

    protected ToStringConsumer jobManagerConsumer;

    protected ToStringConsumer taskManagerConsumer;

    @Parameterized.Parameters(name = "flinkVersion: {0}")
    public static List<String> getFlinkVersion() {
        String flinkVersion = System.getProperty("specifiedFlinkVersion");
        if (flinkVersion != null) {
            return Collections.singletonList(flinkVersion);
        } else {
            return Arrays.asList("1.19.1", "1.20.0");
        }
    }

    @Before
    public void before() throws Exception {
        LOG.info("Starting containers...");
        jobManagerConsumer = new ToStringConsumer();

        String flinkProperties = getFlinkProperties();

        jobManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                        .withExposedPorts(JOB_MANAGER_REST_PORT)
                        .withEnv("FLINK_PROPERTIES", flinkProperties)
                        .withLogConsumer(jobManagerConsumer);
        taskManagerConsumer = new ToStringConsumer();
        taskManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_TM_ALIAS)
                        .withEnv("FLINK_PROPERTIES", flinkProperties)
                        .dependsOn(jobManager)
                        .withLogConsumer(taskManagerConsumer);

        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        LOG.info("Containers are started.");
    }

    @After
    public void after() {
        if (restClusterClient != null) {
            restClusterClient.close();
        }
        if (jobManager != null) {
            jobManager.stop();
        }
        if (taskManager != null) {
            taskManager.stop();
        }
    }

    /** Allow overriding the default flink properties. */
    public void overrideFlinkProperties(String properties) {
        jobManager.withEnv("FLINK_PROPERTIES", properties);
        taskManager.withEnv("FLINK_PROPERTIES", properties);
    }

    /**
     * Submits a SQL job to the running cluster.
     *
     * <p><b>NOTE:</b> You should not use {@code '\t'}.
     */
    public void submitPipelineJob(String pipelineJob, Path... jars)
            throws IOException, InterruptedException {
        for (Path jar : jars) {
            jobManager.copyFileToContainer(
                    MountableFile.forHostPath(jar), "/tmp/flinkCDC/lib/" + jar.getFileName());
        }
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(
                        TestUtils.getResource("flink-cdc.sh", "flink-cdc-dist", "src"), 755),
                "/tmp/flinkCDC/bin/flink-cdc.sh");
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(
                        TestUtils.getResource("flink-cdc.yaml", "flink-cdc-dist", "src"), 755),
                "/tmp/flinkCDC/conf/flink-cdc.yaml");
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("flink-cdc-dist.jar")),
                "/tmp/flinkCDC/lib/flink-cdc-dist.jar");
        Path script = temporaryFolder.newFile().toPath();
        Files.write(script, pipelineJob.getBytes());
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(script), "/tmp/flinkCDC/conf/pipeline.yaml");
        StringBuilder sb = new StringBuilder();
        for (Path jar : jars) {
            sb.append(" --jar /tmp/flinkCDC/lib/").append(jar.getFileName());
        }
        String commands =
                "/tmp/flinkCDC/bin/flink-cdc.sh /tmp/flinkCDC/conf/pipeline.yaml --flink-home /opt/flink"
                        + sb;
        ExecResult execResult = jobManager.execInContainer("bash", "-c", commands);
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the pipeline job.");
        }
    }

    /**
     * Get {@link RestClusterClient} connected to this FlinkContainer.
     *
     * <p>This method lazily initializes the REST client on-demand.
     */
    public RestClusterClient<StandaloneClusterId> getRestClusterClient() {
        if (restClusterClient != null) {
            return restClusterClient;
        }
        checkState(
                jobManager.isRunning(),
                "Cluster client should only be retrieved for a running cluster");
        try {
            final Configuration clientConfiguration = new Configuration();
            clientConfiguration.set(RestOptions.ADDRESS, jobManager.getHost());
            clientConfiguration.set(
                    RestOptions.PORT, jobManager.getMappedPort(JOB_MANAGER_REST_PORT));
            this.restClusterClient =
                    new RestClusterClient<>(clientConfiguration, StandaloneClusterId.getInstance());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to create client for Flink container cluster", e);
        }
        return restClusterClient;
    }

    public void waitUntilJobRunning(Duration timeout) {
        RestClusterClient<?> clusterClient = getRestClusterClient();
        Deadline deadline = Deadline.fromNow(timeout);
        while (deadline.hasTimeLeft()) {
            Collection<JobStatusMessage> jobStatusMessages;
            try {
                jobStatusMessages = clusterClient.listJobs().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.warn("Error when fetching job status.", e);
                continue;
            }
            if (jobStatusMessages != null && !jobStatusMessages.isEmpty()) {
                JobStatusMessage message = jobStatusMessages.iterator().next();
                JobStatus jobStatus = message.getJobState();
                if (jobStatus.isTerminalState()) {
                    throw new ValidationException(
                            String.format(
                                    "Job has been terminated! JobName: %s, JobID: %s, Status: %s",
                                    message.getJobName(),
                                    message.getJobId(),
                                    message.getJobState()));
                } else if (jobStatus == JobStatus.RUNNING) {
                    return;
                }
            }
        }
    }

    protected String getFlinkDockerImageTag() {
        return String.format("flink:%s-scala_2.12", flinkVersion);
    }

    private static Version parseVersion(String version) {
        List<Integer> versionParts =
                Arrays.stream(version.split("\\."))
                        .map(Integer::valueOf)
                        .limit(3)
                        .collect(Collectors.toList());
        return new Version(
                versionParts.get(0), versionParts.get(1), versionParts.get(2), null, null, null);
    }

    private static String getFlinkProperties() {
        return String.join(
                "\n",
                Arrays.asList(
                        "restart-strategy.type: off",
                        "jobmanager.rpc.address: jobmanager",
                        "taskmanager.numberOfTaskSlots: 10",
                        "parallelism.default: 4",
                        "execution.checkpointing.interval: 300",
                        "env.java.opts.all: -Doracle.jdbc.timezoneAsRegion=false"));
    }
}
