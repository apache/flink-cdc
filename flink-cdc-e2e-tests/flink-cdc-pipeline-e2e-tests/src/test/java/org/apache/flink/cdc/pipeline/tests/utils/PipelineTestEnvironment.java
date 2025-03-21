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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.TestLogger;

import com.fasterxml.jackson.core.Version;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Volume;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
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
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String INTER_CONTAINER_MYSQL_ALIAS = "mysql";
    protected static final Duration EVENT_WAITING_TIMEOUT = Duration.ofMinutes(3);
    protected static final Duration STARTUP_WAITING_TIMEOUT = Duration.ofMinutes(5);

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    @ClassRule
    public static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer(MySqlVersion.V8_0)
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases("mysql")
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    // ------------------------------------------------------------------------------------------
    // Flink Variables
    // ------------------------------------------------------------------------------------------
    public static final int JOB_MANAGER_REST_PORT = 8081;
    public static final String INTER_CONTAINER_JM_ALIAS = "jobmanager";
    public static final String INTER_CONTAINER_TM_ALIAS = "taskmanager";
    public static final List<String> EXTERNAL_PROPS =
            Arrays.asList(
                    String.format("jobmanager.rpc.address: %s", INTER_CONTAINER_JM_ALIAS),
                    "jobmanager.bind-host: 0.0.0.0",
                    "taskmanager.bind-host: 0.0.0.0",
                    "rest.bind-address: 0.0.0.0",
                    "rest.address: 0.0.0.0",
                    "jobmanager.memory.process.size: 1GB",
                    "query.server.port: 6125",
                    "blob.server.port: 6124",
                    "taskmanager.numberOfTaskSlots: 10",
                    "parallelism.default: 4",
                    "execution.checkpointing.interval: 300",
                    "state.backend.type: hashmap",
                    "env.java.opts.all: -Doracle.jdbc.timezoneAsRegion=false",
                    "restart-strategy.type: off");
    public static final String FLINK_PROPERTIES = String.join("\n", EXTERNAL_PROPS);

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Nullable protected RestClusterClient<StandaloneClusterId> restClusterClient;
    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;
    protected Volume sharedVolume = new Volume("/tmp/shared");

    protected ToStringConsumer jobManagerConsumer;

    protected ToStringConsumer taskManagerConsumer;

    @Parameterized.Parameters(name = "flinkVersion: {0}")
    public static List<String> getFlinkVersion() {
        String flinkVersion = System.getProperty("specifiedFlinkVersion");
        if (flinkVersion != null) {
            return Collections.singletonList(flinkVersion);
        } else {
            return Arrays.asList("1.19.2", "1.20.1");
        }
    }

    @Before
    public void before() throws Exception {
        LOG.info("Starting containers...");
        jobManagerConsumer = new ToStringConsumer();
        jobManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                        .withExposedPorts(JOB_MANAGER_REST_PORT)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .withCreateContainerCmdModifier(cmd -> cmd.withVolumes(sharedVolume))
                        .withLogConsumer(jobManagerConsumer);
        Startables.deepStart(Stream.of(jobManager)).join();
        runInContainerAsRoot(jobManager, "chmod", "0777", "-R", sharedVolume.toString());
        LOG.info("JobManager is started.");

        taskManagerConsumer = new ToStringConsumer();
        taskManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_TM_ALIAS)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .dependsOn(jobManager)
                        .withVolumesFrom(jobManager, BindMode.READ_WRITE)
                        .withLogConsumer(taskManagerConsumer);
        Startables.deepStart(Stream.of(taskManager)).join();
        runInContainerAsRoot(taskManager, "chmod", "0777", "-R", sharedVolume.toString());
        LOG.info("TaskManager is started.");

        TarballFetcher.fetchLatest(jobManager);
        LOG.info("CDC executables deployed.");
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
    public JobID submitPipelineJob(String pipelineJob, Path... jars) throws Exception {
        return submitPipelineJob(
                TarballFetcher.CdcVersion.SNAPSHOT, pipelineJob, null, false, jars);
    }

    public JobID submitPipelineJob(
            TarballFetcher.CdcVersion version,
            String pipelineJob,
            @Nullable String savepointPath,
            boolean allowNonRestoredState,
            Path... jars)
            throws Exception {

        // Prepare external JAR dependencies
        List<Path> paths = new ArrayList<>(Arrays.asList(jars));
        List<String> containerPaths = new ArrayList<>();
        paths.add(TestUtils.getResource("mysql-driver.jar"));

        for (Path jar : paths) {
            String containerPath = version.workDir() + "/lib/" + jar.getFileName();
            jobManager.copyFileToContainer(MountableFile.forHostPath(jar), containerPath);
            containerPaths.add(containerPath);
        }

        // Attach default MySQL and Values connectors
        containerPaths.add(version.workDir() + "/lib/mysql-cdc-pipeline-connector.jar");
        containerPaths.add(version.workDir() + "/lib/values-cdc-pipeline-connector.jar");

        StringBuilder sb = new StringBuilder();
        for (String containerPath : containerPaths) {
            sb.append(" --jar ").append(containerPath);
        }

        jobManager.copyFileToContainer(
                Transferable.of(pipelineJob), version.workDir() + "/conf/pipeline.yaml");

        String commands =
                version.workDir()
                        + "/bin/flink-cdc.sh "
                        + version.workDir()
                        + "/conf/pipeline.yaml --flink-home /opt/flink"
                        + sb;

        if (savepointPath != null) {
            commands += " --from-savepoint " + savepointPath;
            if (allowNonRestoredState) {
                commands += " --allow-nonRestored-state";
            }
        }
        LOG.info("Execute command: {}", commands);
        ExecResult execResult = jobManager.execInContainer("bash", "-c", commands);
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the pipeline job.");
        }
        return execResult
                .getStdout()
                .lines()
                .filter(line -> line.startsWith("Job ID: "))
                .findFirst()
                .map(line -> line.split(": ")[1])
                .map(JobID::fromHexString)
                .orElse(null);
    }

    public String cancelJob(JobID jobID) throws Exception {
        String savepointPath = "/tmp/cdc/savepoint/sp-" + new Random().nextInt();
        jobManager.execInContainer(
                "bash",
                "-c",
                "flink stop " + jobID.toHexString() + " --savepointPath " + savepointPath);
        return savepointPath;
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

    private void runInContainerAsRoot(GenericContainer<?> container, String... command)
            throws InterruptedException {
        ToStringConsumer stdoutConsumer = new ToStringConsumer();
        ToStringConsumer stderrConsumer = new ToStringConsumer();
        DockerClient dockerClient = DockerClientFactory.instance().client();
        ExecCreateCmdResponse execCreateCmdResponse =
                dockerClient
                        .execCreateCmd(container.getContainerId())
                        .withUser("root")
                        .withCmd(command)
                        .exec();
        FrameConsumerResultCallback callback = new FrameConsumerResultCallback();
        callback.addConsumer(OutputFrame.OutputType.STDOUT, stdoutConsumer);
        callback.addConsumer(OutputFrame.OutputType.STDERR, stderrConsumer);
        dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
    }

    protected List<String> readLines(String resource) throws IOException {
        final URL url = PipelineTestEnvironment.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    protected void validateResult(String... expectedEvents) throws Exception {
        validateResult(Function.identity(), expectedEvents);
    }

    protected void validateResult(Function<String, String> mapper, String... expectedEvents)
            throws Exception {
        validateResult(
                taskManagerConsumer, Stream.of(expectedEvents).map(mapper).toArray(String[]::new));
    }

    protected void validateResult(ToStringConsumer consumer, String... expectedEvents)
            throws Exception {
        for (String event : expectedEvents) {
            waitUntilSpecificEvent(consumer, event);
        }
    }

    protected void waitUntilSpecificEvent(String event) throws Exception {
        waitUntilSpecificEvent(taskManagerConsumer, event);
    }

    protected void waitUntilSpecificEvent(ToStringConsumer consumer, String event)
            throws Exception {
        boolean result = false;
        long endTimeout = System.currentTimeMillis() + EVENT_WAITING_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < endTimeout) {
            String stdout = consumer.toUtf8String();
            if (stdout.contains(event + "\n")) {
                result = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (!result) {
            throw new TimeoutException(
                    "failed to get specific event: "
                            + event
                            + " from stdout: "
                            + consumer.toUtf8String());
        }
    }
}
