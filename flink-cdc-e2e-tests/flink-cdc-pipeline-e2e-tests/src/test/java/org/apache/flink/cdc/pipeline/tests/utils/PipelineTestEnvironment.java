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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Volume;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkState;

/** Test environment running pipeline job on Flink containers. */
@Testcontainers
public abstract class PipelineTestEnvironment extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineTestEnvironment.class);

    protected Integer parallelism = getParallelism();

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

    public static final Network NETWORK = Network.newNetwork();

    @Container
    protected static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer(MySqlVersion.V8_0)
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases(INTER_CONTAINER_MYSQL_ALIAS)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    // ------------------------------------------------------------------------------------------
    // Flink Variables
    // ------------------------------------------------------------------------------------------
    protected static final int JOB_MANAGER_REST_PORT = 8081;
    protected static final String INTER_CONTAINER_JM_ALIAS = "jobmanager";
    protected static final String INTER_CONTAINER_TM_ALIAS = "taskmanager";
    protected static final List<String> EXTERNAL_PROPS =
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
                    "execution.checkpointing.savepoint-dir: file:///opt/flink",
                    "restart-strategy.type: off",
                    // Set off-heap memory explicitly to avoid "java.lang.OutOfMemoryError: Direct
                    // buffer memory" error.
                    "taskmanager.memory.task.off-heap.size: 128mb");
    public static final String FLINK_PROPERTIES = String.join("\n", EXTERNAL_PROPS);

    @Nullable protected RestClusterClient<StandaloneClusterId> restClusterClient;

    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;
    protected Volume sharedVolume = new Volume("/tmp/shared");

    protected ToStringConsumer jobManagerConsumer;

    protected ToStringConsumer taskManagerConsumer;

    protected String flinkVersion = getFlinkVersion();

    public static String getFlinkVersion() {
        String flinkVersion = System.getProperty("specifiedFlinkVersion");
        if (Objects.isNull(flinkVersion)) {
            throw new IllegalArgumentException(
                    "No Flink version specified to run this test. Please use -DspecifiedFlinkVersion to pass one.");
        }
        return flinkVersion;
    }

    protected List<String> copyJarToFlinkLib() {
        return Collections.emptyList();
    }

    @BeforeEach
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

        List<String> jarToCopy = copyJarToFlinkLib();
        if (!jarToCopy.isEmpty()) {
            for (String jar : jarToCopy) {
                jobManager.withCopyFileToContainer(
                        MountableFile.forHostPath(TestUtils.getResource(jar)), "/opt/flink/lib/");
            }
        }

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

    @AfterEach
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

    /**
     * Submits a YAML job to the running cluster with latest CDC version, without from previous
     * savepoint states.
     */
    public JobID submitPipelineJob(String pipelineJob, Path... jars) throws Exception {
        return submitPipelineJob(
                TarballFetcher.CdcVersion.SNAPSHOT, pipelineJob, null, false, jars);
    }

    /**
     * Submits a YAML job to the running cluster with specific CDC version, without from previous
     * savepoint states.
     */
    public JobID submitPipelineJob(
            TarballFetcher.CdcVersion version, String pipelineJob, Path... jars) throws Exception {
        return submitPipelineJob(version, pipelineJob, null, false, jars);
    }

    /** Submits a YAML job to the running cluster with latest CDC version. */
    public JobID submitPipelineJob(
            String pipelineJob,
            @Nullable String savepointPath,
            boolean allowNonRestoredState,
            Path... jars)
            throws Exception {
        return submitPipelineJob(
                TarballFetcher.CdcVersion.SNAPSHOT,
                pipelineJob,
                savepointPath,
                allowNonRestoredState,
                jars);
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
        ExecResult execResult = executeAndCheck(jobManager, commands);
        return Arrays.stream(execResult.getStdout().split("\n"))
                .filter(line -> line.startsWith("Job ID: "))
                .findFirst()
                .map(line -> line.split(": ")[1])
                .map(JobID::fromHexString)
                .orElse(null);
    }

    public String stopJobWithSavepoint(JobID jobID) {
        String savepointPath = "/opt/flink/";
        ExecResult result =
                executeAndCheck(
                        jobManager,
                        "flink",
                        "stop",
                        jobID.toHexString(),
                        "--savepointPath",
                        savepointPath);

        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("Savepoint completed."))
                .findFirst()
                .map(line -> line.split("Path: file:")[1])
                .orElseThrow(
                        () -> new RuntimeException("Failed to parse savepoint path from stdout."));
    }

    public void cancelJob(JobID jobID) {
        executeAndCheck(jobManager, "flink", "cancel", jobID.toHexString());
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
        waitUntilJobState(timeout, JobStatus.RUNNING);
    }

    public void waitUntilJobFinished(Duration timeout) {
        waitUntilJobState(timeout, JobStatus.FINISHED);
    }

    public void waitUntilJobState(Duration timeout, JobStatus expectedStatus) {
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
                if (!expectedStatus.isTerminalState() && jobStatus.isTerminalState()) {
                    throw new ValidationException(
                            String.format(
                                    "Job has been terminated! JobName: %s, JobID: %s, Status: %s",
                                    message.getJobName(),
                                    message.getJobId(),
                                    message.getJobState()));
                } else if (jobStatus == expectedStatus) {
                    return;
                }
            }
        }
    }

    protected String getFlinkDockerImageTag() {
        return String.format("flink:%s-scala_2.12", flinkVersion);
    }

    private ExecResult executeAndCheck(GenericContainer<?> container, String... command) {
        String joinedCommand = String.join(" ", command);
        try {
            LOG.info("Executing command {}", joinedCommand);
            ExecResult execResult =
                    container.execInContainer("bash", "-c", String.join(" ", command));
            LOG.info(execResult.getStdout());
            if (execResult.getExitCode() == 0) {
                LOG.info("Command executed successfully.");
                return execResult;
            } else {
                LOG.error(execResult.getStderr());
                throw new AssertionError(
                        "Failed when submitting the pipeline job. Exit code: "
                                + execResult.getExitCode());
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to execute command " + joinedCommand + " in container " + container);
        }
    }

    public void runInContainerAsRoot(GenericContainer<?> container, String... command)
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
