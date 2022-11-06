/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.tests.utils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.TestLogger;

import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
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
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkState;

/** Test environment running job on Flink containers. */
@RunWith(Parameterized.class)
public abstract class FlinkContainerTestEnvironment extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainerTestEnvironment.class);

    @Parameterized.Parameter public String flinkVersion;

    // ------------------------------------------------------------------------------------------
    // Flink Variables
    // ------------------------------------------------------------------------------------------
    private static final int JOB_MANAGER_REST_PORT = 8081;
    private static final String FLINK_BIN = "bin";
    private static final String INTER_CONTAINER_JM_ALIAS = "jobmanager";
    private static final String INTER_CONTAINER_TM_ALIAS = "taskmanager";
    private static final String FLINK_PROPERTIES =
            String.join(
                    "\n",
                    Arrays.asList(
                            "jobmanager.rpc.address: jobmanager",
                            "taskmanager.numberOfTaskSlots: 10",
                            "parallelism.default: 4",
                            // this is needed for oracle-cdc tests.
                            // see https://stackoverflow.com/a/47062742/4915129
                            "env.java.opts: -Doracle.jdbc.timezoneAsRegion=false"));

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the sink for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    protected static final String INTER_CONTAINER_MYSQL_ALIAS = "mysql";

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Nullable private RestClusterClient<StandaloneClusterId> restClusterClient;

    @ClassRule
    public static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer()
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases(INTER_CONTAINER_MYSQL_ALIAS)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
    protected Path jdbcJar;

    private GenericContainer<?> jobManager;
    private GenericContainer<?> taskManager;

    @Parameterized.Parameters(name = "flinkVersion: {0}")
    public static List<String> getFlinkVersion() {
        return Arrays.asList("1.13.6", "1.14.4", "1.15.2", "1.16.0");
    }

    private static final List<String> FLINK_VERSION_WITH_SCALA_212 =
            Arrays.asList("1.15.2", "1.16.0");

    @Before
    public void before() {
        mysqlInventoryDatabase.createAndInitialize();
        jdbcJar = TestUtils.getResource(getJdbcConnectorResourceName());

        LOG.info("Starting containers...");
        jobManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                        .withExposedPorts(JOB_MANAGER_REST_PORT)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        taskManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_TM_ALIAS)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .dependsOn(jobManager)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

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
        mysqlInventoryDatabase.dropDatabase();
    }

    /**
     * Submits a SQL job to the running cluster.
     *
     * <p><b>NOTE:</b> You should not use {@code '\t'}.
     */
    public void submitSQLJob(List<String> sqlLines, Path... jars)
            throws IOException, InterruptedException {
        SQLJobSubmission job =
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines).addJars(jars).build();
        final List<String> commands = new ArrayList<>();
        Path script = temporaryFolder.newFile().toPath();
        Files.write(script, job.getSqlLines());
        jobManager.copyFileToContainer(MountableFile.forHostPath(script), "/tmp/script.sql");
        commands.add("cat /tmp/script.sql | ");
        commands.add(FLINK_BIN + "/sql-client.sh");
        for (String jar : job.getJars()) {
            commands.add("--jar");
            String containerPath = copyAndGetContainerPath(jobManager, jar);
            commands.add(containerPath);
        }

        ExecResult execResult =
                jobManager.execInContainer("bash", "-c", String.join(" ", commands));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the SQL job.");
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

    private String copyAndGetContainerPath(GenericContainer<?> container, String filePath) {
        Path path = Paths.get(filePath);
        String containerPath = "/tmp/" + path.getFileName();
        container.copyFileToContainer(MountableFile.forHostPath(path), containerPath);
        return containerPath;
    }

    private String getFlinkDockerImageTag() {
        if (FLINK_VERSION_WITH_SCALA_212.contains(flinkVersion)) {
            return String.format("flink:%s-scala_2.12", flinkVersion);
        }
        return String.format("flink:%s-scala_2.11", flinkVersion);
    }

    protected String getJdbcConnectorResourceName() {
        return String.format("jdbc-connector_%s.jar", flinkVersion);
    }
}
