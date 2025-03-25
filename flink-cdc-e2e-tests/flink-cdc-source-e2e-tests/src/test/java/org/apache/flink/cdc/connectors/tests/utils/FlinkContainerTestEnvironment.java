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

package org.apache.flink.cdc.connectors.tests.utils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.TestLogger;

import com.fasterxml.jackson.core.Version;
import com.github.dockerjava.api.DockerClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkState;

/** Test environment running job on Flink containers. */
@Testcontainers
public abstract class FlinkContainerTestEnvironment extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainerTestEnvironment.class);

    // ------------------------------------------------------------------------------------------
    // Flink Variables
    // ------------------------------------------------------------------------------------------
    private static final int JOB_MANAGER_REST_PORT = 8081;
    private static final String FLINK_BIN = "bin";
    private static final String INTER_CONTAINER_JM_ALIAS = "jobmanager";
    private static final String INTER_CONTAINER_TM_ALIAS = "taskmanager";

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the sink for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    protected static final String INTER_CONTAINER_MYSQL_ALIAS = "mysql";

    protected String flinkVersion = getFlinkVersion();

    public static final Network NETWORK = Network.newNetwork();

    @TempDir public Path temporaryFolder;

    @Nullable private RestClusterClient<StandaloneClusterId> restClusterClient;

    @Container
    public static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer(
                                    MySqlVersion.V8_0) // v8 support both ARM and AMD architectures
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

    public static String getFlinkVersion() {
        String flinkVersion = System.getProperty("specifiedFlinkVersion");
        if (Objects.isNull(flinkVersion)) {
            throw new IllegalArgumentException(
                    "No Flink version specified to run this test. Please use -DspecifiedFlinkVersion to pass one.");
        }
        return flinkVersion;
    }

    @BeforeEach
    public void before() {
        mysqlInventoryDatabase.createAndInitialize();
        jdbcJar = TestUtils.getResource(getJdbcConnectorResourceName());

        String flinkProperties = getFlinkProperties(flinkVersion);

        LOG.info("Starting containers...");
        jobManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                        .withExposedPorts(JOB_MANAGER_REST_PORT)
                        .withEnv("FLINK_PROPERTIES", flinkProperties)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        taskManager =
                new GenericContainer<>(getFlinkDockerImageTag())
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_TM_ALIAS)
                        .withEnv("FLINK_PROPERTIES", flinkProperties)
                        .dependsOn(jobManager)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        LOG.info("Containers are started.");
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
        mysqlInventoryDatabase.dropDatabase();
    }

    @AfterAll
    public static void afterClass() {
        DockerClient dockerClient = DockerClientFactory.instance().client();

        // List all containers and remove the ones that are not testcontainers related.
        dockerClient.listContainersCmd().exec().stream()
                .filter(container -> !container.getImage().startsWith("testcontainers"))
                .forEach(
                        container -> {
                            dockerClient.stopContainerCmd(container.getId()).exec();
                            dockerClient.removeContainerCmd(container.getId()).exec();
                        });

        // List all images and remove the ones that are not Flink, MySQL, and TestContainers
        // related.
        dockerClient.listImagesCmd().exec().stream()
                .filter(
                        image ->
                                image.getRepoTags() != null
                                        && Arrays.stream(image.getRepoTags())
                                                .anyMatch(
                                                        tag ->
                                                                !tag.startsWith("flink:")
                                                                        && !tag.startsWith(
                                                                                "testcontainers")
                                                                        && !tag.equals(
                                                                                MYSQL
                                                                                        .getDockerImageName())))
                .forEach(
                        image -> {
                            try {
                                dockerClient.removeImageCmd(image.getId()).exec();
                            } catch (Exception e) {
                                LOG.warn(
                                        "Failed to remove image: {}",
                                        String.join(",", image.getRepoTags()));
                            }
                        });
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
    public void submitSQLJob(List<String> sqlLines, Path... jars)
            throws IOException, InterruptedException {
        SQLJobSubmission job =
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines).addJars(jars).build();
        final List<String> commands = new ArrayList<>();
        Path script = Files.createFile(temporaryFolder.resolve("script.sql"));
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
        return String.format("flink:%s-scala_2.12", flinkVersion);
    }

    protected String getJdbcConnectorResourceName() {
        return String.format("jdbc-connector_%s.jar", flinkVersion);
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

    private static String getFlinkProperties(String flinkVersion) {
        // this is needed for oracle-cdc tests.
        // see https://stackoverflow.com/a/47062742/4915129
        String javaOptsConfig;
        Version version = parseVersion(flinkVersion);
        if (version.compareTo(parseVersion("1.17.0")) >= 0) {
            // Flink 1.17 renames `env.java.opts` to `env.java.opts.all`
            javaOptsConfig = "env.java.opts.all: -Doracle.jdbc.timezoneAsRegion=false";
        } else {
            // Legacy Flink version, might drop their support in near future
            javaOptsConfig = "env.java.opts: -Doracle.jdbc.timezoneAsRegion=false";
        }

        return String.join(
                "\n",
                Arrays.asList(
                        "jobmanager.rpc.address: jobmanager",
                        "taskmanager.numberOfTaskSlots: 10",
                        "parallelism.default: 4",
                        "execution.checkpointing.interval: 300",
                        javaOptsConfig));
    }
}
