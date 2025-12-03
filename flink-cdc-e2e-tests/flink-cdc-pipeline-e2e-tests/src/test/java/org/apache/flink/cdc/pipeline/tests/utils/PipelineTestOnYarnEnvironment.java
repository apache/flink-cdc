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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/** Test environment running pipeline job on YARN mini-cluster. */
public class PipelineTestOnYarnEnvironment extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineTestOnYarnEnvironment.class);

    protected static final YarnConfiguration YARN_CONFIGURATION;
    private YarnClient yarnClient = null;
    protected static MiniYARNCluster yarnCluster = null;

    protected static final String TEST_CLUSTER_NAME_KEY = "flink-yarn-minicluster-name";
    protected static final int NUM_NODEMANAGERS = 2;

    protected static File yarnSiteXML = null;

    @TempDir Path temporaryFolder;

    private static final Duration yarnAppTerminateTimeout = Duration.ofSeconds(120);
    private static final int sleepIntervalInMS = 100;

    // copy from org.apache.flink.yarn.YarnTestBase
    static {
        YARN_CONFIGURATION = new YarnConfiguration();
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 32);
        YARN_CONFIGURATION.setInt(
                YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
                4096); // 4096 is the available memory anyways
        YARN_CONFIGURATION.setBoolean(
                YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4);
        YARN_CONFIGURATION.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);
        YARN_CONFIGURATION.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
        YARN_CONFIGURATION.setInt(
                YarnConfiguration.NM_VCORES, 666); // memory is overwritten in the MiniYARNCluster.
        // so we have to change the number of cores for testing.
        YARN_CONFIGURATION.setFloat(
                YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 99.0F);
        YARN_CONFIGURATION.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, getYarnClasspath());
        YARN_CONFIGURATION.setInt(
                YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 1000);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 5000);
        YARN_CONFIGURATION.set(TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-application");
    }

    @BeforeEach
    public void setupYarnClient() throws Exception {
        if (yarnClient == null) {
            yarnClient = YarnClient.createYarnClient();
            yarnClient.init(getYarnConfiguration());
            yarnClient.start();
        }
    }

    @AfterEach
    public void shutdownYarnClient() {
        yarnClient.stop();
    }

    @AfterAll
    public static void teardown() {

        if (yarnCluster != null) {
            LOG.info("Stopping MiniYarn Cluster");
            yarnCluster.stop();
            yarnCluster = null;
        }

        // Unset FLINK_CONF_DIR, as it might change the behavior of other tests
        Map<String, String> map = new HashMap<>(System.getenv());
        map.remove(ConfigConstants.ENV_FLINK_CONF_DIR);
        map.remove("YARN_CONF_DIR");
        map.remove("IN_TESTS");
        CommonTestUtils.setEnv(map);

        if (yarnSiteXML != null) {
            yarnSiteXML.delete();
        }
    }

    protected static YarnConfiguration getYarnConfiguration() {
        return YARN_CONFIGURATION;
    }

    public static void startMiniYARNCluster() {
        try {
            if (yarnCluster == null) {
                final String testName =
                        YARN_CONFIGURATION.get(PipelineTestOnYarnEnvironment.TEST_CLUSTER_NAME_KEY);
                yarnCluster =
                        new MiniYARNCluster(
                                testName == null ? "YarnTest_" + UUID.randomUUID() : testName,
                                NUM_NODEMANAGERS,
                                1,
                                1);

                yarnCluster.init(YARN_CONFIGURATION);
                yarnCluster.start();
            }

            File targetTestClassesFolder = new File("target/test-classes");
            writeYarnSiteConfigXML(YARN_CONFIGURATION, targetTestClassesFolder);

            Map<String, String> map = new HashMap<String, String>(System.getenv());
            map.put(
                    "IN_TESTS",
                    "yes we are in tests"); // see YarnClusterDescriptor() for more infos
            map.put("YARN_CONF_DIR", targetTestClassesFolder.getAbsolutePath());
            map.put("MAX_LOG_FILE_NUMBER", "10");
            CommonTestUtils.setEnv(map);

            assertThat(yarnCluster.getServiceState()).isEqualTo(Service.STATE.STARTED);
            // wait for the NodeManagers to connect
            while (!yarnCluster.waitForNodeManagersToConnect(500)) {
                LOG.info("Waiting for NodeManagers to connect");
            }
        } catch (Exception e) {
            fail("Starting MiniYARNCluster failed: ", e);
        }
    }

    // write yarn-site.xml to target/test-classes so that flink pick can pick up this when
    // initializing YarnClient properly from classpath
    public static void writeYarnSiteConfigXML(Configuration yarnConf, File targetFolder)
            throws IOException {
        yarnSiteXML = new File(targetFolder, "/yarn-site.xml");
        try (FileWriter writer = new FileWriter(yarnSiteXML)) {
            yarnConf.writeXml(writer);
            writer.flush();
        }
    }

    public String submitPipelineJob(String pipelineJob, Path... jars) throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder();
        Map<String, String> env = getEnv();
        processBuilder.environment().putAll(env);
        Path yamlScript = temporaryFolder.resolve("mysql-to-values.yml");
        Files.write(yamlScript, pipelineJob.getBytes());

        List<String> commandList = new ArrayList<>();
        commandList.add(env.get("FLINK_CDC_HOME") + "/bin/flink-cdc.sh");
        commandList.add("-t");
        commandList.add("yarn-application");
        commandList.add(yamlScript.toAbsolutePath().toString());
        for (Path jar : jars) {
            commandList.add("--jar");
            commandList.add(jar.toString());
        }

        processBuilder.command(commandList);
        LOG.info("starting flink-cdc task with flink on yarn-application");
        Process process = processBuilder.start();
        process.waitFor();
        String applicationIdStr = getApplicationId(process);
        Preconditions.checkNotNull(
                applicationIdStr, "applicationId should not be null, please check logs");
        ApplicationId applicationId = ApplicationId.fromString(applicationIdStr);
        waitApplicationFinished(applicationId, yarnAppTerminateTimeout, sleepIntervalInMS);
        LOG.info("started flink-cdc task with flink on yarn-application");
        return applicationIdStr;
    }

    public Map<String, String> getEnv() {
        Path flinkHome =
                TestUtils.getResource(
                        "flink-\\d+(\\.\\d+)*$",
                        "flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/target");
        Map<String, String> env = new HashMap<>();
        env.put("FLINK_HOME", flinkHome.toString());
        env.put("FLINK_CONF_DIR", flinkHome.resolve("conf").toString());
        addFlinkConf(flinkHome.resolve("conf").resolve("config.yaml"));
        Path flinkcdcHome =
                TestUtils.getResource("flink-cdc-\\d+(\\.\\d+)*(-SNAPSHOT)?$", "flink-cdc-dist");
        env.put("FLINK_CDC_HOME", flinkcdcHome.toString());
        env.put("HADOOP_CLASSPATH", getYarnClasspath());
        return env;
    }

    public void addFlinkConf(Path flinkConf) {
        Map<String, String> configToAppend = new HashMap<>();
        configToAppend.put("akka.ask.timeout", "100s");
        configToAppend.put("web.timeout", "1000000");
        configToAppend.put("taskmanager.slot.timeout", "1000s");
        configToAppend.put("slot.request.timeout", "120000");
        try {
            if (!Files.exists(flinkConf)) {
                throw new FileNotFoundException("config.yaml not found at " + flinkConf);
            }
            List<String> lines = new ArrayList<>(Files.readAllLines(flinkConf));
            for (Map.Entry<String, String> entry : configToAppend.entrySet()) {
                lines.add(entry.getKey() + ": " + entry.getValue());
            }
            Files.write(
                    flinkConf,
                    lines,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to append configuration to config.yaml", e);
        }
    }

    public String getApplicationId(Process process) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            LOG.info(line);
            if (line.startsWith("Job ID")) {
                return line.split(":")[1].trim();
            }
        }
        return null;
    }

    protected void waitApplicationFinished(
            ApplicationId applicationId, Duration timeout, int sleepIntervalInMS) throws Exception {
        Deadline deadline = Deadline.now().plus(timeout);
        YarnApplicationState state =
                getYarnClient().getApplicationReport(applicationId).getYarnApplicationState();

        while (state != YarnApplicationState.FINISHED) {
            if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
                fail("Application became FAILED or KILLED while expecting FINISHED");
            }

            if (deadline.isOverdue()) {
                getYarnClient().killApplication(applicationId);
                fail("Application didn't finish before timeout");
            }

            sleep(sleepIntervalInMS);
            state = getYarnClient().getApplicationReport(applicationId).getYarnApplicationState();
        }
    }

    @Nullable
    protected YarnClient getYarnClient() {
        return yarnClient;
    }

    /**
     * Searches for the yarn.classpath file generated by the "dependency:build-classpath" maven
     * plugin in "flink-yarn-tests".
     *
     * @return a classpath suitable for running all YARN-launched JVMs
     */
    private static String getYarnClasspath() {
        Path yarnClasspathFile = TestUtils.getResource("yarn.classpath");
        try {
            return FileUtils.readFileToString(yarnClasspathFile.toFile(), StandardCharsets.UTF_8);
        } catch (Throwable t) {
            LOG.error(
                    "Error while getting YARN classpath in {}",
                    yarnClasspathFile.toFile().getAbsoluteFile(),
                    t);
            throw new RuntimeException("Error while getting YARN classpath", t);
        }
    }
}
