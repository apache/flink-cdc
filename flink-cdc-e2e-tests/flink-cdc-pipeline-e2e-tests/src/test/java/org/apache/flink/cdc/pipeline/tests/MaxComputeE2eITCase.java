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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.table.api.ValidationException;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for maxcompute cdc pipeline job. */
class MaxComputeE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MaxComputeE2eITCase.class);

    public static final DockerImageName MAXCOMPUTE_IMAGE =
            DockerImageName.parse("maxcompute/maxcompute-emulator:v0.0.7");

    private static final GenericContainer<?> MAXCOMPUTE_CONTAINER =
            new GenericContainer<>(MAXCOMPUTE_IMAGE)
                    .withExposedPorts(8080)
                    .waitingFor(
                            Wait.forLogMessage(".*Started MaxcomputeEmulatorApplication.*\\n", 1))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    public final MaxComputeOptions testOptions =
            MaxComputeOptions.builder("ak", "sk", getEndpoint(), "mocked_mc")
                    .withTunnelEndpoint(getEndpoint())
                    .build();

    @BeforeAll
    static void startContainers() {
        Startables.deepStart(MAXCOMPUTE_CONTAINER).join();
    }

    @AfterAll
    static void stopContainers() {
        MAXCOMPUTE_CONTAINER.stop();
    }

    @Test
    void testSingleSplitSingleTable() throws Exception {
        startTest("SINGLE_SPLIT_SINGLE_TABLE");
        Instance instance =
                SQLTask.run(
                        MaxComputeUtils.getOdps(testOptions),
                        "select * from table1 order by col1;");
        instance.waitForSuccess();
        List<Record> result = SQLTask.getResult(instance);
        LOG.info("{}", result);
        assertThat(result).hasSize(2);
        // 2,x
        assertThat(result.get(0).get(0)).isEqualTo("2");
        assertThat(result.get(0).get(1)).isEqualTo("x");
        // 3, NULL (MaxCompute Emulator use 'NULL' instead of null)
        assertThat(result.get(1).get(0)).isEqualTo("3");
        assertThat(result.get(1).get(1)).isEqualTo("NULL");
    }

    private void startTest(String testSet) throws Exception {
        sendPOST(getEndpoint() + "/init", getEndpoint());

        Odps odps = MaxComputeUtils.getOdps(testOptions);
        odps.tables().delete("table1", true);
        odps.tables().delete("table2", true);

        String pipelineJob =
                "source:\n"
                        + "   type: values\n"
                        + "   name: ValuesSource\n"
                        + "   event-set.id: "
                        + testSet
                        + "\n"
                        + "\n"
                        + "sink:\n"
                        + "   type: maxcompute\n"
                        + "   name: MaxComputeSink\n"
                        + "   access-id: ak\n"
                        + "   access-key: sk\n"
                        + "   endpoint: "
                        + getEndpoint()
                        + "\n"
                        + "   tunnel.endpoint: "
                        + getEndpoint()
                        + "\n"
                        + "   project: mocked_mc\n"
                        + "   buckets-num: 8\n"
                        + "   compress.algorithm: raw\n"
                        + "\n"
                        + "pipeline:\n"
                        + "   parallelism: 4";
        Path maxcomputeCdcJar = TestUtils.getResource("maxcompute-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, maxcomputeCdcJar);
        waitUntilJobFinished(Duration.ofMinutes(10));
        LOG.info("Pipeline job is running");
    }

    private String getEndpoint() {
        String ip;
        if (MAXCOMPUTE_CONTAINER.getHost().equals("localhost")) {
            try {
                ip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                ip = "127.0.0.1";
            }
        } else {
            ip = MAXCOMPUTE_CONTAINER.getHost();
        }
        return "http://" + ip + ":" + MAXCOMPUTE_CONTAINER.getFirstMappedPort();
    }

    public static void sendPOST(String postUrl, String postData) throws Exception {
        URL url = new URL(postUrl);

        HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
        httpURLConnection.setRequestMethod("POST");
        httpURLConnection.setDoOutput(true);
        httpURLConnection.setRequestProperty("Content-Type", "application/json");
        httpURLConnection.setRequestProperty("Content-Length", String.valueOf(postData.length()));

        try (OutputStream outputStream = httpURLConnection.getOutputStream()) {
            outputStream.write(postData.getBytes("UTF-8"));
            outputStream.flush();
        }
        int responseCode = httpURLConnection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("POST request failed with response code: " + responseCode);
        }
    }

    public void waitUntilJobFinished(Duration timeout) {
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
                    if (jobStatus == JobStatus.FINISHED) {
                        return;
                    }
                    throw new ValidationException(
                            String.format(
                                    "Job has been terminated unexpectedly! JobName: %s, JobID: %s, Status: %s",
                                    message.getJobName(),
                                    message.getJobId(),
                                    message.getJobState()));
                }
            }
        }
    }
}
