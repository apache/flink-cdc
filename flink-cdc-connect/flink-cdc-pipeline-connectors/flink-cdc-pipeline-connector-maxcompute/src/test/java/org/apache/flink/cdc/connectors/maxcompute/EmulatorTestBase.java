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

package org.apache.flink.cdc.connectors.maxcompute;

import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;

import com.aliyun.odps.Odps;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

/** init maxcompute-emulator use for e2e test. */
public class EmulatorTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(EmulatorTestBase.class);

    public static final DockerImageName MAXCOMPUTE_IMAGE =
            DockerImageName.parse("maxcompute/maxcompute-emulator:v0.0.7");

    @ClassRule
    public static GenericContainer<?> maxcompute =
            new GenericContainer<>(MAXCOMPUTE_IMAGE)
                    .withExposedPorts(8080)
                    .waitingFor(
                            Wait.forLogMessage(".*Started MaxcomputeEmulatorApplication.*\\n", 1))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    public final MaxComputeOptions testOptions =
            MaxComputeOptions.builder("ak", "sk", getEndpoint(), "mocked_mc").build();

    public final Odps odps = MaxComputeUtils.getOdps(testOptions);

    private String getEndpoint() {
        maxcompute.start();

        String ip;
        if (maxcompute.getHost().equals("localhost")) {
            try {
                ip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                ip = "127.0.0.1";
            }
        } else {
            ip = maxcompute.getHost();
        }
        String endpoint = "http://" + ip + ":" + maxcompute.getFirstMappedPort();
        sendPOST(endpoint + "/init", endpoint);
        return endpoint;
    }

    public static void sendPOST(String postUrl, String postData) {
        try {
            URL url = new URL(postUrl);

            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.setRequestMethod("POST");
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type", "application/json");
            httpURLConnection.setRequestProperty(
                    "Content-Length", String.valueOf(postData.length()));

            try (OutputStream outputStream = httpURLConnection.getOutputStream()) {
                outputStream.write(postData.getBytes("UTF-8"));
                outputStream.flush();
            }
            int responseCode = httpURLConnection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException(
                        "POST request failed with response code: " + responseCode);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
