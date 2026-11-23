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

package org.apache.flink.cdc.connectors.mongodb.utils;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A MongoDB container configured to require TLS connections. Certificates are taken from the
 * test-resource ssl/ directory: server.pem (private key + cert) and ca.crt (CA certificate).
 *
 * <p>The container mounts the server-side PEM and CA certificate into /etc/ssl/ and starts mongod
 * with --tlsMode requireTLS, so every client must present a valid TLS handshake.
 */
public class MongoDBSslContainer extends org.testcontainers.containers.MongoDBContainer {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSslContainer.class);

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)//.*$");

    public static final int MONGODB_PORT = 27017;

    private static final String CONTAINER_SSL_DIR = "/etc/ssl/mongodb/";
    private static final String CONTAINER_SERVER_PEM = CONTAINER_SSL_DIR + "server.pem";
    private static final String CONTAINER_CA_CRT = CONTAINER_SSL_DIR + "ca.crt";

    public MongoDBSslContainer(String imageName) {
        super(imageName);
        withCommand(
                "--replSet",
                "docker-rs",
                "--tlsMode",
                "requireTLS",
                "--tlsCertificateKeyFile",
                CONTAINER_SERVER_PEM,
                "--tlsCAFile",
                CONTAINER_CA_CRT,
                // Allow clients that do not present a certificate (truststore-only clients).
                // Clients that do present one must still have it signed by the CA.
                "--tlsAllowConnectionsWithoutCertificates");
        withCopyFileToContainer(
                MountableFile.forClasspathResource("ssl/server.pem", 0444), CONTAINER_SERVER_PEM);
        withCopyFileToContainer(
                MountableFile.forClasspathResource("ssl/ca.crt", 0444), CONTAINER_CA_CRT);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) {
        // Re-implement replica set init: the parent calls rs.initiate() via mongosh without TLS
        // flags, which would fail against a requireTLS server. We override it to pass --tls.
        initReplicaSetWithTls(reused);
        createFlinkUser();
    }

    private void initReplicaSetWithTls(boolean reused) {
        if (reused) {
            return;
        }
        try {
            ExecResult result =
                    execInContainer(
                            "mongosh",
                            "--tls",
                            "--tlsCertificateKeyFile",
                            CONTAINER_SERVER_PEM,
                            "--tlsCAFile",
                            CONTAINER_CA_CRT,
                            "--tlsAllowInvalidHostnames",
                            "--eval",
                            "rs.initiate()");
            LOG.info(result.getStdout());
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("rs.initiate() failed: " + result.getStderr());
            }
            // Wait until the node becomes primary
            for (int i = 0; i < 30; i++) {
                ExecResult status =
                        execInContainer(
                                "mongosh",
                                "--tls",
                                "--tlsCertificateKeyFile",
                                CONTAINER_SERVER_PEM,
                                "--tlsCAFile",
                                CONTAINER_CA_CRT,
                                "--tlsAllowInvalidHostnames",
                                "--eval",
                                "db.isMaster().ismaster");
                if (status.getStdout().contains("true")) {
                    return;
                }
                Thread.sleep(1000);
            }
            throw new IllegalStateException("Timed out waiting for replica set primary");
        } catch (InterruptedException | IOException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to initialise replica set", e);
        }
    }

    private void createFlinkUser() {
        final String setupFilePath = "docker/mongodb/setup.js";
        final URL setupFile = MongoDBSslContainer.class.getClassLoader().getResource(setupFilePath);
        Assertions.assertThat(setupFile)
                .withFailMessage("Cannot locate " + setupFilePath)
                .isNotNull();
        try {
            String createUserCommand =
                    Files.readAllLines(Paths.get(setupFile.toURI())).stream()
                            .filter(x -> StringUtils.isNotBlank(x) && !x.trim().startsWith("//"))
                            .map(
                                    x -> {
                                        final Matcher m = COMMENT_PATTERN.matcher(x);
                                        return m.matches() ? m.group(1) : x;
                                    })
                            .collect(Collectors.joining(" "));
            ExecResult execResult =
                    execInContainer(
                            "mongosh",
                            "--tls",
                            "--tlsCertificateKeyFile",
                            CONTAINER_SERVER_PEM,
                            "--tlsCAFile",
                            CONTAINER_CA_CRT,
                            "--tlsAllowInvalidHostnames",
                            "--eval",
                            "use admin",
                            "--eval",
                            createUserCommand,
                            "--eval",
                            "console.log('Flink test user created.\\n');");
            LOG.info(execResult.getStdout());
            if (execResult.getExitCode() != 0) {
                throw new IllegalStateException(
                        "Failed to create Flink user: " + execResult.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MongoDBSslContainer withLogConsumer(Consumer<OutputFrame> consumer) {
        return (MongoDBSslContainer) super.withLogConsumer(consumer);
    }

    /** Returns a connection URI with {@code tls=true} appended. */
    public String getTlsConnectionString() {
        return String.format("mongodb://%s:%d/?tls=true", getHost(), getMappedPort(MONGODB_PORT));
    }
}
