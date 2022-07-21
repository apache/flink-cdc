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

package com.ververica.cdc.connectors.mongodb.utils;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;

/** Mongodb test replica container. */
public class MongoDBContainer extends GenericContainer<MongoDBContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBContainer.class);

    private static final String REPLICA_SET_NAME = "rs0";

    private static final String DOCKER_IMAGE_NAME = "mongo:5.0.2";

    public static final String MONGODB_HOST = "mongo0";

    public static final int MONGODB_PORT = 27017;

    public static final String MONGO_SUPER_USER = "superuser";

    public static final String MONGO_SUPER_PASSWORD = "superpw";

    public MongoDBContainer(Network network) {
        super(
                new ImageFromDockerfile()
                        .withFileFromClasspath("random.key", "docker/random.key")
                        .withFileFromClasspath("setup.js", "docker/setup.js")
                        .withDockerfileFromBuilder(
                                builder ->
                                        builder.from(DOCKER_IMAGE_NAME)
                                                .copy(
                                                        "setup.js",
                                                        "/docker-entrypoint-initdb.d/setup.js")
                                                .copy("random.key", "/data/keyfile/random.key")
                                                .run("chown mongodb /data/keyfile/random.key")
                                                .run("chmod 400 /data/keyfile/random.key")
                                                .build()));
        withNetwork(network);
        withNetworkAliases(MONGODB_HOST);
        withExposedPorts(MONGODB_PORT);
        withEnv("MONGO_INITDB_ROOT_USERNAME", MONGO_SUPER_USER);
        withEnv("MONGO_INITDB_ROOT_PASSWORD", MONGO_SUPER_PASSWORD);
        withEnv("MONGO_INITDB_DATABASE", "admin");
        withCommand("--replSet", REPLICA_SET_NAME, "--keyFile", "/data/keyfile/random.key");
        waitingFor(Wait.forLogMessage(".*Replication has not yet been configured.*", 1));
    }

    public String getConnectionString(String username, String password) {
        return String.format(
                "mongodb://%s:%s@%s:%d",
                username, password, getContainerIpAddress(), getMappedPort(MONGODB_PORT));
    }

    public String getHostAndPort() {
        return String.format("%s:%s", getContainerIpAddress(), getMappedPort(MONGODB_PORT));
    }

    public void executeCommand(String command) {
        try {
            LOG.info("Executing mongo command: {}", command);
            ExecResult execResult =
                    execInContainer(
                            "mongo",
                            "-u",
                            MONGO_SUPER_USER,
                            "-p",
                            MONGO_SUPER_PASSWORD,
                            "--eval",
                            command);
            LOG.info(execResult.getStdout());
            if (execResult.getExitCode() != 0) {
                throw new IllegalStateException(
                        "Execute mongo command failed " + execResult.getStdout());
            }
        } catch (InterruptedException | IOException e) {
            throw new IllegalStateException("Execute mongo command failed", e);
        }
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        initReplicaSet();
    }

    private void initReplicaSet() {
        LOG.info("Initializing a single node node replica set...");
        executeCommand(
                String.format(
                        "rs.initiate({ _id : '%s', members: [{ _id: 0, host: '%s:%d'}]})",
                        REPLICA_SET_NAME, MONGODB_HOST, MONGODB_PORT));

        LOG.info("Waiting for single node node replica set initialized...");
        executeCommand(
                String.format(
                        "var attempt = 0; "
                                + "while"
                                + "(%s) "
                                + "{ "
                                + "if (attempt > %d) {quit(1);} "
                                + "print('%s ' + attempt); sleep(100);  attempt++; "
                                + " }",
                        "db.runCommand( { isMaster: 1 } ).ismaster==false",
                        60,
                        "An attempt to await for a single node replica set initialization:"));
    }
}
