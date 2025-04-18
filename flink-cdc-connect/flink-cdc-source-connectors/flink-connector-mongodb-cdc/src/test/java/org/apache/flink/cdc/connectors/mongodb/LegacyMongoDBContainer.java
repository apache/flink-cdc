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

package org.apache.flink.cdc.connectors.mongodb;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Mongodb test container. */
public class LegacyMongoDBContainer extends GenericContainer<LegacyMongoDBContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(LegacyMongoDBContainer.class);

    private static final String DOCKER_IMAGE_NAME = "mongo:5.0.2";

    public static final int MONGODB_PORT = 27017;

    public static final String MONGO_SUPER_USER = "superuser";

    public static final String MONGO_SUPER_PASSWORD = "superpw";

    public static final String FLINK_USER = "flinkuser";

    public static final String FLINK_USER_PASSWORD = "a1?~!@#$%^&*(){}[]<>.,+_-=/|:;";

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)//.*$");

    private final ShardingClusterRole clusterRole;

    public LegacyMongoDBContainer(Network network) {
        this(network, ShardingClusterRole.NONE);
    }

    public LegacyMongoDBContainer(Network network, ShardingClusterRole clusterRole) {
        super(
                new ImageFromDockerfile()
                        .withFileFromClasspath("random.key", "docker/mongodb/random.key")
                        .withFileFromClasspath("setup.js", "docker/mongodb/setup.js")
                        .withDockerfileFromBuilder(
                                builder ->
                                        builder.from(DOCKER_IMAGE_NAME)
                                                .copy(
                                                        "setup.js",
                                                        "/docker-entrypoint-initdb.d/setup.js")
                                                .copy("random.key", "/data/keyfile/random.key")
                                                .run("chown mongodb /data/keyfile/random.key")
                                                .run("chmod 400 /data/keyfile/random.key")
                                                .env("MONGO_INITDB_ROOT_USERNAME", MONGO_SUPER_USER)
                                                .env(
                                                        "MONGO_INITDB_ROOT_PASSWORD",
                                                        MONGO_SUPER_PASSWORD)
                                                .env("MONGO_INITDB_DATABASE", "admin")
                                                .build()));
        this.clusterRole = clusterRole;

        withNetwork(network);
        withNetworkAliases(clusterRole.hostname);
        withExposedPorts(MONGODB_PORT);
        withCommand(ShardingClusterRole.startupCommand(clusterRole));
        waitingFor(clusterRole.waitStrategy);
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
        LOG.info("Preparing a MongoDB Container with sharding cluster role {}...", clusterRole);
        if (clusterRole != ShardingClusterRole.ROUTER) {
            initReplicaSet();
        } else {
            initShard();
        }
    }

    protected void initReplicaSet() {
        LOG.info("Initializing a single node replica set...");
        executeCommand(
                String.format(
                        "rs.initiate({ _id : '%s', configsvr: %s, members: [{ _id: 0, host: '%s:%d'}]})",
                        clusterRole.replicaSetName,
                        clusterRole == ShardingClusterRole.CONFIG,
                        clusterRole.hostname,
                        MONGODB_PORT));

        LOG.info("Waiting for single node replica set initialized...");
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

    protected void initShard() {
        LOG.info("Initializing a sharded cluster...");
        // decrease chunk size from default 64mb to 1mb to make splitter test easier.
        executeCommand(
                "db.getSiblingDB('config').settings.updateOne(\n"
                        + "   { _id: \"chunksize\" },\n"
                        + "   { $set: { _id: \"chunksize\", value: 1 } },\n"
                        + "   { upsert: true }\n"
                        + ");");
        executeCommand(
                String.format(
                        "sh.addShard('%s/%s:%d')",
                        ShardingClusterRole.SHARD.replicaSetName,
                        ShardingClusterRole.SHARD.hostname,
                        MONGODB_PORT));
    }

    /** Executes a mongo command file in separate database. */
    public String executeCommandFileInSeparateDatabase(String fileNameIgnoreSuffix) {
        return executeCommandFileInDatabase(
                fileNameIgnoreSuffix,
                fileNameIgnoreSuffix + "_" + Integer.toUnsignedString(new Random().nextInt(), 36));
    }

    /** Executes a mongo command file, specify a database name. */
    public String executeCommandFileInDatabase(String fileNameIgnoreSuffix, String databaseName) {
        final String dbName = databaseName != null ? databaseName : fileNameIgnoreSuffix;
        final String ddlFile = String.format("ddl/%s.js", fileNameIgnoreSuffix);
        final URL ddlTestFile = LegacyMongoDBContainer.class.getClassLoader().getResource(ddlFile);
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();

        try {
            // use database;
            String command0 = String.format("db = db.getSiblingDB('%s');\n", dbName);
            String command1 =
                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                            .filter(x -> StringUtils.isNotBlank(x) && !x.trim().startsWith("//"))
                            .map(
                                    x -> {
                                        final Matcher m = COMMENT_PATTERN.matcher(x);
                                        return m.matches() ? m.group(1) : x;
                                    })
                            .collect(Collectors.joining("\n"));

            executeCommand(command0 + command1);

            return dbName;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** A MongoDB sharded cluster roles. */
    public enum ShardingClusterRole {
        // Config servers store metadata and configuration settings for the cluster.
        CONFIG("config0", "rs0-config", Wait.forLogMessage(".*[Ww]aiting for connections.*", 2)),

        // Each shard contains a subset of the sharded data. Each shard can be deployed as a replica
        // set.
        SHARD("shard0", "rs0-shard", Wait.forLogMessage(".*[Ww]aiting for connections.*", 2)),

        // The mongos acts as a query router, providing an interface between client applications and
        // the sharded cluster.
        ROUTER("router0", null, Wait.forLogMessage(".*[Ww]aiting for connections.*", 1)),

        // None sharded cluster.
        NONE("mongo0", "rs0", Wait.forLogMessage(".*Replication has not yet been configured.*", 1));

        private final String hostname;
        private final String replicaSetName;
        private final WaitStrategy waitStrategy;

        ShardingClusterRole(String hostname, String replicaSetName, WaitStrategy waitStrategy) {
            this.hostname = hostname;
            this.replicaSetName = replicaSetName;
            this.waitStrategy = waitStrategy;
        }

        public static String startupCommand(ShardingClusterRole clusterRole) {
            switch (clusterRole) {
                case CONFIG:
                    return String.format(
                            "mongod --configsvr --port %d --replSet %s --keyFile /data/keyfile/random.key",
                            MONGODB_PORT, clusterRole.replicaSetName);
                case SHARD:
                    return String.format(
                            "mongod --shardsvr --port %d --replSet %s --keyFile /data/keyfile/random.key",
                            MONGODB_PORT, clusterRole.replicaSetName);
                case ROUTER:
                    return String.format(
                            "mongos --configdb %s/%s:%d --bind_ip_all --keyFile /data/keyfile/random.key",
                            CONFIG.replicaSetName, CONFIG.hostname, MONGODB_PORT);
                case NONE:
                default:
                    return String.format(
                            "mongod --port %d --replSet %s --keyFile /data/keyfile/random.key",
                            MONGODB_PORT, NONE.replicaSetName);
            }
        }
    }
}
