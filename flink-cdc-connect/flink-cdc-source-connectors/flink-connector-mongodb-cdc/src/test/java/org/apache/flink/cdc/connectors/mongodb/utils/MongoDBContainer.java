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
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Random;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Container for testing MongoDB >= 5.0.3. */
public class MongoDBContainer extends org.testcontainers.containers.MongoDBContainer {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBContainer.class);

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)//.*$");

    public static final int MONGODB_PORT = 27017;

    public static final String FLINK_USER = "flinkuser";

    public static final String FLINK_USER_PASSWORD = "a1?~!@#$%^&*(){}[]<>.,+_-=/|:;";

    public MongoDBContainer(String imageName) {
        super(imageName);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarted(containerInfo, reused);

        final String setupFilePath = "docker/mongodb/setup.js";
        final URL setupFile = MongoDBContainer.class.getClassLoader().getResource(setupFilePath);

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
                            "--eval",
                            "use admin",
                            "--eval",
                            createUserCommand,
                            "--eval",
                            "console.log('Flink test user created.\\n');");
            LOG.info(execResult.getStdout());
            if (execResult.getExitCode() != 0) {
                throw new IllegalStateException(
                        "Execute mongo command failed " + execResult.getStderr());
            }
            this.waitingFor(Wait.forLogMessage("Flink test user created.\\s", 1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MongoDBContainer withSharding() {
        return (MongoDBContainer) super.withSharding();
    }

    @Override
    public MongoDBContainer withLogConsumer(Consumer<OutputFrame> consumer) {
        return (MongoDBContainer) super.withLogConsumer(consumer);
    }

    @Override
    public MongoDBContainer withNetwork(Network network) {
        return (MongoDBContainer) super.withNetwork(network);
    }

    @Override
    public MongoDBContainer withNetworkAliases(String... aliases) {
        return (MongoDBContainer) super.withNetworkAliases(aliases);
    }

    @Override
    public MongoDBContainer withStartupTimeout(Duration timeout) {
        return (MongoDBContainer) super.withStartupTimeout(timeout);
    }

    public void executeCommand(String command) {
        try {
            LOG.info("Executing mongo command: {}", command);
            ExecResult execResult = execInContainer("mongosh", "--eval", command);
            LOG.info(execResult.getStdout());
            if (execResult.getExitCode() != 0) {
                throw new IllegalStateException(
                        "Execute mongo command failed " + execResult.getStderr());
            }
        } catch (InterruptedException | IOException e) {
            throw new IllegalStateException("Execute mongo command failed", e);
        }
    }

    public String executeCommandInDatabase(String command, String databaseName) {
        try {
            executeCommand(String.format("db = db.getSiblingDB('%s');\n", databaseName) + command);
            return databaseName;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Executes a mongo command in separate database. */
    public String executeCommandInSeparateDatabase(String command, String baseName) {
        return executeCommandInDatabase(
                command, baseName + "_" + Integer.toUnsignedString(new Random().nextInt(), 36));
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
        final URL ddlTestFile = MongoDBContainer.class.getClassLoader().getResource(ddlFile);
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

    public String getConnectionString() {
        return String.format(
                "mongodb://%s:%d", getContainerIpAddress(), getMappedPort(MONGODB_PORT));
    }

    public String getHostAndPort() {
        return String.format("%s:%s", getContainerIpAddress(), getMappedPort(MONGODB_PORT));
    }
}
