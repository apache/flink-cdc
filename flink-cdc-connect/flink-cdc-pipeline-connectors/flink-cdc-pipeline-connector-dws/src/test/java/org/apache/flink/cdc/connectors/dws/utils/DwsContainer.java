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

package org.apache.flink.cdc.connectors.dws.utils;

import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Docker container for GaussDB. */
@Testcontainers
public class DwsContainer extends JdbcDatabaseContainer<DwsContainer> {

    // Use a pinned public image tag that has been verified to pull and boot locally.
    private static final String DOCKER_IMAGE_NAME = "enmotech/opengauss:3.0.3";

    public static final String DWS_DATABASE = "postgres";
    public static final String DWS_SCHEMA = "gaussdb";
    private static final int DWS_PORT = 5432;
    public static final String DWS_TABLE_NAME = "test";
    public static final String DWS_USERNAME = "gaussdb";
    public static final String DWS_PASSWORD = "Enmo@123";
    public static final String DWS_DATABASE_TEST = "test";

    public DwsContainer() {
        super(DockerImageName.parse(DOCKER_IMAGE_NAME));
        addExposedPorts(DWS_PORT);

        waitingFor(
                new WaitAllStrategy()
                        // The image starts a temporary bootstrap server bound to 127.0.0.1 during
                        // initdb. Waiting on the port alone can mark the container as ready before
                        // the externally reachable server is started.
                        .withStrategy(
                                Wait.forLogMessage(
                                        ".*openGauss  init process complete; ready for start up\\..*",
                                        1))
                        .withStrategy(Wait.forListeningPort())
                        .withStartupTimeout(Duration.of(8, ChronoUnit.MINUTES)));
    }

    @Override
    protected void waitUntilContainerStarted() {
        // Rely on the composed wait strategy above instead of the default JDBC probe, since the
        // image performs an init-time localhost-only bootstrap that is not reachable from the host.
        getWaitStrategy().waitUntilReady(this);
    }

    @Override
    protected @NotNull Set<Integer> getLivenessCheckPorts() {
        return new HashSet<>(Arrays.asList(getMappedPort(DWS_PORT)));
    }

    @Override
    protected void configure() {
        addEnv("GS_PASSWORD", DWS_PASSWORD);
        setStartupAttempts(3);
    }

    public void waitForLog(String regex, int count, int timeoutSeconds) {
        new LogMessageWaitStrategy()
                .withRegEx(regex)
                .withTimes(count)
                .withStartupTimeout(Duration.of(timeoutSeconds, ChronoUnit.SECONDS))
                .waitUntilReady(this);
    }

    @Override
    public String getDriverClassName() {
        return "com.huawei.gauss200.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(DWS_DATABASE);
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:gaussdb://"
                + getHost()
                + ":"
                + getMappedPort(DWS_PORT)
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    @Override
    public String getUsername() {
        return DWS_USERNAME;
    }

    @Override
    public String getPassword() {
        return DWS_PASSWORD;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }
}
