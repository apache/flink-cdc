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

package org.apache.flink.cdc.connectors.doris.sink.utils;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

/** Docker container for Doris. */
@Testcontainers
public class DorisContainer extends JdbcDatabaseContainer<DorisContainer> {

    private static final String DOCKER_IMAGE_NAME = "apache/doris:doris-all-in-one-2.1.0";

    public static final int FE_INNER_PORT = 8030;
    public static final int BE_INNER_PORT = 8040;
    public static final int DB_INNER_PORT = 9030;

    public static final Network NETWORK = Network.newNetwork();

    public String getFeNodes() {
        return String.format("%s:%d", getHost(), getMappedPort(FE_INNER_PORT));
    }

    public String getBeNodes() {
        return String.format("%s:%d", getHost(), getMappedPort(BE_INNER_PORT));
    }

    public String getTableIdentifier() {
        return String.format("%s.%s", DORIS_DATABASE_NAME, DORIS_TABLE_NAME);
    }

    public static final String DORIS_DATABASE_NAME = "doris_database";
    public static final String DORIS_TABLE_NAME = "fallen_angel";
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PASSWORD = "";

    public DorisContainer() {
        super(DockerImageName.parse(DOCKER_IMAGE_NAME));
        setExposedPorts(Arrays.asList(FE_INNER_PORT, BE_INNER_PORT, DB_INNER_PORT));
        setNetwork(NETWORK);
    }

    public DorisContainer(Network network) {
        super(DockerImageName.parse(DOCKER_IMAGE_NAME));
        setExposedPorts(Arrays.asList(FE_INNER_PORT, BE_INNER_PORT, DB_INNER_PORT));
        setNetwork(network);
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
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return "com.mysql.cj.jdbc.Driver";
        } catch (ClassNotFoundException e) {
            return "com.mysql.jdbc.Driver";
        }
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl("");
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getMappedPort(DB_INNER_PORT)
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    public String getJdbcUrl(String databaseName, String username) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://"
                + username
                + "@"
                + getHost()
                + ":"
                + getMappedPort(DB_INNER_PORT)
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    public String getJdbcUrl(String databaseName, String username, String password) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://"
                + username
                + ":"
                + password
                + "@"
                + getHost()
                + ":"
                + getMappedPort(DB_INNER_PORT)
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    @Override
    public String getUsername() {
        return DORIS_USERNAME;
    }

    @Override
    public String getPassword() {
        return DORIS_PASSWORD;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }
}
