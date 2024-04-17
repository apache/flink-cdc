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

package org.apache.flink.cdc.connectors.starrocks.sink.utils;

import org.junit.ClassRule;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Docker container for StarRocks. */
public class StarRocksContainer extends JdbcDatabaseContainer<StarRocksContainer> {

    private static final String DOCKER_IMAGE_NAME = "starrocks/allin1-ubuntu:3.1.10";

    // FE node exposed ports
    public static final int FE_HTTP_PORT = 8030;
    public static final int FE_HTTP_SERVICE_PORT = 8080;
    public static final int FE_RPC_PORT = 9020;
    public static final int FE_QUERY_PORT = 9030;
    public static final int FE_EDIT_LOG_PORT = 9010;

    // BE node exposed ports
    public static final int BE_PORT = 9060;
    public static final int BE_WEB_SERVER_PORT = 8040;
    public static final int BE_HEARBEAT_SERVICE_PORT = 9050;
    public static final int BE_BRPC_PORT = 8060;

    public static final String STARROCKS_DATABASE_NAME = "starrocks_database";
    public static final String STARROCKS_TABLE_NAME = "fallen_angel";
    public static final String STARROCKS_USERNAME = "root";
    public static final String STARROCKS_PASSWORD = "";

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    public StarRocksContainer() {
        super(DockerImageName.parse(DOCKER_IMAGE_NAME));
        setExposedPorts(
                Arrays.asList(
                        FE_HTTP_PORT,
                        FE_HTTP_SERVICE_PORT,
                        FE_RPC_PORT,
                        FE_QUERY_PORT,
                        FE_EDIT_LOG_PORT,
                        BE_PORT,
                        BE_WEB_SERVER_PORT,
                        BE_HEARBEAT_SERVICE_PORT,
                        BE_BRPC_PORT));
        setNetwork(NETWORK);
    }

    public List<String> getLoadUrl() {
        return Collections.singletonList(
                String.format("%s:%d", getHost(), getMappedPort(FE_HTTP_SERVICE_PORT)));
    }

    public String getTableIdentifier() {
        return String.format("%s.%s", STARROCKS_DATABASE_NAME, STARROCKS_TABLE_NAME);
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
                + getMappedPort(FE_QUERY_PORT)
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    @Override
    public String getUsername() {
        return STARROCKS_USERNAME;
    }

    @Override
    public String getPassword() {
        return STARROCKS_PASSWORD;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }
}
