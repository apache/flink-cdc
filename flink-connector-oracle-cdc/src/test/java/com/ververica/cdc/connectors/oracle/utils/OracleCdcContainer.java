/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.utils;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Docker container for Oracle. The difference between this class and {@link
 * org.testcontainers.containers.OracleContainer} is that TC OracleContainer has problems when using
 * base docker image "wnameless/oracle-xe-11g-r2" (can't find log output "DATABASE IS READY TO
 * USE!").
 */
public class OracleCdcContainer extends JdbcDatabaseContainer<OracleCdcContainer> {

    public static final String NAME = "oracle";

    private static final ImageFromDockerfile ORACLE_IMAGE =
            new ImageFromDockerfile("oracle-xe-11g-tmp")
                    .withFileFromClasspath(".", "docker")
                    .withFileFromClasspath(
                            "assets/activate-archivelog.sh", "docker/assets/activate-archivelog.sh")
                    .withFileFromClasspath(
                            "assets/activate-archivelog.sql",
                            "docker/assets/activate-archivelog.sql");

    private static final int ORACLE_PORT = 1521;
    private static final int APEX_HTTP_PORT = 8080;

    private static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 120;

    private String username = "system";
    private String password = "oracle";

    public OracleCdcContainer() {
        super(ORACLE_IMAGE);
        preconfigure();
    }

    private void preconfigure() {
        withStartupTimeoutSeconds(DEFAULT_STARTUP_TIMEOUT_SECONDS);
        withConnectTimeoutSeconds(DEFAULT_CONNECT_TIMEOUT_SECONDS);
        addExposedPorts(ORACLE_PORT, APEX_HTTP_PORT);
    }

    @Override
    protected Integer getLivenessCheckPort() {
        return getMappedPort(ORACLE_PORT);
    }

    @Override
    public String getDriverClassName() {
        return "oracle.jdbc.OracleDriver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:oracle:thin:"
                + getUsername()
                + "/"
                + getPassword()
                + "@"
                + getHost()
                + ":"
                + getOraclePort()
                + ":"
                + getSid();
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public OracleCdcContainer withUsername(String username) {
        this.username = username;
        return self();
    }

    @Override
    public OracleCdcContainer withPassword(String password) {
        this.password = password;
        return self();
    }

    @Override
    public OracleCdcContainer withUrlParam(String paramName, String paramValue) {
        throw new UnsupportedOperationException("The OracleDb does not support this");
    }

    @SuppressWarnings("SameReturnValue")
    public String getSid() {
        return "xe";
    }

    public Integer getOraclePort() {
        return getMappedPort(ORACLE_PORT);
    }

    @SuppressWarnings("unused")
    public Integer getWebPort() {
        return getMappedPort(APEX_HTTP_PORT);
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1 FROM DUAL";
    }
}
