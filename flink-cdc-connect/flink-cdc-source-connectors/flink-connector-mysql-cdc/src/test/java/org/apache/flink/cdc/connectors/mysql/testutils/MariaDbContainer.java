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

package org.apache.flink.cdc.connectors.mysql.testutils;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashSet;
import java.util.Set;

/**
 * Docker container for MariaDB, used by the MariaDB dialect integration tests. Binlog and ROW
 * format are enabled through server command-line flags (rather than a mounted my.cnf) so the
 * container is fully self-contained; MariaDB tracks {@code @@gtid_binlog_pos} automatically once
 * the binary log is on.
 */
@SuppressWarnings("rawtypes")
public class MariaDbContainer extends JdbcDatabaseContainer {

    public static final String IMAGE = "mariadb";
    public static final String VERSION = "11.4";
    public static final int MARIADB_PORT = 3306;

    private final String databaseName = "test";
    private final String username = "root";
    private final String password = "test";

    public MariaDbContainer() {
        this(VERSION);
    }

    public MariaDbContainer(String version) {
        super(DockerImageName.parse(IMAGE + ":" + version));
        addExposedPort(MARIADB_PORT);
        // Enable the binary log in ROW format;
        setCommand("--log-bin=mysql-bin", "--binlog-format=ROW", "--server-id=223344");
    }

    @Override
    protected Set<Integer> getLivenessCheckPorts() {
        return new HashSet<>(getMappedPort(MARIADB_PORT));
    }

    protected void configure() {
        addEnv("MARIADB_DATABASE", databaseName);
        addEnv("MARIADB_ROOT_PASSWORD", password);
        setStartupAttempts(3);
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

    public String getJdbcUrl(String databaseName) {
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getDatabasePort()
                + "/"
                + databaseName
                + "?useSSL=false&&allowPublicKeyRetrieval=true";
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(databaseName);
    }

    public int getDatabasePort() {
        return getMappedPort(MARIADB_PORT);
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
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
    protected String getTestQueryString() {
        return "SELECT 1";
    }
}
