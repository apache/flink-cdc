/*
 * Copyright 2023 Ververica Inc.
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

package com.vervetica.cdc.connectors.vitess.container;

import org.testcontainers.containers.JdbcDatabaseContainer;

/** Vitess container. */
public class VitessContainer extends JdbcDatabaseContainer {

    public static final String IMAGE = "vitess/vttestserver";
    public static final String DEFAULT_TAG = "mysql80";
    private static final Integer VITESS_PORT = 15991;
    public static final Integer GRPC_PORT = VITESS_PORT + 1;
    public static final Integer VTCTLD_GRPC_PORT = VITESS_PORT + 8;
    public static final Integer MYSQL_PORT = VITESS_PORT + 3;

    private String keyspaces = "test";
    private String username = "flinkuser";
    private String password = "flinkpwd";

    public VitessContainer() {
        this(DEFAULT_TAG);
    }

    public VitessContainer(String tag) {
        super(IMAGE + ":" + tag);
    }

    @Override
    protected void configure() {
        addEnv("PORT", VITESS_PORT.toString());
        addEnv("KEYSPACES", getKeyspace());
        addEnv("NUM_SHARDS", "1");
        addEnv("MYSQL_BIND_HOST", "0.0.0.0");
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
        return "jdbc:mysql://" + getHost() + ":" + getMysqlPort() + "/" + getKeyspace();
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    public String getKeyspace() {
        return keyspaces;
    }

    public Integer getMysqlPort() {
        return this.getMappedPort(MYSQL_PORT);
    }

    public Integer getGrpcPort() {
        return this.getMappedPort(GRPC_PORT);
    }

    public Integer getVtctldGrpcPort() {
        return this.getMappedPort(VTCTLD_GRPC_PORT);
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    @Override
    public VitessContainer withDatabaseName(final String keyspace) {
        this.keyspaces = keyspace;
        return this;
    }

    public VitessContainer withKeyspace(String keyspace) {
        this.keyspaces = keyspace;
        return this;
    }

    @Override
    public VitessContainer withUsername(final String username) {
        this.username = username;
        return this;
    }

    @Override
    public VitessContainer withPassword(final String password) {
        this.password = password;
        return this;
    }
}
