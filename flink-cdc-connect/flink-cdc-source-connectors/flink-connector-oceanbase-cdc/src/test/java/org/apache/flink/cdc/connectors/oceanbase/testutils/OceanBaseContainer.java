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

package org.apache.flink.cdc.connectors.oceanbase.testutils;

import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Set;

/** OceanBase container. */
public class OceanBaseContainer extends JdbcDatabaseContainer<OceanBaseContainer> {

    private static final String IMAGE = "oceanbase/oceanbase-ce";

    private static final int SQL_PORT = 2881;
    private static final int RPC_PORT = 2882;
    private static final String ROOT_USER = "root";
    private static final String TEST_DATABASE = "test";
    private static final String DEFAULT_TENANT = "test";
    private static final String DEFAULT_PASSWORD = "";

    private String mode = "mini";
    private String tenantName = DEFAULT_TENANT;
    private String sysPassword = DEFAULT_PASSWORD;
    private String tenantPassword = DEFAULT_PASSWORD;

    public OceanBaseContainer(String version) {
        super(DockerImageName.parse(IMAGE + ":" + version));
        addExposedPorts(SQL_PORT, RPC_PORT);
        setWaitStrategy(Wait.forLogMessage(".*boot success!.*", 1));
    }

    @Override
    protected void configure() {
        addEnv("MODE", mode);
        addEnv("OB_CLUSTER_NAME", "flink-cdc-ci");
        if (!DEFAULT_PASSWORD.equals(sysPassword)) {
            addEnv("OB_SYS_PASSWORD", sysPassword);
        }
        if (!DEFAULT_TENANT.equals(tenantName)) {
            addEnv("OB_TENANT_NAME", tenantName);
        }
        if (!DEFAULT_PASSWORD.equals(tenantPassword)) {
            addEnv("OB_TENANT_PASSWORD", tenantPassword);
        }
    }

    protected void waitUntilContainerStarted() {
        this.getWaitStrategy().waitUntilReady(this);
    }

    public @NotNull Set<Integer> getLivenessCheckPortNumbers() {
        return Collections.singleton(this.getMappedPort(SQL_PORT));
    }

    @Override
    public String getDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    public String getJdbcUrl(String databaseName) {
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getDatabasePort()
                + "/"
                + databaseName
                + "?useSSL=false";
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl("");
    }

    public int getDatabasePort() {
        return getMappedPort(SQL_PORT);
    }

    public String getTenantName() {
        return tenantName;
    }

    @Override
    public String getDatabaseName() {
        return TEST_DATABASE;
    }

    @Override
    public String getUsername() {
        return ROOT_USER + "@" + tenantName;
    }

    @Override
    public String getPassword() {
        return tenantPassword;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    public OceanBaseContainer withMode(String mode) {
        this.mode = mode;
        return this;
    }

    public OceanBaseContainer withTenantName(String tenantName) {
        this.tenantName = tenantName;
        return this;
    }

    public OceanBaseContainer withSysPassword(String sysPassword) {
        this.sysPassword = sysPassword;
        return this;
    }

    public OceanBaseContainer withTenantPassword(String tenantPassword) {
        this.tenantPassword = tenantPassword;
        return this;
    }
}
