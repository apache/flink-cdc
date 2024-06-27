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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Set;

/** OceanBase Log Proxy container. */
public class LogProxyContainer extends GenericContainer<LogProxyContainer> {

    private static final String IMAGE = "oceanbase/oblogproxy-ce";

    private static final int PORT = 2983;
    private static final String ROOT_USER = "root";

    private String sysPassword;

    public LogProxyContainer(String version) {
        super(DockerImageName.parse(IMAGE + ":" + version));
        addExposedPorts(PORT);
        setWaitStrategy(Wait.forLogMessage(".*boot success!.*", 1));
    }

    @Override
    protected void configure() {
        addEnv("OB_SYS_USERNAME", ROOT_USER);
        addEnv("OB_SYS_PASSWORD", sysPassword);
    }

    public @NotNull Set<Integer> getLivenessCheckPortNumbers() {
        return Collections.singleton(this.getMappedPort(PORT));
    }

    public int getPort() {
        return getMappedPort(PORT);
    }

    public LogProxyContainer withSysPassword(String sysPassword) {
        this.sysPassword = sysPassword;
        return this;
    }
}
