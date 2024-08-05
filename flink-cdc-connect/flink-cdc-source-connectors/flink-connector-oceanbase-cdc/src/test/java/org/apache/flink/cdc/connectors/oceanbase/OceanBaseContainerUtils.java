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

package org.apache.flink.cdc.connectors.oceanbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;

/** Utils to help test. */
@SuppressWarnings("resource")
public class OceanBaseContainerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseContainerUtils.class);

    private static final String OB_SYS_PASSWORD = "123456";

    public static final String TENANT = "test";
    public static final String USERNAME = "root@" + TENANT;
    public static final String PASSWORD = "123456";
    public static final String OB_SERVER_HOST = "127.0.0.1";
    public static final int OB_SERVER_PORT = 2881;
    public static final String LOG_PROXY_HOST = "127.0.0.1";
    public static final int LOG_PROXY_PORT = 2983;
    public static final String RS_LIST = "127.0.0.1:2882:2881";

    public static GenericContainer<?> createOceanBaseServer() {
        return new GenericContainer<>("oceanbase/oceanbase-ce:4.2.0.0")
                .withNetworkMode("host")
                .withEnv("MODE", "slim")
                .withEnv("OB_ROOT_PASSWORD", OB_SYS_PASSWORD)
                .withEnv("OB_DATAFILE_SIZE", "1G")
                .withEnv("OB_LOG_DISK_SIZE", "4G")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("ddl/mysql/docker_init.sql"),
                        "/root/boot/init.d/init.sql")
                .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                .withStartupTimeout(Duration.ofMinutes(4))
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    public static GenericContainer<?> createLogProxy() {
        return new GenericContainer<>("whhe/oblogproxy:1.1.3_4x")
                .withNetworkMode("host")
                .withEnv("OB_SYS_PASSWORD", OB_SYS_PASSWORD)
                .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                .withStartupTimeout(Duration.ofMinutes(1))
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }
}
