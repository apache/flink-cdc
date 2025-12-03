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

package org.apache.flink.cdc.connectors.iceberg.sink.utils;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

/** Docker container for Apache Hive 4.0.0. */
public class HiveContainer extends GenericContainer<HiveContainer> {
    private static final String DOCKER_IMAGE_NAME = "apache/hive:4.0.0";
    public static final int METASTORE_PORT = 9083;
    public static final int HIVESERVER2_PORT = 10000;
    public static final Network NETWORK = Network.newNetwork();

    public HiveContainer(String hostWarehouseDir) {
        this(NETWORK, hostWarehouseDir);
    }

    public HiveContainer(Network network, String hostWarehouseDir) {
        super(DockerImageName.parse(DOCKER_IMAGE_NAME));
        setExposedPorts(Arrays.asList(METASTORE_PORT, HIVESERVER2_PORT));
        setNetwork(network);
        withEnv("SERVICE_NAME", "metastore")
                .withEnv(
                        "SERVICE_OPTS",
                        "-Dhive.metastore.disallow.incompatible.col.type.changes=false")
                .withFileSystemBind(hostWarehouseDir, "/opt/hive/warehouse")
                .setWaitStrategy(
                        new LogMessageWaitStrategy()
                                .withRegEx(".*Actual binding is of type.*")
                                .withStartupTimeout(Duration.of(20, ChronoUnit.SECONDS)));
    }

    public String getMetastoreUri() {
        return String.format("thrift://%s:%d", getHost(), getMappedPort(METASTORE_PORT));
    }

    public String getHiveJdbcUrl() {
        return String.format("jdbc:hive2://%s:%d/", getHost(), getMappedPort(HIVESERVER2_PORT));
    }
}
