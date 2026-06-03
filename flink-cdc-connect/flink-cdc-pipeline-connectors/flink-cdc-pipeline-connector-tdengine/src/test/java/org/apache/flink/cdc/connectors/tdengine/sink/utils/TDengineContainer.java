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

package org.apache.flink.cdc.connectors.tdengine.sink.utils;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/** Testcontainers wrapper for TDengine TSDB. */
public class TDengineContainer extends GenericContainer<TDengineContainer> {

    public static final String DEFAULT_USERNAME = "root";
    public static final String DEFAULT_PASSWORD = "taosdata";
    private static final DockerImageName IMAGE = DockerImageName.parse("tdengine/tsdb:latest");

    public TDengineContainer() {
        super(IMAGE);
        addExposedPort(6041);
        withStartupTimeout(Duration.ofMinutes(3));
        waitingFor(Wait.forListeningPort());
    }

    public String getJdbcUrl() {
        return "jdbc:TAOS-WS://" + getHost() + ":" + getMappedPort(6041);
    }
}
