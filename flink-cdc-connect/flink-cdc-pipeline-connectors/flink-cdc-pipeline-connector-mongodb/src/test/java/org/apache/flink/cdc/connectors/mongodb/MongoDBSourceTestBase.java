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

package org.apache.flink.cdc.connectors.mongodb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

/**
 * Basic class for testing MySQL binlog source, this contains a MySQL container which enables
 * binlog.
 */
public class MongoDBSourceTestBase extends TestLogger {
    protected static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceTestBase.class);

    protected static final int DEFAULT_PARALLELISM = 4;
    protected static final MongoDBContainer MYSQL_CONTAINER = createMySqlContainer("");
    protected InMemoryReporter metricReporter = InMemoryReporter.createWithRetainedMetrics();

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .setConfiguration(
                                    metricReporter.addToConfiguration(new Configuration()))
                            .build());

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.stop();
        }
        LOG.info("Containers are stopped.");
    }

    protected static MongoDBContainer createMySqlContainer(String version) {
        return createMySqlContainer(version, "docker/server-gtids/my.cnf");
    }

    protected static MongoDBContainer createMySqlContainer(String version, String configPath) {
        return (MongoDBContainer) new MongoDBContainer(version);
    }
}
