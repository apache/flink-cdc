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

import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.MONGO_SUPER_PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.MONGO_SUPER_USER;

/** Basic class for testing {@link MongoDBSource}. */
@Testcontainers
public abstract class LegacyMongoDBSourceTestBase extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(LegacyMongoDBSourceTestBase.class);
    protected static final int DEFAULT_PARALLELISM = 4;

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

    public static final Network NETWORK = Network.newNetwork();

    @Container
    public static final LegacyMongoDBContainer CONFIG =
            new LegacyMongoDBContainer(NETWORK, LegacyMongoDBContainer.ShardingClusterRole.CONFIG)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Container
    public static final LegacyMongoDBContainer SHARD =
            new LegacyMongoDBContainer(NETWORK, LegacyMongoDBContainer.ShardingClusterRole.SHARD)
                    .dependsOn(CONFIG)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Container
    public static final LegacyMongoDBContainer ROUTER =
            new LegacyMongoDBContainer(NETWORK, LegacyMongoDBContainer.ShardingClusterRole.ROUTER)
                    .dependsOn(SHARD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected static MongoClient mongodbClient;

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(CONFIG)).join();
        Startables.deepStart(Stream.of(SHARD)).join();
        Startables.deepStart(Stream.of(ROUTER)).join();
        initialClient();
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void closeContainers() {
        if (mongodbClient != null) {
            mongodbClient.close();
        }
        if (ROUTER != null) {
            ROUTER.close();
        }
        if (SHARD != null) {
            SHARD.close();
        }
        if (CONFIG != null) {
            CONFIG.close();
        }
    }

    private static void initialClient() {
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(
                                        ROUTER.getConnectionString(
                                                MONGO_SUPER_USER, MONGO_SUPER_PASSWORD)))
                        .build();
        mongodbClient = MongoClients.create(settings);
    }
}
