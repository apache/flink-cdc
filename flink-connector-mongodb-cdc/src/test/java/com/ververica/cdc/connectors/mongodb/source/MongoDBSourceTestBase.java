/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.mongodb.source;

import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.MONGO_SUPER_PASSWORD;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.MONGO_SUPER_USER;

/** Basic class for testing {@link MongoDBSource}. */
public abstract class MongoDBSourceTestBase extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceTestBase.class);
    protected static final int DEFAULT_PARALLELISM = 4;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    @ClassRule
    public static final MongoDBContainer CONFIG =
            new MongoDBContainer(NETWORK, MongoDBContainer.ShardingClusterRole.CONFIG)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final MongoDBContainer SHARD =
            new MongoDBContainer(NETWORK, MongoDBContainer.ShardingClusterRole.SHARD)
                    .dependsOn(CONFIG)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final MongoDBContainer ROUTER =
            new MongoDBContainer(NETWORK, MongoDBContainer.ShardingClusterRole.ROUTER)
                    .dependsOn(SHARD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected static MongoClient mongodbClient;

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(CONFIG)).join();
        Startables.deepStart(Stream.of(SHARD)).join();
        Startables.deepStart(Stream.of(ROUTER)).join();
        initialClient();
        LOG.info("Containers are started.");
    }

    @AfterClass
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
