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

package org.apache.flink.cdc.connectors.mongodb.source;

import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.Objects;
import java.util.stream.Stream;

/** MongoDBSourceTestBase for MongoDB >= 5.0.3. */
public class MongoDBSourceTestBase {

    public MongoDBSourceTestBase() {
        this.mongoContainer =
                new MongoDBContainer("mongo:" + getMongoVersion())
                        .withSharding()
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    public static String getMongoVersion() {
        String specifiedMongoVersion = System.getProperty("specifiedMongoVersion");
        if (Objects.isNull(specifiedMongoVersion)) {
            throw new IllegalArgumentException(
                    "No MongoDB version specified to run this test. Please use -DspecifiedMongoVersion to pass one.");
        }
        return specifiedMongoVersion;
    }

    protected static final int DEFAULT_PARALLELISM = 4;

    public final MongoDBContainer mongoContainer;

    protected MongoClient mongodbClient;

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

    @BeforeEach
    public void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(mongoContainer)).join();

        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(mongoContainer.getConnectionString()))
                        .build();
        mongodbClient = MongoClients.create(settings);

        LOG.info("Containers are started.");
    }

    @AfterEach
    public void stopContainers() {
        LOG.info("Stopping containers...");
        if (mongoContainer != null) {
            mongoContainer.close();
        }
        if (mongodbClient != null) {
            mongodbClient.close();
            mongodbClient = null;
        }
        LOG.info("Containers are stopped.");
    }

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceTestBase.class);
}
