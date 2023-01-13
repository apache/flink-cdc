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

package com.ververica.cdc.connectors.mongodb;

import org.apache.flink.test.util.AbstractTestBase;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.MONGO_SUPER_PASSWORD;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.MONGO_SUPER_USER;

/**
 * Basic class for testing MongoDB source, this contains a MongoDB container which enables change
 * streams.
 */
public class LegacyMongoDBTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(LegacyMongoDBTestBase.class);

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    protected static final MongoDBContainer MONGODB_CONTAINER =
            new MongoDBContainer(NETWORK).withLogConsumer(new Slf4jLogConsumer(LOG));

    protected static MongoClient mongodbClient;

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MONGODB_CONTAINER)).join();
        initialClient();
        LOG.info("Containers are started.");
    }

    private static void initialClient() {
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(
                                        MONGODB_CONTAINER.getConnectionString(
                                                MONGO_SUPER_USER, MONGO_SUPER_PASSWORD)))
                        .build();
        mongodbClient = MongoClients.create(settings);
    }

    protected static MongoDatabase getMongoDatabase(String dbName) {
        return mongodbClient.getDatabase(dbName);
    }
}
