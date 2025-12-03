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

import org.apache.flink.test.util.AbstractTestBase;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.MONGO_SUPER_PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.MONGO_SUPER_USER;

/**
 * Basic class for testing MongoDB source, this contains a MongoDB container which enables change
 * streams.
 */
public class LegacyMongoDBTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(LegacyMongoDBTestBase.class);

    public static final Network NETWORK = Network.newNetwork();

    protected static final LegacyMongoDBContainer MONGODB_CONTAINER =
            new LegacyMongoDBContainer(NETWORK).withLogConsumer(new Slf4jLogConsumer(LOG));

    protected static MongoClient mongodbClient;

    @BeforeAll
    static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MONGODB_CONTAINER)).join();
        initialClient();
        LOG.info("Containers are started.");
    }

    @AfterAll
    static void stopContainers() {
        LOG.info("Stopping containers...");
        MONGODB_CONTAINER.stop();
        LOG.info("Containers are stopped.");
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
