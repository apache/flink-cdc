/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.connection;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A MongoDB Client pools. */
public class MongoClientPools {

    private static final Logger LOG = LoggerFactory.getLogger(MongoClientPools.class);

    private static final MongoClientPools INSTANCE = new MongoClientPools();
    private final Map<ConnectionString, MongoClient> pools = new ConcurrentHashMap<>();

    private MongoClientPools() {}

    public static MongoClientPools getInstance() {
        return INSTANCE;
    }

    public MongoClient getOrCreateMongoClient(MongoDBSourceConfig sourceConfig) {
        return pools.computeIfAbsent(
                sourceConfig.getConnectionString(),
                connectionString -> {
                    LOG.info(
                            "Create and register mongo client {}@{}",
                            connectionString.getUsername(),
                            connectionString.getHosts());
                    return MongoClients.create(connectionString);
                });
    }
}
