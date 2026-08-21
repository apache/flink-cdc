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

package org.apache.flink.cdc.connectors.mongodb.source.connection;

import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import org.apache.flink.cdc.connectors.mongodb.source.utils.MongoDBSslUtils;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A MongoDB Client pools. */
public class MongoClientPool {

    private static final Logger LOG = LoggerFactory.getLogger(MongoClientPool.class);

    private static final MongoClientPool INSTANCE = new MongoClientPool();
    private final Map<MongoDBSourceConfig, MongoClient> pools = new ConcurrentHashMap<>();

    private MongoClientPool() {}

    public static MongoClientPool getInstance() {
        return INSTANCE;
    }

    public MongoClient getOrCreateMongoClient(MongoDBSourceConfig sourceConfig) {
        if (!sourceConfig.isSslEnabled()
                && (sourceConfig.getSslKeyStore() != null
                        || sourceConfig.getSslTrustStore() != null)) {
            LOG.warn(
                    "{} or {} is configured, but {} is false. "
                            + "The keystore and truststore settings will be ignored and the connection will be established without SSL.",
                    MongoDBSourceOptions.SSL_KEYSTORE.key(),
                    MongoDBSourceOptions.SSL_TRUSTSTORE.key(),
                    MongoDBSourceOptions.SSL_ENABLED.key());
        }

        return pools.computeIfAbsent(
                sourceConfig,
                config -> {
                    ConnectionString connectionString =
                            new ConnectionString(config.getConnectionString());
                    LOG.info(
                            "Create and register mongo client {}@{}",
                            connectionString.getUsername(),
                            connectionString.getHosts());

                    MongoClientSettings.Builder settingsBuilder =
                            MongoClientSettings.builder().applyConnectionString(connectionString);

                    if (config.isSslEnabled()) {
                        SSLContext sslContext = MongoDBSslUtils.createSSLContext(config);
                        settingsBuilder.applyToSslSettings(
                                builder -> {
                                    builder.enabled(true);
                                    builder.invalidHostNameAllowed(
                                            config.isSslInvalidHostnameAllowed());
                                    if (sslContext != null) {
                                        builder.context(sslContext);
                                    }
                                });
                    }

                    return MongoClients.create(settingsBuilder.build());
                });
    }
}
