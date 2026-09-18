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

import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.mongodb.source.utils.MongoDBSslUtils;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBSslContainer;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.net.ssl.SSLContext;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests verifying MongoDB with custom SSL context. */
@Testcontainers
class MongoDBSslConnectionITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSslConnectionITCase.class);

    private static final String MONGO_VERSION = getMongoVersion();

    private static final int DEFAULT_PARALLELISM = 4;

    private static MongoDBSslContainer mongo;

    private static String truststorePath;
    private static String keystorePath;

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

    @BeforeAll
    static void startContainer() throws Exception {
        truststorePath = resourcePath("ssl/client-truststore.p12");
        keystorePath = resourcePath("ssl/client-keystore.p12");

        mongo =
                new MongoDBSslContainer("mongo:" + MONGO_VERSION)
                        .withLogConsumer(
                                frame -> LOG.info("[mongodb-tls] {}", frame.getUtf8String()));
        mongo.start();
    }

    @AfterAll
    static void stopContainer() {
        if (mongo != null) {
            mongo.stop();
        }
    }

    @Test
    void truststoreOnlyConnectsAndReadWrites() {
        MongoDBSourceConfig config =
                configFactory()
                        .sslEnabled(true)
                        .sslTrustStore(truststorePath)
                        .sslTrustStorePassword("truststorepass")
                        .sslTrustStoreType("PKCS12")
                        .sslInvalidHostnameAllowed(true)
                        .create(0);

        assertRoundTrip(config, "ssl_truststore_only");
    }

    @Test
    void keystoreAndTruststoreConnectsAndReadWrites() {
        MongoDBSourceConfig config =
                configFactory()
                        .sslEnabled(true)
                        .sslKeyStore(keystorePath)
                        .sslKeyStorePassword("keystorepass")
                        .sslKeyStoreType("PKCS12")
                        .sslTrustStore(truststorePath)
                        .sslTrustStorePassword("truststorepass")
                        .sslTrustStoreType("PKCS12")
                        .sslInvalidHostnameAllowed(true)
                        .create(0);

        assertRoundTrip(config, "ssl_keystore_truststore");
    }

    @Test
    void plainConnectionFailsAgainstTlsServer() {
        String plainUri =
                String.format(
                        "mongodb://%s:%d/",
                        mongo.getHost(), mongo.getMappedPort(MongoDBSslContainer.MONGODB_PORT));
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(plainUri))
                        .build();

        assertThatThrownBy(
                        () -> {
                            try (MongoClient client = MongoClients.create(settings)) {
                                client.getDatabase("admin").runCommand(new Document("ping", 1));
                            }
                        })
                .isInstanceOf(Exception.class);
    }

    @Test
    void jvmDefaultTruststoreIsUsedWhenNoStoresConfigured() throws Exception {
        // Simulate the effect of -Djavax.net.ssl.trustStore=... at JVM startup by installing
        // a custom default SSLContext that trusts our test CA. The driver uses
        // SSLContext.getDefault() when no keystore/truststore is configured in the connector.
        withDefaultSslContext(
                buildSslContextFromTruststore(truststorePath, "truststorepass"),
                () -> {
                    MongoDBSourceConfig config =
                            configFactory()
                                    .sslEnabled(true)
                                    .sslInvalidHostnameAllowed(true)
                                    .create(0);

                    // createSSLContext returns null when no keystore/truststore is configured
                    assertThat(MongoDBSslUtils.createSSLContext(config)).isNull();

                    // Driver falls back to JVM default SSLContext, which now trusts our CA
                    MongoClientSettings settings = buildSettings(config, null);
                    try (MongoClient client = MongoClients.create(settings)) {
                        Document result =
                                client.getDatabase("admin").runCommand(new Document("ping", 1));
                        assertThat(result.getDouble("ok")).isEqualTo(1.0);
                    }
                });
    }

    @Test
    void connectorTruststoreOverridesJvmDefault() throws Exception {
        // Install a default SSLContext that trusts nothing, simulating a hostile JVM default.
        // The connector's explicit truststore config must override it for the connection to
        // succeed.
        withDefaultSslContext(
                emptyTrustingSslContext(),
                () -> {
                    MongoDBSourceConfig config =
                            configFactory()
                                    .sslEnabled(true)
                                    .sslTrustStore(truststorePath)
                                    .sslTrustStorePassword("truststorepass")
                                    .sslTrustStoreType("PKCS12")
                                    .sslInvalidHostnameAllowed(true)
                                    .create(0);

                    assertThat(MongoDBSslUtils.createSSLContext(config)).isNotNull();
                    assertCdcSourceReadsData(config, "ssl_connector_overrides_jvm");
                });
    }

    @Test
    void connectorKeystoreTruststoreOverridesJvmDefault() throws Exception {
        // Same as above but with both keystore and truststore configured in the connector.
        withDefaultSslContext(
                emptyTrustingSslContext(),
                () -> {
                    MongoDBSourceConfig config =
                            configFactory()
                                    .sslEnabled(true)
                                    .sslKeyStore(keystorePath)
                                    .sslKeyStorePassword("keystorepass")
                                    .sslKeyStoreType("PKCS12")
                                    .sslTrustStore(truststorePath)
                                    .sslTrustStorePassword("truststorepass")
                                    .sslTrustStoreType("PKCS12")
                                    .sslInvalidHostnameAllowed(true)
                                    .create(0);

                    assertThat(MongoDBSslUtils.createSSLContext(config)).isNotNull();
                    assertCdcSourceReadsData(config, "ssl_connector_keystore_overrides_jvm");
                });
    }

    /**
     * Starts a real MongoDBSource CDC pipeline, writes a document, and verifies that the source
     * picks it up.
     */
    private void assertCdcSourceReadsData(MongoDBSourceConfig config, String collectionName) {
        String database = "ssl_cdc_test";
        String fullName = database + "." + collectionName;

        SSLContext ctx = MongoDBSslUtils.createSSLContext(config);
        assertThat(ctx).isNotNull();

        MongoClientSettings writeSettings = buildSettings(config, ctx);
        try (MongoClient client = MongoClients.create(writeSettings)) {
            MongoDatabase db = client.getDatabase(database);
            db.getCollection(collectionName).insertOne(new Document("key", "value"));
        }

        // Build and run a real CDC source using the same config
        MongoDBSourceBuilder<String> builder =
                MongoDBSource.<String>builder()
                        .hosts(
                                String.format(
                                        "%s:%d",
                                        mongo.getHost(),
                                        mongo.getMappedPort(MongoDBSslContainer.MONGODB_PORT)))
                        .username(MongoDBContainer.FLINK_USER)
                        .password(MongoDBContainer.FLINK_USER_PASSWORD)
                        .databaseList(database)
                        .collectionList(fullName)
                        .sslEnabled(config.isSslEnabled())
                        .sslInvalidHostnameAllowed(config.isSslInvalidHostnameAllowed())
                        .deserializer(new JsonDebeziumDeserializationSchema());

        if (config.getSslTrustStore() != null) {
            builder.sslTrustStore(config.getSslTrustStore())
                    .sslTrustStorePassword(config.getSslTrustStorePassword())
                    .sslTrustStoreType(config.getSslTrustStoreType());
        }
        if (config.getSslKeyStore() != null) {
            builder.sslKeyStore(config.getSslKeyStore())
                    .sslKeyStorePassword(config.getSslKeyStorePassword())
                    .sslKeyStoreType(config.getSslKeyStoreType());
        }

        MongoDBSource<String> source = builder.build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        try (env;
                CloseableIterator<String> iterator =
                        env.fromSource(
                                        source,
                                        org.apache.flink.api.common.eventtime.WatermarkStrategy
                                                .noWatermarks(),
                                        "MongoDB CDC SSL Source")
                                .executeAndCollect()) {
            env.enableCheckpointing(1000);
            env.setParallelism(1);

            // We inserted exactly 1 document; snapshot phase will emit it. Calling next()
            // once avoids the blocking hasNext() loop.
            String record = iterator.next();
            LOG.info("CDC record: {}", record);
            assertThat(record).contains(collectionName);
        } catch (Exception e) {
            LOG.debug("Error closing execution environment", e);
        }
    }

    private void assertRoundTrip(MongoDBSourceConfig config, String collectionName) {
        SSLContext ctx = MongoDBSslUtils.createSSLContext(config);
        assertThat(ctx).isNotNull();

        MongoClientSettings settings = buildSettings(config, ctx);
        try (MongoClient client = MongoClients.create(settings)) {
            MongoDatabase db = client.getDatabase("ssl_integration_test");
            MongoCollection<Document> col = db.getCollection(collectionName);

            col.insertOne(new Document("hello", "world"));
            List<Document> docs = col.find().into(new ArrayList<>());
            assertThat(docs).hasSize(1);
            assertThat(docs.get(0).getString("hello")).isEqualTo("world");
        }
    }

    private MongoClientSettings buildSettings(MongoDBSourceConfig config, SSLContext sslContext) {
        String uri = mongo.getTlsConnectionString();
        MongoClientSettings.Builder builder =
                MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(uri))
                        .applyToSslSettings(
                                ssl -> {
                                    ssl.enabled(true);
                                    ssl.invalidHostNameAllowed(
                                            config.isSslInvalidHostnameAllowed());
                                    if (sslContext != null) {
                                        ssl.context(sslContext);
                                    }
                                });
        return builder.build();
    }

    private static MongoDBSourceConfigFactory configFactory() {
        return new MongoDBSourceConfigFactory()
                .hosts(
                        String.format(
                                "%s:%d",
                                mongo.getHost(),
                                mongo.getMappedPort(MongoDBSslContainer.MONGODB_PORT)));
    }

    private static String resourcePath(String classpathResource) throws Exception {
        URL url = MongoDBSslConnectionITCase.class.getClassLoader().getResource(classpathResource);
        Objects.requireNonNull(url, "Cannot locate test resource: " + classpathResource);
        return Paths.get(url.toURI()).toAbsolutePath().toString();
    }

    private static void withDefaultSslContext(SSLContext ctx, ThrowingRunnable action)
            throws Exception {
        SSLContext previousDefault = SSLContext.getDefault();
        try {
            SSLContext.setDefault(ctx);
            action.run();
        } finally {
            SSLContext.setDefault(previousDefault);
        }
    }

    private static SSLContext buildSslContextFromTruststore(String path, String password)
            throws Exception {
        java.security.KeyStore ts = java.security.KeyStore.getInstance("PKCS12");
        try (java.io.FileInputStream fis = new java.io.FileInputStream(path)) {
            ts.load(fis, password.toCharArray());
        }
        javax.net.ssl.TrustManagerFactory tmf =
                javax.net.ssl.TrustManagerFactory.getInstance(
                        javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    private static SSLContext emptyTrustingSslContext() throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(
                null,
                new javax.net.ssl.TrustManager[] {
                    new javax.net.ssl.X509TrustManager() {
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[0];
                        }

                        public void checkClientTrusted(
                                java.security.cert.X509Certificate[] c, String a)
                                throws java.security.cert.CertificateException {
                            throw new java.security.cert.CertificateException("untrusted");
                        }

                        public void checkServerTrusted(
                                java.security.cert.X509Certificate[] c, String a)
                                throws java.security.cert.CertificateException {
                            throw new java.security.cert.CertificateException("untrusted");
                        }
                    }
                },
                null);
        return ctx;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static String getMongoVersion() {
        String v = System.getProperty("specifiedMongoVersion");
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException(
                    "No MongoDB version specified. Pass -DspecifiedMongoVersion=<version>.");
        }
        return v;
    }
}
