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

package org.apache.flink.cdc.connectors.mongodb.source.utils;

import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;

import java.net.URL;

/** Unit tests for {@link MongoDBSslUtils}. */
class MongoDBSslUtilsTest {

    private static String keystoreP12Path;
    private static String truststoreP12Path;
    private static String keystoreJksPath;
    private static String truststoreJksPath;

    @BeforeAll
    static void resolveResourcePaths() {
        keystoreP12Path = resourcePath("ssl/client-keystore.p12");
        truststoreP12Path = resourcePath("ssl/client-truststore.p12");
        keystoreJksPath = resourcePath("ssl/client-keystore.jks");
        truststoreJksPath = resourcePath("ssl/client-truststore.jks");
    }

    private static String resourcePath(String name) {
        URL url = MongoDBSslUtilsTest.class.getClassLoader().getResource(name);
        Assertions.assertThat(url).as("test resource '%s' not found", name).isNotNull();
        return url.getPath();
    }

    @Test
    void sslDisabledReturnsNull() {
        MongoDBSourceConfig config = factory().sslEnabled(false).create(0);
        Assertions.assertThat(MongoDBSslUtils.createSSLContext(config)).isNull();
    }

    @Test
    void sslDisabledWithKeystoreStillReturnsNull() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(false)
                        .sslKeyStore(keystoreP12Path)
                        .sslKeyStorePassword("keystorepass")
                        .sslKeyStoreType("PKCS12")
                        .create(0);
        Assertions.assertThat(MongoDBSslUtils.createSSLContext(config)).isNull();
    }

    @Test
    void sslDisabledWithTruststoreStillReturnsNull() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(false)
                        .sslTrustStore(truststoreP12Path)
                        .sslTrustStorePassword("truststorepass")
                        .sslTrustStoreType("PKCS12")
                        .create(0);
        Assertions.assertThat(MongoDBSslUtils.createSSLContext(config)).isNull();
    }

    @Test
    void sslEnabledNoStoresReturnsNull() {
        MongoDBSourceConfig config = factory().sslEnabled(true).create(0);
        Assertions.assertThat(MongoDBSslUtils.createSSLContext(config)).isNull();
    }

    @Test
    void truststoreOnlyPkcs12ProducesContext() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslTrustStore(truststoreP12Path)
                        .sslTrustStorePassword("truststorepass")
                        .sslTrustStoreType("PKCS12")
                        .create(0);
        SSLContext ctx = MongoDBSslUtils.createSSLContext(config);
        Assertions.assertThat(ctx).isNotNull();
        Assertions.assertThat(ctx.getProtocol()).isEqualTo("TLS");
    }

    @Test
    void truststoreOnlyJksProducesContext() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslTrustStore(truststoreJksPath)
                        .sslTrustStorePassword("truststorepass")
                        .sslTrustStoreType("JKS")
                        .create(0);
        SSLContext ctx = MongoDBSslUtils.createSSLContext(config);
        Assertions.assertThat(ctx).isNotNull();
        Assertions.assertThat(ctx.getProtocol()).isEqualTo("TLS");
    }

    @Test
    void truststoreWithoutPasswordProducesContext() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslTrustStore(truststoreP12Path)
                        .sslTrustStoreType("PKCS12")
                        .create(0);
        SSLContext ctx = MongoDBSslUtils.createSSLContext(config);
        Assertions.assertThat(ctx).isNotNull();
    }

    @Test
    void keystoreOnlyPkcs12ProducesContext() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslKeyStore(keystoreP12Path)
                        .sslKeyStorePassword("keystorepass")
                        .sslKeyStoreType("PKCS12")
                        .create(0);
        SSLContext ctx = MongoDBSslUtils.createSSLContext(config);
        Assertions.assertThat(ctx).isNotNull();
        Assertions.assertThat(ctx.getProtocol()).isEqualTo("TLS");
    }

    @Test
    void keystoreOnlyJksProducesContext() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslKeyStore(keystoreJksPath)
                        .sslKeyStorePassword("keystorepass")
                        .sslKeyStoreType("JKS")
                        .create(0);
        SSLContext ctx = MongoDBSslUtils.createSSLContext(config);
        Assertions.assertThat(ctx).isNotNull();
        Assertions.assertThat(ctx.getProtocol()).isEqualTo("TLS");
    }

    @Test
    void keystoreAndTruststorePkcs12ProducesContext() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslKeyStore(keystoreP12Path)
                        .sslKeyStorePassword("keystorepass")
                        .sslKeyStoreType("PKCS12")
                        .sslTrustStore(truststoreP12Path)
                        .sslTrustStorePassword("truststorepass")
                        .sslTrustStoreType("PKCS12")
                        .create(0);
        SSLContext ctx = MongoDBSslUtils.createSSLContext(config);
        Assertions.assertThat(ctx).isNotNull();
        Assertions.assertThat(ctx.getProtocol()).isEqualTo("TLS");
    }

    @Test
    void keystoreAndTruststoreJksProducesContext() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslKeyStore(keystoreJksPath)
                        .sslKeyStorePassword("keystorepass")
                        .sslKeyStoreType("JKS")
                        .sslTrustStore(truststoreJksPath)
                        .sslTrustStorePassword("truststorepass")
                        .sslTrustStoreType("JKS")
                        .create(0);
        SSLContext ctx = MongoDBSslUtils.createSSLContext(config);
        Assertions.assertThat(ctx).isNotNull();
        Assertions.assertThat(ctx.getProtocol()).isEqualTo("TLS");
    }

    @Test
    void keystoreP12AndTruststoreJksMixedTypesProducesContext() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslKeyStore(keystoreP12Path)
                        .sslKeyStorePassword("keystorepass")
                        .sslKeyStoreType("PKCS12")
                        .sslTrustStore(truststoreJksPath)
                        .sslTrustStorePassword("truststorepass")
                        .sslTrustStoreType("JKS")
                        .create(0);
        SSLContext ctx = MongoDBSslUtils.createSSLContext(config);
        Assertions.assertThat(ctx).isNotNull();
    }

    @Test
    void wrongKeystorePasswordThrows() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslKeyStore(keystoreP12Path)
                        .sslKeyStorePassword("wrongpassword")
                        .sslKeyStoreType("PKCS12")
                        .create(0);
        Assertions.assertThatThrownBy(() -> MongoDBSslUtils.createSSLContext(config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unable to read key store file");
    }

    @Test
    void wrongTruststorePasswordThrows() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslTrustStore(truststoreP12Path)
                        .sslTrustStorePassword("wrongpassword")
                        .sslTrustStoreType("PKCS12")
                        .create(0);
        Assertions.assertThatThrownBy(() -> MongoDBSslUtils.createSSLContext(config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unable to read key store file");
    }

    @Test
    void unknownKeystoreTypeThrows() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslKeyStore(keystoreP12Path)
                        .sslKeyStorePassword("keystorepass")
                        .sslKeyStoreType("BOGUSTYPE")
                        .create(0);
        Assertions.assertThatThrownBy(() -> MongoDBSslUtils.createSSLContext(config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unable to read key store file");
    }

    @Test
    void unknownTruststoreTypeThrows() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslTrustStore(truststoreP12Path)
                        .sslTrustStorePassword("truststorepass")
                        .sslTrustStoreType("BOGUSTYPE")
                        .create(0);
        Assertions.assertThatThrownBy(() -> MongoDBSslUtils.createSSLContext(config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unable to read key store file");
    }

    @Test
    void nonExistentKeystorePathThrows() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslKeyStore("/nonexistent/path/keystore.p12")
                        .sslKeyStorePassword("keystorepass")
                        .sslKeyStoreType("PKCS12")
                        .create(0);
        Assertions.assertThatThrownBy(() -> MongoDBSslUtils.createSSLContext(config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unable to read key store file");
    }

    @Test
    void nonExistentTruststorePathThrows() {
        MongoDBSourceConfig config =
                factory()
                        .sslEnabled(true)
                        .sslTrustStore("/nonexistent/path/truststore.p12")
                        .sslTrustStorePassword("truststorepass")
                        .sslTrustStoreType("PKCS12")
                        .create(0);
        Assertions.assertThatThrownBy(() -> MongoDBSslUtils.createSSLContext(config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unable to read key store file");
    }

    private static MongoDBSourceConfigFactory factory() {
        return new MongoDBSourceConfigFactory().hosts("localhost:27017");
    }
}
