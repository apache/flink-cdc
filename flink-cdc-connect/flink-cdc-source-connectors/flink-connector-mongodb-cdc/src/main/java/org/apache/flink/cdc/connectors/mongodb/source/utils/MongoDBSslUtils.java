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

import javax.annotation.Nullable;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

/** Utilities for building SSL context from MongoDB connector configuration. */
public class MongoDBSslUtils {

    private MongoDBSslUtils() {}

    /**
     * Creates a custom {@link SSLContext} from the given {@link MongoDBSourceConfig}. If neither
     * keystore nor truststore is configured, returns {@code null} which lets the driver use the JVM
     * default SSL context.
     */
    @Nullable
    public static SSLContext createSSLContext(MongoDBSourceConfig config) {
        if (!config.isSslEnabled()) {
            return null;
        }

        String keyStorePath = config.getSslKeyStore();
        String trustStorePath = config.getSslTrustStore();

        if (keyStorePath == null && trustStorePath == null) {
            return null;
        }

        try {
            KeyManager[] keyManagers = null;
            if (keyStorePath != null) {
                char[] keyStorePassword =
                        config.getSslKeyStorePassword() != null
                                ? config.getSslKeyStorePassword().toCharArray()
                                : null;
                KeyStore ks =
                        loadKeyStore(
                                config.getSslKeyStoreType(),
                                Paths.get(keyStorePath),
                                keyStorePassword);
                KeyManagerFactory kmf =
                        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, keyStorePassword);
                keyManagers = kmf.getKeyManagers();
            }

            TrustManager[] trustManagers = null;
            if (trustStorePath != null) {
                char[] trustStorePassword =
                        config.getSslTrustStorePassword() != null
                                ? config.getSslTrustStorePassword().toCharArray()
                                : null;
                KeyStore ts =
                        loadKeyStore(
                                config.getSslTrustStoreType(),
                                Paths.get(trustStorePath),
                                trustStorePassword);
                TrustManagerFactory tmf =
                        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
                trustManagers = tmf.getTrustManagers();
            }

            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyManagers, trustManagers, null);
            return context;
        } catch (NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException e) {
            throw new RuntimeException("Unable to create KeyStore/TrustStore manager factory", e);
        } catch (KeyManagementException e) {
            throw new RuntimeException("Unable to initialize SSL context", e);
        }
    }

    private static KeyStore loadKeyStore(String type, Path path, @Nullable char[] password) {
        try (InputStream in = Files.newInputStream(path)) {
            KeyStore ks = KeyStore.getInstance(type);
            ks.load(in, password);
            return ks;
        } catch (IOException
                | KeyStoreException
                | NoSuchAlgorithmException
                | CertificateException e) {
            throw new RuntimeException("Unable to read key store file from '" + path + "'", e);
        }
    }
}
