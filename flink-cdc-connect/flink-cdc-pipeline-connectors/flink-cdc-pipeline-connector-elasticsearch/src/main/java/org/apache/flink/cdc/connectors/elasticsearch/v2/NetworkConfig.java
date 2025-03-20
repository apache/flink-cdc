/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.cdc.connectors.elasticsearch.v2;

import org.apache.flink.util.function.SerializableSupplier;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import java.io.Serializable;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/** A factory that creates valid ElasticsearchClient instances. */
public class NetworkConfig implements Serializable {
    private final List<HttpHost> hosts;

    private final List<Header> headers;

    private final String username;

    private final String password;

    @Nullable private final SerializableSupplier<SSLContext> sslContextSupplier;

    @Nullable private final SerializableSupplier<HostnameVerifier> sslHostnameVerifier;

    public NetworkConfig(
            List<HttpHost> hosts,
            String username,
            String password,
            List<Header> headers,
            SerializableSupplier<SSLContext> sslContextSupplier,
            SerializableSupplier<HostnameVerifier> sslHostnameVerifier) {
        checkState(!hosts.isEmpty(), "Hosts must not be empty");
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.headers = headers;
        this.sslContextSupplier = sslContextSupplier;
        this.sslHostnameVerifier = sslHostnameVerifier;
    }

    public ElasticsearchAsyncClient createEsClient() {
        return new ElasticsearchAsyncClient(
                new RestClientTransport(this.getRestClient(), new JacksonJsonpMapper()));
    }

    private RestClient getRestClient() {
        RestClientBuilder restClientBuilder =
                RestClient.builder(hosts.toArray(new HttpHost[0]))
                        .setHttpClientConfigCallback(
                                httpClientBuilder -> {
                                    if (username != null && password != null) {
                                        httpClientBuilder.setDefaultCredentialsProvider(
                                                getCredentials());
                                    }

                                    if (sslContextSupplier != null) {
                                        httpClientBuilder.setSSLContext(sslContextSupplier.get());
                                    }

                                    if (sslHostnameVerifier != null) {
                                        httpClientBuilder.setSSLHostnameVerifier(
                                                sslHostnameVerifier.get());
                                    }

                                    return httpClientBuilder;
                                });

        if (headers != null) {
            restClientBuilder.setDefaultHeaders(headers.toArray(new Header[0]));
        }

        return restClientBuilder.build();
    }

    private CredentialsProvider getCredentials() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        return credentialsProvider;
    }

    public List<HttpHost> getHosts() {
        return this.hosts;
    }
}
