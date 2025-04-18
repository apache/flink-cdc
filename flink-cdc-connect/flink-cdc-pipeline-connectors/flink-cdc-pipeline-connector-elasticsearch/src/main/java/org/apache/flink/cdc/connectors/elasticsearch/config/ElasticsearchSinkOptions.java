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

package org.apache.flink.cdc.connectors.elasticsearch.config;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.elasticsearch.v2.NetworkConfig;

import org.apache.http.HttpHost;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.SHARDING_SUFFIX_SEPARATOR;

/** Elasticsearch DataSink Options reference {@link ElasticsearchSinkOptions}. */
public class ElasticsearchSinkOptions implements Serializable {

    private final int maxBatchSize;
    private final int maxInFlightRequests;
    private final int maxBufferedRequests;
    private final long maxBatchSizeInBytes;
    private final long maxTimeInBufferMS;
    private final long maxRecordSizeInBytes;
    private final NetworkConfig networkConfig;
    private final int version;
    private final String username;
    private final String password;
    private final Map<TableId, String> shardingKey;
    private final String shardingSeparator;

    /** Constructor for ElasticsearchSinkOptions. */
    public ElasticsearchSinkOptions(
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            NetworkConfig networkConfig,
            int version,
            String username,
            String password) {
        this(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                networkConfig,
                version,
                username,
                password,
                Collections.emptyMap(),
                SHARDING_SUFFIX_SEPARATOR.defaultValue());
    }

    public ElasticsearchSinkOptions(
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            NetworkConfig networkConfig,
            int version,
            String username,
            String password,
            Map<TableId, String> shardingKey,
            String shardingSeparator) {
        this.maxBatchSize = maxBatchSize;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.maxTimeInBufferMS = maxTimeInBufferMS;
        this.maxRecordSizeInBytes = maxRecordSizeInBytes;
        this.networkConfig = networkConfig;
        this.version = version;
        this.username = username;
        this.password = password;
        this.shardingKey = shardingKey;
        this.shardingSeparator = shardingSeparator;
    }

    /** @return the maximum batch size */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    /** @return the maximum number of in-flight requests */
    public int getMaxInFlightRequests() {
        return maxInFlightRequests;
    }

    /** @return the maximum number of buffered requests */
    public int getMaxBufferedRequests() {
        return maxBufferedRequests;
    }

    /** @return the maximum batch size in bytes */
    public long getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    /** @return the maximum time in buffer in milliseconds */
    public long getMaxTimeInBufferMS() {
        return maxTimeInBufferMS;
    }

    /** @return the maximum record size in bytes */
    public long getMaxRecordSizeInBytes() {
        return maxRecordSizeInBytes;
    }

    /** @return the network configuration */
    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    /** @return the list of Elasticsearch hosts */
    public List<HttpHost> getHosts() {
        return networkConfig.getHosts();
    }

    public int getVersion() {
        return version;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Map<TableId, String> getShardingKey() {
        return shardingKey;
    }

    public String getShardingSeparator() {
        return shardingSeparator;
    }
}
