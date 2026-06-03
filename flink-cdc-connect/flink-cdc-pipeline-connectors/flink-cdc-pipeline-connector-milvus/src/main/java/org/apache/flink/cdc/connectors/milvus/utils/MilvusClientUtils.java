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

package org.apache.flink.cdc.connectors.milvus.utils;

import org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkConfig;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

/** Utilities for Milvus Java client creation. */
public class MilvusClientUtils {

    private MilvusClientUtils() {}

    public static MilvusClientV2 createClient(MilvusDataSinkConfig config) {
        ConnectConfig.ConnectConfigBuilder builder =
                ConnectConfig.builder().uri(config.getUri()).dbName(config.getDatabaseName());
        if (config.getToken() != null && !config.getToken().isEmpty()) {
            builder.token(config.getToken());
        }
        if (config.getConnectTimeout() != null && !config.getConnectTimeout().isZero()) {
            builder.connectTimeoutMs(config.getConnectTimeout().toMillis());
        }
        if (config.getRpcDeadline() != null && !config.getRpcDeadline().isZero()) {
            builder.rpcDeadlineMs(config.getRpcDeadline().toMillis());
        }
        return new MilvusClientV2(builder.build());
    }
}
