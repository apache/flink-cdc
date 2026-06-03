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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.cdc.connectors.milvus.utils.MilvusClientUtils;

import com.google.gson.JsonObject;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.UpsertReq;

import java.util.List;

/** Default {@link MilvusClientWrapper} backed by Milvus Java SDK. */
class DefaultMilvusClientWrapper implements MilvusClientWrapper {

    private final MilvusDataSinkConfig config;
    private MilvusClientV2 client;

    DefaultMilvusClientWrapper(MilvusDataSinkConfig config) {
        this.config = config;
        this.client = MilvusClientUtils.createClient(config);
    }

    @Override
    public void upsert(String databaseName, String collectionName, List<JsonObject> rows) {
        upsert(databaseName, collectionName, null, rows);
    }

    @Override
    public void upsert(
            String databaseName,
            String collectionName,
            String partitionName,
            List<JsonObject> rows) {
        UpsertReq req =
                UpsertReq.builder()
                        .databaseName(databaseName)
                        .collectionName(collectionName)
                        .data(rows)
                        .build();
        if (partitionName != null && !partitionName.isEmpty()) {
            req.setPartitionName(partitionName);
        }
        client.upsert(req);
    }

    @Override
    public void delete(String databaseName, String collectionName, List<Object> ids) {
        delete(databaseName, collectionName, null, ids);
    }

    @Override
    public void delete(
            String databaseName, String collectionName, String partitionName, List<Object> ids) {
        DeleteReq req =
                DeleteReq.builder()
                        .databaseName(databaseName)
                        .collectionName(collectionName)
                        .ids(ids)
                        .build();
        if (partitionName != null && !partitionName.isEmpty()) {
            req.setPartitionName(partitionName);
        }
        client.delete(req);
    }

    @Override
    public boolean hasPartition(String databaseName, String collectionName, String partitionName) {
        return client.hasPartition(
                HasPartitionReq.builder()
                        .databaseName(databaseName)
                        .collectionName(collectionName)
                        .partitionName(partitionName)
                        .build());
    }

    @Override
    public void createPartition(String databaseName, String collectionName, String partitionName) {
        client.createPartition(
                CreatePartitionReq.builder()
                        .databaseName(databaseName)
                        .collectionName(collectionName)
                        .partitionName(partitionName)
                        .build());
    }

    @Override
    public void reconnect() {
        closeQuietly();
        this.client = MilvusClientUtils.createClient(config);
    }

    @Override
    public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
