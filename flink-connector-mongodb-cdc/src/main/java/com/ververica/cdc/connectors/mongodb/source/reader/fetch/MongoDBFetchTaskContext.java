/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.reader.fetch;

import com.mongodb.client.MongoClient;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.util.concurrent.BlockingQueue;

/** The context for fetch task that fetching data of snapshot split from MongoDB data source. */
public class MongoDBFetchTaskContext implements FetchTask.Context {

    private final MongoClient mongoClient;
    private final MongoDBSourceConfig sourceConfig;
    @Nullable private BlockingQueue<SourceRecord> copyExistingQueue;

    public MongoDBFetchTaskContext(MongoClient mongoClient, MongoDBSourceConfig sourceConfig) {
        this.mongoClient = mongoClient;
        this.sourceConfig = sourceConfig;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public MongoDBSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    @Nullable
    public BlockingQueue<SourceRecord> getCopyExistingQueue() {
        return copyExistingQueue;
    }

    public void setCopyExistingQueue(BlockingQueue<SourceRecord> copyExistingQueue) {
        this.copyExistingQueue = copyExistingQueue;
    }
}
