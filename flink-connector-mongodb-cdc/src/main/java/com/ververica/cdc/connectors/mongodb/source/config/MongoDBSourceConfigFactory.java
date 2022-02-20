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

package com.ververica.cdc.connectors.mongodb.source.config;

import org.apache.flink.annotation.Internal;

import com.ververica.cdc.connectors.base.config.SourceConfig.Factory;

import java.util.Arrays;
import java.util.List;

import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.BATCH_SIZE;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.COPY_EXISTING;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.COPY_EXISTING_QUEUE_SIZE;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_AWAIT_TIME_MILLIS;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_MAX_BATCH_SIZE;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_PARALLELISM_SNAPSHOT_CHUNK_SIZE_MB;
import static org.apache.flink.util.Preconditions.checkArgument;

/** A factory to construct {@link MongoDBSourceConfig}. */
@Internal
public class MongoDBSourceConfigFactory implements Factory {

    private static final long serialVersionUID = 1L;

    private String hosts;
    private String username;
    private String password;
    private List<String> databaseList;
    private List<String> collectionList;
    private String connectionOptions;
    private Integer batchSize = BATCH_SIZE.defaultValue();
    private Integer pollAwaitTimeMillis = POLL_AWAIT_TIME_MILLIS.defaultValue();
    private Integer pollMaxBatchSize = POLL_MAX_BATCH_SIZE.defaultValue();
    private Boolean copyExisting = COPY_EXISTING.defaultValue();
    private Integer copyExistingQueueSize = COPY_EXISTING_QUEUE_SIZE.defaultValue();
    private Integer heartbeatIntervalMillis = HEARTBEAT_INTERVAL_MILLIS.defaultValue();
    private Integer chunkSizeMB = SCAN_PARALLELISM_SNAPSHOT_CHUNK_SIZE_MB.defaultValue();

    /** The comma-separated list of hostname and port pairs of mongodb servers. */
    public MongoDBSourceConfigFactory hosts(String hosts) {
        this.hosts = hosts;
        return this;
    }

    /**
     * Ampersand (i.e. &) separated MongoDB connection options eg
     * replicaSet=test&connectTimeoutMS=300000
     * https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-connection-options
     */
    public MongoDBSourceConfigFactory connectionOptions(String connectionOptions) {
        this.connectionOptions = connectionOptions;
        return this;
    }

    /** Name of the database user to be used when connecting to MongoDB. */
    public MongoDBSourceConfigFactory username(String username) {
        this.username = username;
        return this;
    }

    /** Password to be used when connecting to MongoDB. */
    public MongoDBSourceConfigFactory password(String password) {
        this.password = password;
        return this;
    }

    /** Regular expressions list that match database names to be monitored. */
    public MongoDBSourceConfigFactory databaseList(String... databases) {
        this.databaseList = Arrays.asList(databases);
        return this;
    }

    /**
     * Regular expressions that match fully-qualified collection identifiers for collections to be
     * monitored. Each identifier is of the form {@code <databaseName>.<collectionName>}.
     */
    public MongoDBSourceConfigFactory collectionList(String... collections) {
        this.collectionList = Arrays.asList(collections);
        return this;
    }

    /**
     * batch.size
     *
     * <p>The cursor batch size. Default: 1024
     */
    public MongoDBSourceConfigFactory batchSize(int batchSize) {
        checkArgument(batchSize >= 0);
        this.batchSize = batchSize;
        return this;
    }

    /**
     * poll.await.time.ms
     *
     * <p>The amount of time to wait before checking for new results on the change stream. Default:
     * 1000
     */
    public MongoDBSourceConfigFactory pollAwaitTimeMillis(int pollAwaitTimeMillis) {
        checkArgument(pollAwaitTimeMillis > 0);
        this.pollAwaitTimeMillis = pollAwaitTimeMillis;
        return this;
    }

    /**
     * poll.max.batch.size
     *
     * <p>Maximum number of change stream documents to include in a single batch when polling for
     * new data. This setting can be used to limit the amount of data buffered internally in the
     * connector. Default: 1024
     */
    public MongoDBSourceConfigFactory pollMaxBatchSize(int pollMaxBatchSize) {
        checkArgument(pollMaxBatchSize > 0);
        this.pollMaxBatchSize = pollMaxBatchSize;
        return this;
    }

    /**
     * copy.existing
     *
     * <p>Copy existing data from source collections and convert them to Change Stream events on
     * their respective topics. Any changes to the data that occur during the copy process are
     * applied once the copy is completed.
     */
    public MongoDBSourceConfigFactory copyExisting(boolean copyExisting) {
        this.copyExisting = copyExisting;
        return this;
    }

    /**
     * copy.existing.queue.size
     *
     * <p>The max size of the queue to use when copying data. Default: 10240
     */
    public MongoDBSourceConfigFactory copyExistingQueueSize(int copyExistingQueueSize) {
        checkArgument(copyExistingQueueSize > 0);
        this.copyExistingQueueSize = copyExistingQueueSize;
        return this;
    }

    /**
     * heartbeat.interval.ms
     *
     * <p>The length of time in milliseconds between sending heartbeat messages. Heartbeat messages
     * contain the post batch resume token and are sent when no source records have been published
     * in the specified interval. This improves the resumability of the connector for low volume
     * namespaces. Use 0 to disable.
     */
    public MongoDBSourceConfigFactory heartbeatIntervalMillis(int heartbeatIntervalMillis) {
        checkArgument(heartbeatIntervalMillis >= 0);
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        return this;
    }

    /**
     * scan.parallelism.snapshot.chunk.size.mb
     *
     * <p>The chunk size mb of parallel snapshot. Default: 64mb.
     */
    public MongoDBSourceConfigFactory chunkSizeMB(int chunkSizeMB) {
        checkArgument(chunkSizeMB > 0);
        this.chunkSizeMB = chunkSizeMB;
        return this;
    }

    /** Creates a new {@link MongoDBSourceConfig} for the given subtask {@code subtaskId}. */
    @Override
    public MongoDBSourceConfig create(int subtaskId) {
        return new MongoDBSourceConfig(
                hosts,
                username,
                password,
                databaseList,
                collectionList,
                connectionOptions,
                batchSize,
                pollAwaitTimeMillis,
                pollMaxBatchSize,
                copyExisting,
                copyExistingQueueSize,
                heartbeatIntervalMillis,
                chunkSizeMB);
    }
}
