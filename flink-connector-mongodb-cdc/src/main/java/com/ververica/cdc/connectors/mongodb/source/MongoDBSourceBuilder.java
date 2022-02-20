/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class for {@link MongoDBSource} to make it easier for the users to construct a {@link
 * MongoDBSource}.
 *
 * <pre>{@code
 * MongoDBSource
 *     .<String>builder()
 *     .hosts("localhost:27017")
 *     .databaseList("mydb")
 *     .collectionList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>Check the Java docs of each individual method to learn more about the settings to build a
 * {@link MongoDBSource}.
 */
@Experimental
@PublicEvolving
public class MongoDBSourceBuilder<T> {

    private final MongoDBSourceConfigFactory configFactory = new MongoDBSourceConfigFactory();
    private DebeziumDeserializationSchema<T> deserializer;

    /** The comma-separated list of hostname and port pairs of mongodb servers. */
    public MongoDBSourceBuilder<T> hosts(String hosts) {
        this.configFactory.hosts(hosts);
        return this;
    }

    /**
     * Ampersand (i.e. &) separated MongoDB connection options eg
     * replicaSet=test&connectTimeoutMS=300000
     * https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-connection-options
     */
    public MongoDBSourceBuilder<T> connectionOptions(String connectionOptions) {
        this.configFactory.connectionOptions(connectionOptions);
        return this;
    }

    /** Name of the database user to be used when connecting to MongoDB. */
    public MongoDBSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    /** Password to be used when connecting to MongoDB. */
    public MongoDBSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    /** Regular expressions list that match database names to be monitored. */
    public MongoDBSourceBuilder<T> databaseList(String... databases) {
        this.configFactory.databaseList(databases);
        return this;
    }

    /**
     * Regular expressions that match fully-qualified collection identifiers for collections to be
     * monitored. Each identifier is of the form {@code <databaseName>.<collectionName>}.
     */
    public MongoDBSourceBuilder<T> collectionList(String... collections) {
        this.configFactory.collectionList(collections);
        return this;
    }

    /**
     * batch.size
     *
     * <p>The cursor batch size. Default: 1024
     */
    public MongoDBSourceBuilder<T> batchSize(int batchSize) {
        this.configFactory.batchSize(batchSize);
        return this;
    }

    /**
     * poll.await.time.ms
     *
     * <p>The amount of time to wait before checking for new results on the change stream. Default:
     * 1000
     */
    public MongoDBSourceBuilder<T> pollAwaitTimeMillis(int pollAwaitTimeMillis) {
        checkArgument(pollAwaitTimeMillis > 0);
        this.configFactory.pollAwaitTimeMillis(pollAwaitTimeMillis);
        return this;
    }

    /**
     * poll.max.batch.size
     *
     * <p>Maximum number of change stream documents to include in a single batch when polling for
     * new data. This setting can be used to limit the amount of data buffered internally in the
     * connector. Default: 1024
     */
    public MongoDBSourceBuilder<T> pollMaxBatchSize(int pollMaxBatchSize) {
        this.configFactory.pollMaxBatchSize(pollMaxBatchSize);
        return this;
    }

    /**
     * copy.existing
     *
     * <p>Copy existing data from source collections and convert them to Change Stream events on
     * their respective topics. Any changes to the data that occur during the copy process are
     * applied once the copy is completed.
     */
    public MongoDBSourceBuilder<T> copyExisting(boolean copyExisting) {
        if (copyExisting) {
            this.configFactory.startupOptions(StartupOptions.initial());
        } else {
            this.configFactory.startupOptions(StartupOptions.latest());
        }
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
    public MongoDBSourceBuilder<T> heartbeatIntervalMillis(int heartbeatIntervalMillis) {
        this.configFactory.heartbeatIntervalMillis(heartbeatIntervalMillis);
        return this;
    }

    /**
     * scan.incremental.snapshot.chunk.size.mb
     *
     * <p>The chunk size mb of incremental snapshot. Default: 64mb.
     */
    public MongoDBSourceBuilder<T> splitSizeMB(int splitSizeMB) {
        this.configFactory.splitSizeMB(splitSizeMB);
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public MongoDBSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    /**
     * The deserializer used to convert from consumed {@link
     * org.apache.kafka.connect.source.SourceRecord}.
     */
    public MongoDBSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /**
     * Build the {@link MongoDBSource}.
     *
     * @return a MongoDBParallelSource with the settings made for this builder.
     */
    public MongoDBSource<T> build() {
        configFactory.validate();
        return new MongoDBSource<>(configFactory, checkNotNull(deserializer));
    }
}
