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

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import java.util.Arrays;
import java.util.stream.Stream;

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

    /** The protocol connected to MongoDB. For example mongodb or mongodb+srv. */
    public MongoDBSourceBuilder<T> scheme(String scheme) {
        this.configFactory.scheme(scheme);
        return this;
    }

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
        this.configFactory.databaseList(
                Arrays.stream(databases)
                        .flatMap(database -> Stream.of(database.split(",")))
                        .toArray(String[]::new));
        return this;
    }

    /**
     * Regular expressions that match fully-qualified collection identifiers for collections to be
     * monitored. Each identifier is of the form {@code <databaseName>.<collectionName>}.
     */
    public MongoDBSourceBuilder<T> collectionList(String... collections) {
        this.configFactory.collectionList(
                Arrays.stream(collections)
                        .flatMap(collection -> Stream.of(collection.split(",")))
                        .toArray(String[]::new));
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
     * scan.startup.mode
     *
     * <p>Optional startup mode for MongoDB CDC consumer, valid enumerations are initial,
     * latest-offset, timestamp. Default: initial
     */
    public MongoDBSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
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
     * scan.incremental.snapshot.chunk.samples
     *
     * <p>The number of samples to take per chunk. Defaults to 20.
     */
    public MongoDBSourceBuilder<T> samplesPerChunk(int samplesPerChunk) {
        this.configFactory.samplesPerChunk(samplesPerChunk);
        return this;
    }

    /**
     * scan.incremental.close-idle-reader.enabled
     *
     * <p>Whether to close idle readers at the end of the snapshot phase. This feature depends on
     * FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be
     * greater than or equal to 1.14, and the configuration <code>
     * 'execution.checkpointing.checkpoints-after-tasks-finish.enabled'</code> needs to be set to
     * true.
     *
     * <p>See more
     * https://cwiki.apache.org/confluence/display/FLINK/FLIP-147%3A+Support+Checkpoints+After+Tasks+Finished.
     */
    public MongoDBSourceBuilder<T> closeIdleReaders(boolean closeIdleReaders) {
        this.configFactory.closeIdleReaders(closeIdleReaders);
        return this;
    }

    /**
     * scan.full-changelog
     *
     * <p>Whether to generate full mode row data by looking up full document pre- and post-image
     * collections. Requires MongoDB >= 6.0.
     */
    public MongoDBSourceBuilder<T> scanFullChangelog(boolean enableFullDocPrePostImage) {
        this.configFactory.scanFullChangelog(enableFullDocPrePostImage);
        return this;
    }

    /**
     * Whether disable cursor timeout during snapshot phase. Defaults to true. Only enable this when
     * MongoDB server doesn't support noCursorTimeout option.
     */
    public MongoDBSourceBuilder<T> disableCursorTimeout(boolean disableCursorTimeout) {
        this.configFactory.disableCursorTimeout(disableCursorTimeout);
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
     * Whether to skip backfill in snapshot reading phase.
     *
     * <p>If backfill is skipped, changes on captured tables during snapshot phase will be consumed
     * later in binlog reading phase instead of being merged into the snapshot.
     *
     * <p>WARNING: Skipping backfill might lead to data inconsistency because some binlog events
     * happened within the snapshot phase might be replayed (only at-least-once semantic is
     * promised). For example updating an already updated value in snapshot, or deleting an already
     * deleted entry in snapshot. These replayed binlog events should be handled specially.
     */
    public MongoDBSourceBuilder<T> skipSnapshotBackfill(boolean skipSnapshotBackfill) {
        this.configFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        return this;
    }

    /**
     * Whether the {@link IncrementalSourceEnumerator} should scan the newly added tables or not.
     */
    public MongoDBSourceBuilder<T> scanNewlyAddedTableEnabled(boolean scanNewlyAddedTableEnabled) {
        this.configFactory.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);
        return this;
    }

    /**
     * Whether the {@link MongoDBSource} should assign the unbounded chunks first or not during
     * snapshot reading phase.
     */
    public MongoDBSourceBuilder<T> assignUnboundedChunkFirst(boolean assignUnboundedChunkFirst) {
        this.configFactory.assignUnboundedChunkFirst(assignUnboundedChunkFirst);
        return this;
    }

    /**
     * Build the {@link MongoDBSource}.
     *
     * @return a MongoDBParallelSource with the settings made for this builder.
     */
    public MongoDBSource<T> build() {
        return new MongoDBSource<>(configFactory, checkNotNull(deserializer));
    }
}
