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

package org.apache.flink.cdc.connectors.mongodb.source.config;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.config.SourceConfig.Factory;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.MONGODB_SCHEME;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.MONGODB_SRV_SCHEME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A factory to construct {@link MongoDBSourceConfig}. */
@Internal
public class MongoDBSourceConfigFactory implements Factory<MongoDBSourceConfig> {

    private static final long serialVersionUID = 1L;

    private String scheme = MongoDBSourceOptions.SCHEME.defaultValue();
    private String hosts;
    private String username;
    private String password;
    private List<String> databaseList;
    private List<String> collectionList;
    private String connectionOptions;
    private Integer batchSize = MongoDBSourceOptions.BATCH_SIZE.defaultValue();
    private Integer pollAwaitTimeMillis =
            MongoDBSourceOptions.POLL_AWAIT_TIME_MILLIS.defaultValue();
    private Integer pollMaxBatchSize = MongoDBSourceOptions.POLL_MAX_BATCH_SIZE.defaultValue();
    private boolean updateLookup = true;
    private StartupOptions startupOptions = StartupOptions.initial();
    private Integer heartbeatIntervalMillis =
            MongoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS.defaultValue();
    private Integer splitMetaGroupSize = CHUNK_META_GROUP_SIZE.defaultValue();
    private Integer splitSizeMB =
            MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB.defaultValue();
    private Integer samplesPerChunk =
            MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES.defaultValue();
    private boolean closeIdleReaders = false;
    private boolean enableFullDocPrePostImage = false;
    private boolean disableCursorTimeout = true;
    protected boolean skipSnapshotBackfill = false;
    protected boolean scanNewlyAddedTableEnabled = false;
    protected boolean assignUnboundedChunkFirst = false;

    /** The protocol connected to MongoDB. For example mongodb or mongodb+srv. */
    public MongoDBSourceConfigFactory scheme(String scheme) {
        checkArgument(
                MONGODB_SCHEME.equals(scheme) || MONGODB_SRV_SCHEME.equals(scheme),
                String.format(
                        "The scheme should either be %s or %s",
                        MONGODB_SCHEME, MONGODB_SRV_SCHEME));
        this.scheme = scheme;
        return this;
    }

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
     * scan.startup.mode
     *
     * <p>Optional startup mode for MongoDB CDC consumer, valid enumerations are initial,
     * latest-offset, timestamp. Default: initial
     */
    public MongoDBSourceConfigFactory startupOptions(StartupOptions startupOptions) {
        checkNotNull(startupOptions);
        switch (startupOptions.startupMode) {
            case INITIAL:
            case SNAPSHOT:
            case LATEST_OFFSET:
            case TIMESTAMP:
                this.startupOptions = startupOptions;
                return this;
            default:
                throw new IllegalArgumentException(
                        "Unsupported startup mode " + startupOptions.startupMode);
        }
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
     * scan.incremental.snapshot.chunk.size.mb
     *
     * <p>The chunk size mb of incremental snapshot. Default: 64mb.
     */
    public MongoDBSourceConfigFactory splitSizeMB(int splitSizeMB) {
        checkArgument(splitSizeMB > 0);
        this.splitSizeMB = splitSizeMB;
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public MongoDBSourceConfigFactory splitMetaGroupSize(int splitMetaGroupSize) {
        this.splitMetaGroupSize = splitMetaGroupSize;
        return this;
    }

    /**
     * scan.incremental.snapshot.chunk.samples
     *
     * <p>The number of samples to take per chunk. Defaults to 20.
     */
    public MongoDBSourceConfigFactory samplesPerChunk(int samplesPerChunk) {
        checkArgument(samplesPerChunk > 0);
        this.samplesPerChunk = samplesPerChunk;
        return this;
    }

    /**
     * Whether to close idle readers at the end of the snapshot phase. This feature depends on
     * FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be
     * greater than or equal to 1.14, and the configuration <code>
     * 'execution.checkpointing.checkpoints-after-tasks-finish.enabled'</code> needs to be set to
     * true.
     *
     * <p>See more
     * https://cwiki.apache.org/confluence/display/FLINK/FLIP-147%3A+Support+Checkpoints+After+Tasks+Finished.
     */
    public MongoDBSourceConfigFactory closeIdleReaders(boolean closeIdleReaders) {
        this.closeIdleReaders = closeIdleReaders;
        return this;
    }

    /**
     * scan.full-changelog
     *
     * <p>Whether to generate full mode row data by looking up full document pre- and post-image
     * collections. Requires MongoDB >= 6.0.
     */
    public MongoDBSourceConfigFactory scanFullChangelog(boolean enableFullDocPrePostImage) {
        this.enableFullDocPrePostImage = enableFullDocPrePostImage;
        return this;
    }

    /**
     * whether pass <code>noCursorTimeout</code> config when creating MongoDB cursor. Defaults to
     * true.
     */
    public MongoDBSourceConfigFactory disableCursorTimeout(boolean disableCursorTimeout) {
        this.disableCursorTimeout = disableCursorTimeout;
        return this;
    }

    /**
     * Whether to backfill the log for each snapshot split in the snapshot phase. Enabling backfill
     * log can provide exactly once semantics, otherwise only provides at-least once semantics. The
     * default action is to backfill log in snapshot phase.");
     */
    public MongoDBSourceConfigFactory skipSnapshotBackfill(boolean skipSnapshotBackfill) {
        this.skipSnapshotBackfill = skipSnapshotBackfill;
        return this;
    }

    public MongoDBSourceConfigFactory scanNewlyAddedTableEnabled(
            boolean scanNewlyAddedTableEnabled) {
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
        return this;
    }

    /**
     * Whether to assign the unbounded chunks first during snapshot reading phase. Defaults to
     * false.
     */
    public MongoDBSourceConfigFactory assignUnboundedChunkFirst(boolean assignUnboundedChunkFirst) {
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
        return this;
    }

    /** Creates a new {@link MongoDBSourceConfig} for the given subtask {@code subtaskId}. */
    @Override
    public MongoDBSourceConfig create(int subtaskId) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);
        return new MongoDBSourceConfig(
                scheme,
                hosts,
                username,
                password,
                databaseList,
                collectionList,
                connectionOptions,
                batchSize,
                pollAwaitTimeMillis,
                pollMaxBatchSize,
                updateLookup,
                startupOptions,
                heartbeatIntervalMillis,
                splitMetaGroupSize,
                splitSizeMB,
                samplesPerChunk,
                closeIdleReaders,
                enableFullDocPrePostImage,
                disableCursorTimeout,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);
    }
}
