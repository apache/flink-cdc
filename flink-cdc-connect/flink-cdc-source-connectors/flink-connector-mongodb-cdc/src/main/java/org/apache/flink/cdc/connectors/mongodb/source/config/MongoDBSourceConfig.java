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

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.buildConnectionString;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A MongoDB Source configuration which is used by {@link MongoDBSource}. */
public class MongoDBSourceConfig implements SourceConfig {

    private static final long serialVersionUID = 1L;

    private final String scheme;
    private final String hosts;
    @Nullable private final String username;
    @Nullable private final String password;
    @Nullable private final List<String> databaseList;
    @Nullable private final List<String> collectionList;
    private final String connectionString;
    private final int batchSize;
    private final int pollAwaitTimeMillis;
    private final int pollMaxBatchSize;
    private final boolean updateLookup;
    private final StartupOptions startupOptions;
    private final int heartbeatIntervalMillis;
    private final int splitMetaGroupSize;
    private final int splitSizeMB;
    private final int samplesPerChunk;
    private final boolean closeIdleReaders;
    private final boolean enableFullDocPrePostImage;
    private final boolean disableCursorTimeout;
    private final boolean skipSnapshotBackfill;
    private final boolean isScanNewlyAddedTableEnabled;
    private final boolean assignUnboundedChunkFirst;

    MongoDBSourceConfig(
            String scheme,
            String hosts,
            @Nullable String username,
            @Nullable String password,
            @Nullable List<String> databaseList,
            @Nullable List<String> collectionList,
            @Nullable String connectionOptions,
            int batchSize,
            int pollAwaitTimeMillis,
            int pollMaxBatchSize,
            boolean updateLookup,
            StartupOptions startupOptions,
            int heartbeatIntervalMillis,
            int splitMetaGroupSize,
            int splitSizeMB,
            int samplesPerChunk,
            boolean closeIdleReaders,
            boolean enableFullDocPrePostImage,
            boolean disableCursorTimeout,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            boolean assignUnboundedChunkFirst) {
        this.scheme = checkNotNull(scheme);
        this.hosts = checkNotNull(hosts);
        this.username = username;
        this.password = password;
        this.databaseList = databaseList;
        this.collectionList = collectionList;
        this.connectionString =
                buildConnectionString(username, password, scheme, hosts, connectionOptions);
        this.batchSize = batchSize;
        this.pollAwaitTimeMillis = pollAwaitTimeMillis;
        this.pollMaxBatchSize = pollMaxBatchSize;
        this.updateLookup = updateLookup;
        this.startupOptions = startupOptions;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.splitSizeMB = splitSizeMB;
        this.samplesPerChunk = samplesPerChunk;
        this.closeIdleReaders = closeIdleReaders;
        this.enableFullDocPrePostImage = enableFullDocPrePostImage;
        this.disableCursorTimeout = disableCursorTimeout;
        this.skipSnapshotBackfill = skipSnapshotBackfill;
        this.isScanNewlyAddedTableEnabled = isScanNewlyAddedTableEnabled;
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
    }

    public String getScheme() {
        return scheme;
    }

    public String getHosts() {
        return hosts;
    }

    public String getConnectionString() {
        return connectionString;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    public String getPassword() {
        return password;
    }

    @Nullable
    public List<String> getDatabaseList() {
        return databaseList;
    }

    @Nullable
    public List<String> getCollectionList() {
        return collectionList;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getPollAwaitTimeMillis() {
        return pollAwaitTimeMillis;
    }

    public int getPollMaxBatchSize() {
        return pollMaxBatchSize;
    }

    public int getHeartbeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    public boolean isUpdateLookup() {
        return updateLookup;
    }

    @Override
    public StartupOptions getStartupOptions() {
        return startupOptions;
    }

    @Override
    public int getSplitSize() {
        return splitSizeMB;
    }

    @Override
    public int getSplitMetaGroupSize() {
        return splitMetaGroupSize;
    }

    public int getSamplesPerChunk() {
        return samplesPerChunk;
    }

    @Override
    public boolean isIncludeSchemaChanges() {
        return false;
    }

    @Override
    public boolean isCloseIdleReaders() {
        return closeIdleReaders;
    }

    public boolean isFullDocPrePostImageEnabled() {
        return enableFullDocPrePostImage;
    }

    public boolean disableCursorTimeout() {
        return disableCursorTimeout;
    }

    @Override
    public boolean isSkipSnapshotBackfill() {
        return skipSnapshotBackfill;
    }

    public boolean isScanNewlyAddedTableEnabled() {
        return isScanNewlyAddedTableEnabled;
    }

    public boolean isAssignUnboundedChunkFirst() {
        return assignUnboundedChunkFirst;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoDBSourceConfig that = (MongoDBSourceConfig) o;
        return batchSize == that.batchSize
                && pollAwaitTimeMillis == that.pollAwaitTimeMillis
                && pollMaxBatchSize == that.pollMaxBatchSize
                && updateLookup == that.updateLookup
                && startupOptions == that.startupOptions
                && heartbeatIntervalMillis == that.heartbeatIntervalMillis
                && splitMetaGroupSize == that.splitMetaGroupSize
                && splitSizeMB == that.splitSizeMB
                && samplesPerChunk == that.samplesPerChunk
                && closeIdleReaders == that.closeIdleReaders
                && Objects.equals(scheme, that.scheme)
                && Objects.equals(hosts, that.hosts)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(databaseList, that.databaseList)
                && Objects.equals(collectionList, that.collectionList)
                && Objects.equals(connectionString, that.connectionString)
                && Objects.equals(skipSnapshotBackfill, that.skipSnapshotBackfill)
                && Objects.equals(isScanNewlyAddedTableEnabled, that.isScanNewlyAddedTableEnabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                scheme,
                hosts,
                username,
                password,
                databaseList,
                collectionList,
                connectionString,
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
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled);
    }
}
