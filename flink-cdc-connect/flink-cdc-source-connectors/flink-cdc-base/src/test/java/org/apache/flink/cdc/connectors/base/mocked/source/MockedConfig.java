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

package org.apache.flink.cdc.connectors.base.mocked.source;

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import java.time.Duration;
import java.util.Objects;

/** Mocked {@link SourceConfig}. */
public class MockedConfig implements SourceConfig {

    private final long tableCount;
    private final long recordCount;
    private final Duration refreshInterval;
    private final StartupOptions startupOptions;
    private final int splitSize;
    private final int splitMetaGroupSize;
    private final boolean closeIdleReaders;
    private final boolean skipSnapshotBackfill;
    private final boolean scanNewlyAddedTableEnabled;
    private final boolean assignUnboundedChunkFirst;
    private final boolean multipleStreamSplitsEnabled;

    MockedConfig(
            long tableCount,
            long recordCount,
            Duration refreshInterval,
            StartupOptions startupOptions,
            int splitSize,
            int splitMetaGroupSize,
            boolean closeIdleReaders,
            boolean skipSnapshotBackfill,
            boolean scanNewlyAddedTableEnabled,
            boolean assignUnboundedChunkFirst,
            boolean multipleStreamSplitsEnabled) {
        this.tableCount = tableCount;
        this.recordCount = recordCount;
        this.refreshInterval = refreshInterval;
        this.startupOptions = startupOptions;
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.closeIdleReaders = closeIdleReaders;
        this.skipSnapshotBackfill = skipSnapshotBackfill;
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
        this.multipleStreamSplitsEnabled = multipleStreamSplitsEnabled;
    }

    public long getTableCount() {
        return tableCount;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public Duration getRefreshInterval() {
        return refreshInterval;
    }

    @Override
    public StartupOptions getStartupOptions() {
        return startupOptions;
    }

    @Override
    public int getSplitSize() {
        return splitSize;
    }

    @Override
    public int getSplitMetaGroupSize() {
        return splitMetaGroupSize;
    }

    @Override
    public boolean isIncludeSchemaChanges() {
        return false;
    }

    @Override
    public boolean isCloseIdleReaders() {
        return closeIdleReaders;
    }

    @Override
    public boolean isSkipSnapshotBackfill() {
        return skipSnapshotBackfill;
    }

    @Override
    public boolean isScanNewlyAddedTableEnabled() {
        return scanNewlyAddedTableEnabled;
    }

    @Override
    public boolean isAssignUnboundedChunkFirst() {
        return assignUnboundedChunkFirst;
    }

    public boolean isMultipleStreamSplitsEnabled() {
        return multipleStreamSplitsEnabled;
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof MockedConfig)) {
            return false;
        }

        MockedConfig that = (MockedConfig) o;
        return tableCount == that.tableCount
                && recordCount == that.recordCount
                && splitSize == that.splitSize
                && splitMetaGroupSize == that.splitMetaGroupSize
                && closeIdleReaders == that.closeIdleReaders
                && skipSnapshotBackfill == that.skipSnapshotBackfill
                && scanNewlyAddedTableEnabled == that.scanNewlyAddedTableEnabled
                && assignUnboundedChunkFirst == that.assignUnboundedChunkFirst
                && Objects.equals(refreshInterval, that.refreshInterval)
                && Objects.equals(startupOptions, that.startupOptions)
                && multipleStreamSplitsEnabled == that.multipleStreamSplitsEnabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableCount,
                recordCount,
                refreshInterval,
                startupOptions,
                splitSize,
                splitMetaGroupSize,
                closeIdleReaders,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst,
                multipleStreamSplitsEnabled);
    }

    @Override
    public String toString() {
        return "MockedConfig{"
                + "tableCount="
                + tableCount
                + ", recordCount="
                + recordCount
                + ", refreshInterval="
                + refreshInterval
                + ", startupOptions="
                + startupOptions
                + ", splitSize="
                + splitSize
                + ", splitMetaGroupSize="
                + splitMetaGroupSize
                + ", closeIdleReaders="
                + closeIdleReaders
                + ", skipSnapshotBackfill="
                + skipSnapshotBackfill
                + ", scanNewlyAddedTableEnabled="
                + scanNewlyAddedTableEnabled
                + ", assignUnboundedChunkFirst="
                + assignUnboundedChunkFirst
                + ", multipleStreamSplitsEnabled="
                + multipleStreamSplitsEnabled
                + '}';
    }
}
