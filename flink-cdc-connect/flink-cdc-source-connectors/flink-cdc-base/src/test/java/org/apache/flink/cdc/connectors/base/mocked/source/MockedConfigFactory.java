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

import org.apache.flink.cdc.connectors.base.config.SourceConfig.Factory;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import java.time.Duration;

/** {@link Factory} class for {@link MockedConfig}. */
public class MockedConfigFactory implements Factory<MockedConfig> {

    private long tableCount;
    private long recordCount;
    private Duration refreshInterval;
    private StartupOptions startupOptions;
    private int splitSize;
    private int splitMetaGroupSize;
    private boolean closeIdleReaders;
    private boolean skipSnapshotBackfill;
    private boolean scanNewlyAddedTableEnabled;
    private boolean assignUnboundedChunkFirst;
    private boolean multipleStreamSplitsEnabled;

    public MockedConfigFactory setTableCount(long tableCount) {
        this.tableCount = tableCount;
        return this;
    }

    public MockedConfigFactory setRecordCount(long recordCount) {
        this.recordCount = recordCount;
        return this;
    }

    public MockedConfigFactory setRefreshInterval(Duration refreshInterval) {
        this.refreshInterval = refreshInterval;
        return this;
    }

    public MockedConfigFactory setStartupOptions(StartupOptions startupOptions) {
        this.startupOptions = startupOptions;
        return this;
    }

    public MockedConfigFactory setSplitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    public MockedConfigFactory setSplitMetaGroupSize(int splitMetaGroupSize) {
        this.splitMetaGroupSize = splitMetaGroupSize;
        return this;
    }

    public MockedConfigFactory setCloseIdleReaders(boolean closeIdleReaders) {
        this.closeIdleReaders = closeIdleReaders;
        return this;
    }

    public MockedConfigFactory setSkipSnapshotBackfill(boolean skipSnapshotBackfill) {
        this.skipSnapshotBackfill = skipSnapshotBackfill;
        return this;
    }

    public MockedConfigFactory setScanNewlyAddedTableEnabled(boolean scanNewlyAddedTableEnabled) {
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
        return this;
    }

    public MockedConfigFactory setAssignUnboundedChunkFirst(boolean assignUnboundedChunkFirst) {
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
        return this;
    }

    public MockedConfigFactory setMultipleStreamSplitsEnabled(boolean multipleStreamSplitsEnabled) {
        this.multipleStreamSplitsEnabled = multipleStreamSplitsEnabled;
        return this;
    }

    @Override
    public MockedConfig create(int subtask) {
        return new MockedConfig(
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
}
