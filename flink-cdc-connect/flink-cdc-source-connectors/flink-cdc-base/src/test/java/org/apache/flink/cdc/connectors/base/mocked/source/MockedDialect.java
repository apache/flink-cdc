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
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.List;
import java.util.Map;

/** {@link DataSourceDialect} for mocked source. */
public class MockedDialect implements DataSourceDialect<MockedConfig> {

    private final MockedDatabase mockedDatabase;

    public MockedDialect(SourceConfig.Factory<MockedConfig> configFactory) {
        MockedConfig config = configFactory.create(-1);
        this.mockedDatabase =
                new MockedDatabase(
                        config.getTableCount(),
                        config.getRecordCount(),
                        config.getRefreshInterval());
    }

    @Override
    public String getName() {
        return "mocked";
    }

    public MockedDatabase getMockedDatabase() {
        return mockedDatabase;
    }

    @Override
    public List<TableId> discoverDataCollections(MockedConfig sourceConfig) {
        return mockedDatabase.getTableIds();
    }

    @Override
    public Map<TableId, TableChanges.TableChange> discoverDataCollectionSchemas(
            MockedConfig sourceConfig) {
        return mockedDatabase.getTableIdsAndSchema();
    }

    @Override
    public Offset displayCurrentOffset(MockedConfig sourceConfig) {
        return new MockedOffset(mockedDatabase.getCurrentOffset());
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(MockedConfig sourceConfig) {
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(MockedConfig sourceConfig) {
        return new MockedChunkSplitter(sourceConfig, mockedDatabase);
    }

    @Override
    public ChunkSplitter createChunkSplitter(
            MockedConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
        return createChunkSplitter(sourceConfig);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new MockedSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new MockedStreamFetchTask(sourceSplitBase.asStreamSplit());
        }
    }

    @Override
    public FetchTask.Context createFetchTaskContext(MockedConfig sourceConfig) {
        return new MockedFetchTaskContext(this, sourceConfig);
    }

    @Override
    public int getNumberOfStreamSplits(MockedConfig sourceConfig) {
        return sourceConfig.isMultipleStreamSplitsEnabled() ? 4 : 1;
    }

    @Override
    public boolean isIncludeDataCollection(MockedConfig sourceConfig, TableId tableId) {
        return mockedDatabase.getTableIds().contains(tableId);
    }
}
