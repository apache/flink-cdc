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

import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Mocked {@link ChunkSplitter}. */
public class MockedChunkSplitter implements ChunkSplitter {

    private final MockedConfig sourceConfig;
    private final MockedDatabase mockedDatabase;

    public MockedChunkSplitter(MockedConfig sourceConfig, MockedDatabase mockedDatabase) {
        this.sourceConfig = sourceConfig;
        this.mockedDatabase = mockedDatabase;
    }

    @Override
    public Collection<SnapshotSplit> generateSplits(TableId tableId) {
        long recordCount = sourceConfig.getRecordCount();
        int chunkSize = sourceConfig.getSplitSize();

        if (recordCount < chunkSize) {
            return Collections.singletonList(
                    new SnapshotSplit(
                            tableId,
                            0,
                            RowType.of(DataTypes.BIGINT().getLogicalType()),
                            new Object[] {Long.MIN_VALUE},
                            new Object[] {Long.MAX_VALUE},
                            null,
                            mockedDatabase.retrieveSchemas(Collections.singletonList(tableId))));
        }

        int splitCount = (int) Math.ceil((double) recordCount / chunkSize);
        List<SnapshotSplit> splits = new ArrayList<>(splitCount);

        for (int i = 0; i < splitCount; i++) {
            long currentMin = i == 0 ? Long.MIN_VALUE : (long) chunkSize * i;
            long currentMax = i == splitCount - 1 ? Long.MAX_VALUE : (long) chunkSize * (i + 1);
            splits.add(
                    new SnapshotSplit(
                            tableId,
                            0,
                            RowType.of(DataTypes.BIGINT().getLogicalType()),
                            new Object[] {currentMin},
                            new Object[] {currentMax},
                            null,
                            mockedDatabase.retrieveSchemas(Collections.singletonList(tableId))));
        }

        return splits;
    }

    @Override
    public void open() {}

    @Override
    public void close() throws Exception {}

    // Mocked chunk splitter doesn't support asynchronously split.
    @Override
    public boolean hasNextChunk() {
        return false;
    }

    @Override
    public ChunkSplitterState snapshotState(long checkpointId) {
        return ChunkSplitterState.NO_SPLITTING_TABLE_STATE;
    }

    @Override
    public TableId getCurrentSplittingTableId() {
        return null;
    }
}
