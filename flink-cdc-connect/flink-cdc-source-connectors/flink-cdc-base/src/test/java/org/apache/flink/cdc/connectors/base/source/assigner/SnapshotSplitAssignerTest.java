/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.base.source.assigner;

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceEnumeratorMetrics;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link SnapshotSplitAssigner}. */
class SnapshotSplitAssignerTest {

    /**
     * FLINK-41010: re-adding a failed snapshot split must roll the assigner status back, otherwise
     * the stream split is created while the re-added split still has no finished offset.
     */
    @Test
    void testAddSplitsRollsBackInitialAssigningFinished() {
        assertAddSplitsRollsBack(
                AssignerStatus.INITIAL_ASSIGNING_FINISHED, AssignerStatus.INITIAL_ASSIGNING);
    }

    @Test
    void testAddSplitsRollsBackNewlyAddedSnapshotFinished() {
        assertAddSplitsRollsBack(
                AssignerStatus.NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED,
                AssignerStatus.NEWLY_ADDED_ASSIGNING);
    }

    private static void assertAddSplitsRollsBack(
            AssignerStatus finishedStatus, AssignerStatus expectedStatus) {
        TableId tableId = new TableId("catalog", "public", "t1");
        SnapshotSplit snapshotSplit =
                new SnapshotSplit(
                        tableId,
                        0,
                        RowType.of(new IntType()),
                        null,
                        null,
                        null,
                        Collections.<TableId, TableChanges.TableChange>emptyMap());

        Map<String, SchemalessSnapshotSplit> assignedSplits = new LinkedHashMap<>();
        assignedSplits.put(snapshotSplit.splitId(), snapshotSplit.toSchemalessSnapshotSplit());
        Map<String, Offset> splitFinishedOffsets = new HashMap<>();
        splitFinishedOffsets.put(snapshotSplit.splitId(), mock(Offset.class));

        SnapshotPendingSplitsState checkpoint =
                new SnapshotPendingSplitsState(
                        new ArrayList<TableId>(),
                        new ArrayList<SchemalessSnapshotSplit>(),
                        assignedSplits,
                        new HashMap<TableId, TableChanges.TableChange>(),
                        splitFinishedOffsets,
                        finishedStatus,
                        new ArrayList<TableId>(),
                        false,
                        true,
                        new HashMap<String, Long>(),
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE);

        SourceConfig sourceConfig = mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect = mock(DataSourceDialect.class);
        when(dialect.createChunkSplitter(any(), any())).thenReturn(mock(ChunkSplitter.class));

        SnapshotSplitAssigner<SourceConfig> assigner =
                new SnapshotSplitAssigner<>(
                        sourceConfig, 1, checkpoint, dialect, mock(OffsetFactory.class));
        assigner.initEnumeratorMetrics(
                new SourceEnumeratorMetrics(
                        mock(SplitEnumeratorMetricGroup.class, RETURNS_DEEP_STUBS)));
        assertThat(assigner.getAssignerStatus()).isEqualTo(finishedStatus);

        List<SourceSplitBase> reAddedSplits = new ArrayList<>();
        reAddedSplits.add(snapshotSplit);
        assigner.addSplits(reAddedSplits);

        assertThat(assigner.getAssignerStatus()).isEqualTo(expectedStatus);
        assertThat(assigner.getNext()).contains(snapshotSplit);
    }
}
