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

package org.apache.flink.cdc.connectors.base.source.assigner;

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReader;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests that the meta groups of finished snapshot splits stay consistent across a checkpoint
 * restore (FLINK-40216).
 *
 * <p>The meta groups served by {@code IncrementalSourceEnumerator#sendStreamMetaRequestEvent} are
 * partitioned from {@link SnapshotSplitAssigner#getFinishedSplitInfos()}, so the assignment order
 * of the splits must be retained across job restarts; otherwise a reader that synchronized only
 * part of the meta groups before a restart receives duplicated split infos while never receiving
 * others.
 */
class MetaGroupOrderingTest {

    private static final TableId TABLE_ID = new TableId("db", null, "t");
    private static final RowType SPLIT_KEY_TYPE =
            new RowType(Collections.singletonList(new RowType.RowField("id", new BigIntType())));

    @Test
    void testFinishedSplitInfosPreserveAssignmentOrderAfterRestore() {
        List<String> assignmentOrder = new ArrayList<>();
        SnapshotSplitAssigner<SourceConfig> restoredAssigner = restoreAssigner(assignmentOrder, 12);

        List<String> restoredOrder =
                restoredAssigner.getFinishedSplitInfos().stream()
                        .map(FinishedSnapshotSplitInfo::getSplitId)
                        .collect(Collectors.toList());

        assertThat(restoredOrder).isEqualTo(assignmentOrder);
    }

    @Test
    void testReaderResumingAfterRestoreReceivesEachSplitExactlyOnce() {
        int metaGroupSize = 2;
        List<String> assignmentOrder = new ArrayList<>();
        SnapshotSplitAssigner<SourceConfig> restoredAssigner = restoreAssigner(assignmentOrder, 12);

        // Before the restart the enumerator serves groups partitioned from the assignment order;
        // the reader synchronized groups 0..2 into its stream split state:
        // [t:0, t:1], [t:2, t:3], [t:4, t:5]
        List<List<String>> groupsBeforeRestart = Lists.partition(assignmentOrder, metaGroupSize);
        List<String> readerState = new ArrayList<>();
        for (int groupId = 0; groupId < 3; groupId++) {
            readerState.addAll(groupsBeforeRestart.get(groupId));
        }

        // The job restarts: the enumerator rebuilds its meta groups from the restored assigner
        List<List<FinishedSnapshotSplitInfo>> groupsAfterRestart =
                Lists.partition(restoredAssigner.getFinishedSplitInfos(), metaGroupSize);

        // The reader resumes requesting group ids based on how many infos it already holds
        int nextGroupId =
                IncrementalSourceReader.getNextMetaGroupId(readerState.size(), metaGroupSize);
        while (readerState.size() < assignmentOrder.size()
                && nextGroupId < groupsAfterRestart.size()) {
            for (FinishedSnapshotSplitInfo info : groupsAfterRestart.get(nextGroupId)) {
                readerState.add(info.getSplitId());
            }
            nextGroupId =
                    IncrementalSourceReader.getNextMetaGroupId(readerState.size(), metaGroupSize);
        }

        assertThat(readerState).containsExactlyElementsOf(assignmentOrder);
    }

    private static SnapshotSplitAssigner<SourceConfig> restoreAssigner(
            List<String> assignmentOrder, int splitCount) {
        LinkedHashMap<String, SchemalessSnapshotSplit> assignedSplits = new LinkedHashMap<>();
        Map<String, Offset> splitFinishedOffsets = new HashMap<>();
        // 10+ splits so that the lexicographic order of split ids (t:0, t:1, t:10, t:11, t:2,
        // ...) differs from the assignment order (t:0, t:1, t:2, ..., t:11)
        for (int i = 0; i < splitCount; i++) {
            String splitId = TABLE_ID + ":" + i;
            assignmentOrder.add(splitId);
            assignedSplits.put(
                    splitId,
                    new SchemalessSnapshotSplit(
                            TABLE_ID,
                            splitId,
                            SPLIT_KEY_TYPE,
                            new Object[] {i},
                            new Object[] {i + 1},
                            null));
            splitFinishedOffsets.put(splitId, null);
        }

        return new SnapshotSplitAssigner<>(
                mock(SourceConfig.class),
                4,
                new SnapshotPendingSplitsState(
                        Collections.singletonList(TABLE_ID),
                        Collections.emptyList(),
                        assignedSplits,
                        Collections.emptyMap(),
                        splitFinishedOffsets,
                        AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                        Collections.emptyList(),
                        false,
                        true,
                        Collections.emptyMap(),
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE),
                mock(DataSourceDialect.class),
                new MockOffsetFactory());
    }

    private static class MockOffsetFactory extends OffsetFactory {
        @Override
        public Offset newOffset(Map<String, String> offset) {
            return null;
        }

        @Override
        public Offset newOffset(String filename, Long position) {
            return null;
        }

        @Override
        public Offset newOffset(Long position) {
            return null;
        }

        @Override
        public Offset createTimestampOffset(long timestampMillis) {
            return null;
        }

        @Override
        public Offset createInitialOffset() {
            return null;
        }

        @Override
        public Offset createNoStoppingOffset() {
            return null;
        }
    }
}
