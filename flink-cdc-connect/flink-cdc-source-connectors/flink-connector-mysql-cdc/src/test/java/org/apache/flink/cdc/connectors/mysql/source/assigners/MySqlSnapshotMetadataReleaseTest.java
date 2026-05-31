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

package org.apache.flink.cdc.connectors.mysql.source.assigners;

import org.apache.flink.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetaAssembledEvent;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.utils.MockMySqlSplitEnumeratorEnumeratorContext;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.cdc.connectors.mysql.testutils.MetricsUtils.getMySqlSplitEnumeratorContext;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for releasing the heavyweight snapshot split metadata in {@link MySqlHybridSplitAssigner}
 * after the source has safely entered the binlog phase.
 *
 * <p>These tests restore the assigner from a finished snapshot checkpoint, so they exercise the
 * whole release protocol purely in memory without needing a running MySQL instance.
 */
class MySqlSnapshotMetadataReleaseTest {

    private static final String DB = "release_test_db";
    private static final String TABLE = "customers";
    private static final int NUM_FINISHED_SPLITS = 5;

    @Test
    void testReleaseAfterMetaAssembledAndCheckpointComplete() {
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(false);

        // enter the binlog phase
        assertThat(assigner.getNext()).isPresent().get().isInstanceOf(MySqlBinlogSplit.class);
        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assertAssignedSplitsSize(assigner, 1, NUM_FINISHED_SPLITS);

        // reader reports it has assembled the complete binlog split metadata
        assigner.onBinlogSplitMetaAssembled(assigner.getBinlogAssignmentGeneration());

        // snapshotState schedules the release but does not clear yet
        assigner.snapshotState(2L);
        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assertAssignedSplitsSize(assigner, 3, NUM_FINISHED_SPLITS);

        // the covering checkpoint completes -> release happens now
        assigner.notifyCheckpointComplete(2L);
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();

        HybridPendingSplitsState state = (HybridPendingSplitsState) assigner.snapshotState(4L);
        assertThat(state.getSnapshotPendingSplits().getAssignedSplits()).isEmpty();
        assertThat(state.getSnapshotPendingSplits().getSplitFinishedOffsets()).isEmpty();
        assertThat(state.getSnapshotPendingSplits().getTableSchemas()).isEmpty();
        // bookkeeping that must be kept so a restore does not re-discover tables
        assertThat(state.getSnapshotPendingSplits().getAlreadyProcessedTables()).isNotEmpty();
        assertThat(state.isBinlogSplitAssigned()).isTrue();

        assigner.close();
    }

    @Test
    void testNoReleaseWithoutMetaAssembledAck() {
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(false);
        assigner.getNext();

        // no onBinlogSplitMetaAssembled() call -> never release
        assigner.snapshotState(1L);
        assigner.notifyCheckpointComplete(1L);

        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assertAssignedSplitsSize(assigner, 2, NUM_FINISHED_SPLITS);
        assigner.close();
    }

    @Test
    void testNoReleaseBeforeCheckpointComplete() {
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(false);
        assigner.getNext();
        assigner.onBinlogSplitMetaAssembled(assigner.getBinlogAssignmentGeneration());

        // schedule release at checkpoint 5, but complete an earlier checkpoint only
        assigner.snapshotState(5L);
        assigner.notifyCheckpointComplete(4L);

        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assertAssignedSplitsSize(assigner, 6, NUM_FINISHED_SPLITS);
        assigner.close();
    }

    @Test
    void testNoReleaseWhenScanNewlyAddedTableEnabled() {
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(true);
        assigner.getNext();
        assigner.onBinlogSplitMetaAssembled(assigner.getBinlogAssignmentGeneration());

        assigner.snapshotState(1L);
        assigner.notifyCheckpointComplete(1L);

        // release is gated off because newly-added-table scanning may need the metadata again
        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assertAssignedSplitsSize(assigner, 2, NUM_FINISHED_SPLITS);
        assigner.close();
    }

    @Test
    void testAddBackResetsScheduledReleaseAndKeepsMetadata() {
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(false);
        MySqlSplit binlogSplit = assigner.getNext().get();
        assigner.onBinlogSplitMetaAssembled(assigner.getBinlogAssignmentGeneration());
        assigner.snapshotState(1L); // schedules release at checkpoint 1

        // the binlog split is added back (reader failover before the release checkpoint completes)
        assigner.addSplits(Collections.singletonList(binlogSplit));

        // completing checkpoint 1 must NOT release, because the schedule was reset on add-back
        assigner.notifyCheckpointComplete(1L);
        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assertAssignedSplitsSize(assigner, 2, NUM_FINISHED_SPLITS);

        // and the binlog split can be recreated with the full metadata intact
        Optional<MySqlSplit> recreated = assigner.getNext();
        assertThat(recreated).isPresent().get().isInstanceOf(MySqlBinlogSplit.class);
        assertThat(recreated.get().asBinlogSplit().getTotalFinishedSplitSize())
                .isEqualTo(NUM_FINISHED_SPLITS);
        assigner.close();
    }

    @Test
    void testStaleAssembledEventFromOldGenerationDoesNotRelease() {
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(false);
        MySqlSplit binlogSplit = assigner.getNext().get();

        // the first reader assembles under generation 0 and reports it
        long staleGeneration = assigner.getBinlogAssignmentGeneration();
        assigner.onBinlogSplitMetaAssembled(staleGeneration);
        assigner.snapshotState(1L); // schedules release at checkpoint 1

        // the reader fails: the binlog split is added back, which bumps the generation
        assigner.addSplits(Collections.singletonList(binlogSplit));
        assertThat(assigner.getBinlogAssignmentGeneration()).isGreaterThan(staleGeneration);

        // the binlog split is re-created for a fresh reader that has not assembled yet
        assertThat(assigner.getNext()).isPresent().get().isInstanceOf(MySqlBinlogSplit.class);

        // a stale assembled event from the failed attempt (old generation) arrives late. It must be
        // ignored, otherwise the metadata would be released while the new reader still needs it.
        assigner.onBinlogSplitMetaAssembled(staleGeneration);
        assigner.snapshotState(2L);
        assigner.notifyCheckpointComplete(2L);
        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assertAssignedSplitsSize(assigner, 3, NUM_FINISHED_SPLITS);

        // once the freshly-assigned reader reports under the current generation, release proceeds
        assigner.onBinlogSplitMetaAssembled(assigner.getBinlogAssignmentGeneration());
        assigner.snapshotState(4L);
        assigner.notifyCheckpointComplete(4L);
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();

        assigner.close();
    }

    @Test
    void testInlineSplitStillReleasesAfterAddBackBumpedGeneration() {
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(false);
        MySqlSplit binlogSplit = assigner.getNext().get();

        // a binlog split failover bumps the generation past the initial 0
        assigner.addSplits(Collections.singletonList(binlogSplit));
        assertThat(assigner.getBinlogAssignmentGeneration()).isGreaterThan(0L);
        assertThat(assigner.getNext()).isPresent().get().isInstanceOf(MySqlBinlogSplit.class);

        // an inline reader never requests meta groups, so it never learns the bumped generation and
        // reports COMPLETE_WITHOUT_META_GENERATION. Release must still happen, otherwise the
        // feature
        // silently stops working for small tables after any failover.
        assigner.onBinlogSplitMetaAssembled(
                BinlogSplitMetaAssembledEvent.COMPLETE_WITHOUT_META_GENERATION);
        assigner.snapshotState(2L);
        assigner.notifyCheckpointComplete(2L);
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();

        assigner.close();
    }

    @Test
    void testRestoreFromReleasedLightState() {
        MySqlHybridSplitAssigner assigner = buildReleasedLightAssigner();

        // No binlog split is re-created and no tables are re-discovered after a light restore.
        assertThat(assigner.getNext()).isEmpty();
        // The transient flag is not persisted, so it starts false after restore.
        assertThat(assigner.isSnapshotMetaReleased()).isFalse();

        // The state stays light (empty heavy maps, binlog split still assigned).
        HybridPendingSplitsState state = (HybridPendingSplitsState) assigner.snapshotState(10L);
        assertThat(state.getSnapshotPendingSplits().getAssignedSplits()).isEmpty();
        assertThat(state.getSnapshotPendingSplits().getSplitFinishedOffsets()).isEmpty();
        assertThat(state.getSnapshotPendingSplits().getAlreadyProcessedTables()).isNotEmpty();
        assertThat(state.isBinlogSplitAssigned()).isTrue();

        // The reader re-sends the assembled event on restore; release re-triggers as a no-op.
        assigner.onBinlogSplitMetaAssembled(assigner.getBinlogAssignmentGeneration());
        assigner.snapshotState(11L);
        assigner.notifyCheckpointComplete(11L);
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();

        assigner.close();
    }

    // ------------------------------------------------------------------------------------------

    private void assertAssignedSplitsSize(
            MySqlHybridSplitAssigner assigner, long checkpointId, int expectedSize) {
        HybridPendingSplitsState state =
                (HybridPendingSplitsState) assigner.snapshotState(checkpointId);
        assertThat(state.getSnapshotPendingSplits().getAssignedSplits()).hasSize(expectedSize);
        assertThat(state.getSnapshotPendingSplits().getSplitFinishedOffsets())
                .hasSize(expectedSize);
    }

    private MySqlSourceConfig buildConfig(boolean scanNewlyAddedTableEnabled) {
        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.initial())
                .databaseList(DB)
                .tableList(DB + "." + TABLE)
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("")
                .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                .serverTimeZone(ZoneId.of("UTC").toString())
                .createConfig(0);
    }

    private MySqlHybridSplitAssigner buildFinishedSnapshotAssigner(
            boolean scanNewlyAddedTableEnabled) {
        MySqlSourceConfig config = buildConfig(scanNewlyAddedTableEnabled);

        TableId tableId = new TableId(null, DB, TABLE);
        RowType splitKeyType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();

        List<TableId> alreadyProcessedTables = Lists.newArrayList(tableId);
        LinkedHashMap<String, MySqlSchemalessSnapshotSplit> assignedSplits = new LinkedHashMap<>();
        Map<String, BinlogOffset> splitFinishedOffsets = new HashMap<>();
        for (int i = 0; i < NUM_FINISHED_SPLITS; i++) {
            String splitId = DB + "." + TABLE + ":" + i;
            Object[] splitStart = i == 0 ? null : new Object[] {i * 2};
            Object[] splitEnd = new Object[] {i * 2 + 2};
            BinlogOffset highWatermark =
                    BinlogOffset.ofBinlogFilePosition("mysql-bin.00001", i + 1);
            assignedSplits.put(
                    splitId,
                    new MySqlSchemalessSnapshotSplit(
                            tableId, splitId, splitKeyType, splitStart, splitEnd, highWatermark));
            splitFinishedOffsets.put(splitId, highWatermark);
        }

        SnapshotPendingSplitsState snapshotState =
                new SnapshotPendingSplitsState(
                        alreadyProcessedTables,
                        new ArrayList<>(),
                        assignedSplits,
                        new HashMap<>(),
                        splitFinishedOffsets,
                        AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                        new ArrayList<>(),
                        false,
                        true,
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        HybridPendingSplitsState checkpoint = new HybridPendingSplitsState(snapshotState, false);
        MockMySqlSplitEnumeratorEnumeratorContext enumeratorContext =
                getMySqlSplitEnumeratorContext();
        return new MySqlHybridSplitAssigner(config, 4, checkpoint, enumeratorContext);
    }

    /**
     * Builds an assigner restored from a "light" checkpoint: the metadata was already released, so
     * the maps are empty, the binlog split is assigned, and only alreadyProcessedTables and the
     * finished status are retained.
     */
    private MySqlHybridSplitAssigner buildReleasedLightAssigner() {
        List<TableId> alreadyProcessedTables = Lists.newArrayList(new TableId(null, DB, TABLE));
        SnapshotPendingSplitsState snapshotState =
                new SnapshotPendingSplitsState(
                        alreadyProcessedTables,
                        new ArrayList<>(),
                        new LinkedHashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                        new ArrayList<>(),
                        false,
                        true,
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        HybridPendingSplitsState checkpoint = new HybridPendingSplitsState(snapshotState, true);
        return new MySqlHybridSplitAssigner(
                buildConfig(false), 4, checkpoint, getMySqlSplitEnumeratorContext());
    }
}
