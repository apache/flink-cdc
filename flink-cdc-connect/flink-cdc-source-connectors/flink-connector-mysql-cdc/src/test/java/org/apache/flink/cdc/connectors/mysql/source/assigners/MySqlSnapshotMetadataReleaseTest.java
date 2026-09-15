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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitAssignedEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetaAssembledEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetaRequestEvent;
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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void testFailFastWhenBothOptionsEnabled() {
        // Enabling both metadata release and scan.newly-added-table is contradictory: the release
        // would drop the metadata the newly-added-table flow needs. Reject it at config build time
        // rather than silently disabling the release.
        assertThatThrownBy(() -> buildConfig(true, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot both be enabled");
    }

    @Test
    void testNoReleaseWhenReleaseMetadataDisabled() {
        // The release optimization is opt-in. With it disabled (the default) the metadata is
        // retained exactly as before this change, so a job can still enable scan.newly-added-table
        // later. This guards against the regression raised in review.
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(false, false);
        assigner.getNext();
        assigner.onBinlogSplitMetaAssembled(assigner.getBinlogAssignmentGeneration());

        assigner.snapshotState(1L);
        assigner.notifyCheckpointComplete(1L);

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

        // an inline reader never requests meta groups, so it never learns the bumped generation
        // and reports COMPLETE_WITHOUT_META_GENERATION. Release must still happen, otherwise the
        // optimization silently stops working for small tables after any failover.
        assigner.onBinlogSplitMetaAssembled(
                BinlogSplitMetaAssembledEvent.COMPLETE_WITHOUT_META_GENERATION);
        assigner.snapshotState(2L);
        assigner.notifyCheckpointComplete(2L);
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();

        assigner.close();
    }

    @Test
    void testEmptyAddBackFromBinlogReaderInvalidatesStaleAssembledEvent() {
        // A reader holding the binlog split can reset without its split being handed back: when the
        // assignment is already checkpoint-covered, Flink restores the split from the reader's own
        // state and calls addSplitsBack with an empty list. The generation must still be bumped and
        // any scheduled release cancelled, otherwise a delayed assembled event from the failed
        // attempt would arm an unsafe release while the restored reader still needs the metadata.
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(false);
        assigner.getNext(); // enter the binlog phase; the binlog split is assigned

        MySqlSourceEnumerator enumerator =
                new MySqlSourceEnumerator(
                        getMySqlSplitEnumeratorContext(),
                        buildConfig(false, true),
                        assigner,
                        Boundedness.CONTINUOUS_UNBOUNDED);

        // subtask 0 reports it holds the binlog split, then assembles it and schedules a release
        enumerator.handleSourceEvent(0, new BinlogSplitAssignedEvent());
        long staleGeneration = assigner.getBinlogAssignmentGeneration();
        assigner.onBinlogSplitMetaAssembled(staleGeneration);
        assigner.snapshotState(1L); // schedules release at checkpoint 1

        // subtask 0 resets, but its assignment was checkpoint-covered, so Flink hands nothing back
        enumerator.addSplitsBack(Collections.emptyList(), 0);

        // completing checkpoint 1 must NOT release: the reset cancelled the schedule
        assigner.notifyCheckpointComplete(1L);
        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assertThat(assigner.getBinlogAssignmentGeneration()).isGreaterThan(staleGeneration);

        // a delayed assembled event from the failed attempt (old generation) is now ignored
        assigner.onBinlogSplitMetaAssembled(staleGeneration);
        assigner.snapshotState(2L);
        assigner.notifyCheckpointComplete(2L);
        assertThat(assigner.isSnapshotMetaReleased()).isFalse();

        assigner.close();
    }

    @Test
    void testStaleBinlogSplitAssignedEventDoesNotCorruptHolder() {
        // A delayed BinlogSplitAssignedEvent from a failed attempt must not overwrite the live
        // binlog holder. If it did, the empty-add-back reset detection (which keys off
        // binlogSplitTaskId) would be defeated and a stale assembled event could arm an unsafe
        // release. Here the real holder is subtask 1; a stale assigned event from subtask 0 arrives
        // and must be ignored, so the later empty add-back from subtask 1 still cancels the
        // release.
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(false);
        assigner.getNext(); // enter the binlog phase

        MySqlSourceEnumerator enumerator =
                new MySqlSourceEnumerator(
                        getMySqlSplitEnumeratorContext(),
                        buildConfig(false, true),
                        assigner,
                        Boundedness.CONTINUOUS_UNBOUNDED);

        // subtask 1 is the live binlog holder; a stale assigned event from subtask 0 must be
        // ignored
        enumerator.handleSourceEvent(1, new BinlogSplitAssignedEvent());
        enumerator.handleSourceEvent(0, new BinlogSplitAssignedEvent());

        long staleGeneration = assigner.getBinlogAssignmentGeneration();
        assigner.onBinlogSplitMetaAssembled(staleGeneration);
        assigner.snapshotState(1L); // schedules release at checkpoint 1

        // the real holder (subtask 1) resets with an empty add-back; because binlogSplitTaskId was
        // not corrupted to 0, the reset is still detected and the scheduled release is cancelled
        enumerator.addSplitsBack(Collections.emptyList(), 1);
        assigner.notifyCheckpointComplete(1L);
        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assertThat(assigner.getBinlogAssignmentGeneration()).isGreaterThan(staleGeneration);

        assigner.close();
    }

    @Test
    void testRestoreFromReleasedLightState() {
        MySqlHybridSplitAssigner assigner = buildReleasedLightAssigner();

        // No binlog split is re-created and no tables are re-discovered after a light restore.
        assertThat(assigner.getNext()).isEmpty();
        // The released flag is restored from the persisted checkpoint value, so after restore the
        // assigner knows the metadata was already released.
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();

        // The state stays light (empty heavy maps, binlog split still assigned).
        HybridPendingSplitsState state = (HybridPendingSplitsState) assigner.snapshotState(10L);
        assertThat(state.getSnapshotPendingSplits().getAssignedSplits()).isEmpty();
        assertThat(state.getSnapshotPendingSplits().getSplitFinishedOffsets()).isEmpty();
        assertThat(state.getSnapshotPendingSplits().getAlreadyProcessedTables()).isNotEmpty();
        assertThat(state.isBinlogSplitAssigned()).isTrue();

        // Already released after restore; re-sending the assembled event and completing a
        // checkpoint is a no-op, and the assigner stays in the released state.
        assigner.onBinlogSplitMetaAssembled(assigner.getBinlogAssignmentGeneration());
        assigner.snapshotState(11L);
        assigner.notifyCheckpointComplete(11L);
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();

        assigner.close();
    }

    @Test
    void testReleasedFlagNotInferredWhenSnapshotSplitsRemain() {
        // The released flag is persisted (PendingSplitsStateSerializer v6), not inferred from state
        // shape. A finished parallelism-1 state that never released (flag defaults false) must
        // restore as not-released even with non-empty remainingSplits and empty assignedSplits.
        // This guards against anyone re-introducing shape-based inference, which previously
        // false-positived here and crash-looped createBinlogSplit, feature-off jobs included.
        List<TableId> alreadyProcessedTables = Lists.newArrayList(new TableId(null, DB, TABLE));
        RowType splitKeyType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<MySqlSchemalessSnapshotSplit> remainingSplits = new ArrayList<>();
        remainingSplits.add(
                new MySqlSchemalessSnapshotSplit(
                        new TableId(null, DB, TABLE),
                        DB + "." + TABLE + ":0",
                        splitKeyType,
                        null,
                        new Object[] {2},
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.00001", 1)));
        SnapshotPendingSplitsState snapshotState =
                new SnapshotPendingSplitsState(
                        alreadyProcessedTables,
                        remainingSplits,
                        new LinkedHashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                        new ArrayList<>(),
                        false,
                        true,
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        HybridPendingSplitsState checkpoint = new HybridPendingSplitsState(snapshotState, false);
        MySqlHybridSplitAssigner assigner =
                new MySqlHybridSplitAssigner(
                        buildConfig(false, true), 1, checkpoint, getMySqlSplitEnumeratorContext());

        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assigner.close();
    }

    @Test
    void testFailFastWhenNewlyAddedEnabledOnReleasedRestore() {
        // A job that released its metadata cannot later enable scan.newly-added-table, because the
        // metadata that flow needs is gone. Restoring such a state with the flag on must fail fast
        // with a clear error rather than silently corrupting the binlog split.
        assertThatThrownBy(() -> buildReleasedLightAssigner(true, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("scan.newly-added-table.enabled cannot be turned on");
    }

    @Test
    void testStaleBinlogMetaRequestAfterLightRestoreIsIgnored() {
        // After restoring from a released light checkpoint the released flag is restored from the
        // persisted checkpoint value, so the enumerator still ignores a stray meta request instead
        // of rebuilding and throwing.
        MySqlHybridSplitAssigner assigner = buildReleasedLightAssigner();
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();

        MySqlSourceEnumerator enumerator =
                new MySqlSourceEnumerator(
                        getMySqlSplitEnumeratorContext(),
                        buildConfig(false, true),
                        assigner,
                        Boundedness.CONTINUOUS_UNBOUNDED);

        assertThatCode(
                        () ->
                                enumerator.handleSourceEvent(
                                        0,
                                        new BinlogSplitMetaRequestEvent(
                                                "binlog-split", 0, NUM_FINISHED_SPLITS)))
                .doesNotThrowAnyException();

        assigner.close();
    }

    @Test
    void testStaleBinlogMetaRequestAfterReleaseIsIgnored() {
        // A stale BinlogSplitMetaRequestEvent from a failed reader attempt can arrive after the
        // snapshot metadata has been released. The enumerator must ignore it instead of rebuilding
        // from the emptied assigner, which would throw FlinkRuntimeException and fail the job.
        MySqlHybridSplitAssigner assigner = buildFinishedSnapshotAssigner(false);
        assigner.getNext();
        assigner.onBinlogSplitMetaAssembled(assigner.getBinlogAssignmentGeneration());
        assigner.snapshotState(1L);
        assigner.notifyCheckpointComplete(1L);
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();

        MySqlSourceEnumerator enumerator =
                new MySqlSourceEnumerator(
                        getMySqlSplitEnumeratorContext(),
                        buildConfig(false, true),
                        assigner,
                        Boundedness.CONTINUOUS_UNBOUNDED);

        assertThatCode(
                        () ->
                                enumerator.handleSourceEvent(
                                        0,
                                        new BinlogSplitMetaRequestEvent(
                                                "binlog-split", 0, NUM_FINISHED_SPLITS)))
                .doesNotThrowAnyException();

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

    private MySqlSourceConfig buildConfig(
            boolean scanNewlyAddedTableEnabled, boolean releaseSnapshotMetadataEnabled) {
        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.initial())
                .databaseList(DB)
                .tableList(DB + "." + TABLE)
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("")
                .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                .releaseSnapshotMetadataEnabled(releaseSnapshotMetadataEnabled)
                .serverTimeZone(ZoneId.of("UTC").toString())
                .createConfig(0);
    }

    private MySqlHybridSplitAssigner buildFinishedSnapshotAssigner(
            boolean scanNewlyAddedTableEnabled) {
        return buildFinishedSnapshotAssigner(scanNewlyAddedTableEnabled, true);
    }

    private MySqlHybridSplitAssigner buildFinishedSnapshotAssigner(
            boolean scanNewlyAddedTableEnabled, boolean releaseSnapshotMetadataEnabled) {
        MySqlSourceConfig config =
                buildConfig(scanNewlyAddedTableEnabled, releaseSnapshotMetadataEnabled);

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
        return buildReleasedLightAssigner(false, true);
    }

    private MySqlHybridSplitAssigner buildReleasedLightAssigner(
            boolean scanNewlyAddedTableEnabled, boolean releaseSnapshotMetadataEnabled) {
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
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE,
                        true);
        HybridPendingSplitsState checkpoint = new HybridPendingSplitsState(snapshotState, true);
        return new MySqlHybridSplitAssigner(
                buildConfig(scanNewlyAddedTableEnabled, releaseSnapshotMetadataEnabled),
                4,
                checkpoint,
                getMySqlSplitEnumeratorContext());
    }
}
