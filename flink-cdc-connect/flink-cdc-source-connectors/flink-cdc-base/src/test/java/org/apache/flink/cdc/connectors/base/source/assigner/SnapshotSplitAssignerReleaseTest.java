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

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsStateSerializer;
import org.apache.flink.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the snapshot-metadata release mechanism in {@link SnapshotSplitAssigner}. Generalizes
 * the MySQL coordinator-memory release from FLINK-39775 to the base incremental framework: once the
 * snapshot phase has finished and the reader holds the full metadata, the coordinator can drop the
 * bulk snapshot metadata so the checkpointed state becomes "light".
 */
class SnapshotSplitAssignerReleaseTest {

    private static final TableId TABLE_ID = new TableId("test_db", null, "customers");

    @Test
    void testReleaseSnapshotMetadataProducesLightCheckpointAndRestores() throws Exception {
        OffsetFactory offsetFactory = nullOffsetFactory();
        SnapshotSplitAssigner<SourceConfig> assigner =
                new SnapshotSplitAssigner<>(
                        (SourceConfig) null,
                        4,
                        heavySnapshotFinishedState(),
                        new MockDataSourceDialect(),
                        offsetFactory);

        // Before release: the coordinator carries the full snapshot metadata for the two
        // finished splits, and it is ready to serve finished-split info to the reader.
        SnapshotPendingSplitsState before = assigner.snapshotState(1L);
        assertThat(assigner.isSnapshotMetaReleased()).isFalse();
        assertThat(before.getAssignedSplits()).hasSize(2);
        assertThat(before.getSplitFinishedOffsets()).hasSize(2);
        assertThat(before.getTableSchemas()).isNotEmpty();
        assertThat(assigner.waitingForFinishedSplits()).isFalse();

        // Release the bulk metadata (the generalized FLINK-39775 mechanism).
        assigner.releaseSnapshotMetadata();
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();

        // After release the checkpoint is light. The heavy maps are empty, while the table
        // list, status and chunk-splitter state are retained so a restore does not re-discover.
        SnapshotPendingSplitsState light = assigner.snapshotState(2L);
        assertThat(light.getAssignedSplits()).isEmpty();
        assertThat(light.getSplitFinishedOffsets()).isEmpty();
        assertThat(light.getTableSchemas()).isEmpty();
        assertThat(light.getSplitFinishedCheckpointIds()).isEmpty();
        assertThat(light.getAlreadyProcessedTables()).containsExactly(TABLE_ID);

        // The light checkpoint round-trips through the state serializer. A released state is only
        // written by a release-enabled (v9) serializer, which persists the released flag.
        PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(sourceSplitSerializer(offsetFactory), true);
        PendingSplitsState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(light));
        assertThat(restored).isEqualTo(light);

        // A fresh assigner restored from the light checkpoint is correct and stays light: it
        // reports no more splits (does not re-snapshot) and produces no heavy metadata.
        SnapshotSplitAssigner<SourceConfig> restoredAssigner =
                new SnapshotSplitAssigner<>(
                        (SourceConfig) null,
                        4,
                        (SnapshotPendingSplitsState) restored,
                        new MockDataSourceDialect(),
                        offsetFactory);
        assertThat(restoredAssigner.noMoreSplits()).isTrue();
        // the released flag survives the checkpoint round-trip and restore (so the restored
        // coordinator knows it already released and will not try to re-serve dropped metadata)
        assertThat(restoredAssigner.isSnapshotMetaReleased()).isTrue();
        SnapshotPendingSplitsState afterRestore = restoredAssigner.snapshotState(3L);
        assertThat(afterRestore.getAssignedSplits()).isEmpty();
        assertThat(afterRestore.getTableSchemas()).isEmpty();
        assertThat(afterRestore.isSnapshotMetaReleased()).isTrue();

        // Releasing again is idempotent.
        assigner.releaseSnapshotMetadata();
        assertThat(assigner.isSnapshotMetaReleased()).isTrue();
    }

    @Test
    void testHybridAssignerRejectsStreamSplitAddedBackAfterRelease() throws Exception {
        OffsetFactory offsetFactory = nullOffsetFactory();
        HybridPendingSplitsState hybridState =
                new HybridPendingSplitsState(heavySnapshotFinishedState(), false);
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            HybridSplitAssigner<SourceConfig> hybrid =
                    new HybridSplitAssigner<>(
                            new StubSourceConfig(),
                            4,
                            hybridState,
                            new MockDataSourceDialect(),
                            offsetFactory,
                            context);

            // Drop the snapshot metadata (the release this change adds).
            hybrid.releaseSnapshotMetadata();
            assertThat(hybrid.isSnapshotMetaReleased()).isTrue();

            // Re-creating the stream split from dropped metadata must fail fast rather than build
            // an empty, incorrect split. Release only fires after a checkpoint covers the
            // assembled split, so this never happens under correct Flink semantics. It is a guard.
            StreamSplit streamSplit =
                    new StreamSplit(
                            "stream-split", null, null, new ArrayList<>(), new HashMap<>(), 2);
            assertThatThrownBy(
                            () ->
                                    hybrid.addSplits(
                                            Collections.singletonList(
                                                    (SourceSplitBase) streamSplit)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("after the snapshot split metadata was released");
        }
    }

    @Test
    void testFailFastWhenReleaseDisabledOnReleasedRestore() throws Exception {
        // A job that already released its snapshot metadata cannot be restarted with the release
        // option off: the metadata is gone, and falling back to the v8 format would drop the
        // marker.
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            assertThatThrownBy(
                            () ->
                                    new HybridSplitAssigner<>(
                                            new StubSourceConfig(false, false),
                                            4,
                                            releasedHybridState(),
                                            new MockDataSourceDialect(),
                                            nullOffsetFactory(),
                                            context))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "scan.incremental.snapshot.metadata.release.enabled cannot be turned off");
        }
    }

    @Test
    void testFailFastWhenNewlyAddedEnabledOnReleasedRestore() throws Exception {
        // A job that released its metadata cannot later enable scan.newly-added-table, because the
        // metadata that flow needs is gone.
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            assertThatThrownBy(
                            () ->
                                    new HybridSplitAssigner<>(
                                            new StubSourceConfig(true, true),
                                            4,
                                            releasedHybridState(),
                                            new MockDataSourceDialect(),
                                            nullOffsetFactory(),
                                            context))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("scan.newly-added-table.enabled cannot be turned on");
        }
    }

    /**
     * A released ("light") hybrid state: empty metadata maps, released flag set, stream assigned.
     */
    private HybridPendingSplitsState releasedHybridState() {
        SnapshotPendingSplitsState released =
                new SnapshotPendingSplitsState(
                        new ArrayList<>(Collections.singletonList(TABLE_ID)),
                        new ArrayList<>(),
                        new LinkedHashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                        new ArrayList<>(),
                        false,
                        true,
                        new HashMap<>(),
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE,
                        true);
        return new HybridPendingSplitsState(released, true);
    }

    /** A snapshot-finished state carrying bulk metadata for two assigned, finished splits. */
    private SnapshotPendingSplitsState heavySnapshotFinishedState() {
        Map<String, SchemalessSnapshotSplit> assignedSplits = new LinkedHashMap<>();
        assignedSplits.put("customers:0", schemalessSplit("customers:0"));
        assignedSplits.put("customers:1", schemalessSplit("customers:1"));

        Map<String, Offset> splitFinishedOffsets = new HashMap<>();
        splitFinishedOffsets.put("customers:0", null);
        splitFinishedOffsets.put("customers:1", null);

        Map<String, Long> splitFinishedCheckpointIds = new HashMap<>();
        splitFinishedCheckpointIds.put("customers:0", 5L);
        splitFinishedCheckpointIds.put("customers:1", 5L);

        return new SnapshotPendingSplitsState(
                new ArrayList<>(Collections.singletonList(TABLE_ID)),
                new ArrayList<>(),
                assignedSplits,
                tableSchema(),
                splitFinishedOffsets,
                AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                new ArrayList<>(),
                false,
                true,
                splitFinishedCheckpointIds,
                ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    private SchemalessSnapshotSplit schemalessSplit(String splitId) {
        return new SchemalessSnapshotSplit(
                TABLE_ID,
                splitId,
                new RowType(
                        Collections.singletonList(new RowType.RowField("id", new BigIntType()))),
                new Object[] {1},
                new Object[] {100},
                null);
    }

    private Map<TableId, TableChanges.TableChange> tableSchema() {
        Map<TableId, TableChanges.TableChange> schema = new HashMap<>();
        Tables tables = new Tables();
        Table table = tables.editOrCreateTable(TABLE_ID).create();
        schema.put(
                TABLE_ID, new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table));
        return schema;
    }

    private OffsetFactory nullOffsetFactory() {
        return new OffsetFactory() {
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
        };
    }

    private SourceSplitSerializer sourceSplitSerializer(OffsetFactory offsetFactory) {
        return new SourceSplitSerializer() {
            @Override
            public OffsetFactory getOffsetFactory() {
                return offsetFactory;
            }
        };
    }

    /** A minimal dialect that only needs to hand back a {@link ChunkSplitter}. */
    private static class MockDataSourceDialect implements DataSourceDialect<SourceConfig> {
        @Override
        public String getName() {
            return "mock";
        }

        @Override
        public List<TableId> discoverDataCollections(SourceConfig sourceConfig) {
            return new ArrayList<>();
        }

        @Override
        public Map<TableId, TableChanges.TableChange> discoverDataCollectionSchemas(
                SourceConfig sourceConfig) {
            return new HashMap<>();
        }

        @Override
        public Offset displayCurrentOffset(SourceConfig sourceConfig) {
            return null;
        }

        @Override
        public boolean isDataCollectionIdCaseSensitive(SourceConfig sourceConfig) {
            return false;
        }

        @Override
        public ChunkSplitter createChunkSplitter(SourceConfig sourceConfig) {
            return new MockChunkSplitter();
        }

        @Override
        public ChunkSplitter createChunkSplitter(
                SourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
            return new MockChunkSplitter();
        }

        @Override
        public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
            return null;
        }

        @Override
        public FetchTask.Context createFetchTaskContext(SourceConfig sourceConfig) {
            return null;
        }

        @Override
        public boolean isIncludeDataCollection(SourceConfig sourceConfig, TableId tableId) {
            return true;
        }
    }

    /** A no-op splitter that snapshots to a serializable {@link ChunkSplitterState}. */
    private static class MockChunkSplitter implements ChunkSplitter {
        @Override
        public void open() {}

        @Override
        public Collection<SnapshotSplit> generateSplits(TableId tableId) {
            return new ArrayList<>();
        }

        @Override
        public boolean hasNextChunk() {
            return false;
        }

        @Override
        public ChunkSplitterState snapshotState(long checkpointId) {
            return new ChunkSplitterState(TABLE_ID, ChunkSplitterState.ChunkBound.middleOf(1), 2);
        }

        @Override
        public TableId getCurrentSplittingTableId() {
            return null;
        }

        @Override
        public void close() {}
    }

    /** A minimal {@link SourceConfig}; only {@code getSplitMetaGroupSize} is consulted here. */
    private static class StubSourceConfig implements SourceConfig {
        private final boolean scanNewlyAddedTableEnabled;
        private final boolean releaseSnapshotMetadataEnabled;

        StubSourceConfig() {
            this(false, false);
        }

        StubSourceConfig(
                boolean scanNewlyAddedTableEnabled, boolean releaseSnapshotMetadataEnabled) {
            this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
            this.releaseSnapshotMetadataEnabled = releaseSnapshotMetadataEnabled;
        }

        @Override
        public StartupOptions getStartupOptions() {
            return null;
        }

        @Override
        public int getSplitSize() {
            return 8096;
        }

        @Override
        public int getSplitMetaGroupSize() {
            return 2;
        }

        @Override
        public boolean isIncludeSchemaChanges() {
            return false;
        }

        @Override
        public boolean isCloseIdleReaders() {
            return false;
        }

        @Override
        public boolean isSkipSnapshotBackfill() {
            return false;
        }

        @Override
        public boolean isScanNewlyAddedTableEnabled() {
            return scanNewlyAddedTableEnabled;
        }

        @Override
        public boolean isAssignUnboundedChunkFirst() {
            return false;
        }

        @Override
        public boolean isReleaseSnapshotMetadataEnabled() {
            return releaseSnapshotMetadataEnabled;
        }
    }
}
