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

package org.apache.flink.cdc.connectors.base.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus;
import org.apache.flink.cdc.connectors.base.source.assigner.SplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitAssignedEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitMetaAssembledEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitMetaEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitMetaRequestEvent;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Coordinator-level end-to-end test for the snapshot-metadata release trigger. Drives the real
 * {@link IncrementalSourceEnumerator} through the event protocol with a {@link
 * MockSplitEnumeratorContext}: the enumerator serves the finished-split metadata, the reader
 * reports it assembled, and the enumerator releases it from the assigner and its own cache once a
 * checkpoint covers the assembled split. Generalizes the MySQL release (FLINK-39775) to the base
 * framework.
 */
class IncrementalSourceEnumeratorReleaseTest {

    private static final TableId TABLE_ID = new TableId("test_db", null, "customers");
    private static final String STREAM_SPLIT_ID = "stream-split";
    private static final OffsetFactory OFFSET_FACTORY = new NullOffsetFactory();

    @Test
    void testReleaseFiresAfterAssembledAndCheckpoint() throws Exception {
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            context.registerReader(new ReaderInfo(0, "localhost"));

            RecordingSplitAssigner assigner = new RecordingSplitAssigner();
            IncrementalSourceEnumerator enumerator =
                    new IncrementalSourceEnumerator(
                            context,
                            new StubSourceConfig(),
                            assigner,
                            Boundedness.CONTINUOUS_UNBOUNDED);
            enumerator.start();

            // 1) The reader requests the metadata group; the enumerator serves it from the assigner
            // and builds its finishedSnapshotSplitMeta cache.
            enumerator.handleSourceEvent(0, new StreamSplitMetaRequestEvent(STREAM_SPLIT_ID, 0, 2));
            assertThat(sentEventsTo(context, 0)).anyMatch(e -> e instanceof StreamSplitMetaEvent);
            assertThat(enumerator.getFinishedSnapshotSplitMeta()).isNotNull();

            // 2) The reader reports the metadata fully assembled, but the release must NOT fire yet
            // (no checkpoint has covered the assembled split).
            enumerator.handleSourceEvent(
                    0, new StreamSplitMetaAssembledEvent(STREAM_SPLIT_ID, 2, 0));
            assertThat(assigner.released).isFalse();

            // 3) A checkpoint is taken (arms the release) and then completes (fires it).
            enumerator.snapshotState(100L);
            assertThat(assigner.released).isFalse();
            enumerator.notifyCheckpointComplete(100L);
            assertThat(assigner.released).isTrue();
            assertThat(assigner.releaseCalls).isEqualTo(1);

            // 4) The enumerator-side metadata cache was cleared too (memory reclaimed).
            assertThat(enumerator.getFinishedSnapshotSplitMeta()).isNull();

            // 5) A stale metadata request after release is ignored gracefully: no exception, and no
            // further metadata is sent to the reader.
            int sentBefore = sentEventsTo(context, 0).size();
            enumerator.handleSourceEvent(0, new StreamSplitMetaRequestEvent(STREAM_SPLIT_ID, 0, 2));
            assertThat(sentEventsTo(context, 0)).hasSize(sentBefore);

            // 6) Release is idempotent across further checkpoints.
            enumerator.notifyCheckpointComplete(101L);
            assertThat(assigner.releaseCalls).isEqualTo(1);
        }
    }

    @Test
    void testReleaseDoesNotFireWhenOptionDisabled() throws Exception {
        // The release is opt-in. With the option off (the default), the metadata is retained
        // exactly as before, even after the assembled event and a checkpoint.
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            context.registerReader(new ReaderInfo(0, "localhost"));

            RecordingSplitAssigner assigner = new RecordingSplitAssigner();
            IncrementalSourceEnumerator enumerator =
                    new IncrementalSourceEnumerator(
                            context,
                            new StubSourceConfig(false, false), // release disabled
                            assigner,
                            Boundedness.CONTINUOUS_UNBOUNDED);
            enumerator.start();

            enumerator.handleSourceEvent(0, new StreamSplitMetaRequestEvent(STREAM_SPLIT_ID, 0, 2));
            enumerator.handleSourceEvent(
                    0, new StreamSplitMetaAssembledEvent(STREAM_SPLIT_ID, 2, 0));
            enumerator.snapshotState(100L);
            enumerator.notifyCheckpointComplete(100L);

            assertThat(assigner.released).isFalse();
            assertThat(enumerator.getFinishedSnapshotSplitMeta()).isNotNull();
        }
    }

    @Test
    void testStaleAssembledReportIsIgnoredAfterStreamSplitReassignment() throws Exception {
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            context.registerReader(new ReaderInfo(0, "localhost"));

            RecordingSplitAssigner assigner = new RecordingSplitAssigner();
            IncrementalSourceEnumerator enumerator =
                    new IncrementalSourceEnumerator(
                            context,
                            new StubSourceConfig(),
                            assigner,
                            Boundedness.CONTINUOUS_UNBOUNDED);
            enumerator.start();

            // The enumerator serves metadata at generation 0.
            enumerator.handleSourceEvent(0, new StreamSplitMetaRequestEvent(STREAM_SPLIT_ID, 0, 2));

            // The stream split's reader fails over: the split is added back, bumping the
            // generation.
            List<SourceSplitBase> addedBack = new ArrayList<>();
            addedBack.add(
                    new StreamSplit(
                            STREAM_SPLIT_ID, null, null, new ArrayList<>(), new HashMap<>(), 2));
            enumerator.addSplitsBack(addedBack, 0);

            // A stale assembled report (generation 0) must NOT trigger a release.
            enumerator.handleSourceEvent(
                    0, new StreamSplitMetaAssembledEvent(STREAM_SPLIT_ID, 2, 0));
            enumerator.snapshotState(100L);
            enumerator.notifyCheckpointComplete(100L);
            assertThat(assigner.released).isFalse();

            // A fresh report at the current generation (1) does trigger the release.
            enumerator.handleSourceEvent(
                    0, new StreamSplitMetaAssembledEvent(STREAM_SPLIT_ID, 2, 1));
            enumerator.snapshotState(101L);
            enumerator.notifyCheckpointComplete(101L);
            assertThat(assigner.released).isTrue();
        }
    }

    @Test
    void testEmptyAddBackFromStreamReaderBumpsGeneration() throws Exception {
        // If the stream reader's assignment was already checkpoint-covered, a reader reset
        // arrives as an empty addSplitsBack. The generation must still bump so a stale assembled
        // report from the failed attempt cannot arm an unsafe release.
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            context.registerReader(new ReaderInfo(0, "localhost"));

            RecordingSplitAssigner assigner = new RecordingSplitAssigner();
            IncrementalSourceEnumerator enumerator =
                    new IncrementalSourceEnumerator(
                            context,
                            new StubSourceConfig(),
                            assigner,
                            Boundedness.CONTINUOUS_UNBOUNDED);
            enumerator.start();

            // serve metadata at generation 0, and record that subtask 0 holds the stream split
            enumerator.handleSourceEvent(0, new StreamSplitMetaRequestEvent(STREAM_SPLIT_ID, 0, 2));
            enumerator.handleSourceEvent(0, new StreamSplitAssignedEvent());

            // The reader resets without handing the split back (checkpoint-covered), which
            // arrives as an empty add-back.
            enumerator.addSplitsBack(new ArrayList<>(), 0);

            // A stale assembled report at the old generation (0) must NOT trigger a release.
            enumerator.handleSourceEvent(
                    0, new StreamSplitMetaAssembledEvent(STREAM_SPLIT_ID, 2, 0));
            enumerator.snapshotState(100L);
            enumerator.notifyCheckpointComplete(100L);
            assertThat(assigner.released).isFalse();

            // A fresh report at the current generation (1) does trigger the release.
            enumerator.handleSourceEvent(
                    0, new StreamSplitMetaAssembledEvent(STREAM_SPLIT_ID, 2, 1));
            enumerator.snapshotState(101L);
            enumerator.notifyCheckpointComplete(101L);
            assertThat(assigner.released).isTrue();
        }
    }

    @Test
    void testReleaseDoesNotFireWhenNewlyAddedTablesEnabled() throws Exception {
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            context.registerReader(new ReaderInfo(0, "localhost"));

            RecordingSplitAssigner assigner = new RecordingSplitAssigner();
            IncrementalSourceEnumerator enumerator =
                    new IncrementalSourceEnumerator(
                            context,
                            new StubSourceConfig(true), // scan.newly.added.tables enabled
                            assigner,
                            Boundedness.CONTINUOUS_UNBOUNDED);
            enumerator.start();

            // The reader serves and assembles the metadata exactly as in the happy path.
            enumerator.handleSourceEvent(0, new StreamSplitMetaRequestEvent(STREAM_SPLIT_ID, 0, 2));
            enumerator.handleSourceEvent(
                    0, new StreamSplitMetaAssembledEvent(STREAM_SPLIT_ID, 2, 0));

            // But because newly-added-tables capture can re-enter snapshot assignment later and the
            // suspended stream reader must rebuild its split from this metadata, the release must
            // NOT arm or fire even after a checkpoint completes. The metadata stays intact.
            enumerator.snapshotState(100L);
            enumerator.notifyCheckpointComplete(100L);
            assertThat(assigner.released).isFalse();
            assertThat(enumerator.getFinishedSnapshotSplitMeta()).isNotNull();
        }
    }

    @SuppressWarnings("unchecked")
    private static List<SourceEvent> sentEventsTo(
            MockSplitEnumeratorContext<SourceSplitBase> context, int subtask) throws Exception {
        Map<Integer, List<SourceEvent>> sent = context.getSentSourceEvent();
        return sent.getOrDefault(subtask, new ArrayList<>());
    }

    /**
     * A {@link SplitAssigner} that records release and serves two finished splits until released.
     */
    private static class RecordingSplitAssigner implements SplitAssigner {
        boolean released = false;
        int releaseCalls = 0;

        @Override
        public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
            if (released) {
                return new ArrayList<>();
            }
            List<FinishedSnapshotSplitInfo> infos = new ArrayList<>();
            infos.add(
                    new FinishedSnapshotSplitInfo(
                            TABLE_ID,
                            "customers:0",
                            new Object[] {1},
                            new Object[] {100},
                            null,
                            OFFSET_FACTORY));
            infos.add(
                    new FinishedSnapshotSplitInfo(
                            TABLE_ID,
                            "customers:1",
                            new Object[] {101},
                            new Object[] {200},
                            null,
                            OFFSET_FACTORY));
            return infos;
        }

        @Override
        public void releaseSnapshotMetadata() {
            released = true;
            releaseCalls++;
        }

        @Override
        public boolean isSnapshotMetaReleased() {
            return released;
        }

        @Override
        public void open() {}

        @Override
        public java.util.Optional<SourceSplitBase> getNext() {
            return java.util.Optional.empty();
        }

        @Override
        public boolean waitingForFinishedSplits() {
            return false;
        }

        @Override
        public boolean noMoreSplits() {
            return true;
        }

        @Override
        public void onFinishedSplits(Map<String, Offset> splitFinishedOffsets) {}

        @Override
        public void addSplits(Collection<SourceSplitBase> splits) {}

        @Override
        public PendingSplitsState snapshotState(long checkpointId) {
            return new StreamPendingSplitsState(true);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public AssignerStatus getAssignerStatus() {
            return AssignerStatus.INITIAL_ASSIGNING_FINISHED;
        }

        @Override
        public void startAssignNewlyAddedTables() {}

        @Override
        public void onStreamSplitUpdated() {}

        @Override
        public void close() {}
    }

    /** A minimal {@link SourceConfig} for the enumerator. */
    private static class StubSourceConfig implements SourceConfig {
        private final boolean scanNewlyAddedTableEnabled;
        private final boolean releaseSnapshotMetadataEnabled;

        StubSourceConfig() {
            this(false, true);
        }

        StubSourceConfig(boolean scanNewlyAddedTableEnabled) {
            this(scanNewlyAddedTableEnabled, true);
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
            return 10;
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

    /** A serializable no-op {@link OffsetFactory}. */
    private static class NullOffsetFactory extends OffsetFactory {
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
