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

package org.apache.flink.cdc.connectors.base.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitMetaAssembledEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitMetaEvent;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IncrementalSourceReader}. */
class IncrementalSourceReaderTest {

    @Test
    void testInitializedStateAppliesSplitToRecordEmitter() {
        TestingReaderContext readerContext = new TestingReaderContext();
        TrackingRecordEmitter recordEmitter =
                new TrackingRecordEmitter(new SourceReaderMetrics(readerContext.metricGroup()));
        TestingIncrementalSourceReader reader =
                new TestingIncrementalSourceReader(
                        recordEmitter, new IncrementalSourceReaderContext(readerContext));

        SnapshotSplit split =
                new SnapshotSplit(
                        new TableId("catalog", "schema", "table"),
                        "split-0",
                        RowType.of(new IntType()),
                        null,
                        null,
                        null,
                        Collections.emptyMap());

        reader.initialize(split);

        assertThat(recordEmitter.appliedSplit).isSameAs(split);
    }

    @Test
    void testReaderSendsStreamSplitMetaAssembledEventForCompleteSplit() throws Exception {
        // When a complete stream split enters reading, the reader must notify the enumerator that
        // its finished snapshot split metadata is fully assembled, so the coordinator can release
        // the retained snapshot metadata.
        TestingReaderContext readerContext = new TestingReaderContext();
        TestingIncrementalSourceReader reader = createReader(readerContext);
        reader.start();

        // a stream split that carries no outstanding finished snapshot split info is already
        // complete on receipt
        StreamSplit completeSplit =
                new StreamSplit(
                        STREAM_SPLIT_ID,
                        new TestOffset(1L),
                        null,
                        new ArrayList<>(),
                        new HashMap<>(),
                        0);
        assertThat(completeSplit.isCompletedSplit()).isTrue();

        reader.addSplits(Collections.singletonList(completeSplit));

        // the split was complete inline (no meta groups requested), so the reader reports the
        // COMPLETE_WITHOUT_META_GENERATION sentinel rather than a served generation
        assertThat(readerContext.getSentEvents())
                .filteredOn(event -> event instanceof StreamSplitMetaAssembledEvent)
                .extracting(
                        event -> ((StreamSplitMetaAssembledEvent) event).getAssignmentGeneration())
                .containsExactly(StreamSplitMetaAssembledEvent.COMPLETE_WITHOUT_META_GENERATION);
        reader.close();
    }

    @Test
    void testReaderEchoesAssignmentGenerationInAssembledEvent() throws Exception {
        // A stream split assembled from divided meta groups must echo the assignment generation it
        // was served under, so the coordinator can reject a stale assembled event from a failed
        // attempt. Drive the group-fetch path: give the reader an incomplete stream split, answer
        // its meta request with a group stamped with a generation, and assert the assembled event
        // the reader sends back carries that same generation.
        TestingReaderContext readerContext = new TestingReaderContext();
        TestingIncrementalSourceReader reader = createReader(readerContext);
        reader.start();

        // one finished split info is still outstanding, so the split is incomplete on receipt
        StreamSplit incompleteSplit =
                new StreamSplit(
                        STREAM_SPLIT_ID,
                        new TestOffset(100L),
                        null,
                        new ArrayList<>(),
                        new HashMap<>(),
                        1);
        assertThat(incompleteSplit.isCompletedSplit()).isFalse();

        reader.addSplits(Collections.singletonList(incompleteSplit));

        // the coordinator answers the reader's meta request with the outstanding group, stamped
        // with the current assignment generation
        int generation = 7;
        FinishedSnapshotSplitInfo finishedInfo =
                new FinishedSnapshotSplitInfo(
                        TABLE_ID,
                        TABLE_ID + ":0",
                        new Object[] {0},
                        new Object[] {100},
                        new TestOffset(50L),
                        TEST_OFFSET_FACTORY);
        reader.handleSourceEvents(
                new StreamSplitMetaEvent(
                        incompleteSplit.splitId(),
                        0,
                        Collections.singletonList(finishedInfo.serialize()),
                        1,
                        generation));

        assertThat(readerContext.getSentEvents())
                .filteredOn(event -> event instanceof StreamSplitMetaAssembledEvent)
                .extracting(
                        event -> ((StreamSplitMetaAssembledEvent) event).getAssignmentGeneration())
                .containsExactly(generation);
        reader.close();
    }

    private static TestingIncrementalSourceReader createReader(TestingReaderContext readerContext) {
        return new TestingIncrementalSourceReader(
                new TrackingRecordEmitter(new SourceReaderMetrics(readerContext.metricGroup())),
                new IncrementalSourceReaderContext(readerContext));
    }

    private static class TestingIncrementalSourceReader
            extends IncrementalSourceReader<Object, SourceConfig> {

        private TestingIncrementalSourceReader(
                TrackingRecordEmitter recordEmitter,
                IncrementalSourceReaderContext incrementalSourceReaderContext) {
            super(
                    new FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>>(),
                    createSplitReaderSupplier(),
                    recordEmitter,
                    new Configuration(),
                    incrementalSourceReaderContext,
                    new StubSourceConfig(),
                    new SourceSplitSerializer() {
                        @Override
                        public OffsetFactory getOffsetFactory() {
                            return TEST_OFFSET_FACTORY;
                        }
                    },
                    new StubDataSourceDialect());
        }

        private static Supplier<IncrementalSourceSplitReader<SourceConfig>>
                createSplitReaderSupplier() {
            return IdleSplitReader::new;
        }

        private SourceSplitState initialize(SourceSplitBase split) {
            return initializedState(split);
        }
    }

    private static class TrackingRecordEmitter extends IncrementalSourceRecordEmitter<Object> {

        private SourceSplitBase appliedSplit;

        private TrackingRecordEmitter(SourceReaderMetrics sourceReaderMetrics) {
            super(
                    new NoOpDebeziumDeserializationSchema(),
                    sourceReaderMetrics,
                    false,
                    TEST_OFFSET_FACTORY);
        }

        @Override
        public void applySplit(SourceSplitBase split) {
            this.appliedSplit = split;
        }
    }

    private static class NoOpDebeziumDeserializationSchema
            implements DebeziumDeserializationSchema<Object> {

        @Override
        public void deserialize(SourceRecord record, Collector<Object> out) {
            // no-op
        }

        @Override
        public TypeInformation<Object> getProducedType() {
            return TypeInformation.of(Object.class);
        }
    }

    /** A split reader that never produces records, so the fetcher thread stays idle. */
    private static class IdleSplitReader extends IncrementalSourceSplitReader<SourceConfig> {

        private final Semaphore wakeUp = new Semaphore(0);

        private IdleSplitReader() {
            super(0, null, null, null, null);
        }

        @Override
        public RecordsWithSplitIds<SourceRecords> fetch() {
            try {
                wakeUp.tryAcquire(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new EmptyRecords();
        }

        @Override
        public void handleSplitsChanges(SplitsChange<SourceSplitBase> splitsChanges) {}

        @Override
        public void wakeUp() {
            wakeUp.release();
        }

        @Override
        public void close() {}
    }

    /** An empty record batch handed back by {@link IdleSplitReader}. */
    private static class EmptyRecords implements RecordsWithSplitIds<SourceRecords> {

        @Override
        public String nextSplit() {
            return null;
        }

        @Override
        public SourceRecords nextRecordFromSplit() {
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }

    /** A minimal {@link SourceConfig} for the reader. */
    private static class StubSourceConfig implements SourceConfig {

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
            return false;
        }

        @Override
        public boolean isAssignUnboundedChunkFirst() {
            return false;
        }

        @Override
        public double getRecordsPerSecond() {
            return -1d;
        }
    }

    /** A dialect that captures every table and discovers no schema. */
    private static class StubDataSourceDialect implements DataSourceDialect<SourceConfig> {

        @Override
        public String getName() {
            return "stub";
        }

        @Override
        public List<TableId> discoverDataCollections(SourceConfig sourceConfig) {
            return Collections.singletonList(TABLE_ID);
        }

        @Override
        public Map<TableId, TableChanges.TableChange> discoverDataCollectionSchemas(
                SourceConfig sourceConfig) {
            return new HashMap<>();
        }

        @Override
        public Offset displayCurrentOffset(SourceConfig sourceConfig) {
            return new TestOffset(0L);
        }

        @Override
        public boolean isDataCollectionIdCaseSensitive(SourceConfig sourceConfig) {
            return true;
        }

        @Override
        public ChunkSplitter createChunkSplitter(SourceConfig sourceConfig) {
            return null;
        }

        @Override
        public ChunkSplitter createChunkSplitter(
                SourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
            return null;
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

    /** An {@link Offset} ordered by a single position. */
    private static class TestOffset extends Offset {

        private static final String POSITION_KEY = "position";

        private TestOffset(long position) {
            this.offset = new HashMap<>();
            this.offset.put(POSITION_KEY, String.valueOf(position));
        }

        @Override
        public int compareTo(Offset that) {
            return Long.compare(position(this), position(that));
        }

        private static long position(Offset offset) {
            return Long.parseLong(offset.getOffset().get(POSITION_KEY));
        }
    }

    /** An {@link OffsetFactory} producing {@link TestOffset}. */
    private static class TestOffsetFactory extends OffsetFactory {

        @Override
        public Offset newOffset(Map<String, String> offset) {
            return new TestOffset(Long.parseLong(offset.get(TestOffset.POSITION_KEY)));
        }

        @Override
        public Offset newOffset(String filename, Long position) {
            return new TestOffset(position);
        }

        @Override
        public Offset newOffset(Long position) {
            return new TestOffset(position);
        }

        @Override
        public Offset createTimestampOffset(long timestampMillis) {
            return new TestOffset(timestampMillis);
        }

        @Override
        public Offset createInitialOffset() {
            return new TestOffset(0L);
        }

        @Override
        public Offset createNoStoppingOffset() {
            return new TestOffset(Long.MAX_VALUE);
        }
    }

    private static final TableId TABLE_ID = new TableId("test_db", null, "customers");

    private static final OffsetFactory TEST_OFFSET_FACTORY = new TestOffsetFactory();
}
