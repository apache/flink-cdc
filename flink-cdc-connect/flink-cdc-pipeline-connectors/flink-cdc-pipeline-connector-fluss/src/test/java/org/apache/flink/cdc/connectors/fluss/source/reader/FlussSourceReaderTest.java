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

package org.apache.flink.cdc.connectors.fluss.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.fluss.sink.v2.metrics.WrapperFlussMetricRegistry;
import org.apache.flink.cdc.connectors.fluss.source.deserializer.FlussRecordDeserializer;
import org.apache.flink.cdc.connectors.fluss.source.event.FinishedKvSnapshotConsumeEvent;
import org.apache.flink.cdc.connectors.fluss.source.event.TableRemovalAckEvent;
import org.apache.flink.cdc.connectors.fluss.source.event.TableSubscriptionEvent;
import org.apache.flink.cdc.connectors.fluss.source.metrics.FlussSourceReaderMetrics;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplitState;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitState;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for subscription-aware split handling in {@link FlussSourceReader}. */
class FlussSourceReaderTest {

    @Test
    void testStagesSplitUntilFirstSubscriptionSnapshotAndIncludesItInCheckpoint() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        FlussSourceReader<Event> reader = newReader(readerContext);
        FlussSplitBase split =
                new FlussLogSplit(
                        PhysicalTablePath.of(TablePath.of("test_db", "test_table")),
                        new TableBucket(1L, 0),
                        0L);

        try {
            reader.addSplits(Collections.singletonList(split));

            List<FlussSplitBase> checkpoint = reader.snapshotState(1L);
            assertThat(checkpoint).containsExactly(split);
            assertThat(readerContext.getSentEvents()).isEmpty();
        } finally {
            reader.close();
        }
    }

    @Test
    void testFirstSubscriptionFenceDropsRestoredSplitButAllowsFreshSplit() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue =
                new FutureCompletingBlockingQueue<>();
        TrackingFetcherManager fetcherManager = new TrackingFetcherManager(queue);
        FlussSourceReader<Event> reader =
                new FlussSourceReader<>(
                        queue,
                        readerContext,
                        new WrapperFlussMetricRegistry(
                                readerContext.metricGroup(), Collections.emptySet()),
                        new FlussRecordEmitter<>(new FlussRecordDeserializer()),
                        fetcherManager);
        FlussLogSplit restoredSplit = testSplit();
        FlussLogSplit freshSplit =
                new FlussLogSplit(
                        restoredSplit.getPhysicalTablePath(), restoredSplit.getTableBucket(), 10L);

        try {
            reader.addSplits(Collections.singletonList(restoredSplit));
            reader.handleSourceEvents(
                    new TableSubscriptionEvent(
                            Collections.singleton(restoredSplit.getTablePath()),
                            Collections.emptyMap(),
                            Collections.singleton(restoredSplit.getTablePath())));

            assertThat(fetcherManager.addedSplits).isEmpty();
            assertThat(fetcherManager.removedTablePaths).isEmpty();

            reader.addSplits(Collections.singletonList(freshSplit));
            assertThat(fetcherManager.addedSplits).containsExactly(freshSplit);
        } finally {
            reader.close();
        }
    }

    @Test
    void testAcknowledgesRemovalAfterFinishedSplitCallback() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue =
                new FutureCompletingBlockingQueue<>();
        TrackingFetcherManager fetcherManager = new TrackingFetcherManager(queue);
        FlussSourceReader<Event> reader =
                new FlussSourceReader<>(
                        queue,
                        readerContext,
                        new WrapperFlussMetricRegistry(
                                readerContext.metricGroup(), Collections.emptySet()),
                        new FlussRecordEmitter<>(new FlussRecordDeserializer()),
                        fetcherManager);
        FlussSplitBase split = testSplit();
        FlussSplitState splitState = reader.initializedState(split);
        long requestId = 7L;

        try {
            reader.handleSourceEvents(
                    new TableSubscriptionEvent(
                            Collections.emptySet(),
                            Collections.singletonMap(split.getTablePath(), requestId)));
            assertThat(fetcherManager.removedTablePaths).containsExactly(split.getTablePath());
            assertThat(readerContext.getSentEvents()).isEmpty();

            reader.onSplitFinished(Collections.singletonMap(split.splitId(), splitState));

            assertThat(readerContext.getSentEvents())
                    .singleElement()
                    .isInstanceOfSatisfying(
                            TableRemovalAckEvent.class,
                            ack ->
                                    assertThat(ack.getCompletedRemovalRequests())
                                            .containsExactlyEntriesOf(
                                                    Collections.singletonMap(
                                                            split.getTablePath(), requestId)));
        } finally {
            reader.close();
        }
    }

    @Test
    void testAcknowledgesRemovalWithoutLocalSplitsWithoutFetcherRemoval() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue =
                new FutureCompletingBlockingQueue<>();
        TrackingFetcherManager fetcherManager = new TrackingFetcherManager(queue);
        FlussSourceReader<Event> reader =
                new FlussSourceReader<>(
                        queue,
                        readerContext,
                        new WrapperFlussMetricRegistry(
                                readerContext.metricGroup(), Collections.emptySet()),
                        new FlussRecordEmitter<>(new FlussRecordDeserializer()),
                        fetcherManager);
        TablePath tablePath = testSplit().getTablePath();

        try {
            reader.handleSourceEvents(
                    new TableSubscriptionEvent(
                            Collections.emptySet(), Collections.singletonMap(tablePath, 1L)));

            assertThat(fetcherManager.removedTablePaths).isEmpty();
            assertThat(readerContext.getSentEvents())
                    .singleElement()
                    .isInstanceOfSatisfying(
                            TableRemovalAckEvent.class,
                            ack ->
                                    assertThat(ack.getCompletedRemovalRequests())
                                            .containsExactlyEntriesOf(
                                                    Collections.singletonMap(tablePath, 1L)));
        } finally {
            reader.close();
        }
    }

    @Test
    void testReaddedSnapshotBucketReportsFinishedAgainAfterRemoval() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue =
                new FutureCompletingBlockingQueue<>();
        TrackingFetcherManager fetcherManager = new TrackingFetcherManager(queue);
        FlussSourceReader<Event> reader =
                new FlussSourceReader<>(
                        queue,
                        readerContext,
                        new WrapperFlussMetricRegistry(
                                readerContext.metricGroup(), Collections.emptySet()),
                        new FlussRecordEmitter<>(new FlussRecordDeserializer()),
                        fetcherManager);
        FlussHybridSnapshotLogSplit split = finishedSnapshotSplit();
        TablePath tablePath = split.getTablePath();

        try {
            reader.handleSourceEvents(
                    new TableSubscriptionEvent(
                            Collections.singleton(tablePath), Collections.emptyMap()));
            reader.addSplits(Collections.singletonList(split));
            reader.snapshotState(1L);
            assertThat(readerContext.getSentEvents())
                    .singleElement()
                    .isInstanceOf(FinishedKvSnapshotConsumeEvent.class);

            readerContext.clearSentEvents();
            reader.handleSourceEvents(
                    new TableSubscriptionEvent(
                            Collections.emptySet(), Collections.singletonMap(tablePath, 1L)));
            reader.onSplitFinished(
                    Collections.singletonMap(
                            split.splitId(), new FlussHybridSnapshotLogSplitState(split)));
            readerContext.clearSentEvents();

            reader.handleSourceEvents(
                    new TableSubscriptionEvent(
                            Collections.singleton(tablePath), Collections.emptyMap()));
            reader.addSplits(Collections.singletonList(split));
            reader.snapshotState(2L);

            assertThat(readerContext.getSentEvents())
                    .singleElement()
                    .isInstanceOfSatisfying(
                            FinishedKvSnapshotConsumeEvent.class,
                            event ->
                                    assertThat(event.getTableBuckets())
                                            .containsExactly(split.getTableBucket()));
        } finally {
            reader.close();
        }
    }

    @Test
    void testPhysicalInventoryDropsExpiredRestoredAndLateSplits() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue =
                new FutureCompletingBlockingQueue<>();
        TrackingFetcherManager fetcherManager = new TrackingFetcherManager(queue);
        FlussSourceReader<Event> reader = newReader(readerContext, queue, fetcherManager);
        FlussLogSplit expiredSplit = partitionSplit("expired", 11L);
        FlussLogSplit liveSplit = partitionSplit("live", 22L);

        try {
            reader.addSplits(Collections.singletonList(expiredSplit));
            reader.addSplits(Collections.singletonList(liveSplit));
            assertThat(reader.snapshotState(1L)).containsExactly(expiredSplit, liveSplit);
            assertThat(fetcherManager.addedSplits).isEmpty();

            reader.handleSourceEvents(
                    new TableSubscriptionEvent(
                            Collections.singleton(liveSplit.getTablePath()),
                            Collections.emptyMap(),
                            Collections.emptySet(),
                            Collections.singleton(liveSplit.getPhysicalTablePath())));

            assertThat(fetcherManager.addedSplits).containsExactly(liveSplit);
            assertThat(reader.snapshotState(2L)).containsExactly(liveSplit);
            assertThat(fetcherManager.removedTablePaths).isEmpty();
            assertThat(fetcherManager.removedPartitionPaths).isEmpty();

            reader.addSplits(Collections.singletonList(expiredSplit));
            assertThat(fetcherManager.addedSplits).containsExactly(liveSplit);
            assertThat(reader.snapshotState(3L)).containsExactly(liveSplit);
            assertThat(readerContext.getSentEvents()).isEmpty();
        } finally {
            reader.close();
        }
    }

    @Test
    void testExpiredActivePartitionLeavesCheckpointOnlyAfterFinishedSplit() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue =
                new FutureCompletingBlockingQueue<>();
        TrackingFetcherManager fetcherManager = new TrackingFetcherManager(queue);
        TrackingRecordEmitter recordEmitter = new TrackingRecordEmitter();
        FlussSourceReader<Event> reader =
                new FlussSourceReader<>(
                        queue,
                        readerContext,
                        new WrapperFlussMetricRegistry(
                                readerContext.metricGroup(), Collections.emptySet()),
                        recordEmitter,
                        fetcherManager);
        FlussLogSplit expiredSplit = partitionSplit("expired", 11L);
        FlussLogSplit liveSplit = partitionSplit("live", 22L);
        FlussLogSplit unpartitionedSplit =
                new FlussLogSplit(
                        PhysicalTablePath.of(TablePath.of("test_db", "unpartitioned")),
                        new TableBucket(2L, 0),
                        30L);
        Set<TablePath> tables =
                new HashSet<>(
                        Arrays.asList(liveSplit.getTablePath(), unpartitionedSplit.getTablePath()));

        try {
            reader.handleSourceEvents(
                    new TableSubscriptionEvent(
                            tables,
                            Collections.emptyMap(),
                            Collections.emptySet(),
                            new HashSet<>(
                                    Arrays.asList(
                                            expiredSplit.getPhysicalTablePath(),
                                            liveSplit.getPhysicalTablePath()))));
            reader.addSplits(Arrays.asList(expiredSplit, liveSplit, unpartitionedSplit));

            reader.handleSourceEvents(
                    new TableSubscriptionEvent(
                            tables,
                            Collections.emptyMap(),
                            Collections.emptySet(),
                            Collections.singleton(liveSplit.getPhysicalTablePath())));

            assertThat(fetcherManager.removedPartitionPaths)
                    .containsExactly(expiredSplit.getPhysicalTablePath());
            assertThat(fetcherManager.removedTablePaths).isEmpty();
            assertThat(recordEmitter.removedTables).isEmpty();
            assertThat(reader.snapshotState(1L))
                    .containsExactlyInAnyOrder(expiredSplit, liveSplit, unpartitionedSplit);

            RecordsBySplits.Builder<FlussSourceRecord> finished = new RecordsBySplits.Builder<>();
            finished.addFinishedSplit(expiredSplit.splitId());
            fetcherManager.getQueue().put(0, finished.build());
            reader.pollNext(new NoOpReaderOutput());

            assertThat(reader.snapshotState(2L))
                    .containsExactlyInAnyOrder(liveSplit, unpartitionedSplit);
            assertThat(readerContext.getSentEvents()).isEmpty();
        } finally {
            reader.close();
        }
    }

    @Test
    void testLegacyTableOnlySnapshotKeepsPartitionSplits() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue =
                new FutureCompletingBlockingQueue<>();
        TrackingFetcherManager fetcherManager = new TrackingFetcherManager(queue);
        FlussSourceReader<Event> reader = newReader(readerContext, queue, fetcherManager);
        FlussLogSplit split = partitionSplit("live", 22L);

        try {
            reader.addSplits(Collections.singletonList(split));
            reader.handleSourceEvents(
                    new TableSubscriptionEvent(
                            Collections.singleton(split.getTablePath()), Collections.emptyMap()));

            assertThat(fetcherManager.addedSplits).containsExactly(split);
            assertThat(fetcherManager.removedPartitionPaths).isEmpty();
            assertThat(reader.snapshotState(1L)).containsExactly(split);
        } finally {
            reader.close();
        }
    }

    private static FlussSourceReader<Event> newReader(
            TestingReaderContext readerContext,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue,
            TrackingFetcherManager fetcherManager) {
        return new FlussSourceReader<>(
                queue,
                readerContext,
                new WrapperFlussMetricRegistry(readerContext.metricGroup(), Collections.emptySet()),
                new FlussRecordEmitter<>(new FlussRecordDeserializer()),
                fetcherManager);
    }

    private static FlussSourceReader<Event> newReader(TestingReaderContext readerContext) {
        return new FlussSourceReader<>(
                new FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>>(),
                readerContext,
                new Configuration(),
                new WrapperFlussMetricRegistry(readerContext.metricGroup(), Collections.emptySet()),
                new FlussSourceReaderMetrics(readerContext.metricGroup()),
                new FlussRecordEmitter<>(new FlussRecordDeserializer()));
    }

    private static FlussLogSplit testSplit() {
        return new FlussLogSplit(
                PhysicalTablePath.of(TablePath.of("test_db", "test_table")),
                new TableBucket(1L, 0),
                0L);
    }

    private static FlussLogSplit partitionSplit(String partitionName, long partitionId) {
        return new FlussLogSplit(
                PhysicalTablePath.of(TablePath.of("test_db", "test_table"), partitionName),
                new TableBucket(1L, partitionId, 0),
                10L);
    }

    private static FlussHybridSnapshotLogSplit finishedSnapshotSplit() {
        return new FlussHybridSnapshotLogSplit(
                PhysicalTablePath.of(TablePath.of("test_db", "test_table")),
                new TableBucket(1L, 0),
                1L,
                0L,
                0L,
                true,
                null,
                null);
    }

    private static class TrackingFetcherManager extends FlussSourceFetcherManager {

        private Set<TablePath> removedTablePaths = Collections.emptySet();
        private Set<PhysicalTablePath> removedPartitionPaths = Collections.emptySet();
        private final List<FlussSplitBase> addedSplits = new ArrayList<>();

        private TrackingFetcherManager(
                FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> queue) {
            super(queue, () -> null);
        }

        @Override
        void removeTables(Set<TablePath> tablePaths) {
            removedTablePaths = tablePaths;
        }

        @Override
        void removePartitions(Set<PhysicalTablePath> partitionPaths) {
            removedPartitionPaths = partitionPaths;
        }

        @Override
        public void addSplits(List<FlussSplitBase> splits) {
            addedSplits.addAll(splits);
        }
    }

    private static class TrackingRecordEmitter extends FlussRecordEmitter<Event> {

        private final List<TablePath> removedTables = new ArrayList<>();

        private TrackingRecordEmitter() {
            super(new FlussRecordDeserializer());
        }

        @Override
        public void removeTable(TablePath tablePath) {
            removedTables.add(tablePath);
            super.removeTable(tablePath);
        }
    }

    private static class NoOpReaderOutput implements ReaderOutput<Event> {

        @Override
        public void collect(Event record) {}

        @Override
        public void collect(Event record, long timestamp) {}

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<Event> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }

    private static class TestingReaderContext implements SourceReaderContext {

        private final SourceReaderMetricGroup metricGroup;
        private final List<SourceEvent> sentEvents = new ArrayList<>();

        private TestingReaderContext() {
            MetricListener metricListener = new MetricListener();
            metricGroup = InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup());
        }

        @Override
        public SourceReaderMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public org.apache.flink.configuration.Configuration getConfiguration() {
            return new org.apache.flink.configuration.Configuration();
        }

        @Override
        public String getLocalHostName() {
            return "localhost";
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
            sentEvents.add(sourceEvent);
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return new UserCodeClassLoader() {
                @Override
                public ClassLoader asClassLoader() {
                    return FlussSourceReaderTest.class.getClassLoader();
                }

                @Override
                public void registerReleaseHookIfAbsent(
                        String releaseHookName, Runnable releaseHook) {}
            };
        }

        private List<SourceEvent> getSentEvents() {
            return sentEvents;
        }

        private void clearSentEvents() {
            sentEvents.clear();
        }
    }
}
