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

import org.apache.flink.api.connector.source.SourceEvent;
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
import java.util.Collections;
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
        public void addSplits(List<FlussSplitBase> splits) {
            addedSplits.addAll(splits);
        }
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
