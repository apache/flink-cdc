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

package org.apache.flink.api.connector.source.ratelimit;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.InputStatus;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Tests for {@link RateLimitedSourceReaderAdapter}. */
class RateLimitedSourceReaderAdapterTest {

    private TrackingSourceReader delegate;
    private SourceReader<String, DummySplit> rateLimitedReader;

    @BeforeEach
    void setUp() {
        delegate = new TrackingSourceReader();
        rateLimitedReader = RateLimitedSourceReaderAdapter.wrapWithRateLimiter(delegate, 1000.0, 1);
    }

    @Test
    void testWrapWithRateLimiterReturnsDifferentInstance() {
        Assertions.assertThat(rateLimitedReader).isNotSameAs(delegate);
    }

    @Test
    void testStartDelegates() {
        rateLimitedReader.start();
        Assertions.assertThat(delegate.startCalled).isTrue();
    }

    @Test
    void testAddSplitsDelegates() {
        List<DummySplit> splits = Arrays.asList(new DummySplit("s1"), new DummySplit("s2"));
        rateLimitedReader.addSplits(splits);
        Assertions.assertThat(delegate.addedSplits).isEqualTo(splits);
    }

    @Test
    void testNotifyNoMoreSplitsDelegates() {
        rateLimitedReader.notifyNoMoreSplits();
        Assertions.assertThat(delegate.notifyNoMoreSplitsCalled).isTrue();
    }

    @Test
    void testHandleSourceEventsDelegates() {
        SourceEvent event = new DummySourceEvent();
        rateLimitedReader.handleSourceEvents(event);
        Assertions.assertThat(delegate.handledEvents).containsExactly(event);
    }

    @Test
    void testSnapshotStateDelegates() throws Exception {
        delegate.snapshotResult = Arrays.asList(new DummySplit("snap"));
        List<DummySplit> result = rateLimitedReader.snapshotState(42L);
        Assertions.assertThat(result).isEqualTo(delegate.snapshotResult);
        Assertions.assertThat(delegate.snapshotCheckpointId).isEqualTo(42L);
    }

    @Test
    void testNotifyCheckpointCompleteDelegates() throws Exception {
        rateLimitedReader.notifyCheckpointComplete(99L);
        Assertions.assertThat(delegate.completedCheckpointId).isEqualTo(99L);
    }

    @Test
    void testPauseOrResumeSplitsDelegates() {
        Collection<String> toPause = Arrays.asList("s1");
        Collection<String> toResume = Arrays.asList("s2");
        rateLimitedReader.pauseOrResumeSplits(toPause, toResume);
        Assertions.assertThat(delegate.pausedSplits).isEqualTo(toPause);
        Assertions.assertThat(delegate.resumedSplits).isEqualTo(toResume);
    }

    @Test
    void testCloseDelegates() throws Exception {
        rateLimitedReader.close();
        Assertions.assertThat(delegate.closeCalled).isTrue();
    }

    @Test
    void testIsAvailableReturnsFutureAndDoesNotBlock() {
        // isAvailable() should return a non-null future (may or may not be done)
        CompletableFuture<Void> future = rateLimitedReader.isAvailable();
        Assertions.assertThat(future).isNotNull();
    }

    @Test
    void testPollNextReturnsNothingAvailableWhenFutureNotCompleted() throws Exception {
        // Without calling isAvailable first, pollNext should return NOTHING_AVAILABLE
        InputStatus status = rateLimitedReader.pollNext(new DummyReaderOutput<>());
        Assertions.assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);
    }

    // ----------- helpers -----------

    private static class DummySplit implements SourceSplit {
        private final String id;

        DummySplit(String id) {
            this.id = id;
        }

        @Override
        public String splitId() {
            return id;
        }
    }

    private static class DummySourceEvent implements SourceEvent {}

    private static class DummyReaderOutput<T> implements ReaderOutput<T> {
        @Override
        public void collect(T record) {}

        @Override
        public void collect(T record, long timestamp) {}

        @Override
        public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public org.apache.flink.api.connector.source.SourceOutput<T> createOutputForSplit(
                String splitId) {
            return null;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }

    private static class TrackingSourceReader implements SourceReader<String, DummySplit> {
        boolean startCalled = false;
        boolean notifyNoMoreSplitsCalled = false;
        boolean closeCalled = false;
        long snapshotCheckpointId = -1L;
        long completedCheckpointId = -1L;
        List<DummySplit> addedSplits = new ArrayList<>();
        List<SourceEvent> handledEvents = new ArrayList<>();
        List<DummySplit> snapshotResult = new ArrayList<>();
        Collection<String> pausedSplits = null;
        Collection<String> resumedSplits = null;

        @Override
        public void start() {
            startCalled = true;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void addSplits(List<DummySplit> splits) {
            addedSplits.addAll(splits);
        }

        @Override
        public void notifyNoMoreSplits() {
            notifyNoMoreSplitsCalled = true;
        }

        @Override
        public void handleSourceEvents(SourceEvent sourceEvent) {
            handledEvents.add(sourceEvent);
        }

        @Override
        public List<DummySplit> snapshotState(long checkpointId) {
            snapshotCheckpointId = checkpointId;
            return snapshotResult;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            completedCheckpointId = checkpointId;
        }

        @Override
        public void pauseOrResumeSplits(
                Collection<String> splitsToPause, Collection<String> splitsToResume) {
            pausedSplits = splitsToPause;
            resumedSplits = splitsToResume;
        }

        @Override
        public void close() throws Exception {
            closeCalled = true;
        }
    }
}
