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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.core.io.InputStatus;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Compatibility adapter for Flink 2.2. Wraps a {@link SourceReader} with rate limiting.
 *
 * <p>Unlike the built-in {@link RateLimitedSourceReader}, this adapter correctly delegates all
 * methods including {@link SourceReader#handleSourceEvents(SourceEvent)}, which is critical for
 * connectors (e.g. MySQL CDC) that rely on source events for split coordination.
 */
@Internal
public class RateLimitedSourceReaderAdapter {

    /**
     * Wraps the given source reader with rate limiting.
     *
     * @param sourceReader the source reader to wrap
     * @param recordsPerSecond the desired rate limit in records per second
     * @param parallelism the current parallelism
     * @return a rate-limited source reader that delegates all methods including handleSourceEvents
     */
    public static <E, SplitT extends SourceSplit> SourceReader<E, SplitT> wrapWithRateLimiter(
            SourceReader<E, SplitT> sourceReader, double recordsPerSecond, int parallelism) {
        RateLimiter<SplitT> rateLimiter =
                RateLimiterStrategy.<SplitT>perSecond(recordsPerSecond)
                        .createRateLimiter(parallelism);
        return new DelegatingRateLimitedSourceReader<>(sourceReader, rateLimiter);
    }

    /**
     * A rate-limited source reader that delegates all methods to the wrapped reader, including
     * {@link #handleSourceEvents(SourceEvent)}.
     */
    private static class DelegatingRateLimitedSourceReader<E, SplitT extends SourceSplit>
            implements SourceReader<E, SplitT> {

        private final SourceReader<E, SplitT> sourceReader;
        private final RateLimiter<SplitT> rateLimiter;
        private CompletableFuture<Void> availabilityFuture = null;

        DelegatingRateLimitedSourceReader(
                SourceReader<E, SplitT> sourceReader, RateLimiter<SplitT> rateLimiter) {
            this.sourceReader = checkNotNull(sourceReader);
            this.rateLimiter = checkNotNull(rateLimiter);
        }

        @Override
        public void start() {
            sourceReader.start();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<E> output) throws Exception {
            if (availabilityFuture == null) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            availabilityFuture = null;
            final InputStatus inputStatus = sourceReader.pollNext(output);
            if (inputStatus == InputStatus.MORE_AVAILABLE) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            return inputStatus;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            if (availabilityFuture == null) {
                availabilityFuture =
                        rateLimiter
                                .acquire()
                                .toCompletableFuture()
                                .thenCombine(sourceReader.isAvailable(), (l, r) -> null);
            }
            return availabilityFuture;
        }

        @Override
        public void addSplits(List<SplitT> splits) {
            sourceReader.addSplits(splits);
        }

        @Override
        public void notifyNoMoreSplits() {
            sourceReader.notifyNoMoreSplits();
        }

        @Override
        public void handleSourceEvents(SourceEvent sourceEvent) {
            sourceReader.handleSourceEvents(sourceEvent);
        }

        @Override
        public List<SplitT> snapshotState(long checkpointId) {
            return sourceReader.snapshotState(checkpointId);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            rateLimiter.notifyCheckpointComplete(checkpointId);
            sourceReader.notifyCheckpointComplete(checkpointId);
        }

        @Override
        public void pauseOrResumeSplits(
                Collection<String> splitsToPause, Collection<String> splitsToResume) {
            sourceReader.pauseOrResumeSplits(splitsToPause, splitsToResume);
        }

        @Override
        public void close() throws Exception {
            sourceReader.close();
        }
    }
}
