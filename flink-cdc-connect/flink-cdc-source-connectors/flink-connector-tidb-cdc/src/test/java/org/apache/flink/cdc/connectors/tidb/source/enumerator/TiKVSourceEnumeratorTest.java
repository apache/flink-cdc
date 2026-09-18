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

package org.apache.flink.cdc.connectors.tidb.source.enumerator;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.cdc.connectors.tidb.source.split.TiKVKeyRangeSplit;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/** Tests for {@link TiKVSourceEnumerator}. */
class TiKVSourceEnumeratorTest {

    @Test
    void restoreSkipsRegionDiscovery() throws Exception {
        RecordingEnumeratorContext context = new RecordingEnumeratorContext(2);
        List<TiKVKeyRangeSplit> unassigned = Arrays.asList(split("tidb-0"), split("tidb-1"));
        TiKVSourceEnumerator enumerator =
                TiKVSourceEnumerator.forRestoredSplits(context, unassigned, 2);

        enumerator.start();

        Assertions.assertThat(enumerator.isEnumerated()).isTrue();
        Assertions.assertThat(enumerator.getUnassignedSplits()).hasSize(2);
    }

    @Test
    void assignsSplitMatchingSubtaskId() throws Exception {
        RecordingEnumeratorContext context = new RecordingEnumeratorContext(2);
        context.registerReader(0);
        context.registerReader(1);
        List<TiKVKeyRangeSplit> unassigned =
                new ArrayList<>(Arrays.asList(split("tidb-0"), split("tidb-1")));
        TiKVSourceEnumerator enumerator =
                TiKVSourceEnumerator.forRestoredSplits(context, unassigned, 2);
        enumerator.start();

        enumerator.handleSplitRequest(1, "host");
        enumerator.handleSplitRequest(0, "host");

        Assertions.assertThat(context.assignmentOf(1).splitId()).isEqualTo("tidb-1");
        Assertions.assertThat(context.assignmentOf(0).splitId()).isEqualTo("tidb-0");
        Assertions.assertThat(enumerator.getUnassignedSplits()).isEmpty();
    }

    @Test
    void restoreDoesNotReassignSplitAlreadyOwnedByReader() throws Exception {
        RecordingEnumeratorContext context = new RecordingEnumeratorContext(2);
        context.registerReader(0);
        // tidb-0 was already checkpointed on reader 0; enumerator only keeps tidb-1.
        TiKVSourceEnumerator enumerator =
                TiKVSourceEnumerator.forRestoredSplits(
                        context, new ArrayList<>(Arrays.asList(split("tidb-1"))), 2);
        enumerator.start();

        enumerator.handleSplitRequest(0, "host");

        Assertions.assertThat(context.assignments).doesNotContainKey(0);
        Assertions.assertThat(context.noMoreSplits).contains(0);
        Assertions.assertThat(enumerator.getUnassignedSplits()).hasSize(1);
    }

    @Test
    void addSplitsBackRequeuesOriginalSplit() throws Exception {
        RecordingEnumeratorContext context = new RecordingEnumeratorContext(1);
        context.registerReader(0);
        TiKVKeyRangeSplit split = split("tidb-0");
        TiKVSourceEnumerator enumerator =
                TiKVSourceEnumerator.forRestoredSplits(
                        context, new ArrayList<>(Arrays.asList(split)), 1);
        enumerator.start();
        enumerator.handleSplitRequest(0, "host");
        Assertions.assertThat(enumerator.getUnassignedSplits()).isEmpty();

        enumerator.addSplitsBack(Arrays.asList(split.withResolvedTs(128L)), 0);
        enumerator.handleSplitRequest(0, "host");

        Assertions.assertThat(context.assignmentOf(0).getResolvedTs()).isEqualTo(128L);
    }

    @Test
    void snapshotStateKeepsEnumeratedFlag() throws Exception {
        RecordingEnumeratorContext context = new RecordingEnumeratorContext(1);
        TiKVSourceEnumerator enumerator =
                TiKVSourceEnumerator.forRestoredSplits(
                        context, new ArrayList<>(Arrays.asList(split("tidb-0"))), 1);
        enumerator.start();

        TiKVEnumeratorState state = enumerator.snapshotState(1L);
        Assertions.assertThat(state.isEnumerated()).isTrue();
        Assertions.assertThat(state.getParallelism()).isEqualTo(1);
        Assertions.assertThat(state.getUnassignedSplits()).hasSize(1);
    }

    @Test
    void rejectsParallelismChangeOnRestore() {
        RecordingEnumeratorContext context = new RecordingEnumeratorContext(4);
        TiKVSourceEnumerator enumerator =
                TiKVSourceEnumerator.forRestoredSplits(context, new ArrayList<>(), 2);

        Assertions.assertThatThrownBy(enumerator::start)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("does not support changing source parallelism");
    }

    private static TiKVKeyRangeSplit split(String splitId) {
        return new TiKVKeyRangeSplit(splitId, new byte[] {1}, new byte[] {2}, -1L);
    }

    private static final class RecordingEnumeratorContext
            implements SplitEnumeratorContext<TiKVKeyRangeSplit> {

        private final int parallelism;
        private final Map<Integer, ReaderInfo> readers = new HashMap<>();
        private final Map<Integer, TiKVKeyRangeSplit> assignments = new HashMap<>();
        private final Set<Integer> noMoreSplits = new HashSet<>();

        private RecordingEnumeratorContext(int parallelism) {
            this.parallelism = parallelism;
        }

        void registerReader(int subtaskId) {
            readers.put(subtaskId, new ReaderInfo(subtaskId, "host-" + subtaskId));
        }

        TiKVKeyRangeSplit assignmentOf(int subtaskId) {
            return assignments.get(subtaskId);
        }

        @Override
        public SplitEnumeratorMetricGroup metricGroup() {
            return null;
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            return readers;
        }

        @Override
        public void assignSplits(SplitsAssignment<TiKVKeyRangeSplit> newSplitAssignments) {
            newSplitAssignments
                    .assignment()
                    .forEach(
                            (subtask, splits) -> {
                                if (!splits.isEmpty()) {
                                    assignments.put(subtask, splits.get(0));
                                }
                            });
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplits.add(subtask);
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {}

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelay,
                long period) {}

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            runnable.run();
        }
    }
}
