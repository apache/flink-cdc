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
import org.apache.flink.cdc.connectors.fluss.sink.v2.metrics.WrapperFlussMetricRegistry;
import org.apache.flink.cdc.connectors.fluss.source.event.FinishedKvSnapshotConsumeEvent;
import org.apache.flink.cdc.connectors.fluss.source.event.TableRemovalAckEvent;
import org.apache.flink.cdc.connectors.fluss.source.event.TableSubscriptionEvent;
import org.apache.flink.cdc.connectors.fluss.source.metrics.FlussSourceReaderMetrics;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplitState;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplitState;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitState;
import org.apache.flink.cdc.source.SingleThreadMultiplexSourceReaderBaseAdapter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A generic {@link org.apache.flink.api.connector.source.SourceReader} for Fluss, built on top of
 * Flink's {@link SingleThreadMultiplexSourceReaderBase}. This delegates the low-level split
 * management, fetcher lifecycle, and availability tracking to the base class, and only provides
 * Fluss-specific initialization logic.
 *
 * <p>The output type {@code T} is determined by the provided {@link FlussDeserializer}, making this
 * reader reusable for different output types.
 *
 * @param <T> The type of output records produced by this reader.
 */
public class FlussSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBaseAdapter<
                FlussSourceRecord, T, FlussSplitBase, FlussSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlussSourceReader.class);

    private final FlussRecordEmitter<T> recordEmitter;
    private final WrapperFlussMetricRegistry metricRegistry;
    private final SourceReaderContext readerContext;
    private final Set<TableBucket> reportedFinishedSnapshotBuckets;
    private final FlussSourceFetcherManager fetcherManager;
    private final List<FlussSplitBase> stagedSplits;
    private final Map<String, FlussSplitBase> activeSplits;
    private final Map<TablePath, Long> pendingRemovalRequests;
    private final Set<TablePath> subscribedTablePaths;
    private boolean receivedSubscriptionSnapshot;

    public FlussSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> elementsQueue,
            SourceReaderContext readerContext,
            org.apache.fluss.config.Configuration flussConfig,
            WrapperFlussMetricRegistry metricRegistry,
            FlussSourceReaderMetrics sourceReaderMetrics,
            FlussRecordEmitter<T> recordEmitter) {
        this(
                elementsQueue,
                readerContext,
                metricRegistry,
                recordEmitter,
                new FlussSourceFetcherManager(
                        elementsQueue,
                        () ->
                                new FlussSplitReader(
                                        flussConfig, metricRegistry, sourceReaderMetrics)));
    }

    FlussSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> elementsQueue,
            SourceReaderContext readerContext,
            WrapperFlussMetricRegistry metricRegistry,
            FlussRecordEmitter<T> recordEmitter,
            FlussSourceFetcherManager fetcherManager) {
        super(elementsQueue, fetcherManager, recordEmitter, new Configuration(), readerContext);
        this.recordEmitter = recordEmitter;
        this.metricRegistry = metricRegistry;
        this.readerContext = readerContext;
        this.reportedFinishedSnapshotBuckets = new HashSet<>();
        this.fetcherManager = fetcherManager;
        this.stagedSplits = new ArrayList<>();
        this.activeSplits = new HashMap<>();
        this.pendingRemovalRequests = new HashMap<>();
        this.subscribedTablePaths = new HashSet<>();
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            metricRegistry.close();
        }
    }

    @Override
    protected FlussSplitState initializedState(FlussSplitBase split) {
        // Restore deserializer schema caches from the recovered split (like MySQL's applySplit)
        recordEmitter.applySplit(split);
        activeSplits.put(split.splitId(), split);
        if (split.isHybridSnapshotLogSplit()) {
            return new FlussHybridSnapshotLogSplitState(split.asHybridSnapshotLogSplit());
        } else if (split.isLogSplit()) {
            return new FlussLogSplitState(split.asLogSplit());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported split type: " + split.getClass().getSimpleName());
        }
    }

    @Override
    protected FlussSplitBase toSplitType(String splitId, FlussSplitState splitState) {
        return splitState.toFlussSplit();
    }

    @Override
    public List<FlussSplitBase> snapshotState(long checkpointId) {
        List<FlussSplitBase> splits = super.snapshotState(checkpointId);
        Set<TableBucket> finishedSnapshotBuckets = new HashSet<>();
        for (FlussSplitBase split : splits) {
            if (split.isHybridSnapshotLogSplit()
                    && split.asHybridSnapshotLogSplit().isSnapshotFinished()
                    && !reportedFinishedSnapshotBuckets.contains(split.getTableBucket())) {
                finishedSnapshotBuckets.add(split.getTableBucket());
            }
        }

        if (!finishedSnapshotBuckets.isEmpty()) {
            LOG.info(
                    "Finished reading KV snapshots for buckets {} at checkpoint {}.",
                    finishedSnapshotBuckets,
                    checkpointId);
            readerContext.sendSourceEventToCoordinator(
                    new FinishedKvSnapshotConsumeEvent(checkpointId, finishedSnapshotBuckets));
            reportedFinishedSnapshotBuckets.addAll(finishedSnapshotBuckets);
        }
        splits.addAll(stagedSplits);
        return splits;
    }

    @Override
    public void addSplits(List<FlussSplitBase> splits) {
        if (!receivedSubscriptionSnapshot) {
            stagedSplits.addAll(splits);
            return;
        }
        activateSplits(splits);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (!(sourceEvent instanceof TableSubscriptionEvent)) {
            super.handleSourceEvents(sourceEvent);
            return;
        }

        TableSubscriptionEvent event = (TableSubscriptionEvent) sourceEvent;
        boolean firstSubscriptionSnapshot = !receivedSubscriptionSnapshot;
        receivedSubscriptionSnapshot = true;
        subscribedTablePaths.clear();
        subscribedTablePaths.addAll(event.getSubscribedTablePaths());
        pendingRemovalRequests.clear();
        pendingRemovalRequests.putAll(event.getPendingRemovalRequests());

        Set<TablePath> activeRemovalTablePaths =
                activeSplits.values().stream()
                        .filter(split -> !isActive(split.getTablePath()))
                        .map(FlussSplitBase::getTablePath)
                        .collect(Collectors.toSet());
        activeSplits.values().stream()
                .filter(split -> activeRemovalTablePaths.contains(split.getTablePath()))
                .map(FlussSplitBase::getTableBucket)
                .forEach(reportedFinishedSnapshotBuckets::remove);

        for (TablePath tablePath : activeRemovalTablePaths) {
            recordEmitter.removeTable(tablePath);
        }
        if (!activeRemovalTablePaths.isEmpty()) {
            fetcherManager.removeTables(activeRemovalTablePaths);
        }

        List<FlussSplitBase> effectiveStagedSplits = new ArrayList<>();
        for (FlussSplitBase split : stagedSplits) {
            if (isActive(split.getTablePath())
                    && (!firstSubscriptionSnapshot
                            || !event.getFencedTablePaths().contains(split.getTablePath()))) {
                effectiveStagedSplits.add(split);
            } else {
                reportedFinishedSnapshotBuckets.remove(split.getTableBucket());
            }
        }
        stagedSplits.clear();
        activateSplits(effectiveStagedSplits);
        acknowledgeCompletedRemovals();
    }

    @Override
    protected void onSplitFinished(Map<String, FlussSplitState> finishedSplitIds) {
        // Fluss source is continuous and unbounded; splits should not normally finish.
        LOG.info("Splits finished: {}", finishedSplitIds.keySet());
        finishedSplitIds.keySet().forEach(activeSplits::remove);
        acknowledgeCompletedRemovals();
    }

    private void activateSplits(List<FlussSplitBase> splits) {
        List<FlussSplitBase> activeSplits = new ArrayList<>();
        for (FlussSplitBase split : splits) {
            if (isActive(split.getTablePath())) {
                activeSplits.add(split);
            }
        }
        if (!activeSplits.isEmpty()) {
            super.addSplits(activeSplits);
        }
    }

    private boolean isActive(TablePath tablePath) {
        return subscribedTablePaths.contains(tablePath)
                && !pendingRemovalRequests.containsKey(tablePath);
    }

    private void acknowledgeCompletedRemovals() {
        Map<TablePath, Long> completed = new HashMap<>();
        for (Map.Entry<TablePath, Long> entry : pendingRemovalRequests.entrySet()) {
            if (activeSplits.values().stream()
                            .noneMatch(split -> split.getTablePath().equals(entry.getKey()))
                    && stagedSplits.stream()
                            .noneMatch(split -> split.getTablePath().equals(entry.getKey()))) {
                completed.put(entry.getKey(), entry.getValue());
            }
        }
        if (!completed.isEmpty()) {
            readerContext.sendSourceEventToCoordinator(new TableRemovalAckEvent(completed));
        }
    }
}
