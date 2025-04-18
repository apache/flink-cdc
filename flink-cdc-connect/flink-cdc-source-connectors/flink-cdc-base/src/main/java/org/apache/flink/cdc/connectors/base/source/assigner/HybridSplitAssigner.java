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

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceEnumeratorMetrics;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isInitialAssigningFinished;
import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigningFinished;
import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;

/** Assigner for Hybrid split which contains snapshot splits and stream splits. */
public class HybridSplitAssigner<C extends SourceConfig> implements SplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(HybridSplitAssigner.class);
    private static final String STREAM_SPLIT_ID = "stream-split";

    private final int splitMetaGroupSize;
    private final C sourceConfig;

    private boolean isStreamSplitAssigned;

    private final SnapshotSplitAssigner<C> snapshotSplitAssigner;

    private final OffsetFactory offsetFactory;

    private final SplitEnumeratorContext<? extends SourceSplit> enumeratorContext;
    private SourceEnumeratorMetrics enumeratorMetrics;

    public HybridSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        this(
                sourceConfig,
                new SnapshotSplitAssigner<>(
                        sourceConfig,
                        currentParallelism,
                        remainingTables,
                        isTableIdCaseSensitive,
                        dialect,
                        offsetFactory),
                false,
                sourceConfig.getSplitMetaGroupSize(),
                offsetFactory,
                enumeratorContext);
    }

    public HybridSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState checkpoint,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        this(
                sourceConfig,
                new SnapshotSplitAssigner<>(
                        sourceConfig,
                        currentParallelism,
                        checkpoint.getSnapshotPendingSplits(),
                        dialect,
                        offsetFactory),
                checkpoint.isStreamSplitAssigned(),
                sourceConfig.getSplitMetaGroupSize(),
                offsetFactory,
                enumeratorContext);
    }

    private HybridSplitAssigner(
            C sourceConfig,
            SnapshotSplitAssigner<C> snapshotSplitAssigner,
            boolean isStreamSplitAssigned,
            int splitMetaGroupSize,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        this.sourceConfig = sourceConfig;
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.isStreamSplitAssigned = isStreamSplitAssigned;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.offsetFactory = offsetFactory;
        this.enumeratorContext = enumeratorContext;
    }

    @Override
    public void open() {
        this.enumeratorMetrics = new SourceEnumeratorMetrics(enumeratorContext.metricGroup());

        if (isStreamSplitAssigned) {
            enumeratorMetrics.enterStreamReading();
        } else {
            enumeratorMetrics.exitStreamReading();
        }

        snapshotSplitAssigner.open();
        // init enumerator metrics
        snapshotSplitAssigner.initEnumeratorMetrics(enumeratorMetrics);
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (isNewlyAddedAssigningSnapshotFinished(getAssignerStatus())) {
            // do not assign split until the adding table process finished
            return Optional.empty();
        }
        if (snapshotSplitAssigner.noMoreSplits()) {
            enumeratorMetrics.exitSnapshotPhase();
            // stream split assigning
            if (isStreamSplitAssigned) {
                // no more splits for the assigner
                LOG.trace(
                        "No more splits for the SnapshotSplitAssigner. StreamSplit is already assigned.");
                return Optional.empty();
            } else if (isInitialAssigningFinished(snapshotSplitAssigner.getAssignerStatus())) {
                // we need to wait snapshot-assigner to be finished before
                // assigning the stream split. Otherwise, records emitted from stream split
                // might be out-of-order in terms of same primary key with snapshot splits.
                isStreamSplitAssigned = true;
                enumeratorMetrics.enterStreamReading();
                StreamSplit streamSplit = createStreamSplit();
                LOG.trace(
                        "SnapshotSplitAssigner is finished: creating a new stream split {}",
                        streamSplit);
                return Optional.of(streamSplit);
            } else if (isNewlyAddedAssigningFinished(snapshotSplitAssigner.getAssignerStatus())) {
                // do not need to create stream split, but send event to wake up the binlog reader
                isStreamSplitAssigned = true;
                enumeratorMetrics.enterStreamReading();
                return Optional.empty();
            } else {
                // stream split is not ready by now
                LOG.trace(
                        "Waiting for SnapshotSplitAssigner to be finished before assigning a new stream split.");
                return Optional.empty();
            }
        } else {
            // snapshot assigner still have remaining splits, assign split from it
            return snapshotSplitAssigner.getNext();
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return snapshotSplitAssigner.waitingForFinishedSplits();
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        return snapshotSplitAssigner.getFinishedSplitInfos();
    }

    @Override
    public void onFinishedSplits(Map<String, Offset> splitFinishedOffsets) {
        snapshotSplitAssigner.onFinishedSplits(splitFinishedOffsets);
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        List<SourceSplitBase> snapshotSplits = new ArrayList<>();
        for (SourceSplitBase split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                // we don't store the split, but will re-create stream split later
                isStreamSplitAssigned = false;
            }
        }
        if (!snapshotSplits.isEmpty()) {
            enumeratorMetrics.exitStreamReading();
        }
        snapshotSplitAssigner.addSplits(snapshotSplits);
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new HybridPendingSplitsState(
                snapshotSplitAssigner.snapshotState(checkpointId), isStreamSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return snapshotSplitAssigner.getAssignerStatus();
    }

    @Override
    public void startAssignNewlyAddedTables() {
        snapshotSplitAssigner.startAssignNewlyAddedTables();
    }

    @Override
    public void onStreamSplitUpdated() {
        snapshotSplitAssigner.onStreamSplitUpdated();
    }

    @Override
    public boolean noMoreSplits() {
        return snapshotSplitAssigner.noMoreSplits() && isStreamSplitAssigned;
    }

    @Override
    public void close() throws IOException {
        snapshotSplitAssigner.close();
    }

    // --------------------------------------------------------------------------------------------

    public StreamSplit createStreamSplit() {
        final List<SchemalessSnapshotSplit> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(SourceSplitBase::splitId))
                        .collect(Collectors.toList());

        Map<String, Offset> splitFinishedOffsets = snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();

        Offset minOffset = null, maxOffset = null;
        for (SchemalessSnapshotSplit split : assignedSnapshotSplit) {
            // find the min and max offset of change log
            Offset changeLogOffset = splitFinishedOffsets.get(split.splitId());
            if (minOffset == null || changeLogOffset.isBefore(minOffset)) {
                minOffset = changeLogOffset;
            }
            if (maxOffset == null || changeLogOffset.isAfter(maxOffset)) {
                maxOffset = changeLogOffset;
            }

            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            changeLogOffset,
                            offsetFactory));
        }

        // If the source is running in snapshot mode, we use the highest watermark among
        // snapshot splits as the ending offset to provide a consistent snapshot view at the moment
        // of high watermark.
        Offset stoppingOffset = offsetFactory.createNoStoppingOffset();
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            stoppingOffset = maxOffset;
        }

        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them
        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
        return new StreamSplit(
                STREAM_SPLIT_ID,
                minOffset == null ? offsetFactory.createInitialOffset() : minOffset,
                stoppingOffset,
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size(),
                false,
                true);
    }
}
