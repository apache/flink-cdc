/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.base.source.assigner;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import io.debezium.schema.DataCollectionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;

/** Assigner for Hybrid split which contains snapshot splits and stream splits. */
public class HybridSplitAssigner<ID extends DataCollectionId, S, C extends SourceConfig>
        implements SplitAssigner<ID, S, C> {

    private static final Logger LOG = LoggerFactory.getLogger(HybridSplitAssigner.class);

    protected final C sourceConfig;

    private boolean isStreamSplitAssigned;

    private Offset startupOffset;

    protected final DataSourceDialect<ID, S, C> dialect;

    protected final OffsetFactory offsetFactory;

    protected final SnapshotSplitAssigner<ID, S, C> snapshotSplitAssigner;

    public HybridSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            List<ID> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect<ID, S, C> dialect,
            OffsetFactory offsetFactory) {
        this(
                sourceConfig,
                dialect,
                new SnapshotSplitAssigner<>(
                        sourceConfig,
                        currentParallelism,
                        remainingTables,
                        isTableIdCaseSensitive,
                        dialect,
                        offsetFactory),
                offsetFactory,
                false,
                null);
    }

    public HybridSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState<ID, S> checkpoint,
            DataSourceDialect<ID, S, C> dialect,
            OffsetFactory offsetFactory) {
        this(
                sourceConfig,
                dialect,
                new SnapshotSplitAssigner<>(
                        sourceConfig,
                        currentParallelism,
                        checkpoint.getSnapshotPendingSplits(),
                        dialect,
                        offsetFactory),
                offsetFactory,
                checkpoint.isStreamSplitAssigned(),
                checkpoint.getStartupOffset());
    }

    private HybridSplitAssigner(
            C sourceConfig,
            DataSourceDialect<ID, S, C> dialect,
            SnapshotSplitAssigner<ID, S, C> snapshotSplitAssigner,
            OffsetFactory offsetFactory,
            boolean isStreamSplitAssigned,
            Offset startupOffset) {
        this.sourceConfig = sourceConfig;
        this.dialect = dialect;
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.offsetFactory = offsetFactory;
        this.isStreamSplitAssigned = isStreamSplitAssigned;
        this.startupOffset = startupOffset;
    }

    @Override
    public void open() {
        snapshotSplitAssigner.open();
        if (!isStreamSplitAssigned && startupOffset == null) {
            this.startupOffset = dialect.displayCurrentOffset(sourceConfig);
        }
    }

    @Override
    public Optional<SourceSplitBase<ID, S>> getNext() {
        if (snapshotSplitAssigner.noMoreSplits()) {
            // stream split assigning
            if (isStreamSplitAssigned) {
                // no more splits for the assigner
                return Optional.empty();
            } else if (snapshotSplitAssigner.isFinished()) {
                // we need to wait snapshot-assigner to be finished before
                // assigning the stream split. Otherwise, records emitted from stream split
                // might be out-of-order in terms of same primary key with snapshot splits.
                isStreamSplitAssigned = true;
                return Optional.of(createStreamSplit());
            } else {
                // stream split is not ready by now
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
    public List<FinishedSnapshotSplitInfo<ID>> getFinishedSplitInfos() {
        return snapshotSplitAssigner.getFinishedSplitInfos();
    }

    @Override
    public void onFinishedSplits(Map<String, Offset> splitFinishedOffsets) {
        snapshotSplitAssigner.onFinishedSplits(splitFinishedOffsets);
    }

    @Override
    public void addSplits(Collection<SourceSplitBase<ID, S>> splits) {
        List<SourceSplitBase<ID, S>> snapshotSplits = new ArrayList<>();
        for (SourceSplitBase<ID, S> split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                // we don't store the split, but will re-create stream split later
                isStreamSplitAssigned = false;
            }
        }
        snapshotSplitAssigner.addSplits(snapshotSplits);
    }

    @Override
    public PendingSplitsState<ID, S> snapshotState(long checkpointId) {
        return new HybridPendingSplitsState<>(
                snapshotSplitAssigner.snapshotState(checkpointId),
                isStreamSplitAssigned,
                startupOffset);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void close() {
        snapshotSplitAssigner.close();
    }

    // --------------------------------------------------------------------------------------------

    protected StreamSplit<ID, S> createStreamSplit() {
        final List<SnapshotSplit<ID, S>> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(SourceSplitBase::splitId))
                        .collect(Collectors.toList());

        Map<String, Offset> splitFinishedOffsets = snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo<ID>> finishedSnapshotSplitInfos = new ArrayList<>();

        Offset minOffset = null;
        for (SnapshotSplit<ID, S> split : assignedSnapshotSplit) {
            // find the min stream offset
            Offset offset = splitFinishedOffsets.get(split.splitId());
            if (minOffset == null || offset.isBefore(minOffset)) {
                minOffset = offset;
            }
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo<>(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            minOffset,
                            offsetFactory));
        }

        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them

        boolean divideMetaToGroups =
                finishedSnapshotSplitInfos.size() > sourceConfig.getSplitMetaGroupSize();
        return new StreamSplit<>(
                STREAM_SPLIT_ID,
                minOffset == null ? startupOffset : minOffset,
                offsetFactory.createNoStoppingOffset(),
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size());
    }

    protected boolean isStreamSplitAssigned() {
        return isStreamSplitAssigned;
    }

    protected Offset getStartupOffset() {
        return startupOffset;
    }
}
