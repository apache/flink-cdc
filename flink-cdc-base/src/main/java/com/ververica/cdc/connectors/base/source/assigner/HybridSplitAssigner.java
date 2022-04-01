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
import io.debezium.relational.TableId;
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

/** Assigner for Hybrid split which contains snapshot splits and stream splits. */
public class HybridSplitAssigner implements SplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(HybridSplitAssigner.class);
    private static final String BINLOG_SPLIT_ID = "binlog-split";

    private final int splitMetaGroupSize;

    private boolean isStreamSplitAssigned;

    private final SnapshotSplitAssigner snapshotSplitAssigner;

    private OffsetFactory offsetFactory;

    public HybridSplitAssigner(
            SourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect dialect,
            OffsetFactory offsetFactory) {
        this(
                new SnapshotSplitAssigner(
                        sourceConfig,
                        currentParallelism,
                        remainingTables,
                        isTableIdCaseSensitive,
                        dialect,
                        offsetFactory),
                false,
                sourceConfig.getSplitMetaGroupSize());
        this.offsetFactory = offsetFactory;
    }

    public HybridSplitAssigner(
            SourceConfig sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState checkpoint,
            DataSourceDialect dialect,
            OffsetFactory offsetFactory) {
        this(
                new SnapshotSplitAssigner(
                        sourceConfig,
                        currentParallelism,
                        checkpoint.getSnapshotPendingSplits(),
                        dialect,
                        offsetFactory),
                checkpoint.isStreamSplitAssigned(),
                sourceConfig.getSplitMetaGroupSize());
    }

    private HybridSplitAssigner(
            SnapshotSplitAssigner snapshotSplitAssigner,
            boolean isStreamSplitAssigned,
            int splitMetaGroupSize) {
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.isStreamSplitAssigned = isStreamSplitAssigned;
        this.splitMetaGroupSize = splitMetaGroupSize;
    }

    @Override
    public void open() {
        snapshotSplitAssigner.open();
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (snapshotSplitAssigner.noMoreSplits()) {
            // binlog split assigning
            if (isStreamSplitAssigned) {
                // no more splits for the assigner
                return Optional.empty();
            } else if (snapshotSplitAssigner.isFinished()) {
                // we need to wait snapshot-assigner to be finished before
                // assigning the binlog split. Otherwise, records emitted from binlog split
                // might be out-of-order in terms of same primary key with snapshot splits.
                isStreamSplitAssigned = true;
                return Optional.of(createStreamSplit());
            } else {
                // binlog split is not ready by now
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
                // we don't store the split, but will re-create binlog split later
                isStreamSplitAssigned = false;
            }
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
    public void close() {
        snapshotSplitAssigner.close();
    }

    // --------------------------------------------------------------------------------------------

    public StreamSplit createStreamSplit() {
        final List<SnapshotSplit> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(SourceSplitBase::splitId))
                        .collect(Collectors.toList());

        Map<String, Offset> splitFinishedOffsets = snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();

        Offset minBinlogOffset = null;
        for (SnapshotSplit split : assignedSnapshotSplit) {
            // find the min binlog offset
            Offset binlogOffset = splitFinishedOffsets.get(split.splitId());
            if (minBinlogOffset == null || binlogOffset.isBefore(minBinlogOffset)) {
                minBinlogOffset = binlogOffset;
            }
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            binlogOffset,
                            offsetFactory));
        }

        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them

        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
        return new StreamSplit(
                BINLOG_SPLIT_ID,
                minBinlogOffset == null ? offsetFactory.createInitialOffset() : minBinlogOffset,
                offsetFactory.createNoStoppingOffset(),
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size());
    }
}
