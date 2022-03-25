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

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Assigner for snapshot split. */
public class SnapshotSplitAssigner implements SplitAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotSplitAssigner.class);

    private final List<TableId> alreadyProcessedTables;
    private final List<SnapshotSplit> remainingSplits;
    private final Map<String, SnapshotSplit> assignedSplits;
    private final Map<String, Offset> splitFinishedOffsets;
    private boolean assignerFinished;

    private final SourceConfig sourceConfig;
    private final int currentParallelism;
    private final LinkedList<TableId> remainingTables;
    private final boolean isRemainingTablesCheckpointed;

    private ChunkSplitter chunkSplitter;
    private boolean isTableIdCaseSensitive;

    @Nullable private Long checkpointIdToFinish;
    private final DataSourceDialect dialect;
    private OffsetFactory offsetFactory;

    public SnapshotSplitAssigner(
            SourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect dialect,
            OffsetFactory offsetFactory) {
        this(
                sourceConfig,
                currentParallelism,
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>(),
                new HashMap<>(),
                false,
                remainingTables,
                isTableIdCaseSensitive,
                true,
                dialect,
                offsetFactory);
    }

    public SnapshotSplitAssigner(
            SourceConfig sourceConfig,
            int currentParallelism,
            SnapshotPendingSplitsState checkpoint,
            DataSourceDialect dialect,
            OffsetFactory offsetFactory) {
        this(
                sourceConfig,
                currentParallelism,
                checkpoint.getAlreadyProcessedTables(),
                checkpoint.getRemainingSplits(),
                checkpoint.getAssignedSplits(),
                checkpoint.getSplitFinishedOffsets(),
                checkpoint.isAssignerFinished(),
                checkpoint.getRemainingTables(),
                checkpoint.isTableIdCaseSensitive(),
                checkpoint.isRemainingTablesCheckpointed(),
                dialect,
                offsetFactory);
    }

    private SnapshotSplitAssigner(
            SourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> alreadyProcessedTables,
            List<SnapshotSplit> remainingSplits,
            Map<String, SnapshotSplit> assignedSplits,
            Map<String, Offset> splitFinishedOffsets,
            boolean assignerFinished,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            boolean isRemainingTablesCheckpointed,
            DataSourceDialect dialect,
            OffsetFactory offsetFactory) {
        this.sourceConfig = sourceConfig;
        this.currentParallelism = currentParallelism;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.assignedSplits = assignedSplits;
        this.splitFinishedOffsets = splitFinishedOffsets;
        this.assignerFinished = assignerFinished;
        this.remainingTables = new LinkedList<>(remainingTables);
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.dialect = dialect;
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void open() {
        chunkSplitter = dialect.createChunkSplitter(sourceConfig);

        // the legacy state didn't snapshot remaining tables, discovery remaining table here
        if (!isRemainingTablesCheckpointed && !assignerFinished) {
            try {
                final List<TableId> discoverTables = dialect.discoverDataCollections(sourceConfig);
                discoverTables.removeAll(alreadyProcessedTables);
                this.remainingTables.addAll(discoverTables);
                this.isTableIdCaseSensitive = dialect.isDataCollectionIdCaseSensitive(sourceConfig);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover remaining tables to capture", e);
            }
        }
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        if (!remainingSplits.isEmpty()) {
            // return remaining splits firstly
            Iterator<SnapshotSplit> iterator = remainingSplits.iterator();
            SnapshotSplit split = iterator.next();
            iterator.remove();
            assignedSplits.put(split.splitId(), split);
            return Optional.of(split);
        } else {
            // it's turn for new table
            TableId nextTable = remainingTables.pollFirst();
            if (nextTable != null) {
                // split the given table into chunks (snapshot splits)
                Collection<SnapshotSplit> splits = chunkSplitter.generateSplits(nextTable);
                remainingSplits.addAll(splits);
                alreadyProcessedTables.add(nextTable);
                return getNext();
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return !allSplitsFinished();
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        if (waitingForFinishedSplits()) {
            LOG.error(
                    "The assigner is not ready to offer finished split information, this should not be called");
            throw new FlinkRuntimeException(
                    "The assigner is not ready to offer finished split information, this should not be called");
        }
        final List<SnapshotSplit> assignedSnapshotSplit =
                assignedSplits.values().stream()
                        .sorted(Comparator.comparing(SourceSplitBase::splitId))
                        .collect(Collectors.toList());
        List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        for (SnapshotSplit split : assignedSnapshotSplit) {
            Offset binlogOffset = splitFinishedOffsets.get(split.splitId());
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            binlogOffset,
                            offsetFactory));
        }
        return finishedSnapshotSplitInfos;
    }

    @Override
    public void onFinishedSplits(Map<String, Offset> splitFinishedOffsets) {
        this.splitFinishedOffsets.putAll(splitFinishedOffsets);
        if (allSplitsFinished()) {
            // Skip the waiting checkpoint when current parallelism is 1 which means we do not need
            // to care about the global output data order of snapshot splits and binlog split.
            if (currentParallelism == 1) {
                assignerFinished = true;
                LOG.info(
                        "Snapshot split assigner received all splits finished and the job parallelism is 1, snapshot split assigner is turn into finished status.");

            } else {
                LOG.info(
                        "Snapshot split assigner received all splits finished, waiting for a complete checkpoint to mark the assigner finished.");
            }
        }
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        for (SourceSplitBase split : splits) {
            remainingSplits.add(split.asSnapshotSplit());
            // we should remove the add-backed splits from the assigned list,
            // because they are failed
            assignedSplits.remove(split.splitId());
            splitFinishedOffsets.remove(split.splitId());
        }
    }

    @Override
    public SnapshotPendingSplitsState snapshotState(long checkpointId) {
        SnapshotPendingSplitsState state =
                new SnapshotPendingSplitsState(
                        alreadyProcessedTables,
                        remainingSplits,
                        assignedSplits,
                        splitFinishedOffsets,
                        assignerFinished,
                        remainingTables,
                        isTableIdCaseSensitive,
                        true);
        // we need a complete checkpoint before mark this assigner to be finished, to wait for all
        // records of snapshot splits are completely processed
        if (checkpointIdToFinish == null && !assignerFinished && allSplitsFinished()) {
            checkpointIdToFinish = checkpointId;
        }
        return state;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // we have waited for at-least one complete checkpoint after all snapshot-splits are
        // finished, then we can mark snapshot assigner as finished.
        if (checkpointIdToFinish != null && !assignerFinished && allSplitsFinished()) {
            assignerFinished = checkpointId >= checkpointIdToFinish;
            LOG.info("Snapshot split assigner is turn into finished status.");
        }
    }

    @Override
    public void close() {}

    /** Indicates there is no more splits available in this assigner. */
    public boolean noMoreSplits() {
        return remainingTables.isEmpty() && remainingSplits.isEmpty();
    }

    /**
     * Returns whether the snapshot split assigner is finished, which indicates there is no more
     * splits and all records of splits have been completely processed in the pipeline.
     */
    public boolean isFinished() {
        return assignerFinished;
    }

    public Map<String, SnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<String, Offset> getSplitFinishedOffsets() {
        return splitFinishedOffsets;
    }

    // -------------------------------------------------------------------------------------------

    /**
     * Returns whether all splits are finished which means no more splits and all assigned splits
     * are finished.
     */
    private boolean allSplitsFinished() {
        return noMoreSplits() && assignedSplits.size() == splitFinishedOffsets.size();
    }
}
