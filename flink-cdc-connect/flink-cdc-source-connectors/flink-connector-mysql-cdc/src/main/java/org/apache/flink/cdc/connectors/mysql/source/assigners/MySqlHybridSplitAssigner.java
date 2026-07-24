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

package org.apache.flink.cdc.connectors.mysql.source.assigners;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetaAssembledEvent;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.util.Preconditions;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link MySqlSplitAssigner} that splits tables into small chunk splits based on primary key
 * range and chunk size and also continue with a binlog split.
 */
public class MySqlHybridSplitAssigner implements MySqlSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlHybridSplitAssigner.class);
    private static final String BINLOG_SPLIT_ID = "binlog-split";

    private final int splitMetaGroupSize;
    private final MySqlSourceConfig sourceConfig;

    private boolean isBinlogSplitAssigned;

    /** Whether the reader has reported holding the complete binlog split. */
    private boolean binlogSplitMetaAssembled;

    /** Checkpoint at which the metadata release was scheduled; released once it completes. */
    @Nullable private Long checkpointIdToReleaseMeta;

    /**
     * Generation of the current binlog split assignment, bumped on every add-back. It rides on the
     * meta groups served to the reader and comes back in the assembled event, so a stale event from
     * a failed attempt (older generation) can't arm the release for a fresh reader. Not
     * checkpointed: it resets to 0 on restore, which is fine because the reader re-learns it.
     */
    private long binlogAssignmentGeneration;

    private final MySqlSnapshotSplitAssigner snapshotSplitAssigner;

    public MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this(
                sourceConfig,
                new MySqlSnapshotSplitAssigner(
                        sourceConfig,
                        currentParallelism,
                        remainingTables,
                        isTableIdCaseSensitive,
                        enumeratorContext),
                false,
                sourceConfig.getSplitMetaGroupSize());
    }

    public MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState checkpoint,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this(
                sourceConfig,
                new MySqlSnapshotSplitAssigner(
                        sourceConfig,
                        currentParallelism,
                        checkpoint.getSnapshotPendingSplits(),
                        enumeratorContext),
                checkpoint.isBinlogSplitAssigned(),
                sourceConfig.getSplitMetaGroupSize());
    }

    private MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            MySqlSnapshotSplitAssigner snapshotSplitAssigner,
            boolean isBinlogSplitAssigned,
            int splitMetaGroupSize) {
        this.sourceConfig = sourceConfig;
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
        this.splitMetaGroupSize = splitMetaGroupSize;
        // Fail fast on an incompatible restore. A job that released its snapshot metadata cannot
        // later enable scan.newly-added-table, because the splits and schemas that flow needs are
        // gone. Reject it loudly instead of silently building an inconsistent binlog split.
        if (sourceConfig.isScanNewlyAddedTableEnabled()
                && isBinlogSplitAssigned
                && snapshotSplitAssigner.isSnapshotMetaReleased()) {
            throw new IllegalStateException(
                    "scan.newly-added-table.enabled cannot be turned on for a job that previously "
                            + "released its snapshot split metadata "
                            + "(scan.incremental.snapshot.metadata.release.enabled=true). The "
                            + "assigned splits, finished offsets and table schemas needed to capture "
                            + "newly added tables are no longer in state. Start the job from a fresh "
                            + "state, or keep metadata release disabled if newly-added-table scanning "
                            + "is required.");
        }
    }

    @Override
    public void open() {
        snapshotSplitAssigner.open();
    }

    @Override
    public Optional<MySqlSplit> getNext() {
        if (AssignerStatus.isNewlyAddedAssigningSnapshotFinished(getAssignerStatus())) {
            // do not assign split until the adding table process finished
            return Optional.empty();
        }
        if (snapshotSplitAssigner.noMoreSplits()) {
            // binlog split assigning
            if (isBinlogSplitAssigned) {
                // no more splits for the assigner
                return Optional.empty();
            } else if (AssignerStatus.isInitialAssigningFinished(
                    snapshotSplitAssigner.getAssignerStatus())) {
                // we need to wait snapshot-assigner to be finished before
                // assigning the binlog split. Otherwise, records emitted from binlog split
                // might be out-of-order in terms of same primary key with snapshot splits.
                isBinlogSplitAssigned = true;
                return Optional.of(createBinlogSplit());
            } else if (AssignerStatus.isNewlyAddedAssigningFinished(
                    snapshotSplitAssigner.getAssignerStatus())) {
                // do not need to create binlog, but send event to wake up the binlog reader
                isBinlogSplitAssigned = true;
                return Optional.empty();
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
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        snapshotSplitAssigner.onFinishedSplits(splitFinishedOffsets);
    }

    @Override
    public void addSplits(Collection<MySqlSplit> splits) {
        List<MySqlSplit> snapshotSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                // we don't store the split, but will re-create binlog split later
                isBinlogSplitAssigned = false;
                // re-creating the binlog split: the reader must re-assemble and re-report
                // before the snapshot metadata can be released again. Bumping the generation
                // invalidates any assembled event still in flight from the failed attempt.
                binlogSplitMetaAssembled = false;
                checkpointIdToReleaseMeta = null;
                binlogAssignmentGeneration++;
            }
        }
        snapshotSplitAssigner.addSplits(snapshotSplits);
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        // Schedule the metadata release once the binlog split is assigned and the reader has
        // assembled it; it happens in notifyCheckpointComplete. Gated behind the opt-in, and
        // skipped when newly-added-table scan is on, since that flow may still need the metadata.
        if (isBinlogSplitAssigned
                && binlogSplitMetaAssembled
                && checkpointIdToReleaseMeta == null
                && !snapshotSplitAssigner.isSnapshotMetaReleased()
                && sourceConfig.isReleaseSnapshotMetadataEnabled()
                && !sourceConfig.isScanNewlyAddedTableEnabled()) {
            checkpointIdToReleaseMeta = checkpointId;
        }
        return new HybridPendingSplitsState(
                snapshotSplitAssigner.snapshotState(checkpointId), isBinlogSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
        // Release the snapshot metadata only after the checkpoint covering the binlog split
        // assignment completes. Doing it here (not in snapshotState) keeps the assignment
        // checkpoint-covered, so addSplitsBack can never return the split to an emptied assigner.
        if (checkpointIdToReleaseMeta != null
                && checkpointId >= checkpointIdToReleaseMeta
                && !snapshotSplitAssigner.isSnapshotMetaReleased()) {
            snapshotSplitAssigner.releaseSnapshotMetadata();
        }
    }

    /**
     * Marks that the reader holds the complete binlog split, arming the metadata release. A
     * reported generation that is neither the current one nor the inline sentinel is ignored, which
     * drops a stale event from a reader attempt that an add-back has already superseded.
     */
    public void onBinlogSplitMetaAssembled(long reportedGeneration) {
        // An inline reader requests no meta groups, so it reports COMPLETE_WITHOUT_META_GENERATION;
        // honor that regardless, since it never needs the coordinator's metadata. Otherwise require
        // an exact match, which drops a stale event from a failed attempt superseded by an
        // add-back.
        if (reportedGeneration != BinlogSplitMetaAssembledEvent.COMPLETE_WITHOUT_META_GENERATION
                && reportedGeneration != binlogAssignmentGeneration) {
            LOG.info(
                    "Ignoring binlog split assembled event for generation {}; current generation is {}.",
                    reportedGeneration,
                    binlogAssignmentGeneration);
            return;
        }
        this.binlogSplitMetaAssembled = true;
    }

    /** Returns the current binlog split assignment generation. */
    public long getBinlogAssignmentGeneration() {
        return binlogAssignmentGeneration;
    }

    /** Returns whether the snapshot split metadata has been released. */
    public boolean isSnapshotMetaReleased() {
        return snapshotSplitAssigner.isSnapshotMetaReleased();
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return snapshotSplitAssigner.getAssignerStatus();
    }

    @Override
    public boolean noMoreSplits() {
        return snapshotSplitAssigner.noMoreSplits() && isBinlogSplitAssigned;
    }

    @Override
    public void startAssignNewlyAddedTables() {
        snapshotSplitAssigner.startAssignNewlyAddedTables();
    }

    @Override
    public void onBinlogSplitUpdated() {
        snapshotSplitAssigner.onBinlogSplitUpdated();
    }

    @Override
    public void close() {
        snapshotSplitAssigner.close();
    }

    // --------------------------------------------------------------------------------------------

    private MySqlBinlogSplit createBinlogSplit() {
        Preconditions.checkState(
                !snapshotSplitAssigner.isSnapshotMetaReleased(),
                "Snapshot metadata was already released; the binlog split must not be re-created "
                        + "after that.");
        final List<MySqlSchemalessSnapshotSplit> assignedSnapshotSplit =
                new ArrayList<>(snapshotSplitAssigner.getAssignedSplits().values());

        Map<String, BinlogOffset> splitFinishedOffsets =
                snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();

        BinlogOffset minBinlogOffset = null;
        BinlogOffset maxBinlogOffset = null;
        for (MySqlSchemalessSnapshotSplit split : assignedSnapshotSplit) {
            // find the min and max binlog offset
            BinlogOffset binlogOffset = splitFinishedOffsets.get(split.splitId());
            if (minBinlogOffset == null || binlogOffset.isBefore(minBinlogOffset)) {
                minBinlogOffset = binlogOffset;
            }
            if (maxBinlogOffset == null || binlogOffset.isAfter(maxBinlogOffset)) {
                maxBinlogOffset = binlogOffset;
            }

            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            binlogOffset));
        }

        // If the source is running in snapshot mode, we use the highest watermark among
        // snapshot splits as the ending offset to provide a consistent snapshot view at the moment
        // of high watermark.
        BinlogOffset stoppingOffset = BinlogOffset.ofNonStopping();
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            stoppingOffset = maxBinlogOffset;
        }

        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them
        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
        return new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                minBinlogOffset == null ? BinlogOffset.ofEarliest() : minBinlogOffset,
                stoppingOffset,
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size());
    }
}
