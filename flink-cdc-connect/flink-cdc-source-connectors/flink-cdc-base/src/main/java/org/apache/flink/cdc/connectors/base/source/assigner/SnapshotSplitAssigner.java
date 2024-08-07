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

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.INITIAL_ASSIGNING;
import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isAssigningFinished;
import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isAssigningSnapshotSplits;
import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;
import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isSnapshotAssigningFinished;

/** Assigner for snapshot split. */
public class SnapshotSplitAssigner<C extends SourceConfig> implements SplitAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotSplitAssigner.class);

    private final List<TableId> alreadyProcessedTables;
    private final List<SchemalessSnapshotSplit> remainingSplits;
    private final Map<String, SchemalessSnapshotSplit> assignedSplits;
    private final Map<TableId, TableChanges.TableChange> tableSchemas;
    private final Map<String, Offset> splitFinishedOffsets;

    private AssignerStatus assignerStatus;

    private final C sourceConfig;
    private final int currentParallelism;
    private final LinkedList<TableId> remainingTables;
    private final boolean isRemainingTablesCheckpointed;

    private ChunkSplitter chunkSplitter;
    private boolean isTableIdCaseSensitive;

    @Nullable private Long checkpointIdToFinish;
    private final DataSourceDialect<C> dialect;
    private final OffsetFactory offsetFactory;

    public SnapshotSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        this(
                sourceConfig,
                currentParallelism,
                new ArrayList<>(),
                new ArrayList<>(),
                new LinkedHashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                INITIAL_ASSIGNING,
                remainingTables,
                isTableIdCaseSensitive,
                true,
                dialect,
                offsetFactory);
    }

    public SnapshotSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            SnapshotPendingSplitsState checkpoint,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        this(
                sourceConfig,
                currentParallelism,
                checkpoint.getAlreadyProcessedTables(),
                checkpoint.getRemainingSplits(),
                checkpoint.getAssignedSplits(),
                checkpoint.getTableSchemas(),
                checkpoint.getSplitFinishedOffsets(),
                checkpoint.getSnapshotAssignerStatus(),
                checkpoint.getRemainingTables(),
                checkpoint.isTableIdCaseSensitive(),
                checkpoint.isRemainingTablesCheckpointed(),
                dialect,
                offsetFactory);
    }

    private SnapshotSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            List<TableId> alreadyProcessedTables,
            List<SchemalessSnapshotSplit> remainingSplits,
            Map<String, SchemalessSnapshotSplit> assignedSplits,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            Map<String, Offset> splitFinishedOffsets,
            AssignerStatus assignerStatus,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            boolean isRemainingTablesCheckpointed,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        this.sourceConfig = sourceConfig;
        this.currentParallelism = currentParallelism;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        // When job restore from savepoint, sort the existing tables and newly added tables
        // to let enumerator only send newly added tables' StreamSplitMetaEvent
        this.assignedSplits =
                assignedSplits.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (o, o2) -> o,
                                        LinkedHashMap::new));
        this.tableSchemas = tableSchemas;
        this.splitFinishedOffsets = splitFinishedOffsets;
        this.assignerStatus = assignerStatus;
        this.remainingTables = new LinkedList<>(remainingTables);
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.dialect = dialect;
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void open() {
        chunkSplitter = dialect.createChunkSplitter(sourceConfig);
        discoveryCaptureTables();
        captureNewlyAddedTables();
    }

    private void discoveryCaptureTables() {
        // discovery the tables lazily
        if (needToDiscoveryTables()) {
            long start = System.currentTimeMillis();
            LOG.debug("The remainingTables is empty, start to discovery tables");
            try {
                List<TableId> discoverTables = dialect.discoverDataCollections(sourceConfig);
                this.remainingTables.addAll(discoverTables);
                LOG.debug(
                        "Discovery tables success, time cost: {} ms.",
                        System.currentTimeMillis() - start);
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to discovery tables to capture", e);
            }
        }
        // when restore the job from legacy savepoint, the legacy state may haven't snapshot
        // remaining tables, discovery remaining table here
        else if (!isRemainingTablesCheckpointed && !isSnapshotAssigningFinished(assignerStatus)) {
            try {
                List<TableId> discoverTables = dialect.discoverDataCollections(sourceConfig);
                discoverTables.removeAll(alreadyProcessedTables);
                this.remainingTables.addAll(discoverTables);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover remaining tables to capture", e);
            }
        }
    }

    private void captureNewlyAddedTables() {
        if (sourceConfig.isScanNewlyAddedTableEnabled()
                && !sourceConfig.getStartupOptions().isSnapshotOnly()) {
            try {
                // check whether we got newly added tables
                final List<TableId> currentCapturedTables =
                        dialect.discoverDataCollections(sourceConfig);
                final Set<TableId> previousCapturedTables = new HashSet<>();
                List<TableId> tablesInRemainingSplits =
                        remainingSplits.stream()
                                .map(SnapshotSplit::getTableId)
                                .collect(Collectors.toList());
                previousCapturedTables.addAll(tablesInRemainingSplits);
                previousCapturedTables.addAll(alreadyProcessedTables);
                previousCapturedTables.addAll(remainingTables);

                // Get the removed tables with the new table filter
                Set<TableId> tablesToRemove = new HashSet<>(previousCapturedTables);
                tablesToRemove.removeAll(currentCapturedTables);

                // Get the newly added tables
                currentCapturedTables.removeAll(previousCapturedTables);
                List<TableId> newlyAddedTables = currentCapturedTables;

                // case 1: there are old tables to remove from state
                if (!tablesToRemove.isEmpty()) {

                    // remove unassigned tables/splits if it does not satisfy new table filter
                    List<String> splitsToRemove = new LinkedList<>();
                    for (Map.Entry<String, SchemalessSnapshotSplit> splitEntry :
                            assignedSplits.entrySet()) {
                        if (tablesToRemove.contains(splitEntry.getValue().getTableId())) {
                            splitsToRemove.add(splitEntry.getKey());
                        }
                    }
                    splitsToRemove.forEach(assignedSplits.keySet()::remove);
                    splitsToRemove.forEach(splitFinishedOffsets.keySet()::remove);
                    tableSchemas
                            .entrySet()
                            .removeIf(schema -> tablesToRemove.contains(schema.getKey()));
                    LOG.info("Enumerator remove tables after restart: {}", tablesToRemove);
                    remainingSplits.removeIf(split -> tablesToRemove.contains(split.getTableId()));
                    remainingTables.removeAll(tablesToRemove);
                    alreadyProcessedTables.removeIf(tableId -> tablesToRemove.contains(tableId));
                }

                // case 2: there are new tables to add
                if (!newlyAddedTables.isEmpty()) {
                    // if job is still in snapshot reading phase, directly add all newly added
                    // tables
                    LOG.info("Found newly added tables, start capture newly added tables process");
                    // add new tables
                    remainingTables.addAll(newlyAddedTables);
                    if (isAssigningFinished(assignerStatus)) {
                        // start the newly added tables process under stream reading phase
                        LOG.info(
                                "Found newly added tables, start capture newly added tables process under stream reading phase");
                        this.startAssignNewlyAddedTables();
                    }
                }
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
            Iterator<SchemalessSnapshotSplit> iterator = remainingSplits.iterator();
            SchemalessSnapshotSplit split = iterator.next();
            iterator.remove();
            assignedSplits.put(split.splitId(), split);
            return Optional.of(split.toSnapshotSplit(tableSchemas.get(split.getTableId())));
        } else {
            // it's turn for new table
            TableId nextTable = remainingTables.pollFirst();
            if (nextTable != null) {
                // split the given table into chunks (snapshot splits)
                Collection<SnapshotSplit> splits = chunkSplitter.generateSplits(nextTable);
                final Map<TableId, TableChanges.TableChange> tableSchema = new HashMap<>();
                if (!splits.isEmpty()) {
                    tableSchema.putAll(splits.iterator().next().getTableSchemas());
                }
                final List<SchemalessSnapshotSplit> schemalessSnapshotSplits =
                        splits.stream()
                                .map(SnapshotSplit::toSchemalessSnapshotSplit)
                                .collect(Collectors.toList());
                remainingSplits.addAll(schemalessSnapshotSplits);
                tableSchemas.putAll(tableSchema);
                alreadyProcessedTables.add(nextTable);
                return getNext();
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return !allSnapshotSplitsFinished();
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        if (waitingForFinishedSplits()) {
            LOG.error(
                    "The assigner is not ready to offer finished split information, this should not be called");
            throw new FlinkRuntimeException(
                    "The assigner is not ready to offer finished split information, this should not be called");
        }
        final List<SchemalessSnapshotSplit> assignedSnapshotSplit =
                new ArrayList<>(assignedSplits.values());
        List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        for (SchemalessSnapshotSplit split : assignedSnapshotSplit) {
            Offset finishedOffset = splitFinishedOffsets.get(split.splitId());
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            finishedOffset,
                            offsetFactory));
        }
        return finishedSnapshotSplitInfos;
    }

    @Override
    public void onFinishedSplits(Map<String, Offset> splitFinishedOffsets) {
        this.splitFinishedOffsets.putAll(splitFinishedOffsets);
        if (allSnapshotSplitsFinished() && isAssigningSnapshotSplits(assignerStatus)) {
            // Skip the waiting checkpoint when current parallelism is 1 which means we do not need
            // to care about the global output data order of snapshot splits and stream split.
            if (currentParallelism == 1) {
                assignerStatus = assignerStatus.onFinish();
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
            tableSchemas.putAll(split.asSnapshotSplit().getTableSchemas());
            remainingSplits.add(split.asSnapshotSplit().toSchemalessSnapshotSplit());
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
                        tableSchemas,
                        splitFinishedOffsets,
                        assignerStatus,
                        remainingTables,
                        isTableIdCaseSensitive,
                        true);
        // we need a complete checkpoint before mark this assigner to be finished, to wait for all
        // records of snapshot splits are completely processed
        if (checkpointIdToFinish == null
                && isAssigningSnapshotSplits(assignerStatus)
                && allSnapshotSplitsFinished()) {
            checkpointIdToFinish = checkpointId;
        }
        return state;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // we have waited for at-least one complete checkpoint after all snapshot-splits are
        // finished, then we can mark snapshot assigner as finished.
        if (checkpointIdToFinish != null
                && isAssigningSnapshotSplits(assignerStatus)
                && allSnapshotSplitsFinished()) {
            if (checkpointId >= checkpointIdToFinish) {
                assignerStatus = assignerStatus.onFinish();
            }
            LOG.info("Snapshot split assigner is turn into finished status.");
        }
    }

    @Override
    public void close() throws IOException {
        dialect.close();
    }

    @Override
    public boolean noMoreSplits() {
        return remainingTables.isEmpty() && remainingSplits.isEmpty();
    }

    /** Indicates current assigner need to discovery tables or not. */
    public boolean needToDiscoveryTables() {
        return remainingTables.isEmpty()
                && remainingSplits.isEmpty()
                && alreadyProcessedTables.isEmpty();
    }

    public Map<String, SchemalessSnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return assignerStatus;
    }

    @Override
    public void startAssignNewlyAddedTables() {
        Preconditions.checkState(
                isAssigningFinished(assignerStatus), "Invalid assigner status {}", assignerStatus);
        assignerStatus = assignerStatus.startAssignNewlyTables();
    }

    @Override
    public void onStreamSplitUpdated() {
        Preconditions.checkState(
                isNewlyAddedAssigningSnapshotFinished(assignerStatus),
                "Invalid assigner status {}",
                assignerStatus);
        assignerStatus = assignerStatus.onStreamSplitUpdated();
    }

    public Map<TableId, TableChanges.TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public Map<String, Offset> getSplitFinishedOffsets() {
        return splitFinishedOffsets;
    }

    // -------------------------------------------------------------------------------------------

    /**
     * Returns whether all splits are finished which means no more splits and all assigned splits
     * are finished.
     */
    private boolean allSnapshotSplitsFinished() {
        return noMoreSplits() && assignedSplits.size() == splitFinishedOffsets.size();
    }
}
