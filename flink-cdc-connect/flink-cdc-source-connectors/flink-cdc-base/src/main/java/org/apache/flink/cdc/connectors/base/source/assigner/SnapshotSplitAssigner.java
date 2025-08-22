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
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceEnumeratorMetrics;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
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
    private final List<TableId> remainingTables;
    private final boolean isRemainingTablesCheckpointed;

    private ChunkSplitter chunkSplitter;
    private boolean isTableIdCaseSensitive;

    @Nullable private Long checkpointIdToFinish;
    private final DataSourceDialect<C> dialect;
    private final OffsetFactory offsetFactory;

    private SourceEnumeratorMetrics enumeratorMetrics;
    private final Map<String, Long> splitFinishedCheckpointIds;
    private static final long UNDEFINED_CHECKPOINT_ID = -1;

    private final Object lock = new Object();
    private ExecutorService splittingExecutorService;
    private volatile Throwable uncaughtSplitterException;

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
                offsetFactory,
                new ConcurrentHashMap<>(),
                ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
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
                offsetFactory,
                new ConcurrentHashMap<>(),
                checkpoint.getChunkSplitterState());
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
            OffsetFactory offsetFactory,
            Map<String, Long> splitFinishedCheckpointIds,
            ChunkSplitterState chunkSplitterState) {
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
        this.remainingTables = new CopyOnWriteArrayList<>(remainingTables);
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.dialect = dialect;
        this.offsetFactory = offsetFactory;
        this.splitFinishedCheckpointIds = splitFinishedCheckpointIds;
        chunkSplitter = createChunkSplitter(sourceConfig, dialect, chunkSplitterState);
    }

    @Override
    public void open() {
        chunkSplitter.open();
        discoveryCaptureTables();
        captureNewlyAddedTables();
        startAsynchronouslySplit();
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
                && !sourceConfig.getStartupOptions().isSnapshotOnly()
                && AssignerStatus.isAssigningFinished(assignerStatus)) {
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

    /** This should be invoked after this class's open method. */
    public void initEnumeratorMetrics(SourceEnumeratorMetrics enumeratorMetrics) {
        this.enumeratorMetrics = enumeratorMetrics;

        this.enumeratorMetrics.enterSnapshotPhase();
        this.enumeratorMetrics.registerMetrics(
                alreadyProcessedTables::size, assignedSplits::size, remainingSplits::size);
        this.enumeratorMetrics.addNewTables(computeTablesPendingSnapshot());
        for (SchemalessSnapshotSplit snapshotSplit : remainingSplits) {
            this.enumeratorMetrics
                    .getTableMetrics(snapshotSplit.getTableId())
                    .addNewSplit(snapshotSplit.splitId());
        }
        for (SchemalessSnapshotSplit snapshotSplit : assignedSplits.values()) {
            this.enumeratorMetrics
                    .getTableMetrics(snapshotSplit.getTableId())
                    .addProcessedSplit(snapshotSplit.splitId());
        }
        for (String splitId : splitFinishedOffsets.keySet()) {
            TableId tableId = SnapshotSplit.extractTableId(splitId);
            this.enumeratorMetrics.getTableMetrics(tableId).addFinishedSplit(splitId);
        }
    }

    // remainingTables + tables has been split but not processed
    private int computeTablesPendingSnapshot() {
        int numTablesPendingSnapshot = remainingTables.size();
        Set<TableId> computedTables = new HashSet<>();
        for (SchemalessSnapshotSplit split : remainingSplits) {
            TableId tableId = split.getTableId();
            if (!computedTables.contains(tableId)
                    && !alreadyProcessedTables.contains(tableId)
                    && !remainingTables.contains(tableId)) {
                computedTables.add(tableId);
                numTablesPendingSnapshot++;
            }
        }
        return numTablesPendingSnapshot;
    }

    private void startAsynchronouslySplit() {
        if (chunkSplitter.hasNextChunk() || !remainingTables.isEmpty()) {
            if (splittingExecutorService == null) {
                ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat("snapshot-splitting").build();
                this.splittingExecutorService = Executors.newSingleThreadExecutor(threadFactory);
            }
            splittingExecutorService.submit(this::splitChunksForRemainingTables);
        }
    }

    private void splitTable(TableId nextTable) {
        LOG.info("Start splitting table {} into chunks...", nextTable);
        long start = System.currentTimeMillis();
        int chunkNum = 0;
        boolean hasRecordSchema = false;
        // split the given table into chunks (snapshot splits)
        do {
            synchronized (lock) {
                Collection<SnapshotSplit> splits;
                try {
                    splits = chunkSplitter.generateSplits(nextTable);
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Error when splitting chunks for " + nextTable, e);
                }

                if (!hasRecordSchema && !splits.isEmpty()) {
                    hasRecordSchema = true;
                    tableSchemas.putAll(splits.iterator().next().getTableSchemas());
                }
                List<String> splitIds = new ArrayList<>();
                for (SnapshotSplit split : splits) {
                    SchemalessSnapshotSplit schemalessSnapshotSplit =
                            split.toSchemalessSnapshotSplit();
                    splitIds.add(schemalessSnapshotSplit.splitId());
                    if (sourceConfig.isAssignUnboundedChunkFirst() && split.getSplitEnd() == null) {
                        // assign unbounded split first
                        remainingSplits.add(0, schemalessSnapshotSplit);
                    } else {
                        remainingSplits.add(schemalessSnapshotSplit);
                    }
                }

                chunkNum += splits.size();
                enumeratorMetrics.getTableMetrics(nextTable).addNewSplits(splitIds);

                if (!chunkSplitter.hasNextChunk()) {
                    remainingTables.remove(nextTable);
                }
                lock.notify();
            }
        } while (chunkSplitter.hasNextChunk());
        long end = System.currentTimeMillis();
        LOG.info(
                "Split table {} into {} chunks, time cost: {}ms.",
                nextTable,
                chunkNum,
                end - start);
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        synchronized (lock) {
            checkSplitterErrors();
            if (!remainingSplits.isEmpty()) {
                // return remaining splits firstly
                Iterator<SchemalessSnapshotSplit> iterator = remainingSplits.iterator();
                SchemalessSnapshotSplit split = iterator.next();
                iterator.remove();
                assignedSplits.put(split.splitId(), split);
                addAlreadyProcessedTablesIfNotExists(split.getTableId());
                enumeratorMetrics
                        .getTableMetrics(split.getTableId())
                        .finishProcessSplit(split.splitId());
                return Optional.of(split.toSnapshotSplit(tableSchemas.get(split.getTableId())));
            } else if (!remainingTables.isEmpty()) {
                try {
                    // wait for the asynchronous split to complete
                    lock.wait();
                } catch (InterruptedException e) {
                    throw new FlinkRuntimeException(
                            "InterruptedException while waiting for asynchronously snapshot split");
                }
                return getNext();
            } else {
                closeExecutorService();
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
        for (String splitId : splitFinishedOffsets.keySet()) {
            splitFinishedCheckpointIds.put(splitId, UNDEFINED_CHECKPOINT_ID);
        }
        LOG.info(
                "splitFinishedCheckpointIds size in onFinishedSplits: {}",
                splitFinishedCheckpointIds == null ? 0 : splitFinishedCheckpointIds.size());
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

            enumeratorMetrics
                    .getTableMetrics(split.asSnapshotSplit().getTableId())
                    .reprocessSplit(split.splitId());
            TableId tableId = split.asSnapshotSplit().getTableId();

            enumeratorMetrics.getTableMetrics(tableId).removeFinishedSplit(split.splitId());
        }
    }

    @Override
    public SnapshotPendingSplitsState snapshotState(long checkpointId) {
        if (splitFinishedCheckpointIds != null && !splitFinishedCheckpointIds.isEmpty()) {
            for (Map.Entry<String, Long> splitFinishedCheckpointId :
                    splitFinishedCheckpointIds.entrySet()) {
                if (splitFinishedCheckpointId.getValue() == UNDEFINED_CHECKPOINT_ID) {
                    splitFinishedCheckpointId.setValue(checkpointId);
                }
            }
            LOG.info(
                    "SnapshotSplitAssigner snapshotState on checkpoint {} with splitFinishedCheckpointIds size {}.",
                    checkpointId,
                    splitFinishedCheckpointIds.size());
        }

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
                        true,
                        splitFinishedCheckpointIds,
                        chunkSplitter.snapshotState(checkpointId));
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

        if (splitFinishedCheckpointIds != null && !splitFinishedCheckpointIds.isEmpty()) {
            Iterator<Map.Entry<String, Long>> iterator =
                    splitFinishedCheckpointIds.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> splitFinishedCheckpointId = iterator.next();
                String splitId = splitFinishedCheckpointId.getKey();
                Long splitCheckpointId = splitFinishedCheckpointId.getValue();
                if (splitCheckpointId != UNDEFINED_CHECKPOINT_ID
                        && checkpointId >= splitCheckpointId) {
                    // record table-level splits metrics
                    TableId tableId = SnapshotSplit.extractTableId(splitId);
                    enumeratorMetrics.getTableMetrics(tableId).addFinishedSplit(splitId);
                    iterator.remove();
                }
            }
            LOG.info(
                    "Checkpoint completed on checkpoint {} with splitFinishedCheckpointIds size {}.",
                    checkpointId,
                    splitFinishedCheckpointIds.size());
        }
    }

    @Override
    public void close() throws IOException {
        closeExecutorService();
        if (chunkSplitter != null) {
            try {
                chunkSplitter.close();
            } catch (Exception e) {
                LOG.warn("Fail to close the chunk splitter.");
            }
        }
        dialect.close();
    }

    private void closeExecutorService() {
        if (splittingExecutorService != null) {
            splittingExecutorService.shutdown();
        }
    }

    private void addAlreadyProcessedTablesIfNotExists(TableId tableId) {
        if (!alreadyProcessedTables.contains(tableId)) {
            alreadyProcessedTables.add(tableId);
            enumeratorMetrics.startSnapshotTables(1);
        }
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
                isAssigningFinished(assignerStatus), "Invalid assigner status %s", assignerStatus);
        assignerStatus = assignerStatus.startAssignNewlyTables();
    }

    @Override
    public void onStreamSplitUpdated() {
        Preconditions.checkState(
                isNewlyAddedAssigningSnapshotFinished(assignerStatus),
                "Invalid assigner status %s",
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

    private void splitChunksForRemainingTables() {
        try {
            // restore from a checkpoint and start to split the table from the previous
            // checkpoint
            if (chunkSplitter.hasNextChunk()) {
                LOG.info(
                        "Start splitting remaining chunks for table {}",
                        chunkSplitter.getCurrentSplittingTableId());
                splitTable(chunkSplitter.getCurrentSplittingTableId());
            }

            // split the remaining tables
            for (TableId nextTable : remainingTables) {
                splitTable(nextTable);
            }
        } catch (Throwable e) {
            synchronized (lock) {
                if (uncaughtSplitterException == null) {
                    uncaughtSplitterException = e;
                } else {
                    uncaughtSplitterException.addSuppressed(e);
                }
                // Release the potential waiting getNext() call
                lock.notify();
            }
        }
    }

    private void checkSplitterErrors() {
        if (uncaughtSplitterException != null) {
            throw new FlinkRuntimeException(
                    "Chunk splitting has encountered exception", uncaughtSplitterException);
        }
    }

    private static ChunkSplitter createChunkSplitter(
            SourceConfig sourceConfig,
            DataSourceDialect dataSourceDialect,
            ChunkSplitterState chunkSplitterState) {
        TableId tableId = chunkSplitterState.getCurrentSplittingTableId();
        return dataSourceDialect.createChunkSplitter(
                sourceConfig,
                !ChunkSplitterState.NO_SPLITTING_TABLE_STATE.equals(chunkSplitterState)
                                && tableId != null
                                && dataSourceDialect.isIncludeDataCollection(sourceConfig, tableId)
                        ? chunkSplitterState
                        : ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }
}
