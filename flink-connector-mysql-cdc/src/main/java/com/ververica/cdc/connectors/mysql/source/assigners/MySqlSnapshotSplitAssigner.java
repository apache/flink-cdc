/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.assigners;

import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.schema.MySqlSchema;
import com.ververica.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.discoverCapturedTables;
import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.openJdbcConnection;
import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isAssigningFinished;
import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isAssigningSnapshotSplits;
import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;
import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isSnapshotAssigningFinished;
import static com.ververica.cdc.connectors.mysql.source.assigners.state.ChunkSplitterState.NO_SPLITTING_TABLE_STATE;

/**
 * A {@link MySqlSplitAssigner} that splits tables into small chunk splits based on primary key
 * range and chunk size.
 *
 * @see MySqlSourceOptions#SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE
 */
public class MySqlSnapshotSplitAssigner implements MySqlSplitAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSnapshotSplitAssigner.class);

    private final List<TableId> alreadyProcessedTables;
    private final List<MySqlSchemalessSnapshotSplit> remainingSplits;
    private final Map<String, MySqlSchemalessSnapshotSplit> assignedSplits;
    private final Map<TableId, TableChanges.TableChange> tableSchemas;
    private final Map<String, BinlogOffset> splitFinishedOffsets;
    private final MySqlSourceConfig sourceConfig;
    private final int currentParallelism;
    private final List<TableId> remainingTables;
    private final boolean isRemainingTablesCheckpointed;

    private final MySqlPartition partition;
    private final Object lock = new Object();

    private volatile Throwable uncaughtSplitterException;
    private AssignerStatus assignerStatus;
    private MySqlChunkSplitter chunkSplitter;
    private boolean isTableIdCaseSensitive;
    private ExecutorService executor;

    @Nullable private Long checkpointIdToFinish;

    public MySqlSnapshotSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive) {
        this(
                sourceConfig,
                currentParallelism,
                new ArrayList<>(),
                new ArrayList<>(),
                new LinkedHashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                AssignerStatus.INITIAL_ASSIGNING,
                remainingTables,
                isTableIdCaseSensitive,
                true,
                NO_SPLITTING_TABLE_STATE);
    }

    public MySqlSnapshotSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            SnapshotPendingSplitsState checkpoint) {
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
                checkpoint.getChunkSplitterState());
    }

    private MySqlSnapshotSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> alreadyProcessedTables,
            List<MySqlSchemalessSnapshotSplit> remainingSplits,
            Map<String, MySqlSchemalessSnapshotSplit> assignedSplits,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            Map<String, BinlogOffset> splitFinishedOffsets,
            AssignerStatus assignerStatus,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            boolean isRemainingTablesCheckpointed,
            ChunkSplitterState chunkSplitterState) {
        this.sourceConfig = sourceConfig;
        this.currentParallelism = currentParallelism;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = new CopyOnWriteArrayList<>(remainingSplits);
        // When job restore from savepoint, sort the existing tables and newly added tables
        // to let enumerator only send newly added tables' BinlogSplitMetaEvent
        this.assignedSplits =
                assignedSplits.entrySet().stream()
                        .sorted(Entry.comparingByKey())
                        .collect(
                                Collectors.toMap(
                                        Entry::getKey,
                                        Entry::getValue,
                                        (o, o2) -> o,
                                        LinkedHashMap::new));
        this.tableSchemas = tableSchemas;
        this.splitFinishedOffsets = splitFinishedOffsets;
        this.assignerStatus = assignerStatus;
        this.remainingTables = new CopyOnWriteArrayList<>(remainingTables);
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.chunkSplitter =
                createChunkSplitter(sourceConfig, isTableIdCaseSensitive, chunkSplitterState);
        this.partition =
                new MySqlPartition(sourceConfig.getMySqlConnectorConfig().getLogicalName());
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
            try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
                final List<TableId> discoverTables = discoverCapturedTables(jdbc, sourceConfig);
                this.remainingTables.addAll(discoverTables);
                this.isTableIdCaseSensitive = DebeziumUtils.isTableIdCaseSensitive(jdbc);
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to discovery tables to capture", e);
            }
            LOG.debug(
                    "Discovery tables success, time cost: {} ms.",
                    System.currentTimeMillis() - start);
        }
        // when restore the job from legacy savepoint, the legacy state may haven't snapshot
        // remaining tables, discovery remaining table here
        else if (!isRemainingTablesCheckpointed && !isSnapshotAssigningFinished(assignerStatus)) {
            try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
                final List<TableId> discoverTables = discoverCapturedTables(jdbc, sourceConfig);
                discoverTables.removeAll(alreadyProcessedTables);
                this.remainingTables.addAll(discoverTables);
                this.isTableIdCaseSensitive = DebeziumUtils.isTableIdCaseSensitive(jdbc);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover remaining tables to capture", e);
            }
        }
    }

    private void captureNewlyAddedTables() {
        if (sourceConfig.isScanNewlyAddedTableEnabled()) {
            // check whether we got newly added tables
            try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
                final List<TableId> currentCapturedTables =
                        discoverCapturedTables(jdbc, sourceConfig);
                final Set<TableId> previousCapturedTables = new HashSet<>();
                List<TableId> tablesInRemainingSplits =
                        remainingSplits.stream()
                                .map(MySqlSnapshotSplit::getTableId)
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
                    for (Entry<String, MySqlSchemalessSnapshotSplit> splitEntry :
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
                        // start the newly added tables process under binlog reading phase
                        LOG.info(
                                "Found newly added tables, start capture newly added tables process under binlog reading phase");
                        this.startAssignNewlyAddedTables();
                    }
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover remaining tables to capture", e);
            }
        }
    }

    private void startAsynchronouslySplit() {
        if (chunkSplitter.hasNextChunk() || !remainingTables.isEmpty()) {
            if (executor == null) {
                ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat("snapshot-splitting").build();
                this.executor = Executors.newSingleThreadExecutor(threadFactory);
            }
            executor.submit(this::splitChunksForRemainingTables);
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
                List<MySqlSnapshotSplit> splits;
                try {
                    splits = chunkSplitter.splitChunks(partition, nextTable);
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Error when splitting chunks for " + nextTable, e);
                }

                if (!hasRecordSchema && !splits.isEmpty()) {
                    hasRecordSchema = true;
                    final Map<TableId, TableChanges.TableChange> tableSchema = new HashMap<>();
                    tableSchema.putAll(splits.iterator().next().getTableSchemas());
                    tableSchemas.putAll(tableSchema);
                }
                final List<MySqlSchemalessSnapshotSplit> schemaLessSnapshotSplits =
                        splits.stream()
                                .map(MySqlSnapshotSplit::toSchemalessSnapshotSplit)
                                .collect(Collectors.toList());
                chunkNum += splits.size();
                remainingSplits.addAll(schemaLessSnapshotSplits);
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
    public Optional<MySqlSplit> getNext() {
        waitTableDiscoveryReady();
        synchronized (lock) {
            checkSplitterErrors();
            if (!remainingSplits.isEmpty()) {
                // return remaining splits firstly
                Iterator<MySqlSchemalessSnapshotSplit> iterator = remainingSplits.iterator();
                MySqlSchemalessSnapshotSplit split = iterator.next();
                remainingSplits.remove(split);
                assignedSplits.put(split.splitId(), split);
                addAlreadyProcessedTablesIfNotExists(split.getTableId());
                return Optional.of(
                        split.toMySqlSnapshotSplit(tableSchemas.get(split.getTableId())));
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
        final List<MySqlSchemalessSnapshotSplit> assignedSnapshotSplit =
                new ArrayList<>(assignedSplits.values());
        List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        for (MySqlSchemalessSnapshotSplit split : assignedSnapshotSplit) {
            BinlogOffset binlogOffset = splitFinishedOffsets.get(split.splitId());
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            binlogOffset));
        }
        return finishedSnapshotSplitInfos;
    }

    @Override
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        this.splitFinishedOffsets.putAll(splitFinishedOffsets);
        if (allSnapshotSplitsFinished() && isAssigningSnapshotSplits(assignerStatus)) {
            // Skip the waiting checkpoint when current parallelism is 1 which means we do not need
            // to care about the global output data order of snapshot splits and binlog split.
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
    public void addSplits(Collection<MySqlSplit> splits) {
        for (MySqlSplit split : splits) {
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
                        true,
                        chunkSplitter.snapshotState(checkpointId));
        // we need a complete checkpoint before mark this assigner to be finished, to wait for
        // all records of snapshot splits are completely processed
        if (checkpointIdToFinish == null
                && !isSnapshotAssigningFinished(assignerStatus)
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
    public void onBinlogSplitUpdated() {
        Preconditions.checkState(
                isNewlyAddedAssigningSnapshotFinished(assignerStatus),
                "Invalid assigner status {}",
                assignerStatus);
        assignerStatus = assignerStatus.onBinlogSplitUpdated();
    }

    @Override
    public void close() {
        closeExecutorService();
        if (chunkSplitter != null) {
            try {
                chunkSplitter.close();
            } catch (Exception e) {
                LOG.warn("Fail to close the chunk splitter.");
            }
        }
    }

    private void closeExecutorService() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    private void addAlreadyProcessedTablesIfNotExists(TableId tableId) {
        if (!alreadyProcessedTables.contains(tableId)) {
            alreadyProcessedTables.add(tableId);
        }
    }

    private void waitTableDiscoveryReady() {
        while (needToDiscoveryTables()) {
            LOG.debug("Current assigner is discovering tables, wait tables ready...");
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                // nothing to do
            }
        }
    }

    /** Indicates there is no more splits available in this assigner. */
    public boolean noMoreSnapshotSplits() {
        return !needToDiscoveryTables() && remainingTables.isEmpty() && remainingSplits.isEmpty();
    }

    /** Indicates current assigner need to discovery tables or not. */
    public boolean needToDiscoveryTables() {
        return remainingTables.isEmpty()
                && remainingSplits.isEmpty()
                && alreadyProcessedTables.isEmpty();
    }

    public Map<String, MySqlSchemalessSnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<TableId, TableChanges.TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public Map<String, BinlogOffset> getSplitFinishedOffsets() {
        return splitFinishedOffsets;
    }

    // -------------------------------------------------------------------------------------------

    /**
     * Returns whether all splits are finished which means no more splits and all assigned splits
     * are finished.
     */
    private boolean allSnapshotSplitsFinished() {
        return noMoreSnapshotSplits() && assignedSplits.size() == splitFinishedOffsets.size();
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

    private static MySqlChunkSplitter createChunkSplitter(
            MySqlSourceConfig sourceConfig,
            boolean isTableIdCaseSensitive,
            ChunkSplitterState chunkSplitterState) {
        MySqlSchema mySqlSchema = new MySqlSchema(sourceConfig, isTableIdCaseSensitive);
        if (!NO_SPLITTING_TABLE_STATE.equals(chunkSplitterState)) {
            TableId tableId = chunkSplitterState.getCurrentSplittingTableId();
            return new MySqlChunkSplitter(
                    mySqlSchema,
                    sourceConfig,
                    tableId != null
                                    && sourceConfig
                                            .getTableFilters()
                                            .dataCollectionFilter()
                                            .isIncluded(tableId)
                            ? chunkSplitterState
                            : NO_SPLITTING_TABLE_STATE);
        }
        return new MySqlChunkSplitter(mySqlSchema, sourceConfig);
    }
}
