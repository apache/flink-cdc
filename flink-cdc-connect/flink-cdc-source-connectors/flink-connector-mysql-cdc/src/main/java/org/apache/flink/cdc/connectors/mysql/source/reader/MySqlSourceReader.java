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

package org.apache.flink.cdc.connectors.mysql.source.reader;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.events.*;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;

/**
 * The source reader for MySQL source splits.
 */
public class MySqlSourceReader<T>
                extends SingleThreadMultiplexSourceReaderBase<
        SourceRecords, T, MySqlSplit, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);
    private final MySqlSourceConfig sourceConfig;
    private final Map<String, MySqlSnapshotSplit> finishedUnackedSplits;
    private final Map<String, MySqlBinlogSplit> uncompletedBinlogSplits;
    private final int subtaskId;
    private final MySqlSourceReaderContext mySqlSourceReaderContext;
    private final MySqlPartition partition;
    private volatile MySqlBinlogSplit suspendedBinlogSplit;
    private List<TableId> tableUnNotified;

    public MySqlSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementQueue,
            Supplier<MySqlSplitReader> splitReaderSupplier,
            RecordEmitter<SourceRecords, T, MySqlSplitState> recordEmitter,
            Configuration config,
            MySqlSourceReaderContext context,
            MySqlSourceConfig sourceConfig) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context.getSourceReaderContext());
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.uncompletedBinlogSplits = new HashMap<>();
        this.subtaskId = context.getSourceReaderContext().getIndexOfSubtask();
        this.mySqlSourceReaderContext = context;
        this.suspendedBinlogSplit = null;
        this.partition =
                new MySqlPartition(sourceConfig.getMySqlConnectorConfig().getLogicalName());
        this.tableUnNotified = new ArrayList<>();
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() <= 1) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected MySqlSplitState initializedState(MySqlSplit split) {
        if (split.isSnapshotSplit()) {
            return new MySqlSnapshotSplitState(split.asSnapshotSplit());
        } else {
            return new MySqlBinlogSplitState(split.asBinlogSplit());
        }
    }

    @Override
    public List<MySqlSplit> snapshotState(long checkpointId) {
        List<MySqlSplit> stateSplits = super.snapshotState(checkpointId);

        // unfinished splits
        List<MySqlSplit> unfinishedSplits =
                stateSplits.stream()
                        .filter(split -> !finishedUnackedSplits.containsKey(split.splitId()))
                        .map(split -> {
                            if (split instanceof MySqlBinlogSplit) {
                                MySqlBinlogSplit mySqlBinlogSplit = (MySqlBinlogSplit) split;
                                mySqlBinlogSplit.getTableUnNotified().clear();
                            }
                            return split;
                        })
                        .collect(Collectors.toList());

        // add finished snapshot splits that did not receive ack yet
        unfinishedSplits.addAll(finishedUnackedSplits.values());

        // add binlog splits who are uncompleted
        unfinishedSplits.addAll(uncompletedBinlogSplits.values());

        // add suspended BinlogSplit
        if (suspendedBinlogSplit != null) {
            unfinishedSplits.add(suspendedBinlogSplit);
        }

        logCurrentBinlogOffsets(unfinishedSplits, checkpointId);

        return unfinishedSplits;
    }

    @Override
    protected void onSplitFinished(Map<String, MySqlSplitState> finishedSplitIds) {
        if (isNewlyAddedTableSplitAndBinlogSplit(finishedSplitIds)) {
            MySqlSplitState mySqlBinlogSplitState = finishedSplitIds.remove(BINLOG_SPLIT_ID);
            finishedSplitIds
                    .values()
                    .forEach(
                            newAddedSplitState ->
                                    finishedUnackedSplits.put(
                                            newAddedSplitState.toMySqlSplit().splitId(),
                                            newAddedSplitState.toMySqlSplit().asSnapshotSplit()));
            Preconditions.checkState(finishedSplitIds.values().size() == 1);
            LOG.info(
                    "Source reader {} finished binlog split and snapshot split {}",
                    subtaskId,
                    finishedSplitIds.values().iterator().next().toMySqlSplit().splitId());
            this.addSplits(Collections.singletonList(mySqlBinlogSplitState.toMySqlSplit()));
        } else {
            Preconditions.checkState(finishedSplitIds.size() == 1);
            for (MySqlSplitState mySqlSplitState : finishedSplitIds.values()) {
                MySqlSplit mySqlSplit = mySqlSplitState.toMySqlSplit();
                if (mySqlSplit.isBinlogSplit()) {
                    // Binlog reader reaches the ending offset of the split. We need to do
                    // nothing under this case.
                    LOG.info("Source reader {} finished binlog split", subtaskId);
                } else {
                    finishedUnackedSplits.put(mySqlSplit.splitId(), mySqlSplit.asSnapshotSplit());
                }
            }
            reportFinishedSnapshotSplitsIfNeed();
        }
        context.sendSplitRequest();
    }

    /**
     * During the newly added table process, for the source reader who holds the binlog split, we
     * return the latest finished snapshot split and binlog split as well, this design let us have
     * opportunity to exchange binlog reading and snapshot reading, we put the binlog split back.
     */
    private boolean isNewlyAddedTableSplitAndBinlogSplit(
            Map<String, MySqlSplitState> finishedSplitIds) {
        return finishedSplitIds.containsKey(BINLOG_SPLIT_ID) && finishedSplitIds.size() == 2;
    }

    @Override
    public void addSplits(List<MySqlSplit> splits) {
        addSplits(splits, true);
    }

    /**
     * Adds a list of splits for this reader to read.
     *
     * @param splits the splits to add.
     * @param checkTableChangeForBinlogSplit to check the captured table list change or not, it
     *     should be true for reader which is during restoration from a checkpoint or savepoint.
     */
    private void addSplits(List<MySqlSplit> splits, boolean checkTableChangeForBinlogSplit) {
        // restore for finished Unacked Splits
        List<MySqlSplit> unfinishedSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            LOG.info("Source reader {} adds split {}", subtaskId, split);
            if (split.isSnapshotSplit()) {
                MySqlSnapshotSplit snapshotSplit = split.asSnapshotSplit();
                // filter snapshot splits data that has been consumed during binlog phase, so we can avoid duplicate data
                if (!tableUnNotified.contains(snapshotSplit.getTableId())) {
                    if (snapshotSplit.isSnapshotReadFinished()) {
                        finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                    } else if (sourceConfig
                            .getTableFilters()
                            .dataCollectionFilter()
                            .isIncluded(split.asSnapshotSplit().getTableId())) {
                        unfinishedSplits.add(split);
                    } else {
                        LOG.info(
                                "The subtask {} is skipping split {} because it does not match new table filter.",
                                subtaskId,
                                split.splitId());
                    }
                }
            } else {
                MySqlBinlogSplit binlogSplit = split.asBinlogSplit();
                boolean checkNewlyAddedTableSchema = false;
                // When restore from a checkpoint, the finished split infos may contain some splits
                // for the deleted tables.
                // We need to remove these splits for the deleted tables at the finished split
                // infos.
                if (checkTableChangeForBinlogSplit) {
                    binlogSplit =
                            MySqlBinlogSplit.filterOutdatedSplitInfos(
                                    binlogSplit,
                                    sourceConfig
                                            .getMySqlConnectorConfig()
                                            .getTableFilters()
                                            .dataCollectionFilter());
                    if (!binlogSplit.getTableUnNotified().isEmpty()) {
                        tableUnNotified = binlogSplit.getTableUnNotified();
                        for (TableId tableId : tableUnNotified) {
                            context.sendSourceEventToCoordinator(new BinlogNewAddedTableEvent(tableId.catalog()
                                    , tableId.schema(), tableId.table()));
                        }
                    }
                    // Try to discovery table schema once for newly added tables when source reader
                    // start or restore
                    checkNewlyAddedTableSchema = !binlogSplit.getTableSchemas().isEmpty()
                            && sourceConfig.isScanNewlyAddedTableEnabled();
                    // check if contains new added tables
                    if (checkNewlyAddedTableSchema) {
                        Map<TableId, TableChanges.TableChange> tableIdTableChangeMap = discoverSchemaForNewAddedTables(binlogSplit);
                        if (!tableIdTableChangeMap.keySet().isEmpty()) {
                            // suspend binlog split at the start.rerun after new added table snapshot finished.
                            binlogSplit = MySqlBinlogSplit.toSuspendSplit(MySqlBinlogSplit.fillTableSchemas(binlogSplit, tableIdTableChangeMap));
                            context.sendSourceEventToCoordinator(new BinlogSplitSuspendEvent());
                        }
                    }
                }

                // the binlog split is suspended
                if (binlogSplit.isSuspended()) {
                    suspendedBinlogSplit = binlogSplit;
                } else if (!binlogSplit.isCompletedSplit()) {
                    uncompletedBinlogSplits.put(binlogSplit.splitId(), binlogSplit);
                    requestBinlogSplitMetaIfNeeded(binlogSplit);
                } else {
                    uncompletedBinlogSplits.remove(binlogSplit.splitId());
                    if (!checkNewlyAddedTableSchema){
                        binlogSplit = discoverTableSchemasForBinlogSplit(binlogSplit, sourceConfig);
                    }
                    unfinishedSplits.add(binlogSplit);
                }
                LOG.info(
                        "Source reader {} received the binlog split : {}.", subtaskId, binlogSplit);
                context.sendSourceEventToCoordinator(new BinlogSplitAssignedEvent());
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including binlog split) to SourceReaderBase
        if (!unfinishedSplits.isEmpty()) {
            super.addSplits(unfinishedSplits);
        } else if (suspendedBinlogSplit
                        != null // request new snapshot split if the binlog split is suspended
                || getNumberOfCurrentlyAssignedSplits()
                        <= 1 // request when all splits are in removed tables
        ) {
            context.sendSplitRequest();
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
            FinishedSnapshotSplitsAckEvent ackEvent = (FinishedSnapshotSplitsAckEvent) sourceEvent;
            LOG.debug(
                    "Source reader {} receives ack event for {} from enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedUnackedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
            // report finished snapshot splits
            LOG.debug(
                    "Source reader {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else if (sourceEvent instanceof BinlogSplitMetaEvent) {
            LOG.debug(
                    "Source reader {} receives binlog meta with group id {}.",
                    subtaskId,
                    ((BinlogSplitMetaEvent) sourceEvent).getMetaGroupId());
            fillMetadataForBinlogSplit((BinlogSplitMetaEvent) sourceEvent);
        } else if (sourceEvent instanceof FinishedBinlogNewTableAddRequestEvent) {
            LOG.info("Source reader {} receives notification that runtime new tables added.", subtaskId);
        } else if (sourceEvent instanceof BinlogSplitStartEvent) {
            updateBinlogSplitAndStart((BinlogSplitStartEvent) sourceEvent);
            LOG.info("Source reader {} receives binlog table add update event.", subtaskId);
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void updateBinlogSplitAndStart(BinlogSplitStartEvent sourceEvent) {
        if (suspendedBinlogSplit != null) {
            List<String> existedSplits =
                    suspendedBinlogSplit.getFinishedSnapshotSplitInfos().stream()
                            .map(FinishedSnapshotSplitInfo::getSplitId)
                            .sorted()
                            .collect(Collectors.toList());
            List<FinishedSnapshotSplitInfo> newAddedMetadataGroup =
                    sourceEvent.getMetaGroup().stream()
                            .map(FinishedSnapshotSplitInfo::deserialize)
                            .filter(r -> !existedSplits.contains(r.getSplitId()))
                            .collect(Collectors.toList());

            MySqlBinlogSplit mySqlBinlogSplit = MySqlBinlogSplit.adjustFinishedSplitInfos(
                    suspendedBinlogSplit, newAddedMetadataGroup);

            suspendedBinlogSplit = null;
            this.addSplits(Collections.singletonList(mySqlBinlogSplit), false);

            context.sendSourceEventToCoordinator(new BinlogSplitUpdateAckEvent());
            LOG.info(
                    "Source reader {} notifies enumerator that binlog split has been updated.",
                    subtaskId);

            mySqlSourceReaderContext.wakeupSuspendedBinlogSplitReader();
            LOG.info(
                    "Source reader {} wakes up suspended binlog reader as binlog split has been updated.",
                    subtaskId);
        } else {
            LOG.warn("Unexpected event {}, this should not happen.", sourceEvent);
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, BinlogOffset> finishedOffsets = new HashMap<>();
            for (MySqlSnapshotSplit split : finishedUnackedSplits.values()) {
                finishedOffsets.put(split.splitId(), split.getHighWatermark());
            }
            FinishedSnapshotSplitsReportEvent reportEvent =
                    new FinishedSnapshotSplitsReportEvent(finishedOffsets);
            context.sendSourceEventToCoordinator(reportEvent);
            LOG.debug(
                    "Source reader {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    finishedOffsets);
        }
    }

    private void requestBinlogSplitMetaIfNeeded(MySqlBinlogSplit binlogSplit) {
        final String splitId = binlogSplit.splitId();
        if (!binlogSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    ChunkUtils.getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            BinlogSplitMetaRequestEvent splitMetaRequestEvent =
                    new BinlogSplitMetaRequestEvent(
                            splitId, nextMetaGroupId, binlogSplit.getTotalFinishedSplitSize());
            context.sendSourceEventToCoordinator(splitMetaRequestEvent);
        } else {
            LOG.info("Source reader {} collects meta of binlog split success", subtaskId);
            this.addSplits(Collections.singletonList(binlogSplit));
        }
    }

    private void fillMetadataForBinlogSplit(BinlogSplitMetaEvent metadataEvent) {
        MySqlBinlogSplit binlogSplit = uncompletedBinlogSplits.get(metadataEvent.getSplitId());
        if (binlogSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int receivedTotalFinishedSplitSize = metadataEvent.getTotalFinishedSplitSize();
            final int expectedMetaGroupId =
                    ChunkUtils.getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            if (receivedTotalFinishedSplitSize < binlogSplit.getTotalFinishedSplitSize()) {
                LOG.warn(
                        "Source reader {} receives out of bound finished split size. The received finished split size is {}, but expected is {}, truncate it",
                        subtaskId,
                        receivedTotalFinishedSplitSize,
                        binlogSplit.getTotalFinishedSplitSize());
                binlogSplit =
                        MySqlBinlogSplit.toNormalBinlogSplit(
                                binlogSplit, receivedTotalFinishedSplitSize);
                uncompletedBinlogSplits.put(binlogSplit.splitId(), binlogSplit);
            } else if (receivedMetaGroupId == expectedMetaGroupId) {
                List<FinishedSnapshotSplitInfo> newAddedMetadataGroup;
                Set<String> existedSplitsOfLastGroup =
                        getExistedSplitsOfLastGroup(
                                binlogSplit.getFinishedSnapshotSplitInfos(),
                                sourceConfig.getSplitMetaGroupSize());
                newAddedMetadataGroup =
                        metadataEvent.getMetaGroup().stream()
                                .map(FinishedSnapshotSplitInfo::deserialize)
                                .filter(r -> !existedSplitsOfLastGroup.contains(r.getSplitId()))
                                .collect(Collectors.toList());

                uncompletedBinlogSplits.put(
                        binlogSplit.splitId(),
                        MySqlBinlogSplit.appendFinishedSplitInfos(
                                binlogSplit, newAddedMetadataGroup));
                LOG.info(
                        "Source reader {} fills metadata of group {} to binlog split",
                        subtaskId,
                        newAddedMetadataGroup.size());
            } else {
                LOG.warn(
                        "Source reader {} receives out of oder binlog meta event for split {}, the received meta group id is {}, but expected is {}, ignore it",
                        subtaskId,
                        metadataEvent.getSplitId(),
                        receivedMetaGroupId,
                        expectedMetaGroupId);
            }
            requestBinlogSplitMetaIfNeeded(uncompletedBinlogSplits.get(binlogSplit.splitId()));
        } else {
            LOG.warn(
                    "Source reader {} receives binlog meta event for split {}, but the uncompleted split map does not contain it",
                    subtaskId,
                    metadataEvent.getSplitId());
        }
    }

    private MySqlBinlogSplit discoverTableSchemasForBinlogSplit(
            MySqlBinlogSplit split,
            MySqlSourceConfig sourceConfig) {
        if (split.getTableSchemas().isEmpty()) {
            try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
                Map<TableId, TableChanges.TableChange> tableSchemas;
                tableSchemas =
                        TableDiscoveryUtils.discoverSchemaForCapturedTables(
                                partition, sourceConfig, jdbc);
                LOG.info(
                        "Source reader {} discovers table schema for binlog split {} success",
                        subtaskId,
                        split.splitId());

                return MySqlBinlogSplit.fillTableSchemas(split, tableSchemas);
            } catch (SQLException e) {
                LOG.error(
                        "Source reader {} failed to obtains table schemas due to {}",
                        subtaskId,
                        e.getMessage());
                throw new FlinkRuntimeException(e);
            }
        } else {
            LOG.warn(
                    "Source reader {} skip the table schema discovery, the binlog split {} has table schemas yet.",
                    subtaskId,
                    split);
            return split;
        }
    }

    public Map<TableId, TableChanges.TableChange> discoverSchemaForNewAddedTables(MySqlBinlogSplit split) {
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            List<TableId> existedTables = new ArrayList<>(split.getTableSchemas().keySet());
            Map<TableId, TableChanges.TableChange> tableSchemas = TableDiscoveryUtils.discoverSchemaForNewAddedTables(
                    partition, existedTables, sourceConfig, jdbc);
            LOG.info(
                    "Source reader {} discovers table schema for binlog split {} success",
                    subtaskId,
                    split.splitId());
            return tableSchemas;
        } catch (SQLException e) {
            LOG.error(
                    "Source reader {} failed to obtains table schemas due to {}",
                    subtaskId,
                    e.getMessage());
            throw new FlinkRuntimeException(e);
        }
    }

    private Set<String> getExistedSplitsOfLastGroup(
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplits, int metaGroupSize) {
        int splitsNumOfLastGroup =
                finishedSnapshotSplits.size() % sourceConfig.getSplitMetaGroupSize();
        if (splitsNumOfLastGroup != 0) {
            int lastGroupStart =
                    ((int) (finishedSnapshotSplits.size() / sourceConfig.getSplitMetaGroupSize()))
                            * metaGroupSize;
            // Keep same order with MySqlHybridSplitAssigner.createBinlogSplit() to avoid
            // 'invalid request meta group id' error
            List<String> sortedFinishedSnapshotSplits =
                    finishedSnapshotSplits.stream()
                            .map(FinishedSnapshotSplitInfo::getSplitId)
                            .sorted()
                            .collect(Collectors.toList());
            return new HashSet<>(
                    sortedFinishedSnapshotSplits.subList(
                            lastGroupStart, lastGroupStart + splitsNumOfLastGroup));
        }
        return new HashSet<>();
    }

    private void logCurrentBinlogOffsets(List<MySqlSplit> splits, long checkpointId) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        for (MySqlSplit split : splits) {
            if (!split.isBinlogSplit()) {
                return;
            }
            BinlogOffset offset = split.asBinlogSplit().getStartingOffset();
            LOG.info("Binlog offset on checkpoint {}: {}", checkpointId, offset);
        }
    }

    @Override
    protected MySqlSplit toSplitType(String splitId, MySqlSplitState splitState) {
        return splitState.toMySqlSplit();
    }

    @VisibleForTesting
    public Map<String, MySqlSnapshotSplit> getFinishedUnackedSplits() {
        return finishedUnackedSplits;
    }
}
