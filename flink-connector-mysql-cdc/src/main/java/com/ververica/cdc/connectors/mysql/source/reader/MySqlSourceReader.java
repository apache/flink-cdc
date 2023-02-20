/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitMetaEvent;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitMetaRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitUpdateAckEvent;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitUpdateRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.LatestFinishedSplitsNumberEvent;
import com.ververica.cdc.connectors.mysql.source.events.LatestFinishedSplitsNumberRequestEvent;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
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

import static com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;
import static com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit.toNormalBinlogSplit;
import static com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit.toSuspendedBinlogSplit;
import static com.ververica.cdc.connectors.mysql.source.utils.ChunkUtils.getNextMetaGroupId;
import static com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils.discoverSchemaForNewAddedTables;

/** The source reader for MySQL source splits. */
public class MySqlSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecords, T, MySqlSplit, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);
    private final MySqlSourceConfig sourceConfig;
    private final Map<String, MySqlSnapshotSplit> finishedUnackedSplits;
    private final Map<String, MySqlBinlogSplit> uncompletedBinlogSplits;
    private final int subtaskId;
    private final MySqlSourceReaderContext mySqlSourceReaderContext;
    private volatile MySqlBinlogSplit suspendedBinlogSplit;

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
                        .collect(Collectors.toList());

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
        boolean requestNextSplit = true;
        if (isNewlyAddedTableSplitAndBinlogSplit(finishedSplitIds)) {
            MySqlSplitState mySqlBinlogSplitState = finishedSplitIds.remove(BINLOG_SPLIT_ID);
            finishedSplitIds
                    .values()
                    .forEach(
                            newAddedSplitState ->
                                    finishedUnackedSplits.put(
                                            newAddedSplitState.toMySqlSplit().splitId(),
                                            newAddedSplitState.toMySqlSplit().asSnapshotSplit()));
            this.addSplits(Collections.singletonList(mySqlBinlogSplitState.toMySqlSplit()));
        } else {
            Preconditions.checkState(finishedSplitIds.size() == 1);
            for (MySqlSplitState mySqlSplitState : finishedSplitIds.values()) {
                MySqlSplit mySqlSplit = mySqlSplitState.toMySqlSplit();
                if (mySqlSplit.isBinlogSplit()) {
                    suspendedBinlogSplit = toSuspendedBinlogSplit(mySqlSplit.asBinlogSplit());
                    LOG.info(
                            "binlog split reader suspended success after the newly added table process, current offset {}",
                            suspendedBinlogSplit.getStartingOffset());
                    context.sendSourceEventToCoordinator(
                            new LatestFinishedSplitsNumberRequestEvent());
                    // do not request next split when the reader is suspended
                    requestNextSplit = false;
                } else {
                    finishedUnackedSplits.put(mySqlSplit.splitId(), mySqlSplit.asSnapshotSplit());
                }
            }
            reportFinishedSnapshotSplitsIfNeed();
        }

        if (requestNextSplit) {
            context.sendSplitRequest();
        }
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
        // restore for finishedUnackedSplits
        List<MySqlSplit> unfinishedSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            LOG.info("Add Split: " + split);
            if (split.isSnapshotSplit()) {
                MySqlSnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                } else {
                    unfinishedSplits.add(split);
                }
            } else {
                MySqlBinlogSplit binlogSplit = split.asBinlogSplit();
                // Try to discovery table schema once for newly added tables when source reader
                // start or restore
                boolean checkNewlyAddedTableSchema =
                        !mySqlSourceReaderContext.isHasAssignedBinlogSplit()
                                && sourceConfig.isScanNewlyAddedTableEnabled();
                mySqlSourceReaderContext.setHasAssignedBinlogSplit(true);

                // the binlog split is suspended
                if (binlogSplit.isSuspended()) {
                    suspendedBinlogSplit = binlogSplit;
                } else if (!binlogSplit.isCompletedSplit()) {
                    uncompletedBinlogSplits.put(split.splitId(), split.asBinlogSplit());
                    requestBinlogSplitMetaIfNeeded(split.asBinlogSplit());
                } else {
                    uncompletedBinlogSplits.remove(split.splitId());
                    MySqlBinlogSplit mySqlBinlogSplit =
                            discoverTableSchemasForBinlogSplit(
                                    split.asBinlogSplit(),
                                    sourceConfig,
                                    checkNewlyAddedTableSchema);
                    unfinishedSplits.add(mySqlBinlogSplit);
                }
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including binlog split) to SourceReaderBase
        if (!unfinishedSplits.isEmpty()) {
            super.addSplits(unfinishedSplits);
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
            FinishedSnapshotSplitsAckEvent ackEvent = (FinishedSnapshotSplitsAckEvent) sourceEvent;
            LOG.debug(
                    "The subtask {} receives ack event for {} from enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedUnackedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
            // report finished snapshot splits
            LOG.debug(
                    "The subtask {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else if (sourceEvent instanceof BinlogSplitMetaEvent) {
            LOG.debug(
                    "The subtask {} receives binlog meta with group id {}.",
                    subtaskId,
                    ((BinlogSplitMetaEvent) sourceEvent).getMetaGroupId());
            fillMetadataForBinlogSplit((BinlogSplitMetaEvent) sourceEvent);
        } else if (sourceEvent instanceof BinlogSplitUpdateRequestEvent) {
            handleBinlogSplitUpdateRequest();
        } else if (sourceEvent instanceof LatestFinishedSplitsNumberEvent) {
            updateBinlogSplit((LatestFinishedSplitsNumberEvent) sourceEvent);
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void handleBinlogSplitUpdateRequest() {
        mySqlSourceReaderContext.suspendBinlogSplitReader();
    }

    private void updateBinlogSplit(LatestFinishedSplitsNumberEvent sourceEvent) {
        if (suspendedBinlogSplit != null) {
            final int finishedSplitsSize = sourceEvent.getLatestFinishedSplitsNumber();
            final MySqlBinlogSplit binlogSplit =
                    toNormalBinlogSplit(suspendedBinlogSplit, finishedSplitsSize);
            suspendedBinlogSplit = null;
            this.addSplits(Collections.singletonList(binlogSplit));

            context.sendSourceEventToCoordinator(new BinlogSplitUpdateAckEvent());
            LOG.info("Notify enumerator that binlog split has been updated.");

            mySqlSourceReaderContext.wakeupSuspendedBinlogSplitReader();
            LOG.info("Wake up suspended binlog reader as binlog split has been updated.");
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
                    "The subtask {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    finishedOffsets);
        }
    }

    private void requestBinlogSplitMetaIfNeeded(MySqlBinlogSplit binlogSplit) {
        final String splitId = binlogSplit.splitId();
        if (!binlogSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            BinlogSplitMetaRequestEvent splitMetaRequestEvent =
                    new BinlogSplitMetaRequestEvent(splitId, nextMetaGroupId);
            context.sendSourceEventToCoordinator(splitMetaRequestEvent);
        } else {
            LOG.info("The meta of binlog split {} has been collected success", splitId);
            this.addSplits(Collections.singletonList(binlogSplit));
        }
    }

    private void fillMetadataForBinlogSplit(BinlogSplitMetaEvent metadataEvent) {
        MySqlBinlogSplit binlogSplit = uncompletedBinlogSplits.get(metadataEvent.getSplitId());
        if (binlogSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int expectedMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            if (receivedMetaGroupId == expectedMetaGroupId) {
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
                LOG.info("Fill metadata of group {} to binlog split", newAddedMetadataGroup.size());
            } else {
                LOG.warn(
                        "Received out of oder binlog meta event for split {}, the received meta group id is {}, but expected is {}, ignore it",
                        metadataEvent.getSplitId(),
                        receivedMetaGroupId,
                        expectedMetaGroupId);
            }
            requestBinlogSplitMetaIfNeeded(uncompletedBinlogSplits.get(binlogSplit.splitId()));
        } else {
            LOG.warn(
                    "Received binlog meta event for split {}, but the uncompleted split map does not contain it",
                    metadataEvent.getSplitId());
        }
    }

    private MySqlBinlogSplit discoverTableSchemasForBinlogSplit(
            MySqlBinlogSplit split,
            MySqlSourceConfig sourceConfig,
            boolean checkNewlyAddedTableSchema) {
        if (split.getTableSchemas().isEmpty() || checkNewlyAddedTableSchema) {
            try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
                Map<TableId, TableChanges.TableChange> tableSchemas;
                if (split.getTableSchemas().isEmpty()) {
                    tableSchemas =
                            TableDiscoveryUtils.discoverSchemaForCapturedTables(sourceConfig, jdbc);
                    LOG.info(
                            "The table schema discovery for binlog split {} success",
                            split.splitId());
                } else {
                    List<TableId> existedTables = new ArrayList<>(split.getTableSchemas().keySet());
                    tableSchemas =
                            discoverSchemaForNewAddedTables(existedTables, sourceConfig, jdbc);
                    LOG.info(
                            "The table schema discovery for new added tables of binlog split {} success",
                            split.splitId());
                }
                return MySqlBinlogSplit.fillTableSchemas(split, tableSchemas);
            } catch (SQLException e) {
                LOG.error("Failed to obtains table schemas due to {}", e.getMessage());
                throw new FlinkRuntimeException(e);
            }
        } else {
            LOG.warn(
                    "The binlog split {} has table schemas yet, skip the table schema discovery",
                    split);
            return split;
        }
    }

    private Set<String> getExistedSplitsOfLastGroup(
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplits, int metaGroupSize) {
        Set<String> existedSplitsOfLastGroup = new HashSet<>();
        int splitsNumOfLastGroup =
                finishedSnapshotSplits.size() % sourceConfig.getSplitMetaGroupSize();
        if (splitsNumOfLastGroup != 0) {
            int lastGroupStart =
                    ((int) (finishedSnapshotSplits.size() / sourceConfig.getSplitMetaGroupSize()))
                            * metaGroupSize;
            existedSplitsOfLastGroup =
                    finishedSnapshotSplits
                            .subList(lastGroupStart, lastGroupStart + splitsNumOfLastGroup).stream()
                            .map(FinishedSnapshotSplitInfo::getSplitId)
                            .collect(Collectors.toSet());
        }
        return existedSplitsOfLastGroup;
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
}
