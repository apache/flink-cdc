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

package com.ververica.cdc.connectors.base.source.reader;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.base.source.meta.events.StreamSplitMetaEvent;
import com.ververica.cdc.connectors.base.source.meta.events.StreamSplitMetaRequestEvent;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplitState;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplitState;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The multi-parallel source reader for table snapshot phase from {@link SnapshotSplit} and then
 * single-parallel source reader for table stream phase from {@link StreamSplit}.
 */
@Experimental
public class JdbcIncrementalSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecord,
                T,
                SourceSplitBase<TableId, TableChange>,
                SourceSplitState<TableId, TableChange>> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcIncrementalSourceReader.class);

    private final Map<String, SnapshotSplit<TableId, TableChange>> finishedUnackedSplits;
    private final Map<String, StreamSplit<TableId, TableChange>> uncompletedBinlogSplits;
    private final int subtaskId;
    private final SourceSplitSerializer<TableId, TableChange> sourceSplitSerializer;
    private final JdbcSourceConfig sourceConfig;
    private final JdbcDataSourceDialect dialect;

    public JdbcIncrementalSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementQueue,
            Supplier<JdbcSourceSplitReader> splitReaderSupplier,
            RecordEmitter<SourceRecord, T, SourceSplitState<TableId, TableChange>> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            JdbcSourceConfig sourceConfig,
            SourceSplitSerializer<TableId, TableChange> sourceSplitSerializer,
            JdbcDataSourceDialect dialect) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context);
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.uncompletedBinlogSplits = new HashMap<>();
        this.subtaskId = context.getIndexOfSubtask();
        this.sourceSplitSerializer = checkNotNull(sourceSplitSerializer);
        this.dialect = dialect;
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected SourceSplitState<TableId, TableChange> initializedState(
            SourceSplitBase<TableId, TableChange> split) {
        if (split.isSnapshotSplit()) {
            return new SnapshotSplitState<>(split.asSnapshotSplit());
        } else {
            return new StreamSplitState<>(split.asStreamSplit());
        }
    }

    @Override
    public List<SourceSplitBase<TableId, TableChange>> snapshotState(long checkpointId) {
        // unfinished splits
        List<SourceSplitBase<TableId, TableChange>> stateSplits = super.snapshotState(checkpointId);

        // add finished snapshot splits that didn't receive ack yet
        stateSplits.addAll(finishedUnackedSplits.values());

        // add binlog splits who are uncompleted
        stateSplits.addAll(uncompletedBinlogSplits.values());

        return stateSplits;
    }

    @Override
    protected void onSplitFinished(
            Map<String, SourceSplitState<TableId, TableChange>> finishedSplitIds) {
        for (SourceSplitState<TableId, TableChange> splitState : finishedSplitIds.values()) {
            SourceSplitBase<TableId, TableChange> sourceSplit = splitState.toSourceSplit();
            checkState(
                    sourceSplit.isSnapshotSplit(),
                    String.format(
                            "Only snapshot split could finish, but the actual split is binlog split %s",
                            sourceSplit));
            finishedUnackedSplits.put(sourceSplit.splitId(), sourceSplit.asSnapshotSplit());
        }
        reportFinishedSnapshotSplitsIfNeed();
        context.sendSplitRequest();
    }

    @Override
    public void addSplits(List<SourceSplitBase<TableId, TableChange>> splits) {
        // restore for finishedUnackedSplits
        List<SourceSplitBase<TableId, TableChange>> unfinishedSplits = new ArrayList<>();
        for (SourceSplitBase<TableId, TableChange> split : splits) {
            if (split.isSnapshotSplit()) {
                SnapshotSplit<TableId, TableChange> snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                } else {
                    unfinishedSplits.add(split);
                }
            } else {
                // the binlog split is uncompleted
                if (!split.asStreamSplit().isCompletedSplit()) {
                    uncompletedBinlogSplits.put(split.splitId(), split.asStreamSplit());
                    requestBinlogSplitMetaIfNeeded(split.asStreamSplit());
                } else {
                    uncompletedBinlogSplits.remove(split.splitId());
                    StreamSplit<TableId, TableChange> streamSplit =
                            discoverTableSchemasForBinlogSplit(split.asStreamSplit());
                    unfinishedSplits.add(streamSplit);
                }
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including binlog split) to SourceReaderBase
        super.addSplits(unfinishedSplits);
    }

    private StreamSplit<TableId, TableChange> discoverTableSchemasForBinlogSplit(
            StreamSplit<TableId, TableChange> split) {
        final String splitId = split.splitId();
        if (split.getTableSchemas().isEmpty()) {
            try {
                Map<TableId, TableChange> tableSchemas =
                        dialect.discoverDataCollectionSchemas(sourceConfig);
                LOG.info("The table schema discovery for binlog split {} success", splitId);
                return StreamSplit.fillTableSchemas(split, tableSchemas);
            } catch (Exception e) {
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
        } else if (sourceEvent instanceof StreamSplitMetaEvent) {
            LOG.debug(
                    "The subtask {} receives binlog meta with group id {}.",
                    subtaskId,
                    ((StreamSplitMetaEvent) sourceEvent).getMetaGroupId());
            fillMetaDataForBinlogSplit((StreamSplitMetaEvent) sourceEvent);
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void fillMetaDataForBinlogSplit(StreamSplitMetaEvent metadataEvent) {
        StreamSplit<TableId, TableChange> binlogSplit =
                uncompletedBinlogSplits.get(metadataEvent.getSplitId());
        if (binlogSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int expectedMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            if (receivedMetaGroupId == expectedMetaGroupId) {
                List<FinishedSnapshotSplitInfo<TableId>> metaDataGroup =
                        metadataEvent.getMetaGroup().stream()
                                .map(sourceSplitSerializer::readFinishedSplitInfo)
                                .collect(Collectors.toList());
                uncompletedBinlogSplits.put(
                        binlogSplit.splitId(),
                        StreamSplit.appendFinishedSplitInfos(binlogSplit, metaDataGroup));

                LOG.info("Fill meta data of group {} to binlog split", metaDataGroup.size());
            } else {
                LOG.warn(
                        "Received out of oder binlog meta event for split {}, the received meta group id is {}, but expected is {}, ignore it",
                        metadataEvent.getSplitId(),
                        receivedMetaGroupId,
                        expectedMetaGroupId);
            }
            requestBinlogSplitMetaIfNeeded(binlogSplit);
        } else {
            LOG.warn(
                    "Received binlog meta event for split {}, but the uncompleted split map does not contain it",
                    metadataEvent.getSplitId());
        }
    }

    private void requestBinlogSplitMetaIfNeeded(StreamSplit<TableId, TableChange> streamSplit) {
        final String splitId = streamSplit.splitId();
        if (!streamSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    getNextMetaGroupId(
                            streamSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            StreamSplitMetaRequestEvent splitMetaRequestEvent =
                    new StreamSplitMetaRequestEvent(splitId, nextMetaGroupId);
            context.sendSourceEventToCoordinator(splitMetaRequestEvent);
        } else {
            LOG.info("The meta of stream split {} has been collected success", splitId);
            this.addSplits(Arrays.asList(streamSplit));
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, Offset> finishedOffsets = new HashMap<>();
            for (SnapshotSplit<TableId, TableChange> split : finishedUnackedSplits.values()) {
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

    /** Returns next meta group id according to received meta number and meta group size. */
    public static int getNextMetaGroupId(int receivedMetaNum, int metaGroupSize) {
        Preconditions.checkState(metaGroupSize > 0);
        return receivedMetaNum % metaGroupSize == 0
                ? (receivedMetaNum / metaGroupSize)
                : (receivedMetaNum / metaGroupSize) + 1;
    }

    @Override
    protected SourceSplitBase<TableId, TableChange> toSplitType(
            String splitId, SourceSplitState<TableId, TableChange> splitState) {
        return splitState.toSourceSplit();
    }
}
