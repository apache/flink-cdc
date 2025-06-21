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

package org.apache.flink.cdc.connectors.base.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsAckEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsReportEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsRequestEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberRequestEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitAssignedEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitMetaEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitMetaRequestEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitUpdateAckEvent;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitUpdateRequestEvent;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplitState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;
import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.filterOutdatedSplitInfos;
import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.toNormalStreamSplit;
import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.toSuspendedStreamSplit;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The multi-parallel source reader for table snapshot phase from {@link SnapshotSplit} and then
 * single-parallel source reader for table stream phase from {@link StreamSplit}.
 */
@Experimental
public class IncrementalSourceReader<T, C extends SourceConfig>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecords, T, SourceSplitBase, SourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceReader.class);

    /**
     * Snapshot spit which is read finished, but not notify and receive ack event from enumerator to
     * update snapshot splits information (such as high_watermark).
     */
    private final Map<String, SnapshotSplit> finishedUnackedSplits;

    /**
     * Stream split which lack and wait for snapshot splits information from enumerator, will
     * addSplit and add back to split reader if snapshot split information is enough.
     */
    protected final Map<String, StreamSplit> uncompletedStreamSplits;

    /**
     * Steam split which is suspended reading in split reader and wait for
     * LatestFinishedSplitsNumberEvent from enumerator, will addSpilt and become
     * uncompletedStreamSplits later.
     */
    protected volatile StreamSplit suspendedStreamSplit;

    private final int subtaskId;
    private final SourceSplitSerializer sourceSplitSerializer;
    protected final C sourceConfig;
    protected final DataSourceDialect<C> dialect;

    private final IncrementalSourceReaderContext incrementalSourceReaderContext;

    public IncrementalSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementQueue,
            Supplier<IncrementalSourceSplitReader<C>> splitReaderSupplier,
            RecordEmitter<SourceRecords, T, SourceSplitState> recordEmitter,
            Configuration config,
            IncrementalSourceReaderContext incrementalSourceReaderContext,
            C sourceConfig,
            SourceSplitSerializer sourceSplitSerializer,
            DataSourceDialect<C> dialect) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                incrementalSourceReaderContext.getSourceReaderContext());
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.uncompletedStreamSplits = new HashMap<>();
        this.subtaskId = context.getIndexOfSubtask();
        this.sourceSplitSerializer = checkNotNull(sourceSplitSerializer);
        this.dialect = dialect;
        this.incrementalSourceReaderContext = incrementalSourceReaderContext;
        this.suspendedStreamSplit = null;
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() <= 1) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (split.isSnapshotSplit()) {
            return new SnapshotSplitState(split.asSnapshotSplit());
        } else {
            return new StreamSplitState(split.asStreamSplit());
        }
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        // unfinished splits
        List<SourceSplitBase> stateSplits = super.snapshotState(checkpointId);

        // unfinished splits
        List<SourceSplitBase> unfinishedSplits =
                stateSplits.stream()
                        .filter(split -> !finishedUnackedSplits.containsKey(split.splitId()))
                        .collect(Collectors.toList());

        // add finished snapshot splits that did not receive ack yet
        unfinishedSplits.addAll(finishedUnackedSplits.values());

        // add stream splits who are uncompleted
        unfinishedSplits.addAll(uncompletedStreamSplits.values());

        // add suspended StreamSplit
        if (suspendedStreamSplit != null) {
            unfinishedSplits.add(suspendedStreamSplit);
        }

        logCurrentStreamOffsets(unfinishedSplits, checkpointId);

        return unfinishedSplits;
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitState> finishedSplitIds) {
        boolean requestNextSplit = true;
        if (isNewlyAddedTableSplitAndStreamSplit(finishedSplitIds)) {
            SourceSplitState streamSplitState = finishedSplitIds.remove(STREAM_SPLIT_ID);
            finishedSplitIds
                    .values()
                    .forEach(
                            newAddedSplitState ->
                                    finishedUnackedSplits.put(
                                            newAddedSplitState.toSourceSplit().splitId(),
                                            newAddedSplitState
                                                    .asSnapshotSplitState()
                                                    .toSourceSplit()));
            Preconditions.checkState(finishedSplitIds.values().size() == 1);
            LOG.info(
                    "Source reader {} finished stream split and snapshot split {}",
                    subtaskId,
                    finishedSplitIds.values().iterator().next().toSourceSplit().splitId());
            this.addSplits(Collections.singletonList(streamSplitState.toSourceSplit()));
        } else {
            Preconditions.checkState(finishedSplitIds.size() == 1);

            for (SourceSplitState splitState : finishedSplitIds.values()) {
                SourceSplitBase sourceSplit = splitState.toSourceSplit();
                if (sourceSplit.isStreamSplit()) {
                    // Two possibilities that finish a stream split:
                    //
                    // 1. Stream reader is suspended by enumerator because new tables have been
                    // finished its snapshot reading.
                    // Under this case incrementalSourceReaderContext.isStreamSplitReaderSuspended()
                    // is true and need to request the latest finished splits number.
                    //
                    // 2. Stream reader reaches the ending offset of the split. We need to do
                    // nothing under this case.
                    if (incrementalSourceReaderContext.isStreamSplitReaderSuspended()) {
                        suspendedStreamSplit = toSuspendedStreamSplit(sourceSplit.asStreamSplit());
                        LOG.info(
                                "Source reader {} suspended stream split reader success after the newly added table process, current offset {}",
                                subtaskId,
                                suspendedStreamSplit.getStartingOffset());
                        context.sendSourceEventToCoordinator(
                                new LatestFinishedSplitsNumberRequestEvent());
                        // do not request next split when the reader is suspended
                        requestNextSplit = false;
                    }
                } else {
                    finishedUnackedSplits.put(sourceSplit.splitId(), sourceSplit.asSnapshotSplit());
                }
            }
            reportFinishedSnapshotSplitsIfNeed();
        }
        if (requestNextSplit) {
            context.sendSplitRequest();
        }
    }

    /**
     * During the newly added table process, for the source reader who holds the stream split, we
     * return the latest finished snapshot split and stream split as well, this design let us have
     * opportunity to exchange stream reading and snapshot reading, we put the stream split back.
     */
    private boolean isNewlyAddedTableSplitAndStreamSplit(
            Map<String, SourceSplitState> finishedSplitIds) {
        return finishedSplitIds.containsKey(STREAM_SPLIT_ID) && finishedSplitIds.size() == 2;
    }

    @Override
    public void addSplits(List<SourceSplitBase> splits) {
        addSplits(splits, true);
    }

    /**
     * Adds a list of splits for this reader to read.
     *
     * @param splits the splits to add.
     * @param checkTableChangeForStreamSplit to check the captured table list change or not, it
     *     should be true for reader which is during restoration from a checkpoint or savepoint.
     */
    private void addSplits(List<SourceSplitBase> splits, boolean checkTableChangeForStreamSplit) {
        // restore for finishedUnackedSplits
        List<SourceSplitBase> unfinishedSplits = new ArrayList<>();
        for (SourceSplitBase split : splits) {
            if (split.isSnapshotSplit()) {
                SnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (dialect.isIncludeDataCollection(sourceConfig, snapshotSplit.getTableId())) {
                    if (snapshotSplit.isSnapshotReadFinished()) {
                        finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                    } else {
                        unfinishedSplits.add(split);
                    }
                } else {
                    LOG.info(
                            "The subtask {} is skipping split {} because it does not match new table filter.",
                            subtaskId,
                            split.splitId());
                }
            } else {
                StreamSplit streamSplit = split.asStreamSplit();
                // When restore from a checkpoint, the finished split infos may contain some splits
                // for the deleted tables.
                // We need to remove these splits for the deleted tables at the finished split
                // infos.
                if (checkTableChangeForStreamSplit) {
                    LOG.info("before checkTableChangeForStreamSplit: " + streamSplit);
                    streamSplit =
                            filterOutdatedSplitInfos(
                                    streamSplit,
                                    (tableId) ->
                                            dialect.isIncludeDataCollection(sourceConfig, tableId));
                    LOG.info("after checkTableChangeForStreamSplit: " + streamSplit);
                }

                // Try to discovery table schema once for newly added tables when source reader
                // start or restore
                boolean checkNewlyAddedTableSchema =
                        !incrementalSourceReaderContext.isHasAssignedStreamSplit()
                                && sourceConfig.isScanNewlyAddedTableEnabled();
                incrementalSourceReaderContext.setHasAssignedStreamSplit(true);

                // the stream split is suspended
                if (streamSplit.isSuspended()) {
                    suspendedStreamSplit = streamSplit;
                } else if (!streamSplit.isCompletedSplit()) {
                    // the stream split is uncompleted
                    uncompletedStreamSplits.put(split.splitId(), split.asStreamSplit());
                    requestStreamSplitMetaIfNeeded(split.asStreamSplit());
                } else {
                    uncompletedStreamSplits.remove(split.splitId());
                    streamSplit =
                            discoverTableSchemasForStreamSplit(
                                    streamSplit, checkNewlyAddedTableSchema);
                    unfinishedSplits.add(streamSplit);
                }

                LOG.info(
                        "Source reader {} received the stream split : {}.", subtaskId, streamSplit);
                context.sendSourceEventToCoordinator(new StreamSplitAssignedEvent());
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including binlog split) to SourceReaderBase
        if (!unfinishedSplits.isEmpty()) {
            super.addSplits(unfinishedSplits);
        } else if (suspendedStreamSplit != null
                || getNumberOfCurrentlyAssignedSplits()
                        <= 1) { // only request new snapshot split if the stream split is suspended
            context.sendSplitRequest();
        }
    }

    private StreamSplit discoverTableSchemasForStreamSplit(
            StreamSplit split, boolean checkNewlyAddedTableSchema) {
        final String splitId = split.splitId();
        if (split.getTableSchemas().isEmpty() || checkNewlyAddedTableSchema) {
            Map<TableId, TableChanges.TableChange> tableSchemas;
            try {
                Map<TableId, TableChanges.TableChange> existTableSchemas = split.getTableSchemas();
                tableSchemas = dialect.discoverDataCollectionSchemas(sourceConfig);
                tableSchemas.putAll(existTableSchemas);
                LOG.info(
                        "Source reader {} discovers table schema for stream split {} success",
                        subtaskId,
                        splitId);
                return StreamSplit.fillTableSchemas(split, tableSchemas);
            } catch (Exception e) {
                LOG.error(
                        "Source reader {} failed to obtains table schemas due to {}",
                        subtaskId,
                        e.getMessage());
                throw new FlinkRuntimeException(e);
            }
        } else {
            LOG.warn(
                    "Source reader {} skip the table schema discovery, the stream split {} has table schemas yet.",
                    subtaskId,
                    split);
            return split;
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
            // Keep same order with HybridSplitAssigner.createStreamSplit() to avoid
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
                    "The subtask {} receives stream meta with group id {}.",
                    subtaskId,
                    ((StreamSplitMetaEvent) sourceEvent).getMetaGroupId());
            fillMetaDataForStreamSplit((StreamSplitMetaEvent) sourceEvent);
        } else if (sourceEvent instanceof StreamSplitUpdateRequestEvent) {
            suspendStreamSplitReader();
        } else if (sourceEvent instanceof LatestFinishedSplitsNumberEvent) {
            updateStreamSplitFinishedSplitsSize((LatestFinishedSplitsNumberEvent) sourceEvent);
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void suspendStreamSplitReader() {
        incrementalSourceReaderContext.suspendStreamSplitReader();
    }

    private void fillMetaDataForStreamSplit(StreamSplitMetaEvent metadataEvent) {
        StreamSplit streamSplit = uncompletedStreamSplits.get(metadataEvent.getSplitId());
        if (streamSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int receivedTotalFinishedSplitSize = metadataEvent.getTotalFinishedSplitSize();
            final int expectedMetaGroupId =
                    getNextMetaGroupId(
                            streamSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            if (receivedTotalFinishedSplitSize < streamSplit.getTotalFinishedSplitSize()) {
                LOG.warn(
                        "Source reader {} receives out of bound finished split size. The received finished split size is {}, but expected is {}, truncate it",
                        subtaskId,
                        receivedTotalFinishedSplitSize,
                        streamSplit.getTotalFinishedSplitSize());
                streamSplit = toNormalStreamSplit(streamSplit, receivedTotalFinishedSplitSize);
                uncompletedStreamSplits.put(streamSplit.splitId(), streamSplit);
            } else if (receivedMetaGroupId == expectedMetaGroupId) {
                Set<String> existedSplitsOfLastGroup =
                        getExistedSplitsOfLastGroup(
                                streamSplit.getFinishedSnapshotSplitInfos(),
                                sourceConfig.getSplitMetaGroupSize());

                List<FinishedSnapshotSplitInfo> newAddedMetadataGroup =
                        metadataEvent.getMetaGroup().stream()
                                .map(sourceSplitSerializer::deserialize)
                                .filter(r -> !existedSplitsOfLastGroup.contains(r.getSplitId()))
                                .collect(Collectors.toList());
                uncompletedStreamSplits.put(
                        streamSplit.splitId(),
                        StreamSplit.appendFinishedSplitInfos(streamSplit, newAddedMetadataGroup));

                LOG.info("Fill metadata of group {} to stream split", newAddedMetadataGroup.size());
            } else {
                LOG.warn(
                        "Received out of oder metadata event for split {}, the received meta group id is {}, but expected is {}, ignore it",
                        metadataEvent.getSplitId(),
                        receivedMetaGroupId,
                        expectedMetaGroupId);
            }
            requestStreamSplitMetaIfNeeded(uncompletedStreamSplits.get(streamSplit.splitId()));
        } else {
            LOG.warn(
                    "Received metadata event for split {}, but the uncompleted split map does not contain it",
                    metadataEvent.getSplitId());
        }
    }

    private void requestStreamSplitMetaIfNeeded(StreamSplit streamSplit) {
        final String splitId = streamSplit.splitId();
        if (!streamSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    getNextMetaGroupId(
                            streamSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            StreamSplitMetaRequestEvent splitMetaRequestEvent =
                    new StreamSplitMetaRequestEvent(
                            splitId, nextMetaGroupId, streamSplit.getTotalFinishedSplitSize());
            context.sendSourceEventToCoordinator(splitMetaRequestEvent);
        } else {
            LOG.info("The meta of stream split {} has been collected success", splitId);
            this.addSplits(Collections.singletonList(streamSplit));
        }
    }

    protected void updateStreamSplitFinishedSplitsSize(
            LatestFinishedSplitsNumberEvent sourceEvent) {
        if (suspendedStreamSplit != null) {
            final int finishedSplitsSize = sourceEvent.getLatestFinishedSplitsNumber();
            final StreamSplit streamSplit =
                    toNormalStreamSplit(suspendedStreamSplit, finishedSplitsSize);
            suspendedStreamSplit = null;
            this.addSplits(Collections.singletonList(streamSplit), false);

            context.sendSourceEventToCoordinator(new StreamSplitUpdateAckEvent());
            LOG.info(
                    "Source reader {} notifies enumerator that stream split has been updated.",
                    subtaskId);

            incrementalSourceReaderContext.wakeupSuspendedStreamSplitReader();
            LOG.info(
                    "Source reader {} wakes up suspended stream reader as stream split has been updated.",
                    subtaskId);
        } else {
            LOG.warn("Unexpected event {}, this should not happen.", sourceEvent);
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, Offset> finishedOffsets = new HashMap<>();
            for (SnapshotSplit split : finishedUnackedSplits.values()) {
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
        return receivedMetaNum / metaGroupSize;
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
        return splitState.toSourceSplit();
    }

    private void logCurrentStreamOffsets(List<SourceSplitBase> splits, long checkpointId) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        for (SourceSplitBase split : splits) {
            if (!split.isStreamSplit()) {
                return;
            }
            Offset offset = split.asStreamSplit().getStartingOffset();
            LOG.info("Stream split offset on checkpoint {}: {}", checkpointId, offset);
        }
    }

    @VisibleForTesting
    public Map<String, SnapshotSplit> getFinishedUnackedSplits() {
        return finishedUnackedSplits;
    }
}
