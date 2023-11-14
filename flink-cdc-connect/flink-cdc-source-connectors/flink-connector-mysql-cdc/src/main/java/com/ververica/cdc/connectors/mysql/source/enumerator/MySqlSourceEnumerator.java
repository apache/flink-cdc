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

package com.ververica.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitAssignedEvent;
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
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;

/**
 * A MySQL CDC source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
@Internal
public class MySqlSourceEnumerator implements SplitEnumerator<MySqlSplit, PendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceEnumerator.class);
    private static final long CHECK_EVENT_INTERVAL = 30_000L;

    private final SplitEnumeratorContext<MySqlSplit> context;
    private final MySqlSourceConfig sourceConfig;
    private final MySqlSplitAssigner splitAssigner;

    // using TreeSet to prefer assigning binlog split to task-0 for easier debug
    private final TreeSet<Integer> readersAwaitingSplit;
    private List<List<FinishedSnapshotSplitInfo>> binlogSplitMeta;

    @Nullable private Integer binlogSplitTaskId;

    public MySqlSourceEnumerator(
            SplitEnumeratorContext<MySqlSplit> context,
            MySqlSourceConfig sourceConfig,
            MySqlSplitAssigner splitAssigner) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @Override
    public void start() {
        splitAssigner.open();
        requestBinlogSplitUpdateIfNeed();
        this.context.callAsync(
                this::getRegisteredReader,
                this::syncWithReaders,
                CHECK_EVENT_INTERVAL,
                CHECK_EVENT_INTERVAL);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<MySqlSplit> splits, int subtaskId) {
        LOG.debug("The enumerator adds splits back: {}", splits);
        Optional<MySqlSplit> binlogSplit =
                splits.stream().filter(MySqlSplit::isBinlogSplit).findAny();
        if (binlogSplit.isPresent()) {
            LOG.info("The enumerator adds add binlog split back: {}", binlogSplit);
            this.binlogSplitTaskId = null;
        }
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // send BinlogSplitUpdateRequestEvent to source reader after newly added table
        // snapshot splits finished.
        if (isNewlyAddedAssigningSnapshotFinished(splitAssigner.getAssignerStatus())) {
            context.sendEventToSourceReader(subtaskId, new BinlogSplitUpdateRequestEvent());
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsReportEvent) {
            LOG.info(
                    "The enumerator under {} receives finished split offsets {} from subtask {}.",
                    splitAssigner.getAssignerStatus(),
                    sourceEvent,
                    subtaskId);
            FinishedSnapshotSplitsReportEvent reportEvent =
                    (FinishedSnapshotSplitsReportEvent) sourceEvent;
            Map<String, BinlogOffset> finishedOffsets = reportEvent.getFinishedOffsets();

            splitAssigner.onFinishedSplits(finishedOffsets);
            requestBinlogSplitUpdateIfNeed();

            // send acknowledge event
            FinishedSnapshotSplitsAckEvent ackEvent =
                    new FinishedSnapshotSplitsAckEvent(new ArrayList<>(finishedOffsets.keySet()));
            context.sendEventToSourceReader(subtaskId, ackEvent);
        } else if (sourceEvent instanceof BinlogSplitMetaRequestEvent) {
            LOG.debug(
                    "The enumerator receives request for binlog split meta from subtask {}.",
                    subtaskId);
            sendBinlogMeta(subtaskId, (BinlogSplitMetaRequestEvent) sourceEvent);
        } else if (sourceEvent instanceof BinlogSplitUpdateAckEvent) {
            LOG.info(
                    "The enumerator receives event that the binlog split has been updated from subtask {}. ",
                    subtaskId);
            splitAssigner.onBinlogSplitUpdated();
        } else if (sourceEvent instanceof LatestFinishedSplitsNumberRequestEvent) {
            LOG.info(
                    "The enumerator receives request from subtask {} for the latest finished splits number after added newly tables. ",
                    subtaskId);
            handleLatestFinishedSplitNumberRequest(subtaskId);
        } else if (sourceEvent instanceof BinlogSplitAssignedEvent) {
            LOG.info(
                    "The enumerator receives notice from subtask {} for the binlog split assignment. ",
                    subtaskId);
            binlogSplitTaskId = subtaskId;
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        splitAssigner.notifyCheckpointComplete(checkpointId);
        // binlog split may be available after checkpoint complete
        assignSplits();
    }

    @Override
    public void close() {
        LOG.info("Closing enumerator...");
        splitAssigner.close();
    }

    // ------------------------------------------------------------------------------------------

    private void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            if (splitAssigner.isStreamSplitAssigned()
                    && sourceConfig.isCloseIdleReaders()
                    && noMoreSnapshotSplits()
                    && (binlogSplitTaskId != null && !binlogSplitTaskId.equals(nextAwaiting))) {
                // close idle readers when snapshot phase finished.
                context.signalNoMoreSplits(nextAwaiting);
                awaitingReader.remove();
                LOG.info("Close idle reader of subtask {}", nextAwaiting);
                continue;
            }

            Optional<MySqlSplit> split = splitAssigner.getNext();
            if (split.isPresent()) {
                final MySqlSplit mySqlSplit = split.get();
                context.assignSplit(mySqlSplit, nextAwaiting);
                if (mySqlSplit instanceof MySqlBinlogSplit) {
                    this.binlogSplitTaskId = nextAwaiting;
                }
                awaitingReader.remove();
                LOG.info("The enumerator assigns split {} to subtask {}", mySqlSplit, nextAwaiting);
            } else {
                // there is no available splits by now, skip assigning
                requestBinlogSplitUpdateIfNeed();
                break;
            }
        }
    }

    private boolean noMoreSnapshotSplits() {
        if (splitAssigner instanceof MySqlHybridSplitAssigner) {
            return ((MySqlHybridSplitAssigner) splitAssigner).noMoreSnapshotSplits();
        } else if (splitAssigner instanceof MySqlBinlogSplitAssigner) {
            return true;
        }
        throw new IllegalStateException(
                "Unexpected subtype of MySqlSplitAssigner class when invoking noMoreSnapshotSplits.");
    }

    private int[] getRegisteredReader() {
        return this.context.registeredReaders().keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    private void syncWithReaders(int[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to:", t);
        }
        // when the SourceEnumerator restores or the communication failed between
        // SourceEnumerator and SourceReader, it may missed some notification event.
        // tell all SourceReader(s) to report there finished but unacked splits.
        if (splitAssigner.waitingForFinishedSplits()) {
            for (int subtaskId : subtaskIds) {
                context.sendEventToSourceReader(
                        subtaskId, new FinishedSnapshotSplitsRequestEvent());
            }
        }

        requestBinlogSplitUpdateIfNeed();
    }

    private void requestBinlogSplitUpdateIfNeed() {
        if (isNewlyAddedAssigningSnapshotFinished(splitAssigner.getAssignerStatus())) {
            for (int subtaskId : getRegisteredReader()) {
                LOG.info(
                        "The enumerator requests subtask {} to update the binlog split after newly added table.",
                        subtaskId);
                context.sendEventToSourceReader(subtaskId, new BinlogSplitUpdateRequestEvent());
            }
        }
    }

    private void sendBinlogMeta(int subTask, BinlogSplitMetaRequestEvent requestEvent) {
        // initialize once
        if (binlogSplitMeta == null) {
            final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos =
                    splitAssigner.getFinishedSplitInfos();
            if (finishedSnapshotSplitInfos.isEmpty()) {
                LOG.error(
                        "The assigner offers empty finished split information, this should not happen");
                throw new FlinkRuntimeException(
                        "The assigner offers empty finished split information, this should not happen");
            }
            binlogSplitMeta =
                    Lists.partition(
                            finishedSnapshotSplitInfos, sourceConfig.getSplitMetaGroupSize());
        }
        final int requestMetaGroupId = requestEvent.getRequestMetaGroupId();

        if (binlogSplitMeta.size() > requestMetaGroupId) {
            List<FinishedSnapshotSplitInfo> metaToSend = binlogSplitMeta.get(requestMetaGroupId);
            BinlogSplitMetaEvent metadataEvent =
                    new BinlogSplitMetaEvent(
                            requestEvent.getSplitId(),
                            requestMetaGroupId,
                            metaToSend.stream()
                                    .map(FinishedSnapshotSplitInfo::serialize)
                                    .collect(Collectors.toList()));
            context.sendEventToSourceReader(subTask, metadataEvent);
        } else {
            LOG.error(
                    "The enumerator received invalid request meta group id {}, the valid meta group id range is [0, {}]",
                    requestMetaGroupId,
                    binlogSplitMeta.size() - 1);
        }
    }

    private void handleLatestFinishedSplitNumberRequest(int subTask) {
        if (splitAssigner instanceof MySqlHybridSplitAssigner) {
            context.sendEventToSourceReader(
                    subTask,
                    new LatestFinishedSplitsNumberEvent(
                            splitAssigner.getFinishedSplitInfos().size()));
        }
    }
}
