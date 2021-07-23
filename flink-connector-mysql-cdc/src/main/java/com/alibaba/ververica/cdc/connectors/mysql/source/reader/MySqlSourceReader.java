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

package com.alibaba.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorAckEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorRequestReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.SourceReaderReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/** The source reader for MySQL source splits. */
public class MySqlSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecord, T, MySqlSplit, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);

    private final HashMap<String, MySqlSnapshotSplit> finishedUnAckedSplits;
    // before the binlog split start, wait at least one complete checkpoint to
    // ensure the order of snapshot records and binlog records in parallel tasks
    private final HashMap<MySqlBinlogSplit, Integer> binlogSplitWithCheckpointCnt;
    private final HashMap<String, BinlogOffset> binlogSplitWithMinHighWatermark;

    private final int subtaskId;

    public MySqlSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementQueue,
            Supplier<MySqlSplitReader> splitReaderSupplier,
            RecordEmitter<SourceRecord, T, MySqlSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context);
        this.finishedUnAckedSplits = new HashMap<>();
        this.binlogSplitWithCheckpointCnt = new HashMap<>();
        this.binlogSplitWithMinHighWatermark = new HashMap<>();
        this.subtaskId = context.getIndexOfSubtask();
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
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
        // unfinished splits
        List<MySqlSplit> stateSplits = super.snapshotState(checkpointId);

        // add finished snapshot splits that didn't receive ack yet
        stateSplits.addAll(finishedUnAckedSplits.values());

        // add the initial binlog to state
        if (binlogSplitWithCheckpointCnt.size() > 0) {
            stateSplits.addAll(binlogSplitWithCheckpointCnt.keySet());
        }
        return stateSplits;
    }

    @Override
    protected void onSplitFinished(Map<String, MySqlSplitState> finishedSplitIds) {
        for (MySqlSplitState mySqlSplitState : finishedSplitIds.values()) {
            MySqlSplit mySqlSplit = mySqlSplitState.toMySqlSplit();
            checkState(
                    mySqlSplit.isSnapshotSplit(),
                    String.format(
                            "Only snapshot split could finish, but the actual split is binlog split %s",
                            mySqlSplit));
            finishedUnAckedSplits.put(mySqlSplit.splitId(), mySqlSplit.asSnapshotSplit());
        }
        reportFinishedSnapshotSplitsIfNeed();
    }

    @Override
    public void addSplits(List<MySqlSplit> splits) {
        for (MySqlSplit split : splits) {
            if (split.isSnapshotSplit()) {
                if (split.asSnapshotSplit().isSnapshotReadFinished()) {
                    this.finishedUnAckedSplits.put(split.splitId(), split.asSnapshotSplit());
                } else {
                    // add all un-finished snapshot splits to SourceReaderBase
                    super.addSplits(Collections.singletonList(split));
                }
            } else {
                if (hasBeenReadBinlogSplit(split.asBinlogSplit())) {
                    // add binlog split has been read, the case restores from state
                    super.addSplits(Collections.singletonList(split));
                } else {
                    // receive new binlog split, check need wait or not
                    if (!binlogSplitWithCheckpointCnt.containsKey(split)) {
                        // wait
                        binlogSplitWithCheckpointCnt.put(split.asBinlogSplit(), 0);
                    } else if (binlogSplitWithCheckpointCnt.get(split) > 0) {
                        // the binlog split has wait more thant one checkpoint
                        // submit it
                        super.addSplits(Collections.singletonList(split));
                    }
                }
            }
        }

        // case for restore from state, notify split enumerator if there're finished snapshot splits
        // and has not report
        reportFinishedSnapshotSplitsIfNeed();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof EnumeratorAckEvent) {
            EnumeratorAckEvent ackEvent = (EnumeratorAckEvent) sourceEvent;
            LOG.info(
                    "The subtask {} receives ack event for {} from Enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedUnAckedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof EnumeratorRequestReportEvent) {
            // report finished snapshot splits
            LOG.info(
                    "The subtask {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnAckedSplits.isEmpty()) {
            final Map<String, BinlogOffset> finishedSplits = new HashMap<>();
            for (MySqlSnapshotSplit split : finishedUnAckedSplits.values()) {
                finishedSplits.put(split.splitId(), split.getHighWatermark());
            }
            SourceReaderReportEvent reportEvent = new SourceReaderReportEvent(finishedSplits);
            context.sendSourceEventToCoordinator(reportEvent);
            LOG.info(
                    "The subtask {} reports finished snapshot splits {}.",
                    subtaskId,
                    finishedSplits);
            // try to request next split
            context.sendSplitRequest();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (binlogSplitWithCheckpointCnt.size() > 0) {
            MySqlBinlogSplit mySqlBinlogSplit =
                    binlogSplitWithCheckpointCnt.keySet().iterator().next();

            binlogSplitWithCheckpointCnt.put(
                    mySqlBinlogSplit, binlogSplitWithCheckpointCnt.get(mySqlBinlogSplit) + 1);

            if (binlogSplitWithCheckpointCnt.get(mySqlBinlogSplit) > 0) {
                this.addSplits(Collections.singletonList(mySqlBinlogSplit));
                binlogSplitWithCheckpointCnt.clear();
            }
        }
    }

    @Override
    protected MySqlSplit toSplitType(String splitId, MySqlSplitState splitState) {
        return splitState.toMySqlSplit();
    }

    private boolean hasBeenReadBinlogSplit(MySqlBinlogSplit binlogSplit) {
        if (binlogSplit.getFinishedSnapshotSplitInfos().isEmpty()) {
            // the latest-offset mode, do not need to wait checkpoint
            return true;
        } else {
            BinlogOffset minHighWatermark = getMinHighWatermark(binlogSplit);
            return binlogSplit.getStartingOffset().isBefore(minHighWatermark);
        }
    }

    private BinlogOffset getMinHighWatermark(MySqlBinlogSplit binlogSplit) {
        final String binlogSplitId = binlogSplit.splitId();
        if (binlogSplitWithMinHighWatermark.containsKey(binlogSplitId)) {
            return binlogSplitWithMinHighWatermark.get(binlogSplitId);
        } else {
            BinlogOffset minBinlogOffset = BinlogOffset.INITIAL_OFFSET;
            for (FinishedSnapshotSplitInfo snapshotSplitInfo :
                    binlogSplit.getFinishedSnapshotSplitInfos()) {
                // find the min HighWatermark of all finished snapshot splits
                if (snapshotSplitInfo.getHighWatermark().compareTo(minBinlogOffset) < 0) {
                    minBinlogOffset = snapshotSplitInfo.getHighWatermark();
                }
            }
            binlogSplitWithMinHighWatermark.put(binlogSplitId, minBinlogOffset);
            return minBinlogOffset;
        }
    }
}
