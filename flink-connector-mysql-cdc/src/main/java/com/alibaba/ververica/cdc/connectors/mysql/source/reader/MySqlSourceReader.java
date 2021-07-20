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
import org.apache.flink.api.java.tuple.Tuple2;
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
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitKind;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** The source reader for MySQL source splits. */
public class MySqlSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecord, T, MySqlSplit, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);

    private final Map<String, MySqlSplit> finishedNoAckSplits;
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
        this.finishedNoAckSplits = new HashMap<>();
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
        return new MySqlSplitState(split);
    }

    @Override
    public List<MySqlSplit> snapshotState(long checkpointId) {
        // unfinished splits
        List<MySqlSplit> stateSplits = super.snapshotState(checkpointId);

        // add finished splits that didn't receive ack yet
        stateSplits.addAll(finishedNoAckSplits.values());
        return stateSplits;
    }

    @Override
    protected void onSplitFinished(Map<String, MySqlSplitState> finishedSplitIds) {
        LOG.info("The split(s) {} read finished.", finishedSplitIds);
        final List<MySqlSplit> splits =
                finishedSplitIds.values().stream()
                        .map(MySqlSplitState::toMySQLSplit)
                        .collect(Collectors.toList());
        reportFinishedSnapshotSplits(splits);
        context.sendSplitRequest();
    }

    @Override
    public void addSplits(List<MySqlSplit> splits) {
        // case for restore from state, notify split enumerator if there're finished snapshot splits
        // and has not report
        splits.stream()
                .filter(
                        split ->
                                split.getSplitKind() == MySqlSplitKind.SNAPSHOT
                                        && split.isSnapshotReadFinished())
                .forEach(split -> this.finishedNoAckSplits.put(split.getSplitId(), split));
        reportFinishedSnapshotSplits(this.finishedNoAckSplits.values());

        // add all un-finished splits(including binlog split) to SourceReaderBase
        super.addSplits(
                splits.stream()
                        .filter(
                                split ->
                                        !(split.getSplitKind() == MySqlSplitKind.SNAPSHOT
                                                && split.isSnapshotReadFinished()))
                        .collect(Collectors.toList()));
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof EnumeratorAckEvent) {
            EnumeratorAckEvent ackEvent = (EnumeratorAckEvent) sourceEvent;
            LOG.info(
                    "The subtask {} receive ack event for {} from Enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedNoAckSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof EnumeratorRequestReportEvent) {
            // report finished snapshot splits
            LOG.info(
                    "The subtask {} receive request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplits(finishedNoAckSplits.values());
            // also try to request new split
            context.sendSplitRequest();
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void reportFinishedSnapshotSplits(Collection<MySqlSplit> splits) {
        if (!splits.isEmpty()) {
            final ArrayList<Tuple2<String, BinlogOffset>> finishedNoAckSplits = new ArrayList<>();
            for (MySqlSplit split : splits) {
                finishedNoAckSplits.add(Tuple2.of(split.getSplitId(), split.getHighWatermark()));
            }
            SourceReaderReportEvent reportEvent = new SourceReaderReportEvent(finishedNoAckSplits);
            context.sendSourceEventToCoordinator(reportEvent);
            LOG.info(
                    "The subtask {} report finished snapshot splits {}.",
                    subtaskId,
                    finishedNoAckSplits);
        }
    }

    @Override
    protected MySqlSplit toSplitType(String splitId, MySqlSplitState splitState) {
        return splitState;
    }
}
