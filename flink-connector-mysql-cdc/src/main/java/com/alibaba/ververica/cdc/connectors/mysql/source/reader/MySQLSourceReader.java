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

import com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorAckEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorRequestReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.SourceReaderReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitKind;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitState;
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
public class MySQLSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecord, T, MySQLSplit, MySQLSplitState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSourceReader.class);

    private final Map<String, MySQLSplit> finishedNoAckSplits;
    private final int subtaskId;

    public MySQLSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementQueue,
            Supplier<MySQLSplitReader> splitReaderSupplier,
            RecordEmitter<SourceRecord, T, MySQLSplitState> recordEmitter,
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
    protected MySQLSplitState initializedState(MySQLSplit split) {
        return new MySQLSplitState(split);
    }

    @Override
    public List<MySQLSplit> snapshotState(long checkpointId) {
        // unfinished splits
        List<MySQLSplit> stateSplits = super.snapshotState(checkpointId);

        // add finished splits that didn't receive ack yet
        stateSplits.addAll(finishedNoAckSplits.values());
        return stateSplits;
    }

    @Override
    protected void onSplitFinished(Map<String, MySQLSplitState> finishedSplitIds) {
        LOGGER.info("The split(s) {} read finished.", finishedSplitIds);
        final List<MySQLSplit> splits =
                finishedSplitIds.values().stream()
                        .map(MySQLSplitState::toMySQLSplit)
                        .collect(Collectors.toList());
        reportFinishedSnapshotSplits(splits);
        context.sendSplitRequest();
    }

    @Override
    public void addSplits(List<MySQLSplit> splits) {
        // case for restore from state, notify split enumerator if there're finished snapshot splits
        // and has not report
        splits.stream()
                .filter(
                        split ->
                                split.getSplitKind() == MySQLSplitKind.SNAPSHOT
                                        && split.isSnapshotReadFinished())
                .forEach(split -> this.finishedNoAckSplits.put(split.getSplitId(), split));
        reportFinishedSnapshotSplits(this.finishedNoAckSplits.values());

        // add all un-finished splits(including binlog split) to SourceReaderBase
        super.addSplits(
                splits.stream()
                        .filter(
                                split ->
                                        !(split.getSplitKind() == MySQLSplitKind.SNAPSHOT
                                                && split.isSnapshotReadFinished()))
                        .collect(Collectors.toList()));
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof EnumeratorAckEvent) {
            EnumeratorAckEvent ackEvent = (EnumeratorAckEvent) sourceEvent;
            LOGGER.info(
                    "The subtask {} receive ack event for {} from Enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedNoAckSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof EnumeratorRequestReportEvent) {
            // report finished snapshot splits
            LOGGER.info(
                    "The subtask {} receive request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplits(finishedNoAckSplits.values());
            // also try to request new split
            context.sendSplitRequest();
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void reportFinishedSnapshotSplits(Collection<MySQLSplit> splits) {
        if (!splits.isEmpty()) {
            final ArrayList<Tuple2<String, BinlogPosition>> finishedNoAckSplits = new ArrayList<>();
            for (MySQLSplit split : splits) {
                finishedNoAckSplits.add(Tuple2.of(split.getSplitId(), split.getHighWatermark()));
            }
            SourceReaderReportEvent reportEvent = new SourceReaderReportEvent(finishedNoAckSplits);
            context.sendSourceEventToCoordinator(reportEvent);
            LOGGER.info(
                    "The subtask {} report finished snapshot splits {}.",
                    subtaskId,
                    finishedNoAckSplits);
        }
    }

    @Override
    protected MySQLSplit toSplitType(String splitId, MySQLSplitState splitState) {
        return splitState;
    }
}
