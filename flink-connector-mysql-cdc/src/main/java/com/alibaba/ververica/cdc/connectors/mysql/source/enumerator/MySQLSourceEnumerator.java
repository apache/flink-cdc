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

package com.alibaba.ververica.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition;
import com.alibaba.ververica.cdc.connectors.mysql.source.assigner.MySQLSnapshotSplitAssigner;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorAckEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorRequestReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.SourceReaderReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitKind;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A MySQL CDC source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
public class MySQLSourceEnumerator implements SplitEnumerator<MySQLSplit, MySQLSourceEnumState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSourceEnumerator.class);
    private final SplitEnumeratorContext<MySQLSplit> context;
    private final MySQLSnapshotSplitAssigner snapshotSplitAssigner;

    private final Map<Integer, List<MySQLSplit>> assignedSplits;
    private final Map<Integer, List<Tuple2<String, BinlogPosition>>> receiveFinishedSnapshotSplits;

    public MySQLSourceEnumerator(
            SplitEnumeratorContext<MySQLSplit> context,
            MySQLSnapshotSplitAssigner snapshotSplitAssigner,
            Map<Integer, List<MySQLSplit>> assignedSplits,
            Map<Integer, List<Tuple2<String, BinlogPosition>>> receiveFinishedSnapshotSplits) {
        this.context = context;
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.assignedSplits = assignedSplits;
        this.receiveFinishedSnapshotSplits = receiveFinishedSnapshotSplits;
    }

    @Override
    public void start() {
        this.snapshotSplitAssigner.open();
        // when the MySQLSourceEnumerator restore, it may missed some report information from reader
        // tell all readers what we have received and request readers report their finished splits
        notifyReaderReceivedFinishedSplits(assignedSplits.keySet().toArray(new Integer[0]));
        notifyReaderReportFinishedSplits(assignedSplits.keySet().toArray(new Integer[0]));
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        Optional<MySQLSplit> split = snapshotSplitAssigner.getNext(requesterHostname);
        // assign snapshot split firstly
        if (split.isPresent()) {
            context.assignSplit(split.get(), subtaskId);

            // record assigned splits
            recordAssignedSplits(split.get(), subtaskId);
            LOGGER.info("Assign snapshot split {} for subtask {}", split.get(), subtaskId);
        }
        // no more snapshot split, try assign binlog split
        else if (couldAssignBinlogSplit(subtaskId)) {
            assignBinlogSplit(subtaskId);
            LOGGER.info("Assign binlog split for subtask {}", subtaskId);
        } else if (hasAssignedBinlogSplit(subtaskId)) {
            context.signalNoMoreSplits(subtaskId);
            LOGGER.info("No available split for subtask {}", subtaskId);
        } else {
            // the binlog split can not assign due to snapshot splits report is incomplete
            // tell reader report finished snapshot splits
            notifyReaderReportFinishedSplits(new Integer[] {subtaskId});
        }
    }

    private void notifyReaderReportFinishedSplits(Integer[] subtaskIds) {
        // call reader report finished snapshot
        for (int subtaskId : subtaskIds) {
            context.sendEventToSourceReader(subtaskId, new EnumeratorRequestReportEvent());
            LOGGER.info("The enumerator call subtask {} to report its finished splits.", subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<MySQLSplit> splits, int subtaskId) {
        snapshotSplitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SourceReaderReportEvent) {
            LOGGER.info(
                    "The enumerator receive snapshot finished report event {} from subtask {}.",
                    sourceEvent,
                    subtaskId);
            SourceReaderReportEvent reportEvent = (SourceReaderReportEvent) sourceEvent;
            final List<Tuple2<String, BinlogPosition>> ackSpitsForReader =
                    receiveFinishedSnapshotSplits.getOrDefault(subtaskId, new ArrayList<>());

            ackSpitsForReader.addAll(reportEvent.getFinishedSplits());
            receiveFinishedSnapshotSplits.put(subtaskId, ackSpitsForReader);

            notifyReaderReceivedFinishedSplits(new Integer[] {subtaskId});
        }
    }

    private void notifyReaderReceivedFinishedSplits(Integer[] subtaskIds) {
        for (int subtaskId : subtaskIds) {
            List<String> splits =
                    receiveFinishedSnapshotSplits.getOrDefault(subtaskId, new ArrayList<>())
                            .stream()
                            .map(r -> r.f0)
                            .collect(Collectors.toList());
            if (!splits.isEmpty()) {
                EnumeratorAckEvent ackEvent = new EnumeratorAckEvent(splits);
                context.sendEventToSourceReader(subtaskId, ackEvent);
            }
        }
    }

    @Override
    public MySQLSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new MySQLSourceEnumState(
                snapshotSplitAssigner.remainingSplits(),
                snapshotSplitAssigner.getAlreadyProcessedTables(),
                assignedSplits,
                receiveFinishedSnapshotSplits);
    }

    @Override
    public void close() throws IOException {
        this.snapshotSplitAssigner.close();
    }

    private boolean hasAssignedBinlogSplit(int subtaskId) {
        final List<MySQLSplit> assignedSplit = assignedSplits.get(subtaskId);
        return assignedSplit.stream().anyMatch(r -> r.getSplitKind().equals(MySQLSplitKind.BINLOG));
    }

    private boolean couldAssignBinlogSplit(int subtaskId) {
        final List<MySQLSplit> assignedSplit = assignedSplits.get(subtaskId);
        final List<Tuple2<String, BinlogPosition>> receiveSnapshotSplits =
                receiveFinishedSnapshotSplits.get(subtaskId);
        if (assignedSplit != null
                && receiveSnapshotSplits != null
                && assignedSplit.size() == receiveSnapshotSplits.size()) {
            return true;
        }
        return false;
    }

    private void assignBinlogSplit(int subtaskId) {
        final List<MySQLSplit> assignedSnapshotSplit =
                assignedSplits.get(subtaskId).stream()
                        .sorted(Comparator.comparing(MySQLSplit::splitId))
                        .collect(Collectors.toList());
        final List<Tuple2<String, BinlogPosition>> receiveSnapshotSplits =
                receiveFinishedSnapshotSplits.get(subtaskId).stream()
                        .sorted(Comparator.comparing(o -> o.f0))
                        .collect(Collectors.toList());

        final List<Tuple5<TableId, String, Object[], Object[], BinlogPosition>> snapshotSplits =
                new ArrayList<>();
        final Map<TableId, SchemaRecord> databaseHistory = new HashMap<>();

        BinlogPosition minBinlogOffset = receiveSnapshotSplits.get(0).f1;
        for (int i = 0; i < assignedSnapshotSplit.size(); i++) {
            MySQLSplit split = assignedSnapshotSplit.get(i);
            // find the min binlog offset
            if (receiveSnapshotSplits.get(i).f1.compareTo(minBinlogOffset) < 0) {
                minBinlogOffset = receiveSnapshotSplits.get(i).f1;
            }
            Tuple2<String, BinlogPosition> splitPosition = receiveSnapshotSplits.get(i);
            snapshotSplits.add(
                    Tuple5.of(
                            split.getTableId(),
                            split.getSplitId(),
                            split.getSplitBoundaryStart(),
                            split.getSplitBoundaryEnd(),
                            splitPosition.f1));
            databaseHistory.putAll(split.getDatabaseHistory());
        }

        final MySQLSplit lastSnapshotSplit =
                assignedSnapshotSplit.get(assignedSnapshotSplit.size() - 1);
        MySQLSplit binlogSplit =
                new MySQLSplit(
                        MySQLSplitKind.BINLOG,
                        lastSnapshotSplit.getTableId(),
                        "binlog-split-" + subtaskId,
                        lastSnapshotSplit.getSplitBoundaryType(),
                        null,
                        null,
                        null,
                        null,
                        true,
                        minBinlogOffset,
                        snapshotSplits,
                        databaseHistory);
        // assign
        context.assignSplit(binlogSplit, subtaskId);
        // record assigned splits
        recordAssignedSplits(binlogSplit, subtaskId);
    }

    private void recordAssignedSplits(MySQLSplit split, int subtaskId) {
        List<MySQLSplit> assignedSplits =
                this.assignedSplits.getOrDefault(subtaskId, new ArrayList<>());
        assignedSplits.add(split);
        this.assignedSplits.put(subtaskId, assignedSplits);
    }
}
