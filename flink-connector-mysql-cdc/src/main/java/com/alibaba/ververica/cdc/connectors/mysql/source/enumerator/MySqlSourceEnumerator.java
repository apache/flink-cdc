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

import com.alibaba.ververica.cdc.connectors.mysql.source.assigner.MySqlSnapshotSplitAssigner;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorAckEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorRequestReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.SourceReaderReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplitKind;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
public class MySqlSourceEnumerator implements SplitEnumerator<MySqlSplit, MySqlSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceEnumerator.class);
    private final SplitEnumeratorContext<MySqlSplit> context;
    private final MySqlSnapshotSplitAssigner snapshotSplitAssigner;

    private final Map<Integer, List<MySqlSplit>> assignedSplits;
    private final Map<Integer, List<Tuple2<String, BinlogOffset>>> receiveFinishedSnapshotSplits;

    public MySqlSourceEnumerator(
            SplitEnumeratorContext<MySqlSplit> context,
            MySqlSnapshotSplitAssigner snapshotSplitAssigner,
            Map<Integer, List<MySqlSplit>> assignedSplits,
            Map<Integer, List<Tuple2<String, BinlogOffset>>> receiveFinishedSnapshotSplits) {
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
        notifyReaderReportFinishedSplitsIfNeed(assignedSplits.keySet().toArray(new Integer[0]));
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        Optional<MySqlSplit> split = snapshotSplitAssigner.getNext(requesterHostname);
        // assign snapshot split firstly
        if (split.isPresent()) {
            context.assignSplit(split.get(), subtaskId);
            // record assigned splits
            recordAssignedSplits(split.get(), subtaskId);
            LOG.info("Assign snapshot split {} for subtask {}", split.get(), subtaskId);
            return;
        } else {
            // no more snapshot split, try assign binlog split
            if (couldAssignBinlogSplit()) {
                assignBinlogSplit(subtaskId);
                LOG.info("Assign binlog split for subtask {}", subtaskId);
                return;
            }
            // no more snapshot split, try notify no more splits
            else if (couldNotifyNoMoreSplits(subtaskId)) {
                context.signalNoMoreSplits(subtaskId);
                LOG.info("No available split for subtask {}", subtaskId);
                return;
            }
            // the binlog split may can not assign due to snapshot splits report is
            // incomplete, tell reader report finished snapshot splits
            notifyReaderReportFinishedSplitsIfNeed(new Integer[] {subtaskId});
        }
    }

    private void notifyReaderReportFinishedSplitsIfNeed(Integer[] subtaskIds) {
        // call reader report finished snapshot
        for (int subtaskId : subtaskIds) {
            final List<MySqlSplit> assignedSplit =
                    assignedSplits.getOrDefault(subtaskId, new ArrayList<>());
            final List<Tuple2<String, BinlogOffset>> ackSpitsForReader =
                    receiveFinishedSnapshotSplits.getOrDefault(subtaskId, new ArrayList<>());
            int assignedSnapshotSplitSize =
                    assignedSplit.stream()
                            .filter(sqlSplit -> sqlSplit.getSplitKind() == MySqlSplitKind.SNAPSHOT)
                            .collect(Collectors.toList())
                            .size();
            if (assignedSnapshotSplitSize > ackSpitsForReader.size()) {
                context.sendEventToSourceReader(subtaskId, new EnumeratorRequestReportEvent());
                LOG.info(
                        "The enumerator call subtask {} to report its finished splits.", subtaskId);
            }
        }
    }

    @Override
    public void addSplitsBack(List<MySqlSplit> splits, int subtaskId) {
        snapshotSplitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SourceReaderReportEvent) {
            LOG.info(
                    "The enumerator receive snapshot finished report event {} from subtask {}.",
                    sourceEvent,
                    subtaskId);
            SourceReaderReportEvent reportEvent = (SourceReaderReportEvent) sourceEvent;
            final List<Tuple2<String, BinlogOffset>> ackSpitsForReader =
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
    public MySqlSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new MySqlSourceEnumState(
                snapshotSplitAssigner.remainingSplits(),
                snapshotSplitAssigner.getAlreadyProcessedTables(),
                assignedSplits,
                receiveFinishedSnapshotSplits);
    }

    @Override
    public void close() throws IOException {
        this.snapshotSplitAssigner.close();
    }

    private boolean couldNotifyNoMoreSplits(int subtaskId) {
        // the task may never be assigned split
        final List<MySqlSplit> assignedSplit = assignedSplits.get(subtaskId);
        if (assignedSplit == null || hasAssignedBinlogSplit()) {
            return true;
        } else {
            return false;
        }
    }

    private boolean hasAssignedBinlogSplit() {
        for (List<MySqlSplit> assignedSplit : assignedSplits.values()) {
            if (assignedSplit != null) {
                return assignedSplit.stream()
                        .anyMatch(r -> r.getSplitKind().equals(MySqlSplitKind.BINLOG));
            }
        }
        return false;
    }

    private boolean couldAssignBinlogSplit() {
        final int assignedSnapshotSplit =
                assignedSplits.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList())
                        .size();
        final int receiveSnapshotSplits =
                receiveFinishedSnapshotSplits.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList())
                        .size();
        // All assigned snapshot splits have finished
        return assignedSnapshotSplit == receiveSnapshotSplits;
    }

    private void assignBinlogSplit(int requestTaskId) {
        final List<MySqlSplit> assignedSnapshotSplit =
                assignedSplits.values().stream()
                        .flatMap(Collection::stream)
                        .sorted(Comparator.comparing(MySqlSplit::splitId))
                        .collect(Collectors.toList());
        final List<Tuple2<String, BinlogOffset>> receiveSnapshotSplits =
                receiveFinishedSnapshotSplits.values().stream()
                        .flatMap(Collection::stream)
                        .sorted(Comparator.comparing(o -> o.f0))
                        .collect(Collectors.toList());

        final List<Tuple5<TableId, String, Object[], Object[], BinlogOffset>> snapshotSplits =
                new ArrayList<>();
        final Map<TableId, SchemaRecord> databaseHistory = new HashMap<>();

        BinlogOffset minBinlogOffset = receiveSnapshotSplits.get(0).f1;
        for (int i = 0; i < assignedSnapshotSplit.size(); i++) {
            MySqlSplit split = assignedSnapshotSplit.get(i);
            // find the min binlog offset
            if (receiveSnapshotSplits.get(i).f1.compareTo(minBinlogOffset) < 0) {
                minBinlogOffset = receiveSnapshotSplits.get(i).f1;
            }
            Tuple2<String, BinlogOffset> splitPosition = receiveSnapshotSplits.get(i);
            snapshotSplits.add(
                    Tuple5.of(
                            split.getTableId(),
                            split.getSplitId(),
                            split.getSplitBoundaryStart(),
                            split.getSplitBoundaryEnd(),
                            splitPosition.f1));
            databaseHistory.putAll(split.getDatabaseHistory());
        }

        final MySqlSplit lastSnapshotSplit =
                assignedSnapshotSplit.get(assignedSnapshotSplit.size() - 1);
        MySqlSplit binlogSplit =
                new MySqlSplit(
                        MySqlSplitKind.BINLOG,
                        lastSnapshotSplit.getTableId(),
                        "binlog-split-" + requestTaskId,
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
        context.assignSplit(binlogSplit, requestTaskId);
        // record assigned splits
        recordAssignedSplits(binlogSplit, requestTaskId);
    }

    private void recordAssignedSplits(MySqlSplit split, int subtaskId) {
        List<MySqlSplit> assignedSplits =
                this.assignedSplits.getOrDefault(subtaskId, new ArrayList<>());
        assignedSplits.add(split);
        this.assignedSplits.put(subtaskId, assignedSplits);
    }
}
