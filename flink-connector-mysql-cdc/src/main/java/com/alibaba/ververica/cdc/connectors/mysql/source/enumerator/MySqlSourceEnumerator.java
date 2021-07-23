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
import org.apache.flink.util.FlinkRuntimeException;

import com.alibaba.ververica.cdc.connectors.mysql.source.assigner.MySqlSplitAssigner;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorAckEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.EnumeratorRequestReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.events.SourceReaderReportEvent;
import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * A MySQL CDC source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
public class MySqlSourceEnumerator implements SplitEnumerator<MySqlSplit, MySqlSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceEnumerator.class);
    private static final long CHECK_EVENT_INTERVAL = 30_000L;
    public static final String BINLOG_SPLIT_ID = "binlog-split";

    private final SplitEnumeratorContext<MySqlSplit> context;
    private final MySqlSplitAssigner splitAssigner;
    private final Map<Integer, List<MySqlSnapshotSplit>> assignedSnapshotSplits;
    private final Map<Integer, Map<String, BinlogOffset>> finishedSnapshotSplits;
    // using TreeSet to prefer assigning binlog split to task-0 for easier debug
    private final TreeSet<Integer> readersAwaitingSplit;
    private boolean binlogSplitAssigned;

    public MySqlSourceEnumerator(
            SplitEnumeratorContext<MySqlSplit> context,
            MySqlSplitAssigner splitAssigner,
            Map<Integer, List<MySqlSnapshotSplit>> assignedSnapshotSplits,
            Map<Integer, Map<String, BinlogOffset>> finishedSnapshotSplits,
            boolean binlogSplitAssigned) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.assignedSnapshotSplits = assignedSnapshotSplits;
        this.binlogSplitAssigned = binlogSplitAssigned;
        this.finishedSnapshotSplits = finishedSnapshotSplits;
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @Override
    public void start() {
        this.splitAssigner.open();
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

    private void assignSplits() {
        if (binlogSplitAssigned) {
            // nothing to do if binlog split has been assigned
            return;
        }

        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            if (splitAssigner.isBinlogSplitAssigner()) {
                Optional<MySqlSplit> split = splitAssigner.getNext();
                if (split.isPresent()) {
                    final MySqlSplit binlogSplit = split.get();
                    context.assignSplit(binlogSplit, nextAwaiting);
                    binlogSplitAssigned = true;
                    LOG.info("Assign binlog split {} for subtask {}", binlogSplit, nextAwaiting);
                }
                return;
            }

            Optional<MySqlSplit> split = getNextSplit();
            if (split.isPresent()) {
                final MySqlSplit mysqlSplit = split.get();
                context.assignSplit(mysqlSplit, nextAwaiting);
                if (mysqlSplit.isSnapshotSplit()) {
                    // record assigned snapshot splits
                    assignedSnapshotSplits
                            .computeIfAbsent(nextAwaiting, k -> new ArrayList<>())
                            .add(mysqlSplit.asSnapshotSplit());
                } else {
                    // record binlog split is assigned
                    binlogSplitAssigned = true;
                }
                LOG.info("Assign split {} to subtask {}", mysqlSplit, nextAwaiting);
                awaitingReader.remove();
            }
        }
    }

    private Optional<MySqlSplit> getNextSplit() {
        if (binlogSplitAssigned) {
            // avoid to enumerate splits if in binlog phase,
            // otherwise, it may assign snapshot splits if there are new created tables after
            // restored.
            return Optional.empty();
        } else {
            Optional<MySqlSplit> snapshotSplit = splitAssigner.getNext();
            if (snapshotSplit.isPresent()) {
                return snapshotSplit;
            } else if (allSnapshotSplitsFinished()) {
                // all snapshot split are processed, try assign binlog split
                return Optional.of(generateBinlogSplit());
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public void addSplitsBack(List<MySqlSplit> splits, int subtaskId) {
        // TODO the better way should assign splits to pending reader
        List<MySqlSplit> snapshotSplits = new ArrayList<>();
        List<MySqlSplit> binlogSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                binlogSplits.add(split);
            }
        }
        if (!snapshotSplits.isEmpty()) {
            splitAssigner.addSplits(snapshotSplits);
        }
        if (!binlogSplits.isEmpty()) {
            binlogSplitAssigned = false;
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // do nothing
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SourceReaderReportEvent) {
            LOG.info(
                    "The enumerator receive snapshot finished report event {} from subtask {}.",
                    sourceEvent,
                    subtaskId);
            SourceReaderReportEvent reportEvent = (SourceReaderReportEvent) sourceEvent;
            finishedSnapshotSplits
                    .computeIfAbsent(subtaskId, k -> new HashMap<>())
                    .putAll(reportEvent.getFinishedSnapshotSplits());
            notifyReaderReceivedFinishedSplits(new Integer[] {subtaskId});
        }
    }

    @Override
    public MySqlSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new MySqlSourceEnumState(
                splitAssigner.remainingSplits(),
                splitAssigner.getAlreadyProcessedTables(),
                assignedSnapshotSplits,
                finishedSnapshotSplits,
                binlogSplitAssigned);
    }

    @Override
    public void close() throws IOException {
        this.splitAssigner.close();
    }

    private void syncWithReaders(Integer[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to ", t);
        }
        // when the SourceEnumerator restore or the communication failed between
        // SourceEnumerator and SourceReader, it may missed some notification event.
        // tell all SourceReader(s) what SourceEnumerator has received and
        // request SourceReader(s) report their finished splits
        notifyReaderReceivedFinishedSplits(subtaskIds);
        notifyReaderReportFinishedSplitsIfNeed(subtaskIds);
        // we may need to assign splits here if all split requests are
        // received before snapshot splits are finished
        assignSplits();
    }

    private void notifyReaderReceivedFinishedSplits(Integer[] subtaskIds) {
        if (binlogSplitAssigned) {
            return;
        }
        for (int subtaskId : subtaskIds) {
            Set<String> splits =
                    finishedSnapshotSplits.getOrDefault(subtaskId, Collections.emptyMap()).keySet();
            if (!splits.isEmpty()) {
                EnumeratorAckEvent ackEvent = new EnumeratorAckEvent(new ArrayList<>(splits));
                context.sendEventToSourceReader(subtaskId, ackEvent);
            }
        }
    }

    private void notifyReaderReportFinishedSplitsIfNeed(Integer[] subtaskIds) {
        // call reader report finished snapshot
        if (binlogSplitAssigned) {
            return;
        }
        for (int subtaskId : subtaskIds) {
            int assignedSize = assignedSnapshotSplits.getOrDefault(subtaskId, emptyList()).size();
            int finishedSize = finishedSnapshotSplits.getOrDefault(subtaskId, emptyMap()).size();
            if (assignedSize > finishedSize) {
                context.sendEventToSourceReader(subtaskId, new EnumeratorRequestReportEvent());
                LOG.info(
                        "The enumerator call subtask {} to report its finished splits.", subtaskId);
            }
        }
    }

    private Integer[] getRegisteredReader() {
        return this.context.registeredReaders().keySet().toArray(new Integer[0]);
    }

    private boolean allSnapshotSplitsFinished() {
        final long assignedSnapshotSplit =
                assignedSnapshotSplits.values().stream().mapToLong(Collection::size).sum();
        final long receiveSnapshotSplits =
                finishedSnapshotSplits.values().stream().mapToLong(Map::size).sum();
        // All assigned snapshot splits have finished
        return assignedSnapshotSplit > 0 && assignedSnapshotSplit == receiveSnapshotSplits;
    }

    private MySqlBinlogSplit generateBinlogSplit() {
        final List<MySqlSplit> assignedSnapshotSplit =
                assignedSnapshotSplits.values().stream()
                        .flatMap(Collection::stream)
                        .sorted(Comparator.comparing(MySqlSplit::splitId))
                        .collect(Collectors.toList());
        final Map<String, BinlogOffset> startingBinlogOffsets = new HashMap<>();
        finishedSnapshotSplits.values().forEach(startingBinlogOffsets::putAll);

        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        final Map<TableId, TableChange> tableSchemas = new HashMap<>();

        BinlogOffset minBinlogOffset = BinlogOffset.INITIAL_OFFSET;
        for (int i = 0; i < assignedSnapshotSplit.size(); i++) {
            MySqlSnapshotSplit split = assignedSnapshotSplit.get(i).asSnapshotSplit();
            // find the min binlog offset
            BinlogOffset binlogOffset = startingBinlogOffsets.get(split.splitId());
            if (binlogOffset.compareTo(minBinlogOffset) < 0) {
                minBinlogOffset = binlogOffset;
            }
            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            binlogOffset));
            tableSchemas.putAll(split.getTableSchemas());
        }

        final MySqlSnapshotSplit lastSnapshotSplit =
                assignedSnapshotSplit.get(assignedSnapshotSplit.size() - 1).asSnapshotSplit();
        return new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                lastSnapshotSplit.getSplitKeyType(),
                minBinlogOffset,
                BinlogOffset.NO_STOPPING_OFFSET,
                finishedSnapshotSplitInfos,
                tableSchemas);
    }
}
