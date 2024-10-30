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

package org.apache.flink.cdc.connectors.mysql.source.split;

import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** The split to describe the binlog of MySql table(s). */
public class MySqlBinlogSplit extends MySqlSplit {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplit.class);
    private static final int TABLES_LENGTH_FOR_LOG = 3;

    private final BinlogOffset startingOffset;
    private final BinlogOffset endingOffset;
    private final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos;
    private final Map<TableId, TableChange> tableSchemas;
    private final int totalFinishedSplitSize;
    private final boolean isSuspended;
    private final String tablesForLog;
    @Nullable transient byte[] serializedFormCache;

    public MySqlBinlogSplit(
            String splitId,
            BinlogOffset startingOffset,
            BinlogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.isSuspended = isSuspended;
        this.tablesForLog = getTablesForLog();
    }

    public MySqlBinlogSplit(
            String splitId,
            BinlogOffset startingOffset,
            BinlogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.isSuspended = false;
        this.tablesForLog = getTablesForLog();
    }

    public BinlogOffset getStartingOffset() {
        return startingOffset;
    }

    public BinlogOffset getEndingOffset() {
        return endingOffset;
    }

    public List<FinishedSnapshotSplitInfo> getFinishedSnapshotSplitInfos() {
        return finishedSnapshotSplitInfos;
    }

    @Override
    public Map<TableId, TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public int getTotalFinishedSplitSize() {
        return totalFinishedSplitSize;
    }

    public boolean isSuspended() {
        return isSuspended;
    }

    public boolean isCompletedSplit() {
        return totalFinishedSplitSize == finishedSnapshotSplitInfos.size();
    }

    private String getTablesForLog() {
        List<TableId> tablesForLog = new ArrayList<>();
        if (tableSchemas != null) {
            List<TableId> tableIds = new ArrayList<>(new TreeSet(tableSchemas.keySet()));
            // Truncate tables length to avoid printing too much log
            tablesForLog = tableIds.subList(0, Math.min(tableIds.size(), TABLES_LENGTH_FOR_LOG));
        }
        return tablesForLog.toString();
    }

    public String getTables() {
        return tablesForLog;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlBinlogSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MySqlBinlogSplit that = (MySqlBinlogSplit) o;
        return totalFinishedSplitSize == that.totalFinishedSplitSize
                && isSuspended == that.isSuspended
                && Objects.equals(startingOffset, that.startingOffset)
                && Objects.equals(endingOffset, that.endingOffset)
                && Objects.equals(finishedSnapshotSplitInfos, that.finishedSnapshotSplitInfos)
                && Objects.equals(tableSchemas, that.tableSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                totalFinishedSplitSize,
                isSuspended);
    }

    @Override
    public String toString() {
        return "MySqlBinlogSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", tables="
                + tablesForLog
                + ", offset="
                + startingOffset
                + ", endOffset="
                + endingOffset
                + ", isSuspended="
                + isSuspended
                + '}';
    }

    // -------------------------------------------------------------------
    // factory utils to build new MySqlBinlogSplit instance
    // -------------------------------------------------------------------
    public static MySqlBinlogSplit appendFinishedSplitInfos(
            MySqlBinlogSplit binlogSplit, List<FinishedSnapshotSplitInfo> splitInfos) {
        // re-calculate the starting binlog offset after the new table added
        BinlogOffset startingOffset = binlogSplit.getStartingOffset();
        for (FinishedSnapshotSplitInfo splitInfo : splitInfos) {
            if (splitInfo.getHighWatermark().isBefore(startingOffset)) {
                startingOffset = splitInfo.getHighWatermark();
            }
        }
        splitInfos.addAll(binlogSplit.getFinishedSnapshotSplitInfos());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                startingOffset,
                binlogSplit.getEndingOffset(),
                splitInfos,
                binlogSplit.getTableSchemas(),
                binlogSplit.getTotalFinishedSplitSize(),
                binlogSplit.isSuspended());
    }

    /**
     * Filter out the outdated finished splits in {@link MySqlBinlogSplit}.
     *
     * <p>When restore from a checkpoint, the finished split infos may contain some splits from the
     * deleted tables. We need to remove these splits from the total finished split infos and update
     * the size, while also removing the outdated tables from the table schemas of binlog split.
     */
    public static MySqlBinlogSplit filterOutdatedSplitInfos(
            MySqlBinlogSplit binlogSplit, Tables.TableFilter currentTableFilter) {
        Map<TableId, TableChange> filteredTableSchemas =
                binlogSplit.getTableSchemas().entrySet().stream()
                        .filter(entry -> currentTableFilter.isIncluded(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Set<TableId> tablesToRemoveInFinishedSnapshotSplitInfos =
                binlogSplit.getFinishedSnapshotSplitInfos().stream()
                        .filter(i -> !currentTableFilter.isIncluded(i.getTableId()))
                        .map(split -> split.getTableId())
                        .collect(Collectors.toSet());
        if (tablesToRemoveInFinishedSnapshotSplitInfos.isEmpty()) {
            return new MySqlBinlogSplit(
                    binlogSplit.splitId,
                    binlogSplit.getStartingOffset(),
                    binlogSplit.getEndingOffset(),
                    binlogSplit.getFinishedSnapshotSplitInfos(),
                    filteredTableSchemas,
                    binlogSplit.totalFinishedSplitSize,
                    binlogSplit.isSuspended());
        }

        LOG.info(
                "Reader remove tables after restart: {}",
                tablesToRemoveInFinishedSnapshotSplitInfos);
        List<FinishedSnapshotSplitInfo> allFinishedSnapshotSplitInfos =
                binlogSplit.getFinishedSnapshotSplitInfos().stream()
                        .filter(
                                i ->
                                        !tablesToRemoveInFinishedSnapshotSplitInfos.contains(
                                                i.getTableId()))
                        .collect(Collectors.toList());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                binlogSplit.getStartingOffset(),
                binlogSplit.getEndingOffset(),
                allFinishedSnapshotSplitInfos,
                filteredTableSchemas,
                binlogSplit.getTotalFinishedSplitSize()
                        - (binlogSplit.getFinishedSnapshotSplitInfos().size()
                                - allFinishedSnapshotSplitInfos.size()),
                binlogSplit.isSuspended());
    }

    public static MySqlBinlogSplit fillTableSchemas(
            MySqlBinlogSplit binlogSplit, Map<TableId, TableChange> tableSchemas) {
        tableSchemas.putAll(binlogSplit.getTableSchemas());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                binlogSplit.getStartingOffset(),
                binlogSplit.getEndingOffset(),
                binlogSplit.getFinishedSnapshotSplitInfos(),
                tableSchemas,
                binlogSplit.getTotalFinishedSplitSize(),
                binlogSplit.isSuspended());
    }

    public static MySqlBinlogSplit toNormalBinlogSplit(
            MySqlBinlogSplit suspendedBinlogSplit, int totalFinishedSplitSize) {
        return new MySqlBinlogSplit(
                suspendedBinlogSplit.splitId,
                suspendedBinlogSplit.getStartingOffset(),
                suspendedBinlogSplit.getEndingOffset(),
                suspendedBinlogSplit.getFinishedSnapshotSplitInfos(),
                suspendedBinlogSplit.getTableSchemas(),
                totalFinishedSplitSize,
                false);
    }

    public static MySqlBinlogSplit toSuspendedBinlogSplit(MySqlBinlogSplit normalBinlogSplit) {
        return new MySqlBinlogSplit(
                normalBinlogSplit.splitId,
                normalBinlogSplit.getStartingOffset(),
                normalBinlogSplit.getEndingOffset(),
                forwardHighWatermarkToStartingOffset(
                        normalBinlogSplit.getFinishedSnapshotSplitInfos(),
                        normalBinlogSplit.getStartingOffset()),
                normalBinlogSplit.getTableSchemas(),
                normalBinlogSplit.getTotalFinishedSplitSize(),
                true);
    }

    /**
     * Forwards {@link FinishedSnapshotSplitInfo#getHighWatermark()} to current binlog reading
     * offset for these snapshot-splits have started the binlog reading, this is pretty useful for
     * newly added table process that we can continue to consume binlog for these splits from the
     * updated high watermark.
     *
     * @param existedSplitInfos
     * @param currentBinlogReadingOffset
     */
    private static List<FinishedSnapshotSplitInfo> forwardHighWatermarkToStartingOffset(
            List<FinishedSnapshotSplitInfo> existedSplitInfos,
            BinlogOffset currentBinlogReadingOffset) {
        List<FinishedSnapshotSplitInfo> updatedSnapshotSplitInfos = new ArrayList<>();
        for (FinishedSnapshotSplitInfo existedSplitInfo : existedSplitInfos) {
            // for split has started read binlog, forward its high watermark to current binlog
            // reading offset
            if (existedSplitInfo.getHighWatermark().isBefore(currentBinlogReadingOffset)) {
                FinishedSnapshotSplitInfo forwardHighWatermarkSnapshotSplitInfo =
                        new FinishedSnapshotSplitInfo(
                                existedSplitInfo.getTableId(),
                                existedSplitInfo.getSplitId(),
                                existedSplitInfo.getSplitStart(),
                                existedSplitInfo.getSplitEnd(),
                                currentBinlogReadingOffset);
                updatedSnapshotSplitInfos.add(forwardHighWatermarkSnapshotSplitInfo);
            } else {
                updatedSnapshotSplitInfos.add(existedSplitInfo);
            }
        }
        return updatedSnapshotSplitInfos;
    }
}
