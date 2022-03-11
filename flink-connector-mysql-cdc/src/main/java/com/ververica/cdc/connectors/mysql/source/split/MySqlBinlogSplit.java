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

package com.ververica.cdc.connectors.mysql.source.split;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** The split to describe the binlog of MySql table(s). */
public class MySqlBinlogSplit extends MySqlSplit {

    private final BinlogOffset startingOffset;
    private final BinlogOffset endingOffset;
    private final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos;
    private final Map<TableId, TableChange> tableSchemas;
    private final int totalFinishedSplitSize;
    private final boolean isSuspended;
    // true means the binlog split is assigned from Enumerator, false means split is assigned from
    // readerState
    private final boolean isInit;
    @Nullable transient byte[] serializedFormCache;

    public MySqlBinlogSplit(
            String splitId,
            BinlogOffset startingOffset,
            BinlogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended,
            boolean isInit) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.isSuspended = isSuspended;
        this.isInit = isInit;
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
        this.isInit = true;
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

    public boolean isInit() {
        return isInit;
    }

    public boolean isCompletedSplit() {
        return totalFinishedSplitSize == finishedSnapshotSplitInfos.size();
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
                && isInit == that.isInit
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
                isSuspended,
                isInit);
    }

    @Override
    public String toString() {
        return "MySqlBinlogSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", offset="
                + startingOffset
                + ", endOffset="
                + endingOffset
                + ", isSuspended="
                + isSuspended
                + ", isInit="
                + isInit
                + '}';
    }

    // -------------------------------------------------------------------
    // factory utils to build new MySqlBinlogSplit instance
    // -------------------------------------------------------------------
    public static MySqlBinlogSplit appendFinishedSplitInfos(
            MySqlBinlogSplit binlogSplit, List<FinishedSnapshotSplitInfo> splitInfos) {
        splitInfos.addAll(binlogSplit.getFinishedSnapshotSplitInfos());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                binlogSplit.getStartingOffset(),
                binlogSplit.getEndingOffset(),
                splitInfos,
                binlogSplit.getTableSchemas(),
                binlogSplit.getTotalFinishedSplitSize(),
                binlogSplit.isSuspended(),
                binlogSplit.isInit());
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
                binlogSplit.isSuspended(),
                binlogSplit.isInit());
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
                false,
                false);
    }

    public static MySqlBinlogSplit toSuspendedBinlogSplit(MySqlBinlogSplit normalBinlogSplit) {
        return new MySqlBinlogSplit(
                normalBinlogSplit.splitId,
                normalBinlogSplit.getStartingOffset(),
                normalBinlogSplit.getEndingOffset(),
                new ArrayList<>(),
                new HashMap<>(),
                normalBinlogSplit.getTotalFinishedSplitSize(),
                true,
                false);
    }

    public static MySqlBinlogSplit toUnInitBinlogSplit(MySqlBinlogSplit initBinlogSplit) {
        return new MySqlBinlogSplit(
                initBinlogSplit.splitId,
                initBinlogSplit.getStartingOffset(),
                initBinlogSplit.getEndingOffset(),
                initBinlogSplit.getFinishedSnapshotSplitInfos(),
                initBinlogSplit.getTableSchemas(),
                initBinlogSplit.getTotalFinishedSplitSize(),
                initBinlogSplit.isSuspended(),
                false);
    }
}
