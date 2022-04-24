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

package com.ververica.cdc.connectors.oracle.source.meta.split;

import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** The split to describe the redo log of Oracle table(s). */
public class OracleRedoLogSplit extends OracleSplit {

    private final RedoLogOffset startingOffset;
    private final RedoLogOffset endingOffset;
    private final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos;
    private final Map<TableId, TableChanges.TableChange> tableSchemas;
    private final int totalFinishedSplitSize;
    private final boolean isSuspended;
    @Nullable transient byte[] serializedFormCache;

    public OracleRedoLogSplit(
            String splitId,
            RedoLogOffset startingOffset,
            RedoLogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.isSuspended = isSuspended;
    }

    public OracleRedoLogSplit(
            String splitId,
            RedoLogOffset startingOffset,
            RedoLogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            int totalFinishedSplitSize) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.isSuspended = false;
    }

    public RedoLogOffset getStartingOffset() {
        return startingOffset;
    }

    public RedoLogOffset getEndingOffset() {
        return endingOffset;
    }

    public List<FinishedSnapshotSplitInfo> getFinishedSnapshotSplitInfos() {
        return finishedSnapshotSplitInfos;
    }

    @Override
    public Map<TableId, TableChanges.TableChange> getTableSchemas() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OracleRedoLogSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        OracleRedoLogSplit that = (OracleRedoLogSplit) o;
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
        return "OracleRedoLogSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", offset="
                + startingOffset
                + ", endOffset="
                + endingOffset
                + ", isSuspended="
                + isSuspended
                + '}';
    }
}
