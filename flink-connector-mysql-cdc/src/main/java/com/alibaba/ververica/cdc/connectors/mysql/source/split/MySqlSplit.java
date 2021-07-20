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

package com.alibaba.ververica.cdc.connectors.mysql.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** The split of table comes from a Table that splits by primary key. */
public class MySqlSplit implements SourceSplit {

    private final MySqlSplitKind splitKind;
    private final TableId tableId;
    private final String splitId;
    private final RowType splitBoundaryType;

    // the fields for snapshot split
    @Nullable private final Object[] splitBoundaryStart;
    @Nullable private final Object[] splitBoundaryEnd;
    @Nullable private final BinlogOffset lowWatermark;
    @Nullable private final BinlogOffset highWatermark;
    private final boolean snapshotReadFinished;

    // the fields for binlog split
    private final BinlogOffset offset;
    // (tableId, splitId, splitStart, splitEnd, highWatermark)
    private final List<Tuple5<TableId, String, Object[], Object[], BinlogOffset>>
            finishedSplitsInfo;

    // TODO: The databaseHistory can be shared in splitReader for all snapshot splits
    //  rather than here. In that way, we can deal schema change well even during
    //  snapshotting phase. Now, both snapshot split and binlog split need this field.
    private final Map<TableId, SchemaRecord> databaseHistory;
    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link MySqlSplitSerializer}.
     */
    @Nullable transient byte[] serializedFormCache;

    public MySqlSplit(
            MySqlSplitKind splitKind,
            TableId tableId,
            String splitId,
            RowType splitBoundaryType,
            @Nullable Object[] splitBoundaryStart,
            @Nullable Object[] splitBoundaryEnd,
            @Nullable BinlogOffset lowWatermark,
            @Nullable BinlogOffset highWatermark,
            boolean snapshotReadFinished,
            @Nullable BinlogOffset offset,
            List<Tuple5<TableId, String, Object[], Object[], BinlogOffset>> finishedSplitsInfo,
            Map<TableId, SchemaRecord> databaseHistory) {
        this.splitKind = splitKind;
        this.tableId = tableId;
        this.splitId = splitId;
        this.splitBoundaryType = splitBoundaryType;
        this.splitBoundaryStart = splitBoundaryStart;
        this.splitBoundaryEnd = splitBoundaryEnd;
        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;
        this.snapshotReadFinished = snapshotReadFinished;
        this.offset = offset;
        this.finishedSplitsInfo = finishedSplitsInfo;
        this.databaseHistory = databaseHistory;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public MySqlSplitKind getSplitKind() {
        return splitKind;
    }

    public TableId getTableId() {
        return tableId;
    }

    public String getSplitId() {
        return splitId;
    }

    public RowType getSplitBoundaryType() {
        return splitBoundaryType;
    }

    @Nullable
    public Object[] getSplitBoundaryStart() {
        return splitBoundaryStart;
    }

    @Nullable
    public Object[] getSplitBoundaryEnd() {
        return splitBoundaryEnd;
    }

    @Nullable
    public BinlogOffset getLowWatermark() {
        return lowWatermark;
    }

    @Nullable
    public BinlogOffset getHighWatermark() {
        return highWatermark;
    }

    public boolean isSnapshotReadFinished() {
        return snapshotReadFinished;
    }

    public BinlogOffset getOffset() {
        return offset;
    }

    public List<Tuple5<TableId, String, Object[], Object[], BinlogOffset>> getFinishedSplitsInfo() {
        return finishedSplitsInfo;
    }

    public Map<TableId, SchemaRecord> getDatabaseHistory() {
        return databaseHistory;
    }

    @Override
    public String toString() {
        if (splitKind == MySqlSplitKind.SNAPSHOT) {
            return "MySQLSplit{"
                    + "splitKind="
                    + splitKind
                    + ", tableId="
                    + tableId
                    + ", splitId='"
                    + splitId
                    + '\''
                    + ", splitBoundaryType="
                    + splitBoundaryType
                    + ", splitBoundaryStart="
                    + Arrays.toString(splitBoundaryStart)
                    + ", splitBoundaryEnd="
                    + Arrays.toString(splitBoundaryEnd)
                    + ", snapshotReadFinished="
                    + snapshotReadFinished
                    + ", lowWatermark="
                    + lowWatermark
                    + ", highWatermark="
                    + highWatermark
                    + '}';
        } else {
            return "MySQLSplit{"
                    + "splitKind="
                    + splitKind
                    + ", tableId="
                    + tableId
                    + ", splitId='"
                    + splitId
                    + '\''
                    + ", splitBoundaryType="
                    + splitBoundaryType
                    + ", binlogPosition='"
                    + offset
                    + '\''
                    + '}';
        }
    }
}
