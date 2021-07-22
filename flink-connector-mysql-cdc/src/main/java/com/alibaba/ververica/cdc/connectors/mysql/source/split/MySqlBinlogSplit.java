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

import org.apache.flink.table.types.logical.RowType;

import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** The split to describe the binlog of MySql table(s). */
public class MySqlBinlogSplit extends MySqlSplit {

    private final BinlogOffset startingOffset;
    private final BinlogOffset endingOffset;
    private final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos;

    @Nullable transient byte[] serializedFormCache;

    public MySqlBinlogSplit(
            String splitId,
            RowType splitKeyType,
            BinlogOffset startingOffset,
            BinlogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas) {
        super(splitId, splitKeyType, tableSchemas);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
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
    public String toString() {
        return "MySqlBinlogSplit{"
                + ", splitId='"
                + splitId
                + '\''
                + ", splitKeyType="
                + splitKeyType
                + ", offset="
                + startingOffset
                + ", endOffset="
                + endingOffset
                + '}';
    }
}
