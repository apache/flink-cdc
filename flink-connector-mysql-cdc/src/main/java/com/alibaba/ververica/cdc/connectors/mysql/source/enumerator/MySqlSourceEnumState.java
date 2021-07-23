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

import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** The state of MySQL CDC source enumerator. */
public class MySqlSourceEnumState {

    /** The splits in the checkpoint. */
    private final Collection<MySqlSplit> remainingSplits;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final Collection<TableId> alreadyProcessedTables;

    /**
     * The snapshot splits that the {@link MySqlSourceEnumerator} has assigned to {@link
     * MySqlSplitReader}s.
     */
    private final Map<Integer, List<MySqlSnapshotSplit>> assignedSnapshotSplits;

    /** The flag indicates whether the binlog split has been assigned. */
    private final boolean binlogSplitAssigned;

    /**
     * The finished (snapshot) splits that the {@link MySqlSourceEnumerator} has received from
     * {@link MySqlSplitReader}s.
     */
    private final Map<Integer, Map<String, BinlogOffset>> finishedSnapshotSplits;

    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link MySqlSourceEnumStateSerializer}.
     */
    @Nullable transient byte[] serializedFormCache;

    public MySqlSourceEnumState(
            Collection<MySqlSplit> remainingSplits,
            Collection<TableId> alreadyProcessedTables,
            Map<Integer, List<MySqlSnapshotSplit>> assignedSnapshotSplits,
            Map<Integer, Map<String, BinlogOffset>> finishedSnapshotSplits,
            boolean binlogSplitAssigned) {
        this.remainingSplits = remainingSplits;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.assignedSnapshotSplits = assignedSnapshotSplits;
        this.finishedSnapshotSplits = finishedSnapshotSplits;
        this.binlogSplitAssigned = binlogSplitAssigned;
    }

    public Collection<TableId> getAlreadyProcessedTables() {
        return alreadyProcessedTables;
    }

    public Collection<MySqlSplit> getRemainingSplits() {
        return remainingSplits;
    }

    public Map<Integer, List<MySqlSnapshotSplit>> getAssignedSnapshotSplits() {
        return assignedSnapshotSplits;
    }

    public boolean isBinlogSplitAssigned() {
        return binlogSplitAssigned;
    }

    public Map<Integer, Map<String, BinlogOffset>> getFinishedSnapshotSplits() {
        return finishedSnapshotSplits;
    }
}
