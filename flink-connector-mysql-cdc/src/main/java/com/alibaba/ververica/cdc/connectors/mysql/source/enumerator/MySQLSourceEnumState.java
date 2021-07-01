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

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitReader;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** The state of MySQL CDC source enumerator. */
public class MySQLSourceEnumState {

    /** The splits in the checkpoint. */
    private final Collection<MySQLSplit> remainingSplits;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final Collection<TableId> alreadyProcessedTables;

    /**
     * The splits that the {@link MySQLSourceEnumerator} has assigned to {@link MySQLSplitReader}s.
     */
    private final Map<Integer, List<MySQLSplit>> assignedSplits;

    /**
     * The finished (snapshot) splits that the {@link MySQLSourceEnumerator} has received from
     * {@link MySQLSplitReader}s.
     */
    private final Map<Integer, List<Tuple2<String, BinlogPosition>>> finishedSnapshotSplits;

    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link MySQLSourceEnumStateSerializer}.
     */
    @Nullable transient byte[] serializedFormCache;

    public MySQLSourceEnumState(
            Collection<MySQLSplit> remainingSplits,
            Collection<TableId> alreadyProcessedTables,
            Map<Integer, List<MySQLSplit>> assignedSplits,
            Map<Integer, List<Tuple2<String, BinlogPosition>>> finishedSnapshotSplits) {
        this.remainingSplits = remainingSplits;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.assignedSplits = assignedSplits;
        this.finishedSnapshotSplits = finishedSnapshotSplits;
    }

    public Collection<TableId> getAlreadyProcessedTables() {
        return alreadyProcessedTables;
    }

    public Collection<MySQLSplit> getRemainingSplits() {
        return remainingSplits;
    }

    public Map<Integer, List<MySQLSplit>> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<Integer, List<Tuple2<String, BinlogPosition>>> getFinishedSnapshotSplits() {
        return finishedSnapshotSplits;
    }
}
