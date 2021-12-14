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

package com.ververica.cdc.connectors.mysql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

import com.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;

import java.util.List;

/**
 * The {@link SourceEvent} that {@link MySqlSourceEnumerator} sends to {@link MySqlSourceReader} to
 * notify the finished snapshot splits has been received, i.e. acknowledge for {@link
 * FinishedSnapshotSplitsReportEvent}.
 */
public class FinishedSnapshotSplitsAckEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final List<String> finishedSplits;
    private final int alreadyProcessedTableCount;
    private final int remainingTableCount;
    private final int finishedSplitCount;
    private final int assignedSplitCount;
    private final int remainingSplitCount;

    public FinishedSnapshotSplitsAckEvent(
            final List<String> finishedSplits,
            final int alreadyProcessedTableCount,
            final int remainingTableCount,
            final int finishedSplitCount,
            final int assignedSplitCount,
            final int remainingSplitCount) {
        this.finishedSplits = finishedSplits;
        this.alreadyProcessedTableCount = alreadyProcessedTableCount;
        this.remainingTableCount = remainingTableCount;
        this.finishedSplitCount = finishedSplitCount;
        this.assignedSplitCount = assignedSplitCount;
        this.remainingSplitCount = remainingSplitCount;
    }

    public List<String> getFinishedSplits() {
        return finishedSplits;
    }

    public int getAlreadyProcessedTableCount() {
        return alreadyProcessedTableCount;
    }

    public int getRemainingTableCount() {
        return remainingTableCount;
    }

    public int getFinishedSplitCount() {
        return finishedSplitCount;
    }

    public int getAssignedSplitCount() {
        return assignedSplitCount;
    }

    public int getRemainingSplitCount() {
        return remainingSplitCount;
    }

    @Override
    public String toString() {
        return "FinishedSnapshotSplitsAckEvent{"
                + "finishedSplits="
                + finishedSplits
                + ", alreadyProcessedTableCount="
                + alreadyProcessedTableCount
                + ", remainingTableCount="
                + remainingTableCount
                + ", finishedSplitCount="
                + finishedSplitCount
                + ", assignedSplitCount="
                + assignedSplitCount
                + ", remainingSplitCount="
                + remainingSplitCount
                + '}';
    }
}
