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

package com.ververica.cdc.connectors.mongodb.source.assigners.state;

import com.ververica.cdc.connectors.mongodb.source.assigners.AssignerStatus;
import com.ververica.cdc.connectors.mongodb.source.enumerator.MongoDBSourceEnumerator;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBSplitReader;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSnapshotSplit;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBStreamSplit;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A checkpoint of the current state of the containing the currently pending splits that are not yet
 * assigned.
 */
public class PendingSplitsState {

    /** Whether stream split is assigned. */
    private final boolean isStreamSplitAssigned;

    /** The collections in the checkpoint. */
    private final List<CollectionId> remainingCollections;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final List<CollectionId> alreadyProcessedCollections;

    /** The snapshot splits in the checkpoint. */
    private final List<MongoDBSnapshotSplit> remainingSnapshotSplits;

    /**
     * The snapshot splits that the {@link MongoDBSourceEnumerator} has assigned to {@link
     * MongoDBSplitReader}s.
     */
    private final Map<String, MongoDBSnapshotSplit> assignedSnapshotSplits;

    /**
     * The offsets of finished snapshot splits that the {@link MongoDBSourceEnumerator} has received
     * from {@link MongoDBSplitReader}s.
     */
    private final List<String> finishedSnapshotSplits;

    /** The {@link AssignerStatus} that indicates the snapshot assigner status. */
    private final AssignerStatus assignerStatus;

    @Nullable private final MongoDBStreamSplit remainingStreamSplit;

    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link PendingSplitsStateSerializer}.
     */
    @Nullable transient byte[] serializedFormCache;

    public PendingSplitsState(
            boolean isStreamSplitAssigned,
            List<CollectionId> remainingCollections,
            List<CollectionId> alreadyProcessedCollections,
            List<MongoDBSnapshotSplit> remainingSnapshotSplits,
            Map<String, MongoDBSnapshotSplit> assignedSnapshotSplits,
            List<String> finishedSnapshotSplits,
            @Nullable MongoDBStreamSplit remainingStreamSplit,
            AssignerStatus assignerStatus) {
        this.isStreamSplitAssigned = isStreamSplitAssigned;
        this.remainingCollections = remainingCollections;
        this.alreadyProcessedCollections = alreadyProcessedCollections;
        this.remainingSnapshotSplits = remainingSnapshotSplits;
        this.assignedSnapshotSplits = assignedSnapshotSplits;
        this.finishedSnapshotSplits = finishedSnapshotSplits;
        this.remainingStreamSplit = remainingStreamSplit;
        this.assignerStatus = assignerStatus;
    }

    public boolean isStreamSplitAssigned() {
        return isStreamSplitAssigned;
    }

    public List<CollectionId> getRemainingCollections() {
        return remainingCollections;
    }

    public List<CollectionId> getAlreadyProcessedCollections() {
        return alreadyProcessedCollections;
    }

    public List<MongoDBSnapshotSplit> getRemainingSnapshotSplits() {
        return remainingSnapshotSplits;
    }

    public Map<String, MongoDBSnapshotSplit> getAssignedSnapshotSplits() {
        return assignedSnapshotSplits;
    }

    public List<String> getFinishedSnapshotSplits() {
        return finishedSnapshotSplits;
    }

    public AssignerStatus getAssignerStatus() {
        return assignerStatus;
    }

    @Nullable
    public MongoDBStreamSplit getRemainingStreamSplit() {
        return remainingStreamSplit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PendingSplitsState)) {
            return false;
        }
        PendingSplitsState that = (PendingSplitsState) o;
        return assignerStatus == that.assignerStatus
                && isStreamSplitAssigned == that.isStreamSplitAssigned
                && Objects.equals(remainingCollections, that.remainingCollections)
                && Objects.equals(alreadyProcessedCollections, that.alreadyProcessedCollections)
                && Objects.equals(remainingSnapshotSplits, that.remainingSnapshotSplits)
                && Objects.equals(assignedSnapshotSplits, that.assignedSnapshotSplits)
                && Objects.equals(finishedSnapshotSplits, that.finishedSnapshotSplits)
                && Objects.equals(remainingStreamSplit, that.remainingStreamSplit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                assignerStatus,
                isStreamSplitAssigned,
                remainingCollections,
                alreadyProcessedCollections,
                remainingSnapshotSplits,
                assignedSnapshotSplits,
                finishedSnapshotSplits,
                remainingStreamSplit);
    }

    @Override
    public String toString() {
        return "MongoDBPendingSplitsState{"
                + "remainingCollections="
                + remainingCollections
                + ", alreadyProcessedCollections="
                + alreadyProcessedCollections
                + ", remainingSnapshotSplits="
                + remainingSnapshotSplits
                + ", assignedSnapshotSplits="
                + assignedSnapshotSplits
                + ", finishedSnapshotSplits="
                + finishedSnapshotSplits
                + ", remainingStreamSplit="
                + remainingStreamSplit
                + ", assignerStatus="
                + assignerStatus
                + '}';
    }
}
