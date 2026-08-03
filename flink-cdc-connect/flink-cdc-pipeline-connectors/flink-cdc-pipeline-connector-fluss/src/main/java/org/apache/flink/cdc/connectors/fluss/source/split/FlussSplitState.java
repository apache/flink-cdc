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

package org.apache.flink.cdc.connectors.fluss.source.split;

import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

/**
 * Abstract base class for the mutable state of a {@link FlussSplitBase}. Concrete subclasses track
 * the reading progress for each split type and convert back to an immutable split on checkpoint.
 *
 * @see FlussLogSplitState
 * @see FlussHybridSnapshotLogSplitState
 */
public abstract class FlussSplitState {

    protected final FlussSplitBase split;

    /** Tracks the latest schema ID seen by this split (updated during record emission). */
    private @Nullable Integer schemaId;

    /** Tracks the {@link RowType} corresponding to {@link #schemaId}. */
    private @Nullable RowType rowType;

    public FlussSplitState(FlussSplitBase split) {
        this.split = split;
        this.schemaId = split.getSchemaId();
        this.rowType = split.getRowType();
    }

    /** Checks whether this split state is a hybrid snapshot log split state. */
    public final boolean isHybridSnapshotLogSplitState() {
        return getClass() == FlussHybridSnapshotLogSplitState.class;
    }

    /** Checks whether this split state is a log split state. */
    public final boolean isLogSplitState() {
        return getClass() == FlussLogSplitState.class;
    }

    /** Casts this split state into a {@link FlussHybridSnapshotLogSplitState}. */
    public final FlussHybridSnapshotLogSplitState asHybridSnapshotLogSplitState() {
        return (FlussHybridSnapshotLogSplitState) this;
    }

    /** Casts this split state into a {@link FlussLogSplitState}. */
    public final FlussLogSplitState asLogSplitState() {
        return (FlussLogSplitState) this;
    }

    /** Converts this mutable state back to an immutable split for checkpointing. */
    public abstract FlussSplitBase toFlussSplit();

    public @Nullable Integer getSchemaId() {
        return schemaId;
    }

    public @Nullable RowType getRowType() {
        return rowType;
    }

    /**
     * Updates the schema tracking with the latest schemaId and corresponding RowType. Called by the
     * record emitter when processing records.
     */
    public void updateSchema(int schemaId, RowType rowType) {
        this.schemaId = schemaId;
        this.rowType = rowType;
    }
}
