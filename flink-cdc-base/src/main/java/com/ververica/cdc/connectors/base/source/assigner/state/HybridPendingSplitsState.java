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

package com.ververica.cdc.connectors.base.source.assigner.state;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import io.debezium.schema.DataCollectionId;

import java.util.Objects;

/** A {@link PendingSplitsState} for pending hybrid (snapshot & stream) splits. */
public class HybridPendingSplitsState<ID extends DataCollectionId, S>
        extends PendingSplitsState<ID, S> {
    private final SnapshotPendingSplitsState<ID, S> snapshotPendingSplits;
    private final boolean isStreamSplitAssigned;
    private final Offset startupOffset;

    public HybridPendingSplitsState(
            SnapshotPendingSplitsState<ID, S> snapshotPendingSplits,
            boolean isStreamSplitAssigned,
            Offset initialOffset) {
        this.snapshotPendingSplits = snapshotPendingSplits;
        this.isStreamSplitAssigned = isStreamSplitAssigned;
        this.startupOffset = initialOffset;
    }

    public SnapshotPendingSplitsState<ID, S> getSnapshotPendingSplits() {
        return snapshotPendingSplits;
    }

    public boolean isStreamSplitAssigned() {
        return isStreamSplitAssigned;
    }

    public Offset getStartupOffset() {
        return startupOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HybridPendingSplitsState<?, ?> that = (HybridPendingSplitsState<?, ?>) o;
        return isStreamSplitAssigned == that.isStreamSplitAssigned
                && Objects.equals(startupOffset, that.startupOffset)
                && Objects.equals(snapshotPendingSplits, that.snapshotPendingSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotPendingSplits, isStreamSplitAssigned, startupOffset);
    }

    @Override
    public String toString() {
        return "HybridPendingSplitsState{"
                + "snapshotPendingSplits="
                + snapshotPendingSplits
                + ", isStreamSplitAssigned="
                + isStreamSplitAssigned
                + ", startupOffset="
                + startupOffset
                + '}';
    }
}
