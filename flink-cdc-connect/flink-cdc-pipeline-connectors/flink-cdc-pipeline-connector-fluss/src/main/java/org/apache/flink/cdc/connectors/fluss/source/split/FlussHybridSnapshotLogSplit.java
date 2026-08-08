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

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * A split that first reads a full KV snapshot from a Fluss table bucket, then switches to reading
 * change log from the snapshot's log offset. This is used by the "full" startup mode for primary
 * key tables.
 *
 * <p>Extends {@link FlussSnapshotSplit} with additional log-reading state: the log starting offset
 * and whether the snapshot phase is already finished (used for checkpoint recovery).
 */
public class FlussHybridSnapshotLogSplit extends FlussSnapshotSplit {

    private final long logStartingOffset;
    private final boolean snapshotFinished;

    /** Creates a new hybrid split with snapshot not yet finished. */
    public FlussHybridSnapshotLogSplit(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            long snapshotId,
            long logStartingOffset) {
        this(tablePath, tableBucket, snapshotId, 0, logStartingOffset, false, null, null);
    }

    /** Full constructor with schema info, typically used during checkpoint recovery. */
    public FlussHybridSnapshotLogSplit(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            long snapshotId,
            long recordsToSkip,
            long logStartingOffset,
            boolean snapshotFinished,
            @Nullable Integer schemaId,
            @Nullable RowType rowType) {
        super(tablePath, tableBucket, snapshotId, recordsToSkip, schemaId, rowType);
        this.logStartingOffset = logStartingOffset;
        this.snapshotFinished = snapshotFinished;
    }

    public long getLogStartingOffset() {
        return logStartingOffset;
    }

    public boolean isSnapshotFinished() {
        return snapshotFinished;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussHybridSnapshotLogSplit that = (FlussHybridSnapshotLogSplit) o;
        return getSnapshotId() == that.getSnapshotId()
                && getRecordsToSkip() == that.getRecordsToSkip()
                && logStartingOffset == that.logStartingOffset
                && snapshotFinished == that.snapshotFinished
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(tableBucket, that.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tablePath,
                tableBucket,
                getSnapshotId(),
                getRecordsToSkip(),
                logStartingOffset,
                snapshotFinished);
    }

    @Override
    public String toString() {
        return "FlussHybridSnapshotLogSplit{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", snapshotId="
                + getSnapshotId()
                + ", recordsToSkip="
                + getRecordsToSkip()
                + ", logStartingOffset="
                + logStartingOffset
                + ", snapshotFinished="
                + snapshotFinished
                + '}';
    }
}
