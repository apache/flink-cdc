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
 * A split that reads a bounded KV snapshot from a Fluss table bucket. This is the base class for
 * snapshot-based reading and is extended by {@link FlussHybridSnapshotLogSplit} to support reading
 * a snapshot followed by change log.
 */
public abstract class FlussSnapshotSplit extends FlussSplitBase {

    private final long snapshotId;
    private final long recordsToSkip;

    /** Constructor for snapshot-based split implementations. */
    protected FlussSnapshotSplit(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            long snapshotId,
            long recordsToSkip,
            @Nullable Integer schemaId,
            @Nullable RowType rowType) {
        super(tablePath, tableBucket, schemaId, rowType);
        this.snapshotId = snapshotId;
        this.recordsToSkip = recordsToSkip;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getRecordsToSkip() {
        return recordsToSkip;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussSnapshotSplit that = (FlussSnapshotSplit) o;
        return snapshotId == that.snapshotId
                && recordsToSkip == that.recordsToSkip
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(tableBucket, that.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableBucket, snapshotId, recordsToSkip);
    }

    @Override
    public String toString() {
        return "FlussSnapshotSplit{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", snapshotId="
                + snapshotId
                + ", recordsToSkip="
                + recordsToSkip
                + '}';
    }
}
