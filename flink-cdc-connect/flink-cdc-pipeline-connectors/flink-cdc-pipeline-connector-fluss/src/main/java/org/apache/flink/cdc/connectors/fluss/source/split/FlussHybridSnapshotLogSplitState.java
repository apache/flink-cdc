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

/** The state of {@link FlussHybridSnapshotLogSplit}. */
public class FlussHybridSnapshotLogSplitState extends FlussSplitState {

    /** The records to skip while reading a snapshot. */
    private long recordsToSkip;

    /** Whether the snapshot reading is finished. */
    private boolean snapshotFinished;

    /** The next log offset to read. */
    private long nextOffset;

    public FlussHybridSnapshotLogSplitState(FlussHybridSnapshotLogSplit hybridSplit) {
        super(hybridSplit);
        this.recordsToSkip = hybridSplit.getRecordsToSkip();
        this.snapshotFinished = hybridSplit.isSnapshotFinished();
        this.nextOffset = hybridSplit.getLogStartingOffset();
    }

    public void setRecordsToSkip(long recordsToSkip) {
        this.recordsToSkip = recordsToSkip;
    }

    public void setNextOffset(long nextOffset) {
        // if set offset, means snapshot is finished
        snapshotFinished = true;
        this.nextOffset = nextOffset;
    }

    @Override
    public FlussHybridSnapshotLogSplit toFlussSplit() {
        FlussHybridSnapshotLogSplit hybrid = split.asHybridSnapshotLogSplit();
        return new FlussHybridSnapshotLogSplit(
                split.getPhysicalTablePath(),
                split.getTableBucket(),
                hybrid.getSnapshotId(),
                recordsToSkip,
                nextOffset,
                snapshotFinished,
                getSchemaId(),
                getRowType());
    }
}
