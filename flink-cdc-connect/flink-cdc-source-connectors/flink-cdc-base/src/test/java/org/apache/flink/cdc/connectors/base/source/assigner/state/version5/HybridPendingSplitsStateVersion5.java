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

package org.apache.flink.cdc.connectors.base.source.assigner.state.version5;

import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;

/**
 * The 5th version of HybridPendingSplitsState. The modification of the 6th version: Change
 * isAssignerFinished(boolean) to assignStatus in SnapshotPendingSplitsState to represent a more
 * comprehensive assignment status.
 */
public class HybridPendingSplitsStateVersion5 extends PendingSplitsState {
    private final SnapshotPendingSplitsStateVersion5 snapshotPendingSplits;
    private final boolean isStreamSplitAssigned;

    public HybridPendingSplitsStateVersion5(
            SnapshotPendingSplitsStateVersion5 snapshotPendingSplits,
            boolean isStreamSplitAssigned) {
        this.snapshotPendingSplits = snapshotPendingSplits;
        this.isStreamSplitAssigned = isStreamSplitAssigned;
    }

    public SnapshotPendingSplitsStateVersion5 getSnapshotPendingSplits() {
        return snapshotPendingSplits;
    }

    public boolean isStreamSplitAssigned() {
        return isStreamSplitAssigned;
    }
}
