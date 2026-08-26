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

package org.apache.flink.cdc.connectors.fluss.source.enumerator;

import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;

import org.apache.fluss.metadata.PhysicalTablePath;

import java.util.List;
import java.util.Set;

/** The state of the {@link FlussSourceEnumerator}, used for checkpointing. */
public class FlussSourceEnumState {

    private final Set<PhysicalTablePath> assignedPhysicalTablePaths;
    private final List<FlussSplitBase> remainingSplits;
    private final String leaseId;

    public FlussSourceEnumState(
            Set<PhysicalTablePath> assignedPhysicalTablePaths,
            List<FlussSplitBase> remainingSplits,
            String leaseId) {
        this.assignedPhysicalTablePaths = assignedPhysicalTablePaths;
        this.remainingSplits = remainingSplits;
        this.leaseId = leaseId;
    }

    public Set<PhysicalTablePath> getAssignedPhysicalTablePaths() {
        return assignedPhysicalTablePaths;
    }

    public List<FlussSplitBase> getRemainingSplits() {
        return remainingSplits;
    }

    public String getLeaseId() {
        return leaseId;
    }
}
