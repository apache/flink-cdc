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

package org.apache.flink.cdc.connectors.fluss.source.event;

import org.apache.flink.api.connector.source.SourceEvent;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Authoritative source-table subscription snapshot sent from the enumerator to a reader. */
public class TableSubscriptionEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final Set<TablePath> subscribedTablePaths;
    private final Map<TablePath, Long> pendingRemovalRequests;
    private final Set<TablePath> fencedTablePaths;
    private final Set<PhysicalTablePath> subscribedPhysicalTablePaths;
    private final Map<PhysicalTablePath, Long> pendingPartitionRemovalRequests;
    private final Set<PhysicalTablePath> fencedPartitionPaths;

    public TableSubscriptionEvent(
            Set<TablePath> subscribedTablePaths,
            Map<TablePath, Long> pendingRemovalRequests,
            Set<TablePath> fencedTablePaths,
            Set<PhysicalTablePath> subscribedPhysicalTablePaths) {
        this(
                subscribedTablePaths,
                pendingRemovalRequests,
                fencedTablePaths,
                subscribedPhysicalTablePaths,
                Collections.emptyMap(),
                Collections.emptySet());
    }

    public TableSubscriptionEvent(
            Set<TablePath> subscribedTablePaths,
            Map<TablePath, Long> pendingRemovalRequests,
            Set<TablePath> fencedTablePaths,
            Set<PhysicalTablePath> subscribedPhysicalTablePaths,
            Map<PhysicalTablePath, Long> pendingPartitionRemovalRequests,
            Set<PhysicalTablePath> fencedPartitionPaths) {
        this.subscribedTablePaths =
                Collections.unmodifiableSet(new LinkedHashSet<>(subscribedTablePaths));
        this.pendingRemovalRequests =
                Collections.unmodifiableMap(new LinkedHashMap<>(pendingRemovalRequests));
        this.fencedTablePaths = Collections.unmodifiableSet(new LinkedHashSet<>(fencedTablePaths));
        this.subscribedPhysicalTablePaths =
                Collections.unmodifiableSet(new LinkedHashSet<>(subscribedPhysicalTablePaths));
        this.pendingPartitionRemovalRequests =
                Collections.unmodifiableMap(new LinkedHashMap<>(pendingPartitionRemovalRequests));
        this.fencedPartitionPaths =
                Collections.unmodifiableSet(new LinkedHashSet<>(fencedPartitionPaths));
    }

    public Set<TablePath> getSubscribedTablePaths() {
        return subscribedTablePaths;
    }

    public Map<TablePath, Long> getPendingRemovalRequests() {
        return pendingRemovalRequests;
    }

    public Set<TablePath> getFencedTablePaths() {
        return fencedTablePaths;
    }

    /** Returns the complete successful discovery. */
    public Set<PhysicalTablePath> getSubscribedPhysicalTablePaths() {
        return subscribedPhysicalTablePaths;
    }

    public Map<PhysicalTablePath, Long> getPendingPartitionRemovalRequests() {
        return pendingPartitionRemovalRequests;
    }

    public Set<PhysicalTablePath> getFencedPartitionPaths() {
        return fencedPartitionPaths;
    }

    @Override
    public String toString() {
        return "TableSubscriptionEvent{subscribedTablePaths="
                + subscribedTablePaths
                + ", pendingRemovalRequests="
                + pendingRemovalRequests
                + ", fencedTablePaths="
                + fencedTablePaths
                + ", subscribedPhysicalTablePaths="
                + subscribedPhysicalTablePaths
                + ", pendingPartitionRemovalRequests="
                + pendingPartitionRemovalRequests
                + ", fencedPartitionPaths="
                + fencedPartitionPaths
                + '}';
    }
}
