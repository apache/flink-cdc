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

    public TableSubscriptionEvent(
            Set<TablePath> subscribedTablePaths, Map<TablePath, Long> pendingRemovalRequests) {
        this(subscribedTablePaths, pendingRemovalRequests, Collections.emptySet());
    }

    public TableSubscriptionEvent(
            Set<TablePath> subscribedTablePaths,
            Map<TablePath, Long> pendingRemovalRequests,
            Set<TablePath> fencedTablePaths) {
        this.subscribedTablePaths =
                Collections.unmodifiableSet(new LinkedHashSet<>(subscribedTablePaths));
        this.pendingRemovalRequests =
                Collections.unmodifiableMap(new LinkedHashMap<>(pendingRemovalRequests));
        this.fencedTablePaths = Collections.unmodifiableSet(new LinkedHashSet<>(fencedTablePaths));
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
}
