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
import java.util.Map;

/** Acknowledges completed table and partition removal requests from a source reader. */
public class TableRemovalAckEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final Map<TablePath, Long> completedRemovalRequests;
    private final Map<PhysicalTablePath, Long> completedPartitionRemovalRequests;

    public TableRemovalAckEvent(Map<TablePath, Long> completedRemovalRequests) {
        this(completedRemovalRequests, Collections.emptyMap());
    }

    public TableRemovalAckEvent(
            Map<TablePath, Long> completedRemovalRequests,
            Map<PhysicalTablePath, Long> completedPartitionRemovalRequests) {
        this.completedRemovalRequests =
                Collections.unmodifiableMap(new LinkedHashMap<>(completedRemovalRequests));
        this.completedPartitionRemovalRequests =
                Collections.unmodifiableMap(new LinkedHashMap<>(completedPartitionRemovalRequests));
    }

    public Map<TablePath, Long> getCompletedRemovalRequests() {
        return completedRemovalRequests;
    }

    public Map<PhysicalTablePath, Long> getCompletedPartitionRemovalRequests() {
        return completedPartitionRemovalRequests;
    }

    @Override
    public String toString() {
        return "TableRemovalAckEvent{completedRemovalRequests="
                + completedRemovalRequests
                + ", completedPartitionRemovalRequests="
                + completedPartitionRemovalRequests
                + '}';
    }
}
