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

import org.apache.fluss.metadata.TableBucket;

import java.util.Set;

/** Reports KV snapshots whose consumption is included in a reader checkpoint. */
public class FinishedKvSnapshotConsumeEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final long checkpointId;
    private final Set<TableBucket> tableBuckets;

    public FinishedKvSnapshotConsumeEvent(long checkpointId, Set<TableBucket> tableBuckets) {
        this.checkpointId = checkpointId;
        this.tableBuckets = tableBuckets;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public Set<TableBucket> getTableBuckets() {
        return tableBuckets;
    }

    @Override
    public String toString() {
        return "FinishedKvSnapshotConsumeEvent{"
                + "checkpointId="
                + checkpointId
                + ", tableBuckets="
                + tableBuckets
                + '}';
    }
}
