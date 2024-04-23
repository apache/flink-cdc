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

package org.apache.flink.cdc.connectors.maxcompute.sink;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Map;

/** partition data change event by bucket before write to maxcompute. */
public class PartitionOperator extends AbstractStreamOperator<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent> {

    private final int downstreamParallelism;
    private final int maxComputeBucketSize;
    private transient Map<TableId, Schema> schemaMaps;
    private transient Map<TableId, MaxComputeHashFunction> cachedHashFunctions;

    public PartitionOperator(int downstreamParallelism, int maxComputeBucketSize) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.downstreamParallelism = downstreamParallelism;
        this.maxComputeBucketSize = maxComputeBucketSize;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.schemaMaps = new HashMap<>();
        this.cachedHashFunctions = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof CreateTableEvent) {
            // Init hash function
            TableId tableId = ((CreateTableEvent) event).tableId();
            schemaMaps.put(tableId, ((CreateTableEvent) event).getSchema());
            cachedHashFunctions.put(tableId, createHashFunction(tableId));
            broadcastEvent(event);
        } else if (event instanceof SchemaChangeEvent) {
            // Update hash function
            TableId tableId = ((SchemaChangeEvent) event).tableId();
            Schema newSchema =
                    SchemaUtils.applySchemaChangeEvent(
                            schemaMaps.get(tableId), (SchemaChangeEvent) event);
            schemaMaps.put(tableId, newSchema);
            cachedHashFunctions.put(tableId, createHashFunction(tableId));
            // Broadcast SchemaChangeEvent
            broadcastEvent(event);
        } else if (event instanceof FlushEvent) {
            // Broadcast FlushEvent
            broadcastEvent(event);
        } else if (event instanceof DataChangeEvent) {
            // Partition DataChangeEvent by table ID and primary keys
            partitionBy(((DataChangeEvent) event));
        }
    }

    private MaxComputeHashFunction createHashFunction(TableId tableId) {
        return new MaxComputeHashFunction(schemaMaps.get(tableId), maxComputeBucketSize);
    }

    private void partitionBy(DataChangeEvent dataChangeEvent) throws Exception {
        output.collect(
                new StreamRecord<>(
                        new PartitioningEvent(
                                dataChangeEvent,
                                cachedHashFunctions
                                                .get(dataChangeEvent.tableId())
                                                .apply(dataChangeEvent)
                                        % downstreamParallelism)));
    }

    private void broadcastEvent(Event toBroadcast) {
        for (int i = 0; i < downstreamParallelism; i++) {
            output.collect(new StreamRecord<>(new PartitioningEvent(toBroadcast, i)));
        }
    }
}
