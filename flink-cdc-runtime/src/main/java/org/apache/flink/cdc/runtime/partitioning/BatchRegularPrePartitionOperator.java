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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Operator for processing events from {@link SchemaOperator} before {@link EventPartitioner} with
 * regular topology in batch mode.
 */
@Internal
public class BatchRegularPrePartitionOperator extends AbstractStreamOperator<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    private final int downstreamParallelism;
    private final HashFunctionProvider<DataChangeEvent> hashFunctionProvider;

    private transient Map<TableId, HashFunction<DataChangeEvent>> cachedHashFunctions;
    private transient volatile Map<TableId, Schema> originalSchemaMap;

    public BatchRegularPrePartitionOperator(
            int downstreamParallelism, HashFunctionProvider<DataChangeEvent> hashFunctionProvider) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.downstreamParallelism = downstreamParallelism;
        this.hashFunctionProvider = hashFunctionProvider;
    }

    @Override
    public void open() throws Exception {
        super.open();
        cachedHashFunctions = new HashMap<>();
        originalSchemaMap = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof CreateTableEvent) {
            // Update hash function
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            TableId tableId = createTableEvent.tableId();
            originalSchemaMap.put(tableId, createTableEvent.getSchema());
            cachedHashFunctions.put(tableId, recreateHashFunction(tableId));
            // Broadcast CreateTableEvent
            broadcastEvent(event);
        } else if (event instanceof DataChangeEvent) {
            // Partition DataChangeEvent by table ID and primary keys
            partitionBy(((DataChangeEvent) event));
        }
    }

    private void partitionBy(DataChangeEvent dataChangeEvent) throws Exception {
        output.collect(
                new StreamRecord<>(
                        PartitioningEvent.ofRegular(
                                dataChangeEvent,
                                cachedHashFunctions
                                                .get(dataChangeEvent.tableId())
                                                .hashcode(dataChangeEvent)
                                        % downstreamParallelism)));
    }

    private void broadcastEvent(Event toBroadcast) {
        for (int i = 0; i < downstreamParallelism; i++) {
            // Deep-copying each event is required since downstream subTasks might run in the same
            // JVM
            Event copiedEvent = EventSerializer.INSTANCE.copy(toBroadcast);
            output.collect(new StreamRecord<>(PartitioningEvent.ofRegular(copiedEvent, i)));
        }
    }

    private Schema loadSchemaFromCache(TableId tableId) {
        Optional<Schema> schema;
        try {
            schema = Optional.ofNullable(originalSchemaMap.get(tableId));
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to request latest schema for table \"%s\"", tableId), e);
        }
        if (!schema.isPresent()) {
            throw new IllegalStateException(
                    String.format(
                            "Schema is never registered or outdated for table \"%s\"", tableId));
        }
        return schema.get();
    }

    private HashFunction<DataChangeEvent> recreateHashFunction(TableId tableId) {
        return hashFunctionProvider.getHashFunction(tableId, loadSchemaFromCache(tableId));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // Needless to do anything, since AbstractStreamOperator#snapshotState and #processElement
        // is guaranteed not to be mixed together.
    }
}
