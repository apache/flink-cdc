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
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.AbstractStreamOperatorAdapter;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Operator for processing events from {@link SchemaOperator} before {@link EventPartitioner} with
 * regular topology.
 */
@Internal
public class RegularPrePartitionOperator extends AbstractStreamOperatorAdapter<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    private final OperatorID schemaOperatorId;
    private final int downstreamParallelism;
    private final HashFunctionProvider<DataChangeEvent> hashFunctionProvider;
    private final boolean requiresPrimaryKeyUpdateSplit;

    private transient SchemaEvolutionClient schemaEvolutionClient;
    private transient LoadingCache<TableId, PartitioningFunctions> cachedPartitioningFunctions;

    public RegularPrePartitionOperator(
            OperatorID schemaOperatorId,
            int downstreamParallelism,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider) {
        this(schemaOperatorId, downstreamParallelism, hashFunctionProvider, false);
    }

    public RegularPrePartitionOperator(
            OperatorID schemaOperatorId,
            int downstreamParallelism,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider,
            boolean requiresPrimaryKeyUpdateSplit) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.schemaOperatorId = schemaOperatorId;
        this.downstreamParallelism = downstreamParallelism;
        this.hashFunctionProvider = hashFunctionProvider;
        this.requiresPrimaryKeyUpdateSplit = requiresPrimaryKeyUpdateSplit;
    }

    @Override
    public void open() throws Exception {
        super.open();
        TaskOperatorEventGateway toCoordinator =
                getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway();
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, schemaOperatorId);
        cachedPartitioningFunctions = createCache();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            // Update hash function
            TableId tableId = ((SchemaChangeEvent) event).tableId();
            cachedPartitioningFunctions.put(tableId, recreatePartitioningFunctions(tableId));
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

    private void partitionBy(DataChangeEvent dataChangeEvent) throws Exception {
        PartitioningFunctions functions =
                cachedPartitioningFunctions.get(dataChangeEvent.tableId());
        for (DataChangeEvent event : functions.split(dataChangeEvent)) {
            output.collect(
                    new StreamRecord<>(
                            PartitioningEvent.ofRegular(
                                    event,
                                    functions.hashFunction.hashcode(event)
                                            % downstreamParallelism)));
        }
    }

    private void broadcastEvent(Event toBroadcast) {
        for (int i = 0; i < downstreamParallelism; i++) {
            // Deep-copying each event is required since downstream subTasks might run in the same
            // JVM
            Event copiedEvent = EventSerializer.INSTANCE.copy(toBroadcast);
            output.collect(new StreamRecord<>(PartitioningEvent.ofRegular(copiedEvent, i)));
        }
    }

    private Schema loadLatestSchemaFromRegistry(TableId tableId) {
        Optional<Schema> schema;
        try {
            schema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
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

    private PartitioningFunctions recreatePartitioningFunctions(TableId tableId) {
        Schema schema = loadLatestSchemaFromRegistry(tableId);
        return new PartitioningFunctions(
                hashFunctionProvider.getHashFunction(tableId, schema),
                requiresPrimaryKeyUpdateSplit ? new PrimaryKeyUpdateSplitter(schema) : null);
    }

    private LoadingCache<TableId, PartitioningFunctions> createCache() {
        return CacheBuilder.newBuilder()
                .expireAfterAccess(CACHE_EXPIRE_DURATION)
                .build(
                        new CacheLoader<TableId, PartitioningFunctions>() {
                            @Override
                            public PartitioningFunctions load(TableId key) {
                                return recreatePartitioningFunctions(key);
                            }
                        });
    }

    private static final class PartitioningFunctions {
        private final HashFunction<DataChangeEvent> hashFunction;
        private final PrimaryKeyUpdateSplitter splitter;

        private PartitioningFunctions(
                HashFunction<DataChangeEvent> hashFunction, PrimaryKeyUpdateSplitter splitter) {
            this.hashFunction = hashFunction;
            this.splitter = splitter;
        }

        private List<DataChangeEvent> split(DataChangeEvent event) {
            return splitter == null ? Collections.singletonList(event) : splitter.split(event);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // Needless to do anything, since AbstractStreamOperator#snapshotState and #processElement
        // is guaranteed not to be mixed together.
    }
}
