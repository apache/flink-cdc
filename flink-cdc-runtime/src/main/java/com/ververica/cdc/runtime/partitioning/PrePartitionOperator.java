/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.partitioning;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.OperationType;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.runtime.operators.sink.SchemaEvolutionClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Operator for processing events from {@link
 * com.ververica.cdc.runtime.operators.schema.SchemaOperator} before {@link EventPartitioner}.
 */
@Internal
public class PrePartitionOperator extends AbstractStreamOperator<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent> {

    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    private final OperatorID schemaOperatorId;
    private final int downstreamParallelism;

    private transient SchemaEvolutionClient schemaEvolutionClient;
    private transient LoadingCache<TableId, HashFunction> cachedHashFunctions;

    public PrePartitionOperator(OperatorID schemaOperatorId, int downstreamParallelism) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.schemaOperatorId = schemaOperatorId;
        this.downstreamParallelism = downstreamParallelism;
    }

    @Override
    public void open() throws Exception {
        super.open();
        TaskOperatorEventGateway toCoordinator =
                getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway();
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, schemaOperatorId);
        cachedHashFunctions = createCache();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            // Update hash function
            TableId tableId = ((SchemaChangeEvent) event).tableId();
            cachedHashFunctions.put(tableId, recreateHashFunction(tableId));
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

    private Schema loadLatestSchemaFromRegistry(TableId tableId) {
        Optional<Schema> schema;
        try {
            schema = schemaEvolutionClient.getLatestSchema(tableId);
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

    private HashFunction recreateHashFunction(TableId tableId) {
        return new HashFunction(loadLatestSchemaFromRegistry(tableId));
    }

    private LoadingCache<TableId, HashFunction> createCache() {
        return CacheBuilder.newBuilder()
                .expireAfterAccess(CACHE_EXPIRE_DURATION)
                .build(
                        new CacheLoader<TableId, HashFunction>() {
                            @Override
                            public HashFunction load(TableId key) {
                                return recreateHashFunction(key);
                            }
                        });
    }

    @VisibleForTesting
    static class HashFunction implements Function<DataChangeEvent, Integer> {
        private final List<RecordData.FieldGetter> primaryKeyGetters;

        public HashFunction(Schema schema) {
            primaryKeyGetters = createFieldGetters(schema);
        }

        @Override
        public Integer apply(DataChangeEvent event) {
            List<Object> objectsToHash = new ArrayList<>();
            // Table ID
            TableId tableId = event.tableId();
            Optional.ofNullable(tableId.getNamespace()).ifPresent(objectsToHash::add);
            Optional.ofNullable(tableId.getSchemaName()).ifPresent(objectsToHash::add);
            objectsToHash.add(tableId.getTableName());

            // Primary key
            RecordData data =
                    event.op().equals(OperationType.DELETE) ? event.before() : event.after();
            for (RecordData.FieldGetter primaryKeyGetter : primaryKeyGetters) {
                objectsToHash.add(primaryKeyGetter.getFieldOrNull(data));
            }

            // Calculate hash
            return (Objects.hash(objectsToHash.toArray()) * 31) & 0x7FFFFFFF;
        }

        private List<RecordData.FieldGetter> createFieldGetters(Schema schema) {
            List<RecordData.FieldGetter> fieldGetters =
                    new ArrayList<>(schema.primaryKeys().size());
            int[] primaryKeyPositions =
                    schema.primaryKeys().stream()
                            .mapToInt(
                                    pk -> {
                                        int i = 0;
                                        while (!schema.getColumns().get(i).getName().equals(pk)) {
                                            ++i;
                                        }
                                        if (i >= schema.getColumnCount()) {
                                            throw new IllegalStateException(
                                                    String.format(
                                                            "Unable to find column \"%s\" which is defined as primary key",
                                                            pk));
                                        }
                                        return i;
                                    })
                            .toArray();
            for (int primaryKeyPosition : primaryKeyPositions) {
                fieldGetters.add(
                        RecordData.createFieldGetter(
                                schema.getColumns().get(primaryKeyPosition).getType(),
                                primaryKeyPosition));
            }
            return fieldGetters;
        }
    }
}
