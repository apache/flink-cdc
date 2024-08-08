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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** A schema process function that applies user-defined transform logics. */
public class TransformSchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {

    private final List<Tuple5<String, String, String, String, String>> transformRules;
    private transient List<Tuple2<Selectors, Optional<TransformProjection>>> transforms;
    private final Map<TableId, TableChangeInfo> tableChangeInfoMap;
    private transient Map<TableId, TransformProjectionProcessor> processorMap;
    private final List<Tuple2<Selectors, SchemaMetadataTransform>> schemaMetadataTransformers;
    private transient ListState<byte[]> state;

    public static TransformSchemaOperator.Builder newBuilder() {
        return new TransformSchemaOperator.Builder();
    }

    /** Builder of {@link TransformSchemaOperator}. */
    public static class Builder {
        private final List<Tuple5<String, String, String, String, String>> transformRules =
                new ArrayList<>();

        public TransformSchemaOperator.Builder addTransform(
                String tableInclusions,
                @Nullable String projection,
                String primaryKey,
                String partitionKey,
                String tableOption) {
            transformRules.add(
                    Tuple5.of(tableInclusions, projection, primaryKey, partitionKey, tableOption));
            return this;
        }

        public TransformSchemaOperator build() {
            return new TransformSchemaOperator(transformRules);
        }
    }

    private TransformSchemaOperator(
            List<Tuple5<String, String, String, String, String>> transformRules) {
        this.transformRules = transformRules;
        this.tableChangeInfoMap = new ConcurrentHashMap<>();
        this.processorMap = new ConcurrentHashMap<>();
        this.schemaMetadataTransformers = new ArrayList<>();
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        transforms = new ArrayList<>();
        for (Tuple5<String, String, String, String, String> transformRule : transformRules) {
            String tableInclusions = transformRule.f0;
            String projection = transformRule.f1;
            String primaryKeys = transformRule.f2;
            String partitionKeys = transformRule.f3;
            String tableOptions = transformRule.f4;
            Selectors selectors =
                    new Selectors.SelectorsBuilder().includeTables(tableInclusions).build();
            transforms.add(new Tuple2<>(selectors, TransformProjection.of(projection)));
            schemaMetadataTransformers.add(
                    new Tuple2<>(
                            selectors,
                            new SchemaMetadataTransform(primaryKeys, partitionKeys, tableOptions)));
        }
        this.processorMap = new ConcurrentHashMap<>();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        OperatorStateStore stateStore = context.getOperatorStateStore();
        ListStateDescriptor<byte[]> descriptor =
                new ListStateDescriptor<>("originalSchemaState", byte[].class);
        state = stateStore.getUnionListState(descriptor);
        if (context.isRestored()) {
            for (byte[] serializedTableInfo : state.get()) {
                TableChangeInfo stateTableChangeInfo =
                        TableChangeInfo.SERIALIZER.deserialize(
                                TableChangeInfo.SERIALIZER.getVersion(), serializedTableInfo);
                tableChangeInfoMap.put(stateTableChangeInfo.getTableId(), stateTableChangeInfo);
            }
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        state.update(
                new ArrayList<>(
                        tableChangeInfoMap.values().stream()
                                .map(
                                        tableChangeInfo -> {
                                            try {
                                                return TableChangeInfo.SERIALIZER.serialize(
                                                        tableChangeInfo);
                                            } catch (IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        })
                                .collect(Collectors.toList())));
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        clearOperator();
    }

    @Override
    public void close() throws Exception {
        super.close();
        clearOperator();
        this.state = null;
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof CreateTableEvent) {
            event = cacheCreateTable((CreateTableEvent) event);
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof SchemaChangeEvent) {
            event = cacheChangeSchema((SchemaChangeEvent) event);
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof DataChangeEvent) {
            output.collect(new StreamRecord<>(processDataChangeEvent(((DataChangeEvent) event))));
        }
    }

    private SchemaChangeEvent cacheCreateTable(CreateTableEvent event) {
        TableId tableId = event.tableId();
        Schema originalSchema = event.getSchema();
        event = transformCreateTableEvent(event);
        Schema newSchema = (event).getSchema();
        tableChangeInfoMap.put(tableId, TableChangeInfo.of(tableId, originalSchema, newSchema));
        return event;
    }

    private SchemaChangeEvent cacheChangeSchema(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        TableChangeInfo tableChangeInfo = tableChangeInfoMap.get(tableId);
        Schema originalSchema =
                SchemaUtils.applySchemaChangeEvent(tableChangeInfo.getOriginalSchema(), event);
        Schema newSchema =
                SchemaUtils.applySchemaChangeEvent(tableChangeInfo.getTransformedSchema(), event);
        tableChangeInfoMap.put(tableId, TableChangeInfo.of(tableId, originalSchema, newSchema));
        return event;
    }

    private CreateTableEvent transformCreateTableEvent(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();

        for (Tuple2<Selectors, SchemaMetadataTransform> transform : schemaMetadataTransformers) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId)) {
                createTableEvent =
                        new CreateTableEvent(
                                tableId,
                                transformSchemaMetaData(
                                        createTableEvent.getSchema(), transform.f1));
            }
        }

        for (Tuple2<Selectors, Optional<TransformProjection>> transform : transforms) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId) && transform.f1.isPresent()) {
                TransformProjection transformProjection = transform.f1.get();
                if (transformProjection.isValid()) {
                    if (!processorMap.containsKey(tableId)) {
                        processorMap.put(
                                tableId, TransformProjectionProcessor.of(transformProjection));
                    }
                    TransformProjectionProcessor transformProjectionProcessor =
                            processorMap.get(tableId);
                    // update the columns of projection and add the column of projection into Schema
                    return transformProjectionProcessor.processCreateTableEvent(createTableEvent);
                }
            }
        }
        return createTableEvent;
    }

    private Schema transformSchemaMetaData(
            Schema schema, SchemaMetadataTransform schemaMetadataTransform) {
        Schema.Builder schemaBuilder = Schema.newBuilder().setColumns(schema.getColumns());
        if (!schemaMetadataTransform.getPrimaryKeys().isEmpty()) {
            schemaBuilder.primaryKey(schemaMetadataTransform.getPrimaryKeys());
        } else {
            schemaBuilder.primaryKey(schema.primaryKeys());
        }
        if (!schemaMetadataTransform.getPartitionKeys().isEmpty()) {
            schemaBuilder.partitionKey(schemaMetadataTransform.getPartitionKeys());
        } else {
            schemaBuilder.partitionKey(schema.partitionKeys());
        }
        if (!schemaMetadataTransform.getOptions().isEmpty()) {
            schemaBuilder.options(schemaMetadataTransform.getOptions());
        } else {
            schemaBuilder.options(schema.options());
        }
        return schemaBuilder.build();
    }

    private DataChangeEvent processDataChangeEvent(DataChangeEvent dataChangeEvent)
            throws Exception {
        TableId tableId = dataChangeEvent.tableId();
        for (Tuple2<Selectors, Optional<TransformProjection>> transform : transforms) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId) && transform.f1.isPresent()) {
                TransformProjection transformProjection = transform.f1.get();
                if (transformProjection.isValid()) {
                    return processProjection(transformProjection, dataChangeEvent);
                }
            }
        }
        return dataChangeEvent;
    }

    private DataChangeEvent processProjection(
            TransformProjection transformProjection, DataChangeEvent dataChangeEvent)
            throws Exception {
        TableId tableId = dataChangeEvent.tableId();
        TableChangeInfo tableChangeInfo = tableChangeInfoMap.get(tableId);
        if (!processorMap.containsKey(tableId) || !processorMap.get(tableId).hasTableChangeInfo()) {
            processorMap.put(
                    tableId, TransformProjectionProcessor.of(tableChangeInfo, transformProjection));
        }
        TransformProjectionProcessor transformProjectionProcessor = processorMap.get(tableId);
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if (before != null) {
            BinaryRecordData projectedBefore =
                    transformProjectionProcessor.processFillDataField(before);
            dataChangeEvent = DataChangeEvent.projectBefore(dataChangeEvent, projectedBefore);
        }
        if (after != null) {
            BinaryRecordData projectedAfter =
                    transformProjectionProcessor.processFillDataField(after);
            dataChangeEvent = DataChangeEvent.projectAfter(dataChangeEvent, projectedAfter);
        }
        return dataChangeEvent;
    }

    private void clearOperator() {
        this.transforms = null;
        this.processorMap = null;
    }
}
