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
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A data process function that filters out columns which aren't (directly & indirectly) referenced.
 */
public class PreTransformOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;

    private final List<TransformRule> transformRules;
    private transient List<PreTransformer> transforms;
    private final Map<TableId, PreTransformChangeInfo> preTransformChangeInfoMap;
    private final List<Tuple2<Selectors, SchemaMetadataTransform>> schemaMetadataTransformers;
    private transient ListState<byte[]> state;
    private final List<Tuple2<String, String>> udfFunctions;
    private List<UserDefinedFunctionDescriptor> udfDescriptors;
    private Map<TableId, PreTransformProcessor> preTransformProcessorMap;
    private Map<TableId, Boolean> hasAsteriskMap;
    private Map<TableId, List<String>> referencedColumnsMap;

    public static PreTransformOperator.Builder newBuilder() {
        return new PreTransformOperator.Builder();
    }

    /** Builder of {@link PreTransformOperator}. */
    public static class Builder {
        private final List<TransformRule> transformRules = new ArrayList<>();

        private final List<Tuple2<String, String>> udfFunctions = new ArrayList<>();

        public PreTransformOperator.Builder addTransform(
                String tableInclusions, @Nullable String projection, @Nullable String filter) {
            transformRules.add(new TransformRule(tableInclusions, projection, filter, "", "", ""));
            return this;
        }

        public PreTransformOperator.Builder addTransform(
                String tableInclusions,
                @Nullable String projection,
                @Nullable String filter,
                String primaryKey,
                String partitionKey,
                String tableOption) {
            transformRules.add(
                    new TransformRule(
                            tableInclusions,
                            projection,
                            filter,
                            primaryKey,
                            partitionKey,
                            tableOption));
            return this;
        }

        public PreTransformOperator.Builder addUdfFunctions(
                List<Tuple2<String, String>> udfFunctions) {
            this.udfFunctions.addAll(udfFunctions);
            return this;
        }

        public PreTransformOperator build() {
            return new PreTransformOperator(transformRules, udfFunctions);
        }
    }

    private PreTransformOperator(
            List<TransformRule> transformRules, List<Tuple2<String, String>> udfFunctions) {
        this.transformRules = transformRules;
        this.preTransformChangeInfoMap = new ConcurrentHashMap<>();
        this.preTransformProcessorMap = new ConcurrentHashMap<>();
        this.schemaMetadataTransformers = new ArrayList<>();
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.udfFunctions = udfFunctions;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        this.udfDescriptors =
                this.udfFunctions.stream()
                        .map(udf -> new UserDefinedFunctionDescriptor(udf.f0, udf.f1))
                        .collect(Collectors.toList());

        // Initialize data fields in advance because they might be accessed in
        // `::initializeState` function when restoring from a previous state.
        this.transforms = new ArrayList<>();
        for (TransformRule transformRule : transformRules) {
            String tableInclusions = transformRule.getTableInclusions();
            String projection = transformRule.getProjection();
            String filter = transformRule.getFilter();
            String primaryKeys = transformRule.getPrimaryKey();
            String partitionKeys = transformRule.getPartitionKey();
            String tableOptions = transformRule.getTableOption();
            Selectors selectors =
                    new Selectors.SelectorsBuilder().includeTables(tableInclusions).build();
            transforms.add(
                    new PreTransformer(
                            selectors,
                            TransformProjection.of(projection).orElse(null),
                            TransformFilter.of(filter, udfDescriptors).orElse(null)));
            schemaMetadataTransformers.add(
                    new Tuple2<>(
                            selectors,
                            new SchemaMetadataTransform(primaryKeys, partitionKeys, tableOptions)));
        }
        this.preTransformProcessorMap = new ConcurrentHashMap<>();
        this.hasAsteriskMap = new ConcurrentHashMap<>();
        this.referencedColumnsMap = new ConcurrentHashMap<>();
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
                PreTransformChangeInfo stateTableChangeInfo =
                        PreTransformChangeInfo.SERIALIZER.deserialize(
                                PreTransformChangeInfo.SERIALIZER.getVersion(),
                                serializedTableInfo);
                preTransformChangeInfoMap.put(
                        stateTableChangeInfo.getTableId(), stateTableChangeInfo);

                CreateTableEvent restoredCreateTableEvent =
                        new CreateTableEvent(
                                stateTableChangeInfo.getTableId(),
                                stateTableChangeInfo.getPreTransformedSchema());
                // hasAsteriskMap and referencedColumnsMap needs to be recalculated after restoring
                // from a checkpoint.
                cacheTransformRuleInfo(restoredCreateTableEvent);

                // Since PostTransformOperator doesn't preserve state, pre-transformed schema
                // information needs to be passed by PreTransformOperator.
                output.collect(new StreamRecord<>(restoredCreateTableEvent));
            }
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        state.update(
                new ArrayList<>(
                        preTransformChangeInfoMap.values().stream()
                                .map(
                                        tableChangeInfo -> {
                                            try {
                                                return PreTransformChangeInfo.SERIALIZER.serialize(
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
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            preTransformProcessorMap.remove(createTableEvent.tableId());
            output.collect(new StreamRecord<>(cacheCreateTable(createTableEvent)));
        } else if (event instanceof DropTableEvent) {
            preTransformProcessorMap.remove(((DropTableEvent) event).tableId());
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof TruncateTableEvent) {
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            preTransformProcessorMap.remove(schemaChangeEvent.tableId());
            cacheChangeSchema(schemaChangeEvent)
                    .ifPresent(e -> output.collect(new StreamRecord<>(e)));
        } else if (event instanceof DataChangeEvent) {
            output.collect(new StreamRecord<>(processDataChangeEvent(((DataChangeEvent) event))));
        }
    }

    private SchemaChangeEvent cacheCreateTable(CreateTableEvent event) {
        TableId tableId = event.tableId();
        Schema originalSchema = event.getSchema();
        event = transformCreateTableEvent(event);
        Schema newSchema = (event).getSchema();
        preTransformChangeInfoMap.put(
                tableId, PreTransformChangeInfo.of(tableId, originalSchema, newSchema));
        return event;
    }

    private Optional<SchemaChangeEvent> cacheChangeSchema(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        PreTransformChangeInfo tableChangeInfo = preTransformChangeInfoMap.get(tableId);
        Schema originalSchema =
                SchemaUtils.applySchemaChangeEvent(tableChangeInfo.getSourceSchema(), event);
        Schema preTransformedSchema = tableChangeInfo.getPreTransformedSchema();
        Optional<SchemaChangeEvent> schemaChangeEvent =
                SchemaUtils.transformSchemaChangeEvent(
                        hasAsteriskMap.get(tableId), referencedColumnsMap.get(tableId), event);
        if (schemaChangeEvent.isPresent()) {
            preTransformedSchema =
                    SchemaUtils.applySchemaChangeEvent(
                            tableChangeInfo.getPreTransformedSchema(), schemaChangeEvent.get());
        }
        preTransformChangeInfoMap.put(
                tableId, PreTransformChangeInfo.of(tableId, originalSchema, preTransformedSchema));
        return schemaChangeEvent;
    }

    private void cacheTransformRuleInfo(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();
        Set<String> referencedColumnsSet =
                transforms.stream()
                        .filter(t -> t.getSelectors().isMatch(tableId))
                        .flatMap(
                                rule ->
                                        TransformParser.generateReferencedColumns(
                                                rule.getProjection()
                                                        .map(TransformProjection::getProjection)
                                                        .orElse(null),
                                                rule.getFilter()
                                                        .map(TransformFilter::getExpression)
                                                        .orElse(null),
                                                createTableEvent.getSchema().getColumns())
                                                .stream())
                        .map(Column::getName)
                        .collect(Collectors.toSet());

        boolean hasAsterisk =
                transforms.stream()
                        .filter(t -> t.getSelectors().isMatch(tableId))
                        .anyMatch(
                                t ->
                                        TransformParser.hasAsterisk(
                                                t.getProjection()
                                                        .map(TransformProjection::getProjection)
                                                        .orElse(null)));

        hasAsteriskMap.put(createTableEvent.tableId(), hasAsterisk);
        referencedColumnsMap.put(
                createTableEvent.tableId(),
                createTableEvent.getSchema().getColumnNames().stream()
                        .filter(referencedColumnsSet::contains)
                        .collect(Collectors.toList()));
    }

    private CreateTableEvent transformCreateTableEvent(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();
        PreTransformChangeInfo tableChangeInfo = preTransformChangeInfoMap.get(tableId);
        cacheTransformRuleInfo(createTableEvent);
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

        for (PreTransformer transform : transforms) {
            Selectors selectors = transform.getSelectors();
            if (selectors.isMatch(tableId) && transform.getProjection().isPresent()) {
                TransformProjection transformProjection = transform.getProjection().get();
                TransformFilter transformFilter = transform.getFilter().orElse(null);
                if (transformProjection.isValid()) {
                    if (!preTransformProcessorMap.containsKey(tableId)) {
                        preTransformProcessorMap.put(
                                tableId,
                                new PreTransformProcessor(
                                        tableChangeInfo, transformProjection, transformFilter));
                    }
                    PreTransformProcessor preTransformProcessor =
                            preTransformProcessorMap.get(tableId);
                    // TODO: Currently this code wrongly filters out rows that weren't referenced in
                    // the first matching transform rule but in the following transform rules.
                    return preTransformProcessor.preTransformCreateTableEvent(createTableEvent);
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
        for (PreTransformer transform : transforms) {
            Selectors selectors = transform.getSelectors();

            if (selectors.isMatch(tableId) && transform.getProjection().isPresent()) {
                TransformProjection transformProjection = transform.getProjection().get();
                TransformFilter transformFilter = transform.getFilter().orElse(null);
                if (transformProjection.isValid()) {
                    return processProjection(transformProjection, transformFilter, dataChangeEvent);
                }
            }
        }
        return dataChangeEvent;
    }

    private DataChangeEvent processProjection(
            TransformProjection transformProjection,
            @Nullable TransformFilter transformFilter,
            DataChangeEvent dataChangeEvent) {
        TableId tableId = dataChangeEvent.tableId();
        PreTransformChangeInfo tableChangeInfo = preTransformChangeInfoMap.get(tableId);
        if (!preTransformProcessorMap.containsKey(tableId)
                || !preTransformProcessorMap.get(tableId).hasTableChangeInfo()) {
            preTransformProcessorMap.put(
                    tableId,
                    new PreTransformProcessor(
                            tableChangeInfo, transformProjection, transformFilter));
        }
        PreTransformProcessor preTransformProcessor = preTransformProcessorMap.get(tableId);
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if (before != null) {
            BinaryRecordData projectedBefore = preTransformProcessor.processFillDataField(before);
            dataChangeEvent = DataChangeEvent.projectBefore(dataChangeEvent, projectedBefore);
        }
        if (after != null) {
            BinaryRecordData projectedAfter = preTransformProcessor.processFillDataField(after);
            dataChangeEvent = DataChangeEvent.projectAfter(dataChangeEvent, projectedAfter);
        }
        return dataChangeEvent;
    }

    private void clearOperator() {
        this.transforms = null;
        this.preTransformProcessorMap = null;
    }
}
