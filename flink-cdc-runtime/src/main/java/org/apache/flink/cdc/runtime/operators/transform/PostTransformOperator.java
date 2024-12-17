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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.transform.converter.PostTransformConverter;
import org.apache.flink.cdc.runtime.operators.transform.converter.PostTransformConverters;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A data process function that performs column filtering, calculated column evaluation & final
 * projection.
 */
public class PostTransformOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String timezone;
    private final List<TransformRule> transformRules;
    private transient List<PostTransformer> transforms;

    /** keep the relationship of TableId and table information. */
    private final Map<TableId, PostTransformChangeInfo> postTransformChangeInfoMap;

    private final List<Tuple3<String, String, Map<String, String>>> udfFunctions;
    private List<UserDefinedFunctionDescriptor> udfDescriptors;
    private transient Map<String, Object> udfFunctionInstances;

    private transient Map<Tuple2<TableId, TransformProjection>, TransformProjectionProcessor>
            transformProjectionProcessorMap;
    private transient Map<Tuple2<TableId, TransformFilter>, TransformFilterProcessor>
            transformFilterProcessorMap;
    private final Map<TableId, Boolean> hasAsteriskMap;
    private final Map<TableId, List<String>> projectedColumnsMap;

    public static PostTransformOperator.Builder newBuilder() {
        return new PostTransformOperator.Builder();
    }

    /** Builder of {@link PostTransformOperator}. */
    public static class Builder {
        private final List<TransformRule> transformRules = new ArrayList<>();
        private String timezone;
        private final List<Tuple3<String, String, Map<String, String>>> udfFunctions =
                new ArrayList<>();

        public PostTransformOperator.Builder addTransform(
                String tableInclusions,
                @Nullable String projection,
                @Nullable String filter,
                String primaryKey,
                String partitionKey,
                String tableOptions,
                String postTransformConverter,
                SupportedMetadataColumn[] supportedMetadataColumns) {
            transformRules.add(
                    new TransformRule(
                            tableInclusions,
                            projection,
                            filter,
                            primaryKey,
                            partitionKey,
                            tableOptions,
                            postTransformConverter,
                            supportedMetadataColumns));
            return this;
        }

        public PostTransformOperator.Builder addTransform(
                String tableInclusions, @Nullable String projection, @Nullable String filter) {
            transformRules.add(
                    new TransformRule(
                            tableInclusions,
                            projection,
                            filter,
                            "",
                            "",
                            "",
                            null,
                            new SupportedMetadataColumn[0]));
            return this;
        }

        public PostTransformOperator.Builder addTimezone(String timezone) {
            if (PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(timezone)) {
                this.timezone = ZoneId.systemDefault().toString();
            } else {
                this.timezone = timezone;
            }
            return this;
        }

        public PostTransformOperator.Builder addUdfFunctions(
                List<Tuple3<String, String, Map<String, String>>> udfFunctions) {
            this.udfFunctions.addAll(udfFunctions);
            return this;
        }

        public PostTransformOperator build() {
            return new PostTransformOperator(transformRules, timezone, udfFunctions);
        }
    }

    private PostTransformOperator(
            List<TransformRule> transformRules,
            String timezone,
            List<Tuple3<String, String, Map<String, String>>> udfFunctions) {
        this.transformRules = transformRules;
        this.timezone = timezone;
        this.postTransformChangeInfoMap = new ConcurrentHashMap<>();
        this.transformFilterProcessorMap = new ConcurrentHashMap<>();
        this.transformProjectionProcessorMap = new ConcurrentHashMap<>();
        this.udfFunctions = udfFunctions;
        this.udfFunctionInstances = new ConcurrentHashMap<>();
        this.hasAsteriskMap = new HashMap<>();
        this.projectedColumnsMap = new HashMap<>();
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        udfDescriptors =
                udfFunctions.stream()
                        .map(udf -> new UserDefinedFunctionDescriptor(udf.f0, udf.f1, udf.f2))
                        .collect(Collectors.toList());
    }

    @Override
    public void open() throws Exception {
        super.open();
        transforms =
                transformRules.stream()
                        .map(
                                transformRule -> {
                                    String tableInclusions = transformRule.getTableInclusions();
                                    String projection = transformRule.getProjection();
                                    String filterExpression = transformRule.getFilter();

                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new PostTransformer(
                                            selectors,
                                            TransformProjection.of(projection).orElse(null),
                                            TransformFilter.of(filterExpression, udfDescriptors)
                                                    .orElse(null),
                                            PostTransformConverters.of(
                                                    transformRule.getPostTransformConverter()),
                                            transformRule.getSupportedMetadataColumns());
                                })
                        .collect(Collectors.toList());
        this.transformProjectionProcessorMap = new ConcurrentHashMap<>();
        this.transformFilterProcessorMap = new ConcurrentHashMap<>();
        this.udfFunctionInstances = new ConcurrentHashMap<>();
        udfDescriptors.forEach(
                udf -> {
                    try {
                        Class<?> clazz = Class.forName(udf.getClasspath());
                        udfFunctionInstances.put(udf.getName(), clazz.newInstance());
                    } catch (ClassNotFoundException
                            | InstantiationException
                            | IllegalAccessException e) {
                        throw new RuntimeException("Failed to instantiate UDF function " + udf);
                    }
                });
        initializeUdf();
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

        // Clean up UDF instances
        destroyUdf();
        udfFunctionInstances.clear();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            transformProjectionProcessorMap
                    .keySet()
                    .removeIf(e -> Objects.equals(e.f0, schemaChangeEvent.tableId()));
            transformFilterProcessorMap
                    .keySet()
                    .removeIf(e -> Objects.equals(e.f0, schemaChangeEvent.tableId()));
            cacheSchema(schemaChangeEvent).ifPresent(e -> output.collect(new StreamRecord<>(e)));
        } else if (event instanceof DataChangeEvent) {
            Optional<DataChangeEvent> dataChangeEventOptional =
                    processDataChangeEvent(((DataChangeEvent) event));
            if (dataChangeEventOptional.isPresent()) {
                output.collect(new StreamRecord<>(dataChangeEventOptional.get()));
            }
        }
    }

    private Optional<SchemaChangeEvent> cacheSchema(SchemaChangeEvent event) throws Exception {
        TableId tableId = event.tableId();
        List<String> columnNamesBeforeChange = Collections.emptyList();

        if (event instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            Set<String> projectedColumnsSet =
                    transforms.stream()
                            .filter(t -> t.getSelectors().isMatch(tableId))
                            .flatMap(
                                    rule ->
                                            TransformParser.generateProjectionColumns(
                                                    rule.getProjection()
                                                            .map(TransformProjection::getProjection)
                                                            .orElse(null),
                                                    createTableEvent.getSchema().getColumns(),
                                                    udfDescriptors,
                                                    rule.getSupportedMetadataColumns())
                                                    .stream())
                            .map(ProjectionColumn::getColumnName)
                            .collect(Collectors.toSet());

            boolean notTransformed =
                    transforms.stream().noneMatch(t -> t.getSelectors().isMatch(tableId));

            if (notTransformed) {
                // If this TableId isn't presented in any transform block, it should behave like a
                // "*" projection and should be regarded as asterisk-ful.
                hasAsteriskMap.put(tableId, true);
            } else {
                boolean hasAsterisk =
                        transforms.stream()
                                .filter(t -> t.getSelectors().isMatch(tableId))
                                .anyMatch(
                                        t ->
                                                TransformParser.hasAsterisk(
                                                        t.getProjection()
                                                                .map(
                                                                        TransformProjection
                                                                                ::getProjection)
                                                                .orElse(null)));

                hasAsteriskMap.put(tableId, hasAsterisk);
            }
            projectedColumnsMap.put(
                    tableId,
                    createTableEvent.getSchema().getColumnNames().stream()
                            .filter(projectedColumnsSet::contains)
                            .collect(Collectors.toList()));
        } else {
            columnNamesBeforeChange =
                    getPostTransformChangeInfo(tableId).getPreTransformedSchema().getColumnNames();
        }

        Schema schema;
        if (event instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            schema = createTableEvent.getSchema();
        } else {
            schema =
                    SchemaUtils.applySchemaChangeEvent(
                            getPostTransformChangeInfo(tableId).getPreTransformedSchema(), event);
        }

        Schema projectedSchema = transformSchema(tableId, schema);
        postTransformChangeInfoMap.put(
                tableId, PostTransformChangeInfo.of(tableId, projectedSchema, schema));

        if (event instanceof CreateTableEvent) {
            return Optional.of(new CreateTableEvent(tableId, projectedSchema));
        } else if (hasAsteriskMap.getOrDefault(tableId, true)) {
            // See comments in PreTransformOperator#cacheChangeSchema method.
            return SchemaUtils.transformSchemaChangeEvent(true, columnNamesBeforeChange, event);
        } else {
            return SchemaUtils.transformSchemaChangeEvent(
                    false, projectedColumnsMap.get(tableId), event);
        }
    }

    private PostTransformChangeInfo getPostTransformChangeInfo(TableId tableId) {
        PostTransformChangeInfo tableInfo = postTransformChangeInfoMap.get(tableId);
        if (tableInfo == null) {
            throw new RuntimeException(
                    "Schema for " + tableId + " not found. This shouldn't happen.");
        }
        return tableInfo;
    }

    private Schema transformSchema(TableId tableId, Schema schema) {
        List<Schema> newSchemas = new ArrayList<>();
        for (PostTransformer transform : transforms) {
            Selectors selectors = transform.getSelectors();
            if (selectors.isMatch(tableId) && transform.getProjection().isPresent()) {
                TransformProjection transformProjection = transform.getProjection().get();
                if (transformProjection.isValid()) {
                    if (!transformProjectionProcessorMap.containsKey(
                            Tuple2.of(tableId, transformProjection))) {
                        transformProjectionProcessorMap.put(
                                Tuple2.of(tableId, transformProjection),
                                TransformProjectionProcessor.of(
                                        transformProjection,
                                        timezone,
                                        udfDescriptors,
                                        getUdfFunctionInstances(),
                                        transform.getSupportedMetadataColumns()));
                    }
                    TransformProjectionProcessor postTransformProcessor =
                            transformProjectionProcessorMap.get(
                                    Tuple2.of(tableId, transformProjection));
                    // update the columns of projection and add the column of projection into Schema
                    newSchemas.add(
                            postTransformProcessor.processSchemaChangeEvent(
                                    schema, transform.getSupportedMetadataColumns()));
                }
            }
        }
        if (newSchemas.isEmpty()) {
            return schema;
        }

        return SchemaMergingUtils.getCommonSchema(newSchemas);
    }

    private List<Object> getUdfFunctionInstances() {
        return udfDescriptors.stream()
                .map(e -> udfFunctionInstances.get(e.getName()))
                .collect(Collectors.toList());
    }

    private Optional<DataChangeEvent> processDataChangeEvent(DataChangeEvent dataChangeEvent)
            throws Exception {
        TableId tableId = dataChangeEvent.tableId();
        PostTransformChangeInfo tableInfo = getPostTransformChangeInfo(tableId);
        List<Optional<DataChangeEvent>> transformedDataChangeEventOptionalList = new ArrayList<>();
        long epochTime = System.currentTimeMillis();
        for (PostTransformer transform : transforms) {
            Selectors selectors = transform.getSelectors();

            if (selectors.isMatch(tableId)) {
                Optional<DataChangeEvent> dataChangeEventOptional = Optional.of(dataChangeEvent);
                Optional<TransformProjection> transformProjectionOptional =
                        transform.getProjection();
                Optional<TransformFilter> transformFilterOptional = transform.getFilter();

                if (transformFilterOptional.isPresent()
                        && transformFilterOptional.get().isVaild()) {
                    TransformFilter transformFilter = transformFilterOptional.get();
                    if (!transformFilterProcessorMap.containsKey(
                            Tuple2.of(tableId, transformFilter))) {
                        transformFilterProcessorMap.put(
                                Tuple2.of(tableId, transformFilter),
                                TransformFilterProcessor.of(
                                        tableInfo,
                                        transformFilter,
                                        timezone,
                                        udfDescriptors,
                                        getUdfFunctionInstances(),
                                        transform.getSupportedMetadataColumns()));
                    }
                    TransformFilterProcessor transformFilterProcessor =
                            transformFilterProcessorMap.get(Tuple2.of(tableId, transformFilter));
                    dataChangeEventOptional =
                            processFilter(
                                    transformFilterProcessor,
                                    dataChangeEventOptional.get(),
                                    epochTime);
                }
                if (dataChangeEventOptional.isPresent()
                        && transformProjectionOptional.isPresent()
                        && transformProjectionOptional.get().isValid()) {
                    TransformProjection transformProjection = transformProjectionOptional.get();
                    if (!transformProjectionProcessorMap.containsKey(
                                    Tuple2.of(tableId, transformProjection))
                            || !transformProjectionProcessorMap
                                    .get(Tuple2.of(tableId, transformProjection))
                                    .hasTableInfo()) {
                        transformProjectionProcessorMap.put(
                                Tuple2.of(tableId, transformProjection),
                                TransformProjectionProcessor.of(
                                        tableInfo,
                                        transformProjection,
                                        timezone,
                                        udfDescriptors,
                                        getUdfFunctionInstances(),
                                        transform.getSupportedMetadataColumns()));
                    }
                    TransformProjectionProcessor postTransformProcessor =
                            transformProjectionProcessorMap.get(
                                    Tuple2.of(tableId, transformProjection));
                    dataChangeEventOptional =
                            processProjection(
                                    postTransformProcessor,
                                    dataChangeEventOptional.get(),
                                    epochTime);
                }
                if (dataChangeEventOptional.isPresent()
                        && transform.getPostTransformConverter().isPresent()) {
                    dataChangeEventOptional =
                            convertDataChangeEvent(
                                    dataChangeEventOptional.get(),
                                    transform.getPostTransformConverter().get());
                }
                transformedDataChangeEventOptionalList.add(dataChangeEventOptional);
            }
        }

        if (transformedDataChangeEventOptionalList.isEmpty()) {
            return processPostProjection(tableInfo, dataChangeEvent);
        } else {
            for (Optional<DataChangeEvent> dataChangeEventOptional :
                    transformedDataChangeEventOptionalList) {
                if (dataChangeEventOptional.isPresent()) {
                    return processPostProjection(tableInfo, dataChangeEventOptional.get());
                }
            }
            return Optional.empty();
        }
    }

    private Optional<DataChangeEvent> convertDataChangeEvent(
            DataChangeEvent dataChangeEvent, PostTransformConverter postTransformConverter) {
        return postTransformConverter.convert(dataChangeEvent);
    }

    private Optional<DataChangeEvent> processFilter(
            TransformFilterProcessor transformFilterProcessor,
            DataChangeEvent dataChangeEvent,
            long epochTime)
            throws Exception {
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        Map<String, String> meta = dataChangeEvent.meta();
        // insert and update event only process afterData, delete only process beforeData
        if (after != null) {
            if (transformFilterProcessor.process(
                    after, epochTime, opTypeToRowKind(dataChangeEvent.op(), '+'), meta)) {
                return Optional.of(dataChangeEvent);
            } else {
                return Optional.empty();
            }
        } else if (before != null) {
            if (transformFilterProcessor.process(
                    before, epochTime, opTypeToRowKind(dataChangeEvent.op(), '-'), meta)) {
                return Optional.of(dataChangeEvent);
            } else {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private Optional<DataChangeEvent> processProjection(
            TransformProjectionProcessor postTransformProcessor,
            DataChangeEvent dataChangeEvent,
            long epochTime) {
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if (before != null) {
            BinaryRecordData projectedBefore =
                    postTransformProcessor.processData(
                            before,
                            epochTime,
                            opTypeToRowKind(dataChangeEvent.op(), '-'),
                            dataChangeEvent.meta());
            dataChangeEvent = DataChangeEvent.projectBefore(dataChangeEvent, projectedBefore);
        }
        if (after != null) {
            BinaryRecordData projectedAfter =
                    postTransformProcessor.processData(
                            after,
                            epochTime,
                            opTypeToRowKind(dataChangeEvent.op(), '+'),
                            dataChangeEvent.meta());
            dataChangeEvent = DataChangeEvent.projectAfter(dataChangeEvent, projectedAfter);
        }
        return Optional.of(dataChangeEvent);
    }

    private Optional<DataChangeEvent> processPostProjection(
            PostTransformChangeInfo tableInfo, DataChangeEvent dataChangeEvent) throws Exception {
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if (before != null) {
            BinaryRecordData projectedBefore = projectRecord(tableInfo, before);
            dataChangeEvent = DataChangeEvent.projectBefore(dataChangeEvent, projectedBefore);
        }
        if (after != null) {
            BinaryRecordData projectedAfter = projectRecord(tableInfo, after);
            dataChangeEvent = DataChangeEvent.projectAfter(dataChangeEvent, projectedAfter);
        }
        return Optional.of(dataChangeEvent);
    }

    private BinaryRecordData projectRecord(
            PostTransformChangeInfo tableInfo, BinaryRecordData recordData) {
        List<Object> valueList = new ArrayList<>();
        RecordData.FieldGetter[] fieldGetters = tableInfo.getPostTransformedFieldGetters();

        for (RecordData.FieldGetter fieldGetter : fieldGetters) {
            valueList.add(fieldGetter.getFieldOrNull(recordData));
        }

        return tableInfo
                .getRecordDataGenerator()
                .generate(valueList.toArray(new Object[valueList.size()]));
    }

    private void clearOperator() {
        this.transforms = null;
        this.transformProjectionProcessorMap = null;
        this.transformFilterProcessorMap = null;
        TransformExpressionCompiler.cleanUp();
    }

    private void initializeUdf() {
        udfDescriptors.forEach(
                udf -> {
                    try {
                        if (udf.isCdcPipelineUdf()) {
                            // We use reflection to invoke UDF methods since we may add more methods
                            // into UserDefinedFunction interface, thus the provided UDF classes
                            // might not be compatible with the interface definition in CDC common.
                            Object udfInstance = udfFunctionInstances.get(udf.getName());
                            UserDefinedFunctionContext userDefinedFunctionContext =
                                    () -> Configuration.fromMap(udf.getParameters());
                            udfInstance
                                    .getClass()
                                    .getMethod("open", UserDefinedFunctionContext.class)
                                    .invoke(udfInstance, userDefinedFunctionContext);
                        } else {
                            // Do nothing, Flink-style UDF lifecycle hooks are not supported
                        }
                    } catch (InvocationTargetException
                            | NoSuchMethodException
                            | IllegalAccessException ex) {
                        throw new RuntimeException("Failed to initialize UDF " + udf, ex);
                    }
                });
    }

    private void destroyUdf() {
        udfDescriptors.forEach(
                udf -> {
                    try {
                        if (udf.isCdcPipelineUdf()) {
                            // We use reflection to invoke UDF methods since we may add more methods
                            // into UserDefinedFunction interface, thus the provided UDF classes
                            // might not be compatible with the interface definition in CDC common.
                            Object udfInstance = udfFunctionInstances.get(udf.getName());
                            udfInstance.getClass().getMethod("close").invoke(udfInstance);
                        } else {
                            // Do nothing, Flink-style UDF lifecycle hooks are not supported
                        }
                    } catch (InvocationTargetException
                            | NoSuchMethodException
                            | IllegalAccessException ex) {
                        throw new RuntimeException("Failed to destroy UDF " + udf, ex);
                    }
                });
    }

    private String opTypeToRowKind(OperationType opType, char beforeOrAfter) {
        return String.format("%c%c", beforeOrAfter, opType.name().charAt(0));
    }
}
