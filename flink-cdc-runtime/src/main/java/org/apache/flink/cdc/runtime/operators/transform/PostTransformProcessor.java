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
import org.apache.flink.cdc.common.converter.JavaObjectConverter;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.pipeline.DecimalPrecisionMode;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.transform.converter.PostTransformConverters;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.event.CreateTableEventSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.cdc.runtime.typeutils.BinaryInternalObjectConverter;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava31.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/** Shared processor for synchronous and asynchronous post-transform execution. */
class PostTransformProcessor implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostTransformProcessor.class);
    private static final int TABLE_STATE_VERSION = 2;

    private final String timezone;
    private final DecimalPrecisionMode decimalPrecisionMode;
    private final List<TransformRule> transformRules;
    private final Map<TableId, PostTransformTableInfo> tableInfoMap;

    // Tuple3 items are: function name, class path, and extra options.
    private final List<Tuple3<String, String, Map<String, String>>> udfFunctions;

    // Serializable AI model clients keyed by model name, e.g. myModel.
    private final Map<String, AiModelClient> modelClients;

    private transient List<PostTransformer> transformers;
    private transient List<UserDefinedFunctionDescriptor> udfDescriptors;
    private transient List<Object> udfFunctionInstances;
    private transient ThreadLocal<Table<TableId, PostTransformer, TransformProjectionProcessor>>
            projectionProcessors;
    private transient ThreadLocal<Table<TableId, PostTransformer, TransformFilterProcessor>>
            filterProcessors;
    private transient Queue<Table<TableId, PostTransformer, TransformProjectionProcessor>>
            projectionProcessorCaches;
    private transient Queue<Table<TableId, PostTransformer, TransformFilterProcessor>>
            filterProcessorCaches;
    private transient LoadingCache<TableId, Optional<PostTransformer>> transformersCache;

    PostTransformProcessor(
            List<TransformRule> transformRules,
            String timezone,
            DecimalPrecisionMode decimalPrecisionMode,
            List<Tuple3<String, String, Map<String, String>>> udfFunctions,
            Map<String, AiModelClient> modelClients) {
        this.timezone = timezone;
        this.decimalPrecisionMode = decimalPrecisionMode;
        this.transformRules = transformRules;
        this.tableInfoMap = new ConcurrentHashMap<>();
        this.udfFunctions = udfFunctions;
        this.modelClients = modelClients;
    }

    void open() {
        this.projectionProcessorCaches = new ConcurrentLinkedQueue<>();
        this.filterProcessorCaches = new ConcurrentLinkedQueue<>();
        this.projectionProcessors =
                ThreadLocal.withInitial(
                        () -> {
                            Table<TableId, PostTransformer, TransformProjectionProcessor>
                                    processors = HashBasedTable.create();
                            projectionProcessorCaches.add(processors);
                            return processors;
                        });
        this.filterProcessors =
                ThreadLocal.withInitial(
                        () -> {
                            Table<TableId, PostTransformer, TransformFilterProcessor> processors =
                                    HashBasedTable.create();
                            filterProcessorCaches.add(processors);
                            return processors;
                        });

        initializeAiModelClients();
        initializeUdf();

        this.transformers = createTransformers();
        this.transformersCache =
                CacheBuilder.newBuilder()
                        .maximumSize(1024)
                        .build(
                                new CacheLoader<>() {
                                    @Override
                                    public Optional<PostTransformer> load(TableId tableId) {
                                        return getEffectiveTransformer(tableId);
                                    }
                                });
    }

    void close() {
        TransformExpressionCompiler.cleanUp();
        destroyUdf();
        destroyAiModelClients();
        if (transformersCache != null) {
            transformersCache.invalidateAll();
        }
    }

    Optional<Event> process(Event event) {
        if (event == null) {
            return Optional.empty();
        }

        if (!(event instanceof ChangeEvent)) {
            throw new UnsupportedOperationException("Unexpected stream record event: " + event);
        }

        ChangeEvent changeEvent = (ChangeEvent) event;
        TableId tableId = changeEvent.tableId();
        Optional<PostTransformer> transformer = transformersCache.getUnchecked(tableId);

        if (transformer.isEmpty()) {
            cachePassthroughSchemaEvent(event);
            return Optional.of(event);
        }

        if (event instanceof CreateTableEvent) {
            Optional<Event> result =
                    processCreateTableEvent((CreateTableEvent) event, transformer.get());
            invalidateCache(tableId);
            return result;
        } else if (event instanceof SchemaChangeEvent) {
            Optional<Event> result =
                    processSchemaChangeEvent((SchemaChangeEvent) event, transformer.get());
            invalidateCache(tableId);
            return result;
        } else if (event instanceof DataChangeEvent) {
            return processDataChangeEvent((DataChangeEvent) event, transformer.get());
        } else {
            throw new UnsupportedOperationException("Unexpected stream record event: " + event);
        }
    }

    TransformException wrapTransformException(String command, Event event, Throwable throwable) {
        Throwable cause = throwable;
        if (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof TransformException) {
            return (TransformException) cause;
        }

        TableId tableId = null;
        Schema schemaBefore = null;
        Schema schemaAfter = null;
        if (event instanceof ChangeEvent) {
            tableId = ((ChangeEvent) event).tableId();
            PostTransformTableInfo tableInfo = tableInfoMap.get(tableId);
            if (tableInfo != null) {
                schemaBefore = tableInfo.changeInfo.getPreTransformedSchema();
                schemaAfter = tableInfo.changeInfo.getPostTransformedSchema();
            }
        }
        return new TransformException(command, event, tableId, schemaBefore, schemaAfter, cause);
    }

    @Nullable
    CreateTableEvent getOutputCreateTableEvent(TableId tableId) {
        PostTransformTableInfo tableInfo = tableInfoMap.get(tableId);
        return tableInfo == null ? null : tableInfo.outputCreateTableEvent;
    }

    List<byte[]> serializeTableStates() throws IOException {
        List<byte[]> result = new ArrayList<>(tableInfoMap.size());
        for (PostTransformTableInfo tableInfo : tableInfoMap.values()) {
            result.add(serializeTableState(tableInfo));
        }
        return result;
    }

    void restoreTableState(byte[] serializedTableState) throws IOException {
        TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
        SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
        CreateTableEventSerializer createTableEventSerializer = CreateTableEventSerializer.INSTANCE;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedTableState);
                DataInputStream in = new DataInputStream(bais)) {
            int version = in.readInt();
            if (version != TABLE_STATE_VERSION) {
                throw new IOException(
                        "Unrecognized async post-transform table state version " + version);
            }
            TableId tableId = tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
            Schema preTransformedSchema =
                    schemaSerializer.deserialize(new DataInputViewStreamWrapper(in));
            Schema postTransformedSchema =
                    schemaSerializer.deserialize(new DataInputViewStreamWrapper(in));
            CreateTableEvent outputCreateTableEvent = null;
            if (in.readBoolean()) {
                outputCreateTableEvent =
                        createTableEventSerializer.deserialize(new DataInputViewStreamWrapper(in));
            }
            cacheTableState(
                    tableId, preTransformedSchema, postTransformedSchema, outputCreateTableEvent);
        }
    }

    private byte[] serializeTableState(PostTransformTableInfo tableInfo) throws IOException {
        TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
        SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
        CreateTableEventSerializer createTableEventSerializer = CreateTableEventSerializer.INSTANCE;
        PostTransformChangeInfo changeInfo = tableInfo.changeInfo;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(TABLE_STATE_VERSION);
            tableIdSerializer.serialize(
                    changeInfo.getTableId(), new DataOutputViewStreamWrapper(out));
            schemaSerializer.serialize(
                    changeInfo.getPreTransformedSchema(), new DataOutputViewStreamWrapper(out));
            schemaSerializer.serialize(
                    changeInfo.getPostTransformedSchema(), new DataOutputViewStreamWrapper(out));
            out.writeBoolean(tableInfo.outputCreateTableEvent != null);
            if (tableInfo.outputCreateTableEvent != null) {
                createTableEventSerializer.serialize(
                        tableInfo.outputCreateTableEvent, new DataOutputViewStreamWrapper(out));
            }
            return baos.toByteArray();
        }
    }

    private void cachePassthroughSchemaEvent(Event event) {
        if (event instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            cacheTableState(
                    createTableEvent.tableId(),
                    createTableEvent.getSchema(),
                    createTableEvent.getSchema(),
                    createTableEvent);
        } else if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            PostTransformTableInfo tableInfo = tableInfoMap.get(schemaChangeEvent.tableId());
            if (tableInfo != null) {
                Schema nextSchema =
                        SchemaUtils.applySchemaChangeEvent(
                                tableInfo.changeInfo.getPreTransformedSchema(), schemaChangeEvent);
                CreateTableEvent nextOutputCreateTableEvent =
                        applySchemaChangeEventToOutputCreateTableEvent(
                                tableInfo.outputCreateTableEvent, schemaChangeEvent);
                cacheTableState(
                        schemaChangeEvent.tableId(),
                        nextSchema,
                        nextSchema,
                        tableInfo.hasAsterisk,
                        nextOutputCreateTableEvent);
            }
        }
    }

    private Optional<Event> processCreateTableEvent(
            CreateTableEvent event, PostTransformer effectiveTransformer) {
        TableId tableId = event.tableId();
        Schema preSchema = event.getSchema();
        Schema postSchema =
                SchemaUtils.ensurePkNonNull(transformSchema(preSchema, effectiveTransformer));
        CreateTableEvent outputCreateTableEvent = new CreateTableEvent(tableId, postSchema);

        cacheTableState(
                tableId,
                preSchema,
                postSchema,
                hasAsterisk(effectiveTransformer),
                outputCreateTableEvent);
        return Optional.of(outputCreateTableEvent);
    }

    private Optional<Event> processSchemaChangeEvent(
            SchemaChangeEvent event, PostTransformer effectiveTransformer) {
        TableId tableId = event.tableId();
        PostTransformTableInfo tableInfo = checkNotNull(tableInfoMap.get(tableId));
        PostTransformChangeInfo info = tableInfo.changeInfo;

        Schema prevPreSchema = info.getPreTransformedSchema();
        Schema nextPreSchema = SchemaUtils.applySchemaChangeEvent(prevPreSchema, event);
        Schema nextPostSchema =
                SchemaUtils.ensurePkNonNull(transformSchema(nextPreSchema, effectiveTransformer));

        Schema prevPostSchema = info.getPostTransformedSchema();
        List<String> columnNamesBeforeChange = prevPostSchema.getColumnNames();
        Optional<SchemaChangeEvent> outputEvent;
        if (tableInfo.hasAsterisk) {
            // See comments in PreTransformOperator#cacheChangeSchema method.
            outputEvent =
                    SchemaUtils.transformSchemaChangeEvent(true, columnNamesBeforeChange, event);
        } else {
            outputEvent =
                    SchemaUtils.transformSchemaChangeEvent(
                            false, tableInfo.projectedColumns, event);
        }

        CreateTableEvent nextOutputCreateTableEvent =
                outputEvent
                        .map(
                                transformedEvent ->
                                        applySchemaChangeEventToOutputCreateTableEvent(
                                                tableInfo.outputCreateTableEvent, transformedEvent))
                        .orElse(tableInfo.outputCreateTableEvent);
        cacheTableState(
                tableId,
                nextPreSchema,
                nextPostSchema,
                tableInfo.hasAsterisk,
                nextOutputCreateTableEvent);
        return outputEvent.map(Event.class::cast);
    }

    @Nullable
    private CreateTableEvent applySchemaChangeEventToOutputCreateTableEvent(
            @Nullable CreateTableEvent outputCreateTableEvent, SchemaChangeEvent event) {
        if (outputCreateTableEvent == null) {
            return null;
        }
        Schema schema =
                SchemaUtils.applySchemaChangeEvent(outputCreateTableEvent.getSchema(), event);
        return new CreateTableEvent(event.tableId(), schema);
    }

    private Optional<Event> processDataChangeEvent(
            DataChangeEvent event, PostTransformer effectiveTransformer) {
        TableId tableId = event.tableId();
        PostTransformChangeInfo info = checkNotNull(tableInfoMap.get(tableId)).changeInfo;

        TransformContext context = new TransformContext();
        context.epochTime = System.currentTimeMillis();
        context.meta = event.meta();

        String beforeOp = event.opTypeString(false);
        String afterOp = event.opTypeString(true);
        TransformProjectionProcessor projectionProcessor =
                getProjectionProcessor(tableId, effectiveTransformer);
        TransformFilterProcessor filterProcessor =
                getFilterProcessor(tableId, effectiveTransformer);

        BinaryRecordData beforeRow = null;
        BinaryRecordData afterRow = null;
        boolean beforeFilterPassed = false;
        boolean afterFilterPassed = false;

        if (event.before() != null) {
            context.opType = beforeOp;
            Tuple2<BinaryRecordData, Boolean> result =
                    transformRecord(
                            event.before(), info, projectionProcessor, filterProcessor, context);
            beforeRow = result.f0;
            beforeFilterPassed = result.f1;
        }
        if (event.after() != null) {
            context.opType = afterOp;
            Tuple2<BinaryRecordData, Boolean> result =
                    transformRecord(
                            event.after(), info, projectionProcessor, filterProcessor, context);
            afterRow = result.f0;
            afterFilterPassed = result.f1;
        }

        DataChangeEvent finalEvent;
        switch (event.op()) {
            case INSERT:
            case REPLACE:
                if (!afterFilterPassed) {
                    return Optional.empty();
                }
                finalEvent = DataChangeEvent.projectRecords(event, beforeRow, afterRow);
                break;
            case DELETE:
                if (!beforeFilterPassed) {
                    return Optional.empty();
                }
                finalEvent = DataChangeEvent.projectRecords(event, beforeRow, afterRow);
                break;
            case UPDATE:
                if (beforeFilterPassed && afterFilterPassed) {
                    finalEvent = DataChangeEvent.projectRecords(event, beforeRow, afterRow);
                } else if (beforeFilterPassed) {
                    finalEvent = DataChangeEvent.deleteEvent(tableId, beforeRow, event.meta());
                } else if (afterFilterPassed) {
                    finalEvent = DataChangeEvent.insertEvent(tableId, afterRow, event.meta());
                } else {
                    return Optional.empty();
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operation type: " + event.op());
        }

        if (effectiveTransformer.getPostTransformConverter().isPresent()) {
            return effectiveTransformer
                    .getPostTransformConverter()
                    .get()
                    .convert(finalEvent)
                    .map(Event.class::cast);
        }
        return Optional.of(finalEvent);
    }

    private Schema transformSchema(Schema preSchema, PostTransformer transformer) {
        List<ProjectionColumn> projectionColumns =
                TransformParser.generateProjectionColumns(
                        transformer
                                .getProjection()
                                .map(TransformProjection::getProjection)
                                .orElse(null),
                        preSchema.getColumns(),
                        udfDescriptors,
                        transformer.getSupportedMetadataColumns(),
                        decimalPrecisionMode);
        return preSchema.copy(
                projectionColumns.stream()
                        .map(ProjectionColumn::getColumn)
                        .collect(Collectors.toList()));
    }

    private Tuple2<BinaryRecordData, Boolean> transformRecord(
            RecordData recordData,
            PostTransformChangeInfo info,
            @Nullable TransformProjectionProcessor projectionProcessor,
            @Nullable TransformFilterProcessor filterProcessor,
            TransformContext context) {
        RecordData.FieldGetter[] preFieldGetters = info.getPreTransformedFieldGetters();
        Schema preSchema = info.getPreTransformedSchema();
        Schema postSchema = info.getPostTransformedSchema();
        BinaryRecordDataGenerator postGenerator = info.getPostTransformedRecordDataGenerator();

        Object[] preRow = new Object[preFieldGetters.length];
        for (int i = 0; i < preFieldGetters.length; i++) {
            preRow[i] =
                    JavaObjectConverter.convertToJava(
                            preFieldGetters[i].getFieldOrNull(recordData),
                            preSchema.getColumnDataTypes().get(i));
        }

        Object[] postRow =
                projectionProcessor != null ? projectionProcessor.project(preRow, context) : preRow;
        boolean filterPassed =
                filterProcessor == null || filterProcessor.test(preRow, postRow, context);

        Object[] postRowBinary = new Object[postSchema.getColumnCount()];
        for (int i = 0; i < postRow.length; i++) {
            postRowBinary[i] =
                    BinaryInternalObjectConverter.convertToInternal(
                            postRow[i], postSchema.getColumnDataTypes().get(i));
        }
        synchronized (postGenerator) {
            return Tuple2.of(postGenerator.generate(postRowBinary), filterPassed);
        }
    }

    private Optional<PostTransformer> getEffectiveTransformer(TableId tableId) {
        for (PostTransformer transformer : transformers) {
            if (transformer.getSelectors().isMatch(tableId)) {
                return Optional.of(transformer);
            }
        }
        return Optional.empty();
    }

    private TransformProjectionProcessor getProjectionProcessor(
            TableId tableId, PostTransformer postTransformer) {
        Table<TableId, PostTransformer, TransformProjectionProcessor> processors =
                projectionProcessors.get();
        if (!processors.contains(tableId, postTransformer)) {
            PostTransformChangeInfo changeInfo = checkNotNull(tableInfoMap.get(tableId)).changeInfo;
            processors.put(
                    tableId,
                    postTransformer,
                    new TransformProjectionProcessor(
                            changeInfo,
                            postTransformer
                                    .getProjection()
                                    .map(TransformProjection::getProjection)
                                    .orElse(null),
                            timezone,
                            decimalPrecisionMode,
                            udfDescriptors,
                            udfFunctionInstances,
                            postTransformer.getSupportedMetadataColumns(),
                            modelClients));
        }
        return processors.get(tableId, postTransformer);
    }

    private TransformFilterProcessor getFilterProcessor(
            TableId tableId, PostTransformer postTransformer) {
        Table<TableId, PostTransformer, TransformFilterProcessor> processors =
                filterProcessors.get();
        if (!processors.contains(tableId, postTransformer)) {
            if (!postTransformer.getFilter().isPresent()) {
                processors.put(
                        tableId,
                        postTransformer,
                        TransformFilterProcessor.ofNoOp(decimalPrecisionMode));
            } else {
                PostTransformChangeInfo changeInfo =
                        checkNotNull(tableInfoMap.get(tableId)).changeInfo;
                processors.put(
                        tableId,
                        postTransformer,
                        TransformFilterProcessor.of(
                                changeInfo,
                                postTransformer.getFilter().orElse(null),
                                timezone,
                                decimalPrecisionMode,
                                udfDescriptors,
                                udfFunctionInstances,
                                postTransformer.getSupportedMetadataColumns(),
                                modelClients));
            }
        }
        return processors.get(tableId, postTransformer);
    }

    private void invalidateCache(TableId tableId) {
        projectionProcessorCaches.forEach(processors -> processors.row(tableId).clear());
        filterProcessorCaches.forEach(processors -> processors.row(tableId).clear());
    }

    private List<PostTransformer> createTransformers() {
        List<PostTransformer> list = new ArrayList<>();
        for (TransformRule rule : transformRules) {
            Selectors selectors =
                    new Selectors.SelectorsBuilder()
                            .includeTables(rule.getTableInclusions())
                            .build();
            list.add(
                    new PostTransformer(
                            selectors,
                            TransformProjection.of(rule.getProjection()).orElse(null),
                            TransformFilter.of(rule.getFilter()).orElse(null),
                            PostTransformConverters.of(rule.getPostTransformConverter())
                                    .orElse(null),
                            rule.getSupportedMetadataColumns()));
        }
        return list;
    }

    private void initializeUdf() {
        this.udfDescriptors =
                udfFunctions.stream()
                        .map(UserDefinedFunctionDescriptor::new)
                        .collect(Collectors.toList());
        this.udfFunctionInstances = new ArrayList<>();

        for (UserDefinedFunctionDescriptor udf : udfDescriptors) {
            try {
                Class<?> clazz = Class.forName(udf.getClasspath());
                Object udfInstance = clazz.getDeclaredConstructor().newInstance();
                udfFunctionInstances.add(udfInstance);

                if (udf.isCdcPipelineUdf()) {
                    UserDefinedFunctionContext userDefinedFunctionContext =
                            () -> Configuration.fromMap(udf.getParameters());
                    udfInstance
                            .getClass()
                            .getMethod("open", UserDefinedFunctionContext.class)
                            .invoke(udfInstance, userDefinedFunctionContext);
                }
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to instantiate UDF function " + udf, e);
            }
        }
    }

    private void destroyUdf() {
        if (udfDescriptors == null || udfFunctionInstances == null) {
            return;
        }
        for (int i = 0; i < udfDescriptors.size(); i++) {
            UserDefinedFunctionDescriptor udf = udfDescriptors.get(i);
            try {
                if (udf.isCdcPipelineUdf()) {
                    Object udfInstance = udfFunctionInstances.get(i);
                    udfInstance.getClass().getMethod("close").invoke(udfInstance);
                }
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to destroy UDF " + udf, e);
            }
        }
        udfDescriptors.clear();
        udfFunctionInstances.clear();
    }

    private void initializeAiModelClients() {
        for (Map.Entry<String, AiModelClient> entry : modelClients.entrySet()) {
            try {
                entry.getValue().open();
                LOG.info("Successfully opened AI model client '{}'.", entry.getKey());
            } catch (Exception e) {
                LOG.error("Failed to open AI model client '{}'.", entry.getKey(), e);
                throw new FlinkRuntimeException(
                        "Failed to initialize AI model: " + entry.getKey(), e);
            }
        }
    }

    private void destroyAiModelClients() {
        for (Map.Entry<String, AiModelClient> entry : modelClients.entrySet()) {
            try {
                entry.getValue().close();
                LOG.info("Successfully closed AI model client '{}'.", entry.getKey());
            } catch (Exception e) {
                LOG.warn("Failed to close AI model client '{}'.", entry.getKey(), e);
            }
        }
    }

    private void cacheTableState(
            TableId tableId,
            Schema preSchema,
            Schema postSchema,
            @Nullable CreateTableEvent outputCreateTableEvent) {
        cacheTableState(
                tableId, preSchema, postSchema, hasAsterisk(tableId), outputCreateTableEvent);
    }

    private void cacheTableState(
            TableId tableId,
            Schema preSchema,
            Schema postSchema,
            boolean hasAsterisk,
            @Nullable CreateTableEvent outputCreateTableEvent) {
        tableInfoMap.put(
                tableId,
                new PostTransformTableInfo(
                        PostTransformChangeInfo.of(tableId, preSchema, postSchema),
                        outputCreateTableEvent,
                        hasAsterisk,
                        projectedColumns(preSchema, postSchema)));
    }

    private boolean hasAsterisk(TableId tableId) {
        for (TransformRule rule : transformRules) {
            Selectors selectors =
                    new Selectors.SelectorsBuilder()
                            .includeTables(rule.getTableInclusions())
                            .build();
            if (selectors.isMatch(tableId)) {
                return rule.getProjection() != null
                        && TransformParser.hasAsterisk(rule.getProjection());
            }
        }
        return false;
    }

    private boolean hasAsterisk(PostTransformer transformer) {
        return transformer.getProjection().isPresent()
                && TransformParser.hasAsterisk(transformer.getProjection().get().getProjection());
    }

    private List<String> projectedColumns(Schema preSchema, Schema postSchema) {
        return preSchema.getColumnNames().stream()
                .filter(postSchema.getColumnNames()::contains)
                .collect(Collectors.toList());
    }

    private static final class PostTransformTableInfo {

        private final PostTransformChangeInfo changeInfo;
        @Nullable private final CreateTableEvent outputCreateTableEvent;
        private final boolean hasAsterisk;
        private final List<String> projectedColumns;

        private PostTransformTableInfo(
                PostTransformChangeInfo changeInfo,
                @Nullable CreateTableEvent outputCreateTableEvent,
                boolean hasAsterisk,
                List<String> projectedColumns) {
            this.changeInfo = changeInfo;
            this.outputCreateTableEvent = outputCreateTableEvent;
            this.hasAsterisk = hasAsterisk;
            this.projectedColumns = Collections.unmodifiableList(new ArrayList<>(projectedColumns));
        }
    }
}
