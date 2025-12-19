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
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.transform.converter.PostTransformConverters;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/**
 * A data process function that performs column filtering, calculated column evaluation & final
 * projection.
 */
public class PostTransformOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String timezone;
    private final List<TransformRule> transformRules;
    private final Map<TableId, Boolean> hasAsteriskMap;
    private final Map<TableId, List<String>> projectedColumnsMap;
    private final Map<TableId, PostTransformChangeInfo> postTransformInfoMap;

    // Tuple3 items are: function name, class path, and extra options.
    private final List<Tuple3<String, String, Map<String, String>>> udfFunctions;

    private transient List<PostTransformer> transformers;
    private transient List<UserDefinedFunctionDescriptor> udfDescriptors;
    private transient List<Object> udfFunctionInstances;

    // Querying a TransformProjectionProcessor with an upstream TableId and effective
    // post-transformer.
    private transient Table<TableId, PostTransformer, TransformProjectionProcessor>
            projectionProcessors;
    private transient Table<TableId, PostTransformer, TransformFilterProcessor> filterProcessors;

    public static PostTransformOperatorBuilder newBuilder() {
        return new PostTransformOperatorBuilder();
    }

    PostTransformOperator(
            List<TransformRule> transformRules,
            String timezone,
            List<Tuple3<String, String, Map<String, String>>> udfFunctions) {
        this.timezone = timezone;
        this.transformRules = transformRules;
        this.hasAsteriskMap = new HashMap<>();
        this.projectedColumnsMap = new HashMap<>();
        this.postTransformInfoMap = new ConcurrentHashMap<>();
        this.udfFunctions = udfFunctions;
    }

    @Override
    public void open() throws Exception {
        super.open();

        // Initialize multi-key lookup tables
        this.projectionProcessors = HashBasedTable.create();
        this.filterProcessors = HashBasedTable.create();

        // Be sure to initialize UDF related fields before creating transformers
        initializeUdf();

        this.transformers = createTransformers();
    }

    @Override
    public void close() throws Exception {
        super.close();
        TransformExpressionCompiler.cleanUp();
        destroyUdf();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        try {
            processElementInternal(element);
        } catch (Exception e) {
            Event event = element.getValue();
            TableId tableId = null;
            Schema schemaBefore = null;
            Schema schemaAfter = null;

            if (event instanceof ChangeEvent) {
                tableId = ((ChangeEvent) event).tableId();
                PostTransformChangeInfo info = postTransformInfoMap.get(tableId);
                if (info != null) {
                    schemaBefore = info.getPreTransformedSchema();
                    schemaAfter = info.getPostTransformedSchema();
                }
            }

            throw new TransformException(
                    "post-transform", event, tableId, schemaBefore, schemaAfter, e);
        }
    }

    private void processElementInternal(StreamRecord<Event> element) {
        Event event = element.getValue();
        if (event == null) {
            return;
        }

        // Reject processing non-schema or data change events.
        if (!(event instanceof ChangeEvent)) {
            throw new UnsupportedOperationException("Unexpected stream record event: " + event);
        }

        ChangeEvent changeEvent = (ChangeEvent) event;
        TableId tableId = changeEvent.tableId();
        List<PostTransformer> transformers = getEffectiveTransformers(tableId);

        // Short-circuit if there's no effective transformers.
        if (transformers.isEmpty()) {
            output.collect(element);
            return;
        }

        if (event instanceof CreateTableEvent) {
            processCreateTableEvent((CreateTableEvent) event, transformers)
                    .map(StreamRecord::new)
                    .ifPresent(output::collect);
            invalidateCache(tableId);
        } else if (event instanceof SchemaChangeEvent) {
            processSchemaChangeEvent((SchemaChangeEvent) event, transformers)
                    .map(StreamRecord::new)
                    .ifPresent(output::collect);
            invalidateCache(tableId);
        } else if (event instanceof DataChangeEvent) {
            processDataChangeEvent((DataChangeEvent) event, transformers)
                    .map(StreamRecord::new)
                    .ifPresent(output::collect);
        } else {
            throw new UnsupportedOperationException("Unexpected stream record event: " + event);
        }
    }

    // -------------------
    // Key methods for processing upstream events.
    // -------------------

    /**
     * Apply effective transform rules to {@link CreateTableEvent}s based on effective transformers.
     */
    private Optional<Event> processCreateTableEvent(
            CreateTableEvent event, List<PostTransformer> effectiveTransformers) {
        TableId tableId = event.tableId();
        Schema preSchema = event.getSchema();

        // Apply transform rules and verify we can get a deterministic post schema
        List<Schema> schemas =
                effectiveTransformers.stream()
                        .map(trans -> transformSchema(preSchema, trans))
                        .collect(Collectors.toList());

        Schema postSchema =
                SchemaUtils.ensurePkNonNull(SchemaMergingUtils.strictlyMergeSchemas(schemas));

        // Update transform info map
        postTransformInfoMap.put(
                tableId, PostTransformChangeInfo.of(tableId, preSchema, postSchema));

        // Update "if-table-has-been–wildcard–matched" map
        boolean wildcardMatched =
                effectiveTransformers.stream()
                        .map(PostTransformer::getProjection)
                        .flatMap(this::optionalToStream)
                        .map(TransformProjection::getProjection)
                        .anyMatch(TransformParser::hasAsterisk);
        hasAsteriskMap.put(tableId, wildcardMatched);
        projectedColumnsMap.put(
                tableId,
                preSchema.getColumnNames().stream()
                        .filter(postSchema.getColumnNames()::contains)
                        .collect(Collectors.toList()));

        return Optional.of(new CreateTableEvent(tableId, postSchema));
    }

    /**
     * Apply effective transform rules to other {@link SchemaChangeEvent}s based on effective
     * transformers and existing {@link PostTransformChangeInfo}.
     */
    private Optional<Event> processSchemaChangeEvent(
            SchemaChangeEvent event, List<PostTransformer> effectiveTransformers) {
        TableId tableId = event.tableId();
        PostTransformChangeInfo info = checkNotNull(postTransformInfoMap.get(tableId));

        // Apply schema change event to the pre-transformed schema
        Schema prevPreSchema = info.getPreTransformedSchema();
        Schema nextPreSchema = SchemaUtils.applySchemaChangeEvent(prevPreSchema, event);

        // Apply transform rules and verify we can get a deterministic post schema
        List<Schema> schemas =
                effectiveTransformers.stream()
                        .map(trans -> transformSchema(nextPreSchema, trans))
                        .collect(Collectors.toList());

        Schema nextPostSchema =
                SchemaUtils.ensurePkNonNull(SchemaMergingUtils.strictlyMergeSchemas(schemas));

        // Update transform info map
        postTransformInfoMap.put(
                tableId, PostTransformChangeInfo.of(tableId, nextPreSchema, nextPostSchema));

        // Prepare transformed schema change events
        Schema prevPostSchema = info.getPostTransformedSchema();
        List<String> columnNamesBeforeChange = prevPostSchema.getColumnNames();

        if (hasAsteriskMap.getOrDefault(tableId, true)) {
            // See comments in PreTransformOperator#cacheChangeSchema method.
            return SchemaUtils.transformSchemaChangeEvent(true, columnNamesBeforeChange, event)
                    .map(Event.class::cast);
        } else {
            return SchemaUtils.transformSchemaChangeEvent(
                            false, projectedColumnsMap.get(tableId), event)
                    .map(Event.class::cast);
        }
    }

    /** Apply projection rules to given {@link DataChangeEvent}. */
    private Optional<Event> processDataChangeEvent(
            DataChangeEvent event, List<PostTransformer> effectiveTransformers) {
        TableId tableId = event.tableId();
        PostTransformChangeInfo info = checkNotNull(postTransformInfoMap.get(tableId));

        // Prepare transform context
        TransformContext context = new TransformContext();
        context.epochTime = System.currentTimeMillis();
        context.meta = event.meta();

        String beforeOp = event.opTypeString(false);
        String afterOp = event.opTypeString(true);

        for (PostTransformer transformer : effectiveTransformers) {
            TransformProjectionProcessor projectionProcessor =
                    getProjectionProcessor(tableId, transformer);
            TransformFilterProcessor filterProcessor = getFilterProcessor(tableId, transformer);

            RecordData beforeRow = null;
            RecordData afterRow = null;
            boolean filterPassed = true;

            if (event.before() != null) {
                context.opType = beforeOp;
                Tuple2<BinaryRecordData, Boolean> result =
                        transformRecord(
                                event.before(),
                                info,
                                projectionProcessor,
                                filterProcessor,
                                context);
                beforeRow = result.f0;
                filterPassed = result.f1;
            }

            if (event.after() != null) {
                context.opType = afterOp;
                Tuple2<BinaryRecordData, Boolean> result =
                        transformRecord(
                                event.after(), info, projectionProcessor, filterProcessor, context);
                afterRow = result.f0;
                filterPassed = result.f1;
            }

            if (filterPassed) {
                DataChangeEvent finalEvent =
                        DataChangeEvent.projectRecords(event, beforeRow, afterRow);
                if (transformer.getPostTransformConverter().isPresent()) {
                    return transformer
                            .getPostTransformConverter()
                            .get()
                            .convert(finalEvent)
                            .map(Event.class::cast);
                } else {
                    return Optional.of(finalEvent);
                }
            }
        }

        // Events with no matching filters satisfied won't be emitted to downstream.
        return Optional.empty();
    }

    /**
     * Generates transformed version of schema based on upstream schema and effective transformer.
     */
    private Schema transformSchema(Schema preSchema, PostTransformer transformer) {
        List<ProjectionColumn> projectionColumns =
                TransformParser.generateProjectionColumns(
                        transformer
                                .getProjection()
                                .map(TransformProjection::getProjection)
                                .orElse(null),
                        preSchema.getColumns(),
                        udfDescriptors,
                        transformer.getSupportedMetadataColumns());
        return preSchema.copy(
                projectionColumns.stream()
                        .map(ProjectionColumn::getColumn)
                        .collect(Collectors.toList()));
    }

    /** Projects given {@link RecordData} based on given processor. */
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
                    DataTypeConverter.convertToOriginal(
                            preFieldGetters[i].getFieldOrNull(recordData),
                            preSchema.getColumnDataTypes().get(i));
        }

        Object[] postRow =
                projectionProcessor != null ? projectionProcessor.project(preRow, context) : preRow;

        // Filter predicate test might refer to both PreTransformed only columns (that have been
        // eliminated from transform result) and PostTransformed only columns (that do not exist
        // until expression evaluation finishes). So we need pass both rows to FilterProcessor.
        boolean filterPassed =
                filterProcessor == null || filterProcessor.test(preRow, postRow, context);

        Object[] postRowBinary = new Object[postSchema.getColumnCount()];
        for (int i = 0; i < postRow.length; i++) {
            postRowBinary[i] =
                    DataTypeConverter.convert(postRow[i], postSchema.getColumnDataTypes().get(i));
        }
        return Tuple2.of(postGenerator.generate(postRowBinary), filterPassed);
    }

    // -------------------
    // Convenience methods for coping with transient fields.
    // -------------------

    /** Obtain effective transformers based on given {@link TableId}. */
    private List<PostTransformer> getEffectiveTransformers(TableId tableId) {
        List<PostTransformer> effectiveTransformers = new ArrayList<>();
        for (PostTransformer transformer : transformers) {
            if (transformer.getSelectors().isMatch(tableId)) {
                effectiveTransformers.add(transformer);

                // Transform module works with "First-match" rule. If we have met an uncondition
                // transform rule (without any filtering expression), then any following transform
                // rule will not be effective.
                if (!transformer.getFilter().isPresent()) {
                    break;
                }
            }
        }
        return effectiveTransformers;
    }

    /**
     * Get the unique {@link TransformProjectionProcessor} based on provided {@link TableId} and
     * {@link PostTransformer}.
     */
    private TransformProjectionProcessor getProjectionProcessor(
            TableId tableId, PostTransformer postTransformer) {
        if (!projectionProcessors.contains(tableId, postTransformer)) {
            PostTransformChangeInfo changeInfo = postTransformInfoMap.get(tableId);
            projectionProcessors.put(
                    tableId,
                    postTransformer,
                    new TransformProjectionProcessor(
                            changeInfo,
                            postTransformer
                                    .getProjection()
                                    .map(TransformProjection::getProjection)
                                    .orElse(null),
                            timezone,
                            udfDescriptors,
                            udfFunctionInstances,
                            postTransformer.getSupportedMetadataColumns()));
        }
        return projectionProcessors.get(tableId, postTransformer);
    }

    /**
     * Get the unique {@link TransformFilterProcessor} based on provided {@link TableId} and {@link
     * PostTransformer}.
     */
    private TransformFilterProcessor getFilterProcessor(
            TableId tableId, PostTransformer postTransformer) {
        if (!filterProcessors.contains(tableId, postTransformer)) {
            if (!postTransformer.getFilter().isPresent()) {
                filterProcessors.put(tableId, postTransformer, TransformFilterProcessor.ofNoOp());
            } else {
                PostTransformChangeInfo changeInfo = postTransformInfoMap.get(tableId);
                filterProcessors.put(
                        tableId,
                        postTransformer,
                        TransformFilterProcessor.of(
                                changeInfo,
                                postTransformer.getFilter().orElse(null),
                                timezone,
                                udfDescriptors,
                                udfFunctionInstances,
                                postTransformer.getSupportedMetadataColumns()));
            }
        }
        return filterProcessors.get(tableId, postTransformer);
    }

    /**
     * Flush caches saved for given {@link TableId}. Be sure to invalidate caches after its schema
     * has been changed!
     */
    private void invalidateCache(TableId tableId) {
        projectionProcessors.row(tableId).clear();
        filterProcessors.row(tableId).clear();
    }

    private List<PostTransformer> createTransformers() {
        List<PostTransformer> list = new ArrayList<>();
        for (TransformRule rule : transformRules) {
            String projection = rule.getProjection();
            String filterExpression = rule.getFilter();
            String tableInclusions = rule.getTableInclusions();
            Selectors selectors =
                    new Selectors.SelectorsBuilder().includeTables(tableInclusions).build();
            PostTransformer apply =
                    new PostTransformer(
                            selectors,
                            TransformProjection.of(projection).orElse(null),
                            TransformFilter.of(filterExpression, udfDescriptors).orElse(null),
                            PostTransformConverters.of(rule.getPostTransformConverter())
                                    .orElse(null),
                            rule.getSupportedMetadataColumns());
            list.add(apply);
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
                    // We use reflection to invoke UDF methods since we may add more methods
                    // into UserDefinedFunction interface, thus the provided UDF classes
                    // might not be compatible with the interface definition in CDC common.
                    UserDefinedFunctionContext userDefinedFunctionContext =
                            () -> Configuration.fromMap(udf.getParameters());
                    udfInstance
                            .getClass()
                            .getMethod("open", UserDefinedFunctionContext.class)
                            .invoke(udfInstance, userDefinedFunctionContext);
                }
                // Do nothing for Flink-style UDF since their lifecycle hooks are not supported
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
                // Do nothing for Flink-style UDF since their lifecycle hooks are not supported
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to destroy UDF " + udf, e);
            }
        }
        udfDescriptors.clear();
        udfFunctionInstances.clear();
    }

    /** Backport of {@code Optional#stream} before Java 11. */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private <T> Stream<T> optionalToStream(Optional<T> optional) {
        return optional.map(Stream::of).orElseGet(Stream::empty);
    }
}
