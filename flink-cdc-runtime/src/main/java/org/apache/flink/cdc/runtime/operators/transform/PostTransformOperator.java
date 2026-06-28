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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaColumnCaseFormat;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.AbstractStreamOperatorAdapter;
import org.apache.flink.cdc.runtime.operators.transform.converter.PostTransformConverters;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.cdc.runtime.typeutils.BinaryInternalObjectConverter;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava31.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/**
 * A data process function that performs column filtering, calculated column evaluation & final
 * projection.
 */
public class PostTransformOperator extends AbstractStreamOperatorAdapter<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostTransformOperator.class);

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

    private transient LoadingCache<TableId, Optional<PostTransformer>> transformersCache;

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
        this.transformersCache =
                CacheBuilder.newBuilder()
                        .maximumSize(1024)
                        .build(
                                new CacheLoader<TableId, Optional<PostTransformer>>() {
                                    @Override
                                    public Optional<PostTransformer> load(TableId tableId) {
                                        return getEffectiveTransformer(tableId);
                                    }
                                });
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
        Optional<PostTransformer> transformer = transformersCache.getUnchecked(tableId);

        // Short-circuit if there's no effective transformers.
        if (transformer.isEmpty()) {
            output.collect(element);
            return;
        }

        if (event instanceof CreateTableEvent) {
            processCreateTableEvent((CreateTableEvent) event, transformer.get())
                    .map(StreamRecord::new)
                    .ifPresent(output::collect);
            invalidateCache(tableId);
        } else if (event instanceof SchemaChangeEvent) {
            processSchemaChangeEvent((SchemaChangeEvent) event, transformer.get())
                    .map(StreamRecord::new)
                    .ifPresent(output::collect);
            invalidateCache(tableId);
        } else if (event instanceof DataChangeEvent) {
            processDataChangeEvent((DataChangeEvent) event, transformer.get())
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
            CreateTableEvent event, PostTransformer effectiveTransformer) {
        TableId tableId = event.tableId();
        Schema preSchema = event.getSchema();

        Schema postSchema =
                SchemaUtils.ensurePkNonNull(
                        transformSchema(preSchema, effectiveTransformer, tableId));

        // Update transform info map
        postTransformInfoMap.put(
                tableId, PostTransformChangeInfo.of(tableId, preSchema, postSchema));

        // Update "if-table-has-been–wildcard–matched" map
        boolean wildcardMatched =
                effectiveTransformer.getProjection().isPresent()
                        && TransformParser.hasAsterisk(
                                effectiveTransformer.getProjection().get().getProjection());

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
            SchemaChangeEvent event, PostTransformer effectiveTransformer) {
        TableId tableId = event.tableId();
        PostTransformChangeInfo info = checkNotNull(postTransformInfoMap.get(tableId));

        // Apply schema change event to the pre-transformed schema
        Schema prevPreSchema = info.getPreTransformedSchema();
        Schema nextPreSchema = SchemaUtils.applySchemaChangeEvent(prevPreSchema, event);

        Schema nextPostSchema =
                SchemaUtils.ensurePkNonNull(
                        transformSchema(nextPreSchema, effectiveTransformer, tableId));

        // Update transform info map
        postTransformInfoMap.put(
                tableId, PostTransformChangeInfo.of(tableId, nextPreSchema, nextPostSchema));

        // Prepare transformed schema change events
        Schema prevPostSchema = info.getPostTransformedSchema();
        List<String> columnNamesBeforeChange = prevPostSchema.getColumnNames();

        if (isColumnSchemaChangeEvent(event) && prevPostSchema.equals(nextPostSchema)) {
            return Optional.empty();
        }

        Optional<SchemaChangeEvent> rewrittenEvent =
                rewriteSchemaChangeEvent(
                        event,
                        prevPreSchema,
                        nextPreSchema,
                        prevPostSchema,
                        nextPostSchema,
                        effectiveTransformer);
        if (rewrittenEvent.isPresent()) {
            return rewrittenEvent.map(Event.class::cast);
        }

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
            DataChangeEvent event, PostTransformer effectiveTransformer) {
        TableId tableId = event.tableId();
        PostTransformChangeInfo info = checkNotNull(postTransformInfoMap.get(tableId));

        // Prepare transform context
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
        // For UPDATE events, before and after filter results may differ, requiring op type
        // conversion:
        //   before=Y, after=Y -> UPDATE;  before=Y, after=N -> DELETE;
        //   before=N, after=Y -> INSERT;  before=N, after=N -> drop.
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

    /**
     * Generates transformed version of schema based on upstream schema and effective transformer.
     *
     * <p>Primary/partition keys follow explicit YAML keys when set; otherwise provable 1:1 lineage
     * and {@link SchemaColumnCaseFormat} apply for post-projection names.
     */
    private Schema transformSchema(Schema preSchema, PostTransformer transformer, TableId tableId) {
        SchemaColumnCaseFormat caseFormat = transformer.getSchemaColumnCaseFormat();
        List<ProjectionColumn> projectionColumns =
                TransformParser.generateProjectionColumns(
                        transformer
                                .getProjection()
                                .map(TransformProjection::getProjection)
                                .orElse(null),
                        preSchema.getColumns(),
                        udfDescriptors,
                        transformer.getSupportedMetadataColumns());

        List<Column> newColumns =
                SchemaColumnCaseFormatter.applyCaseFormatToColumns(projectionColumns, caseFormat);

        Set<String> projectedColumnNames =
                newColumns.stream().map(Column::getName).collect(Collectors.toSet());
        Map<String, String> lineageMap =
                SchemaColumnCaseFormatter.buildProvableLineageMap(projectionColumns, caseFormat);

        List<String> newPrimaryKeys =
                SchemaColumnCaseFormatter.resolveProjectedKeys(
                        preSchema.primaryKeys(),
                        transformer.getExplicitPrimaryKeys(),
                        lineageMap,
                        projectedColumnNames,
                        tableId,
                        "primary",
                        caseFormat);
        List<String> newPartitionKeys =
                SchemaColumnCaseFormatter.resolveProjectedKeys(
                        preSchema.partitionKeys(),
                        transformer.getExplicitPartitionKeys(),
                        lineageMap,
                        projectedColumnNames,
                        tableId,
                        "partition",
                        caseFormat);

        return Schema.newBuilder()
                .setColumns(newColumns)
                .primaryKey(newPrimaryKeys)
                .partitionKey(newPartitionKeys)
                .options(preSchema.options())
                .comment(preSchema.comment())
                .build();
    }

    private static List<String> parseCommaSeparatedKeys(@Nullable String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    private Optional<SchemaChangeEvent> rewriteSchemaChangeEvent(
            SchemaChangeEvent event,
            Schema prevPreSchema,
            Schema nextPreSchema,
            Schema prevPostSchema,
            Schema nextPostSchema,
            PostTransformer transformer) {
        if (event instanceof RenameColumnEvent) {
            return rewriteRenameColumnEvent(
                            (RenameColumnEvent) event,
                            prevPreSchema,
                            nextPreSchema,
                            prevPostSchema,
                            nextPostSchema,
                            transformer)
                    .map(SchemaChangeEvent.class::cast);
        }
        if (event instanceof AlterColumnTypeEvent) {
            return rewriteAlterColumnTypeEvent(
                            (AlterColumnTypeEvent) event, prevPostSchema, nextPostSchema)
                    .map(SchemaChangeEvent.class::cast);
        }
        if (event instanceof AddColumnEvent) {
            return rewriteAddColumnEvent(event.tableId(), prevPostSchema, nextPostSchema)
                    .map(SchemaChangeEvent.class::cast);
        }
        if (event instanceof DropColumnEvent) {
            return rewriteDropColumnEvent(event.tableId(), prevPostSchema, nextPostSchema)
                    .map(SchemaChangeEvent.class::cast);
        }
        return Optional.empty();
    }

    private static boolean isColumnSchemaChangeEvent(SchemaChangeEvent event) {
        return event instanceof AddColumnEvent
                || event instanceof AlterColumnTypeEvent
                || event instanceof DropColumnEvent
                || event instanceof RenameColumnEvent;
    }

    private Optional<AlterColumnTypeEvent> rewriteAlterColumnTypeEvent(
            AlterColumnTypeEvent event, Schema prevPostSchema, Schema nextPostSchema) {
        Map<String, DataType> rewrittenTypeMapping = new LinkedHashMap<>();
        Map<String, DataType> rewrittenOldTypeMapping = new LinkedHashMap<>();
        Map<String, String> rewrittenComments = new LinkedHashMap<>();

        for (Column nextColumn : nextPostSchema.getColumns()) {
            Optional<Column> prevColumnOptional = prevPostSchema.getColumn(nextColumn.getName());
            if (!prevColumnOptional.isPresent()) {
                continue;
            }
            Column prevColumn = prevColumnOptional.get();
            boolean typeChanged = !Objects.equals(prevColumn.getType(), nextColumn.getType());
            boolean commentChanged =
                    !Objects.equals(prevColumn.getComment(), nextColumn.getComment());
            if (typeChanged) {
                rewrittenTypeMapping.put(nextColumn.getName(), nextColumn.getType());
                rewrittenOldTypeMapping.put(nextColumn.getName(), prevColumn.getType());
            }
            if (commentChanged || (typeChanged && nextColumn.getComment() != null)) {
                rewrittenComments.put(nextColumn.getName(), nextColumn.getComment());
            }
        }

        if (rewrittenTypeMapping.isEmpty() && rewrittenComments.isEmpty()) {
            return Optional.empty();
        }

        if (event.hasPreSchema() && !rewrittenOldTypeMapping.isEmpty()) {
            return Optional.of(
                    new AlterColumnTypeEvent(
                            event.tableId(),
                            rewrittenTypeMapping,
                            rewrittenOldTypeMapping,
                            rewrittenComments));
        }
        return Optional.of(
                new AlterColumnTypeEvent(
                        event.tableId(),
                        rewrittenTypeMapping,
                        Collections.emptyMap(),
                        rewrittenComments));
    }

    private Optional<AddColumnEvent> rewriteAddColumnEvent(
            TableId tableId, Schema prevPostSchema, Schema nextPostSchema) {
        Set<String> prevPostColumnNames = new HashSet<>(prevPostSchema.getColumnNames());
        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        String previousColumnName = null;
        for (Column column : nextPostSchema.getColumns()) {
            if (!prevPostColumnNames.contains(column.getName())) {
                AddColumnEvent.ColumnPosition position =
                        previousColumnName == null
                                ? AddColumnEvent.ColumnPosition.FIRST
                                : AddColumnEvent.ColumnPosition.AFTER;
                addedColumns.add(
                        new AddColumnEvent.ColumnWithPosition(
                                column, position, previousColumnName));
            }
            previousColumnName = column.getName();
        }

        if (addedColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new AddColumnEvent(tableId, addedColumns));
    }

    private Optional<DropColumnEvent> rewriteDropColumnEvent(
            TableId tableId, Schema prevPostSchema, Schema nextPostSchema) {
        Set<String> nextPostColumnNames = new HashSet<>(nextPostSchema.getColumnNames());
        List<String> droppedColumnNames =
                prevPostSchema.getColumnNames().stream()
                        .filter(columnName -> !nextPostColumnNames.contains(columnName))
                        .collect(Collectors.toList());
        if (droppedColumnNames.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new DropColumnEvent(tableId, droppedColumnNames));
    }

    private Optional<RenameColumnEvent> rewriteRenameColumnEvent(
            RenameColumnEvent event,
            Schema prevPreSchema,
            Schema nextPreSchema,
            Schema prevPostSchema,
            Schema nextPostSchema,
            PostTransformer transformer) {
        Optional<RenameColumnEvent> rewrittenEvent =
                rewriteRenameColumnEventFromPostSchema(
                        event.tableId(), prevPostSchema, nextPostSchema);
        if (rewrittenEvent.isPresent()) {
            return rewrittenEvent;
        }

        Map<String, String> prevLineageMap = buildProvableLineageMap(prevPreSchema, transformer);
        Map<String, String> nextLineageMap = buildProvableLineageMap(nextPreSchema, transformer);
        Map<String, String> rewrittenNameMapping = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : event.getNameMapping().entrySet()) {
            String oldPreColumnName =
                    SchemaUtils.resolveExistingColumnName(prevPreSchema, entry.getKey());
            String newPreColumnName =
                    SchemaUtils.resolveExistingColumnName(nextPreSchema, entry.getValue());
            String oldPostColumnName = prevLineageMap.get(oldPreColumnName);
            String newPostColumnName = nextLineageMap.get(newPreColumnName);
            if (oldPostColumnName == null
                    || newPostColumnName == null
                    || oldPostColumnName.equals(newPostColumnName)) {
                continue;
            }
            rewrittenNameMapping.put(oldPostColumnName, newPostColumnName);
        }

        if (rewrittenNameMapping.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new RenameColumnEvent(event.tableId(), rewrittenNameMapping));
    }

    private Optional<RenameColumnEvent> rewriteRenameColumnEventFromPostSchema(
            TableId tableId, Schema prevPostSchema, Schema nextPostSchema) {
        if (prevPostSchema.getColumnCount() != nextPostSchema.getColumnCount()) {
            return Optional.empty();
        }
        Map<String, String> nameMapping = new LinkedHashMap<>();
        List<Column> prevColumns = prevPostSchema.getColumns();
        List<Column> nextColumns = nextPostSchema.getColumns();
        for (int i = 0; i < prevColumns.size(); i++) {
            Column prevColumn = prevColumns.get(i);
            Column nextColumn = nextColumns.get(i);
            if (prevColumn.getName().equals(nextColumn.getName())) {
                continue;
            }
            if (!prevColumn.copy(nextColumn.getName()).equals(nextColumn)) {
                return Optional.empty();
            }
            nameMapping.put(prevColumn.getName(), nextColumn.getName());
        }
        if (nameMapping.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new RenameColumnEvent(tableId, nameMapping));
    }

    private Map<String, String> buildProvableLineageMap(
            Schema preSchema, PostTransformer transformer) {
        List<ProjectionColumn> projectionColumns =
                TransformParser.generateProjectionColumns(
                        transformer
                                .getProjection()
                                .map(TransformProjection::getProjection)
                                .orElse("*"),
                        preSchema.getColumns(),
                        udfDescriptors,
                        transformer.getSupportedMetadataColumns());
        return SchemaColumnCaseFormatter.buildProvableLineageMap(
                projectionColumns, transformer.getSchemaColumnCaseFormat());
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
                    JavaObjectConverter.convertToJava(
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
                    BinaryInternalObjectConverter.convertToInternal(
                            postRow[i], postSchema.getColumnDataTypes().get(i));
        }
        return Tuple2.of(postGenerator.generate(postRowBinary), filterPassed);
    }

    // -------------------
    // Convenience methods for coping with transient fields.
    // -------------------

    /** Obtain effective transformer based on given {@link TableId}. */
    private Optional<PostTransformer> getEffectiveTransformer(TableId tableId) {
        for (PostTransformer transformer : transformers) {
            if (transformer.getSelectors().isMatch(tableId)) {
                return Optional.of(transformer);
            }
        }
        return Optional.empty();
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
                            TransformFilter.of(filterExpression).orElse(null),
                            PostTransformConverters.of(rule.getPostTransformConverter())
                                    .orElse(null),
                            rule.getSupportedMetadataColumns(),
                            parseCommaSeparatedKeys(rule.getPrimaryKey()),
                            parseCommaSeparatedKeys(rule.getPartitionKey()),
                            rule.getSchemaColumnCaseFormat());
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
}
