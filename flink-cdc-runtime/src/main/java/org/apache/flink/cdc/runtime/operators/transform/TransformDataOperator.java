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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** A data process function that applies user-defined transform logics. */
public class TransformDataOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {

    private SchemaEvolutionClient schemaEvolutionClient;
    private final OperatorID schemaOperatorID;
    private final String timezone;
    private final List<Tuple3<String, String, String>> transformRules;
    private transient List<
                    Tuple4<
                            Selectors,
                            Optional<TransformProjection>,
                            Optional<TransformFilter>,
                            Boolean>>
            transforms;

    /** keep the relationship of TableId and table information. */
    private final Map<TableId, TableInfo> tableInfoMap;

    private transient Map<Tuple2<TableId, TransformProjection>, TransformProjectionProcessor>
            transformProjectionProcessorMap;
    private transient Map<Tuple2<TableId, TransformFilter>, TransformFilterProcessor>
            transformFilterProcessorMap;

    public static TransformDataOperator.Builder newBuilder() {
        return new TransformDataOperator.Builder();
    }

    /** Builder of {@link TransformDataOperator}. */
    public static class Builder {
        private final List<Tuple3<String, String, String>> transformRules = new ArrayList<>();
        private OperatorID schemaOperatorID;
        private String timezone;

        public TransformDataOperator.Builder addTransform(
                String tableInclusions, @Nullable String projection, @Nullable String filter) {
            transformRules.add(Tuple3.of(tableInclusions, projection, filter));
            return this;
        }

        public TransformDataOperator.Builder addSchemaOperatorID(OperatorID schemaOperatorID) {
            this.schemaOperatorID = schemaOperatorID;
            return this;
        }

        public TransformDataOperator.Builder addTimezone(String timezone) {
            if (PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(timezone)) {
                this.timezone = ZoneId.systemDefault().toString();
            } else {
                this.timezone = timezone;
            }
            return this;
        }

        public TransformDataOperator build() {
            return new TransformDataOperator(transformRules, schemaOperatorID, timezone);
        }
    }

    private TransformDataOperator(
            List<Tuple3<String, String, String>> transformRules,
            OperatorID schemaOperatorID,
            String timezone) {
        this.transformRules = transformRules;
        this.schemaOperatorID = schemaOperatorID;
        this.timezone = timezone;
        this.tableInfoMap = new ConcurrentHashMap<>();
        this.transformFilterProcessorMap = new ConcurrentHashMap<>();
        this.transformProjectionProcessorMap = new ConcurrentHashMap<>();
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        schemaEvolutionClient =
                new SchemaEvolutionClient(
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway(),
                        schemaOperatorID);
    }

    @Override
    public void open() throws Exception {
        super.open();
        transforms =
                transformRules.stream()
                        .map(
                                tuple3 -> {
                                    String tableInclusions = tuple3.f0;
                                    String projection = tuple3.f1;
                                    String filterExpression = tuple3.f2;

                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple4<>(
                                            selectors,
                                            TransformProjection.of(projection),
                                            TransformFilter.of(filterExpression),
                                            containFilteredComputedColumn(
                                                    projection, filterExpression));
                                })
                        .collect(Collectors.toList());
        this.transformFilterProcessorMap = new ConcurrentHashMap<>();
        this.transformProjectionProcessorMap = new ConcurrentHashMap<>();
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
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        schemaEvolutionClient.registerSubtask(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            event = cacheSchema((SchemaChangeEvent) event);
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof DataChangeEvent) {
            Optional<DataChangeEvent> dataChangeEventOptional =
                    processDataChangeEvent(((DataChangeEvent) event));
            if (dataChangeEventOptional.isPresent()) {
                output.collect(new StreamRecord<>(dataChangeEventOptional.get()));
            }
        }
    }

    private SchemaChangeEvent cacheSchema(SchemaChangeEvent event) throws Exception {
        TableId tableId = event.tableId();
        Schema newSchema;
        if (event instanceof CreateTableEvent) {
            newSchema = ((CreateTableEvent) event).getSchema();
        } else {
            newSchema =
                    SchemaUtils.applySchemaChangeEvent(
                            getTableInfoFromSchemaEvolutionClient(tableId).getSchema(), event);
        }
        transformSchema(tableId, newSchema);
        tableInfoMap.put(tableId, TableInfo.of(tableId, newSchema));
        return event;
    }

    private TableInfo getTableInfoFromSchemaEvolutionClient(TableId tableId) throws Exception {
        TableInfo tableInfo = tableInfoMap.get(tableId);
        if (tableInfo == null) {
            Optional<Schema> schemaOptional = schemaEvolutionClient.getLatestSchema(tableId);
            if (schemaOptional.isPresent()) {
                tableInfo = TableInfo.of(tableId, schemaOptional.get());
            } else {
                throw new RuntimeException(
                        "Could not find schema message from SchemaRegistry for " + tableId);
            }
        }
        return tableInfo;
    }

    private void transformSchema(TableId tableId, Schema schema) throws Exception {
        for (Tuple4<Selectors, Optional<TransformProjection>, Optional<TransformFilter>, Boolean>
                transform : transforms) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId) && transform.f1.isPresent()) {
                TransformProjection transformProjection = transform.f1.get();
                if (transformProjection.isValid()) {
                    if (!transformProjectionProcessorMap.containsKey(
                            Tuple2.of(tableId, transformProjection))) {
                        transformProjectionProcessorMap.put(
                                Tuple2.of(tableId, transformProjection),
                                TransformProjectionProcessor.of(transformProjection));
                    }
                    TransformProjectionProcessor transformProjectionProcessor =
                            transformProjectionProcessorMap.get(
                                    Tuple2.of(tableId, transformProjection));
                    // update the columns of projection and add the column of projection into Schema
                    transformProjectionProcessor.processSchemaChangeEvent(schema);
                }
            }
        }
    }

    private Optional<DataChangeEvent> processDataChangeEvent(DataChangeEvent dataChangeEvent)
            throws Exception {
        TableId tableId = dataChangeEvent.tableId();
        List<Optional<DataChangeEvent>> transformedDataChangeEventOptionalList = new ArrayList<>();
        long epochTime = System.currentTimeMillis();
        for (Tuple4<Selectors, Optional<TransformProjection>, Optional<TransformFilter>, Boolean>
                transform : transforms) {
            Selectors selectors = transform.f0;
            Boolean isPreProjection = transform.f3;
            if (selectors.isMatch(tableId)) {
                Optional<DataChangeEvent> dataChangeEventOptional = Optional.of(dataChangeEvent);
                Optional<TransformProjection> transformProjectionOptional = transform.f1;
                if (isPreProjection
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
                                        getTableInfoFromSchemaEvolutionClient(tableId),
                                        transformProjection,
                                        timezone));
                    }
                    TransformProjectionProcessor transformProjectionProcessor =
                            transformProjectionProcessorMap.get(
                                    Tuple2.of(tableId, transformProjection));
                    dataChangeEventOptional =
                            processProjection(
                                    transformProjectionProcessor,
                                    dataChangeEventOptional.get(),
                                    epochTime);
                }
                Optional<TransformFilter> transformFilterOptional = transform.f2;
                if (transformFilterOptional.isPresent()
                        && transformFilterOptional.get().isVaild()) {
                    TransformFilter transformFilter = transformFilterOptional.get();
                    if (!transformFilterProcessorMap.containsKey(
                            Tuple2.of(tableId, transformFilter))) {
                        transformFilterProcessorMap.put(
                                Tuple2.of(tableId, transformFilter),
                                TransformFilterProcessor.of(
                                        getTableInfoFromSchemaEvolutionClient(tableId),
                                        transformFilter,
                                        timezone));
                    }
                    TransformFilterProcessor transformFilterProcessor =
                            transformFilterProcessorMap.get(Tuple2.of(tableId, transformFilter));
                    dataChangeEventOptional =
                            processFilter(
                                    transformFilterProcessor,
                                    dataChangeEventOptional.get(),
                                    epochTime);
                }
                if (!isPreProjection
                        && dataChangeEventOptional.isPresent()
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
                                        getTableInfoFromSchemaEvolutionClient(tableId),
                                        transformProjection,
                                        timezone));
                    }
                    TransformProjectionProcessor transformProjectionProcessor =
                            transformProjectionProcessorMap.get(
                                    Tuple2.of(tableId, transformProjection));
                    dataChangeEventOptional =
                            processProjection(
                                    transformProjectionProcessor,
                                    dataChangeEventOptional.get(),
                                    epochTime);
                }
                transformedDataChangeEventOptionalList.add(dataChangeEventOptional);
            }
        }
        if (transformedDataChangeEventOptionalList.isEmpty()) {
            return Optional.of(dataChangeEvent);
        } else {
            for (Optional<DataChangeEvent> dataChangeEventOptional :
                    transformedDataChangeEventOptionalList) {
                if (dataChangeEventOptional.isPresent()) {
                    return dataChangeEventOptional;
                }
            }
            return Optional.empty();
        }
    }

    private Optional<DataChangeEvent> processFilter(
            TransformFilterProcessor transformFilterProcessor,
            DataChangeEvent dataChangeEvent,
            long epochTime)
            throws Exception {
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        // insert and update event only process afterData, delete only process beforeData
        if (after != null) {
            if (transformFilterProcessor.process(after, epochTime)) {
                return Optional.of(dataChangeEvent);
            } else {
                return Optional.empty();
            }
        } else if (before != null) {
            if (transformFilterProcessor.process(before, epochTime)) {
                return Optional.of(dataChangeEvent);
            } else {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private Optional<DataChangeEvent> processProjection(
            TransformProjectionProcessor transformProjectionProcessor,
            DataChangeEvent dataChangeEvent,
            long epochTime)
            throws Exception {
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if (before != null) {
            BinaryRecordData projectedBefore =
                    transformProjectionProcessor.processData(before, epochTime);
            dataChangeEvent = DataChangeEvent.projectBefore(dataChangeEvent, projectedBefore);
        }
        if (after != null) {
            BinaryRecordData projectedAfter =
                    transformProjectionProcessor.processData(after, epochTime);
            dataChangeEvent = DataChangeEvent.projectAfter(dataChangeEvent, projectedAfter);
        }
        return Optional.of(dataChangeEvent);
    }

    private boolean containFilteredComputedColumn(String projection, String filter) {
        boolean contain = false;
        if (StringUtils.isNullOrWhitespaceOnly(projection)
                || StringUtils.isNullOrWhitespaceOnly(filter)) {
            return contain;
        }
        List<String> computedColumnNames = TransformParser.parseComputedColumnNames(projection);
        List<String> filteredColumnNames = TransformParser.parseFilterColumnNameList(filter);
        for (String computedColumnName : computedColumnNames) {
            if (filteredColumnNames.contains(computedColumnName)) {
                return true;
            }
        }
        return contain;
    }

    private void clearOperator() {
        this.transforms = null;
        this.transformProjectionProcessorMap = null;
        this.transformFilterProcessorMap = null;
        TransformExpressionCompiler.cleanUp();
    }
}
