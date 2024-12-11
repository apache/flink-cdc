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

package org.apache.flink.cdc.runtime.operators.schema;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.utils.ChangeEventUtils;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeProcessingResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultResponse;
import org.apache.flink.cdc.runtime.operators.schema.metrics.SchemaOperatorMetrics;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;

/**
 * The operator will evolve schemas in {@link SchemaRegistry} for incoming {@link
 * SchemaChangeEvent}s and block the stream for tables before their schema changes finish.
 */
@Internal
public class SchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SchemaOperator.class);
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    private final List<RouteRule> routingRules;

    private final String timezone;

    /**
     * Storing route source table selector, sink table name (before symbol replacement), and replace
     * symbol in a tuple.
     */
    private transient List<Tuple3<Selectors, String, String>> routes;

    private transient TaskOperatorEventGateway toCoordinator;
    private transient SchemaEvolutionClient schemaEvolutionClient;
    private transient LoadingCache<TableId, Schema> originalSchema;
    private transient LoadingCache<TableId, Schema> evolvedSchema;
    private transient LoadingCache<TableId, Boolean> schemaDivergesMap;

    /**
     * Storing mapping relations between upstream tableId (source table) mapping to downstream
     * tableIds (sink tables).
     */
    private transient LoadingCache<TableId, List<TableId>> tableIdMappingCache;

    private final long rpcTimeOutInMillis;
    private final SchemaChangeBehavior schemaChangeBehavior;

    private transient SchemaOperatorMetrics schemaOperatorMetrics;
    private transient int subTaskId;

    @VisibleForTesting
    public SchemaOperator(List<RouteRule> routingRules) {
        this.routingRules = routingRules;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT.toMillis();
        this.schemaChangeBehavior = SchemaChangeBehavior.EVOLVE;
        this.timezone = "UTC";
    }

    @VisibleForTesting
    public SchemaOperator(List<RouteRule> routingRules, Duration rpcTimeOut) {
        this.routingRules = routingRules;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = SchemaChangeBehavior.EVOLVE;
        this.timezone = "UTC";
    }

    @VisibleForTesting
    public SchemaOperator(
            List<RouteRule> routingRules,
            Duration rpcTimeOut,
            SchemaChangeBehavior schemaChangeBehavior) {
        this.routingRules = routingRules;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.timezone = "UTC";
    }

    public SchemaOperator(
            List<RouteRule> routingRules,
            Duration rpcTimeOut,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        this.routingRules = routingRules;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.rpcTimeOutInMillis = rpcTimeOut.toMillis();
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.timezone = timezone;
    }

    @Override
    public void open() throws Exception {
        super.open();
        schemaOperatorMetrics =
                new SchemaOperatorMetrics(
                        getRuntimeContext().getMetricGroup(), schemaChangeBehavior);
        subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        this.toCoordinator = containingTask.getEnvironment().getOperatorCoordinatorEventGateway();
        routes =
                routingRules.stream()
                        .map(
                                rule -> {
                                    String tableInclusions = rule.sourceTable;
                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple3<>(
                                            selectors, rule.sinkTable, rule.replaceSymbol);
                                })
                        .collect(Collectors.toList());
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, getOperatorID());
        evolvedSchema =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Schema>() {
                                    @Override
                                    public Schema load(TableId tableId) {
                                        return getLatestEvolvedSchema(tableId);
                                    }
                                });
        originalSchema =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Schema>() {
                                    @Override
                                    public Schema load(TableId tableId) throws Exception {
                                        return getLatestOriginalSchema(tableId);
                                    }
                                });
        schemaDivergesMap =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, Boolean>() {
                                    @Override
                                    public Boolean load(TableId tableId) throws Exception {
                                        return checkSchemaDiverges(tableId);
                                    }
                                });
        tableIdMappingCache =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(CACHE_EXPIRE_DURATION)
                        .build(
                                new CacheLoader<TableId, List<TableId>>() {
                                    @Override
                                    public List<TableId> load(TableId tableId) {
                                        return getRoutedTables(tableId);
                                    }
                                });
    }

    /**
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     */
    @Override
    public void processElement(StreamRecord<Event> streamRecord)
            throws InterruptedException, TimeoutException, ExecutionException {
        Event event = streamRecord.getValue();
        if (event instanceof SchemaChangeEvent) {
            processSchemaChangeEvents((SchemaChangeEvent) event);
        } else if (event instanceof DataChangeEvent) {
            processDataChangeEvents(streamRecord, (DataChangeEvent) event);
        } else {
            throw new RuntimeException("Unknown event type in Stream record: " + event);
        }
    }

    private void processSchemaChangeEvents(SchemaChangeEvent event)
            throws InterruptedException, TimeoutException, ExecutionException {
        TableId tableId = event.tableId();
        LOG.info(
                "{}> Table {} received SchemaChangeEvent {} and start to be blocked.",
                subTaskId,
                tableId,
                event);
        handleSchemaChangeEvent(tableId, event);

        if (event instanceof DropTableEvent) {
            // Update caches unless event is a Drop table event. In that case, no schema will be
            // available / necessary.
            return;
        }

        originalSchema.put(tableId, getLatestOriginalSchema(tableId));
        schemaDivergesMap.put(tableId, checkSchemaDiverges(tableId));

        List<TableId> optionalRoutedTable = getRoutedTables(tableId);
        if (!optionalRoutedTable.isEmpty()) {
            tableIdMappingCache
                    .get(tableId)
                    .forEach(routed -> evolvedSchema.put(routed, getLatestEvolvedSchema(routed)));
        } else {
            evolvedSchema.put(tableId, getLatestEvolvedSchema(tableId));
        }
    }

    private void processDataChangeEvents(StreamRecord<Event> streamRecord, DataChangeEvent event) {
        TableId tableId = event.tableId();
        List<TableId> optionalRoutedTable = getRoutedTables(tableId);
        if (!optionalRoutedTable.isEmpty()) {
            optionalRoutedTable.forEach(
                    evolvedTableId -> {
                        output.collect(
                                new StreamRecord<>(
                                        normalizeSchemaChangeEvents(event, evolvedTableId, false)));
                    });
        } else if (Boolean.FALSE.equals(schemaDivergesMap.getIfPresent(tableId))) {
            output.collect(new StreamRecord<>(normalizeSchemaChangeEvents(event, true)));
        } else {
            output.collect(streamRecord);
        }
    }

    private DataChangeEvent normalizeSchemaChangeEvents(
            DataChangeEvent event, boolean tolerantMode) {
        return normalizeSchemaChangeEvents(event, event.tableId(), tolerantMode);
    }

    private DataChangeEvent normalizeSchemaChangeEvents(
            DataChangeEvent event, TableId renamedTableId, boolean tolerantMode) {
        try {
            Schema originalSchema = this.originalSchema.get(event.tableId());
            Schema evolvedTableSchema = evolvedSchema.get(renamedTableId);
            if (originalSchema.equals(evolvedTableSchema)) {
                return ChangeEventUtils.recreateDataChangeEvent(event, renamedTableId);
            }
            switch (event.op()) {
                case INSERT:
                    return DataChangeEvent.insertEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.after(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode),
                            event.meta());
                case UPDATE:
                    return DataChangeEvent.updateEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.before(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode),
                            regenerateRecordData(
                                    event.after(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode),
                            event.meta());
                case DELETE:
                    return DataChangeEvent.deleteEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.before(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode),
                            event.meta());
                case REPLACE:
                    return DataChangeEvent.replaceEvent(
                            renamedTableId,
                            regenerateRecordData(
                                    event.after(),
                                    originalSchema,
                                    evolvedTableSchema,
                                    tolerantMode),
                            event.meta());
                default:
                    throw new IllegalArgumentException(
                            String.format("Unrecognized operation type \"%s\"", event.op()));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to fill null for empty columns", e);
        }
    }

    private RecordData regenerateRecordData(
            RecordData recordData,
            Schema originalSchema,
            Schema routedTableSchema,
            boolean tolerantMode) {
        // Regenerate record data
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>();
        for (Column column : routedTableSchema.getColumns()) {
            String columnName = column.getName();
            int columnIndex = originalSchema.getColumnNames().indexOf(columnName);
            if (columnIndex == -1) {
                fieldGetters.add(new NullFieldGetter());
            } else {
                RecordData.FieldGetter fieldGetter =
                        RecordData.createFieldGetter(
                                originalSchema.getColumn(columnName).get().getType(), columnIndex);
                // Check type compatibility, ignoring nullability
                if (originalSchema
                        .getColumn(columnName)
                        .get()
                        .getType()
                        .nullable()
                        .equals(column.getType().nullable())) {
                    fieldGetters.add(fieldGetter);
                } else {
                    fieldGetters.add(
                            new TypeCoercionFieldGetter(
                                    originalSchema.getColumn(columnName).get().getType(),
                                    column.getType(),
                                    fieldGetter,
                                    tolerantMode,
                                    timezone));
                }
            }
        }
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(
                        routedTableSchema.getColumnDataTypes().toArray(new DataType[0]));
        return recordDataGenerator.generate(
                fieldGetters.stream()
                        .map(fieldGetter -> fieldGetter.getFieldOrNull(recordData))
                        .toArray());
    }

    private List<TableId> getRoutedTables(TableId originalTableId) {
        return routes.stream()
                .filter(route -> route.f0.isMatch(originalTableId))
                .map(route -> resolveReplacement(originalTableId, route))
                .collect(Collectors.toList());
    }

    private TableId resolveReplacement(
            TableId originalTable, Tuple3<Selectors, String, String> route) {
        if (route.f2 != null) {
            return TableId.parse(route.f1.replace(route.f2, originalTable.getTableName()));
        }
        return TableId.parse(route.f1);
    }

    private void handleSchemaChangeEvent(TableId tableId, SchemaChangeEvent schemaChangeEvent)
            throws InterruptedException, TimeoutException {

        if (schemaChangeBehavior == SchemaChangeBehavior.EXCEPTION
                && schemaChangeEvent.getType() != SchemaChangeEventType.CREATE_TABLE) {
            // CreateTableEvent should be applied even in EXCEPTION mode
            throw new RuntimeException(
                    String.format(
                            "Refused to apply schema change event %s in EXCEPTION mode.",
                            schemaChangeEvent));
        }

        // The request will block if another schema change event is being handled
        SchemaChangeResponse response = requestSchemaChange(tableId, schemaChangeEvent);
        if (response.isAccepted()) {
            LOG.info("{}> Sending the FlushEvent for table {}.", subTaskId, tableId);
            output.collect(new StreamRecord<>(new FlushEvent(tableId)));
            List<SchemaChangeEvent> expectedSchemaChangeEvents = response.getSchemaChangeEvents();
            schemaOperatorMetrics.increaseSchemaChangeEvents(expectedSchemaChangeEvents.size());

            // The request will block until flushing finished in each sink writer
            SchemaChangeResultResponse schemaEvolveResponse = requestSchemaChangeResult();
            List<SchemaChangeEvent> finishedSchemaChangeEvents =
                    schemaEvolveResponse.getFinishedSchemaChangeEvents();

            // Update evolved schema changes based on apply results
            finishedSchemaChangeEvents.forEach(e -> output.collect(new StreamRecord<>(e)));
        } else if (response.isDuplicate()) {
            LOG.info(
                    "{}> Schema change event {} has been handled in another subTask already.",
                    subTaskId,
                    schemaChangeEvent);
        } else if (response.isIgnored()) {
            LOG.info(
                    "{}> Schema change event {} has been ignored. No schema evolution needed.",
                    subTaskId,
                    schemaChangeEvent);
        } else {
            throw new IllegalStateException("Unexpected response status " + response);
        }
    }

    private SchemaChangeResponse requestSchemaChange(
            TableId tableId, SchemaChangeEvent schemaChangeEvent)
            throws InterruptedException, TimeoutException {
        long schemaEvolveTimeOutMillis = System.currentTimeMillis() + rpcTimeOutInMillis;
        while (true) {
            SchemaChangeResponse response =
                    sendRequestToCoordinator(
                            new SchemaChangeRequest(tableId, schemaChangeEvent, subTaskId));
            if (response.isRegistryBusy()) {
                if (System.currentTimeMillis() < schemaEvolveTimeOutMillis) {
                    LOG.info(
                            "{}> Schema Registry is busy now, waiting for next request...",
                            subTaskId);
                    Thread.sleep(1000);
                } else {
                    throw new TimeoutException("TimeOut when requesting schema change");
                }
            } else {
                return response;
            }
        }
    }

    private SchemaChangeResultResponse requestSchemaChangeResult()
            throws InterruptedException, TimeoutException {
        CoordinationResponse coordinationResponse =
                sendRequestToCoordinator(new SchemaChangeResultRequest());
        long nextRpcTimeOutMillis = System.currentTimeMillis() + rpcTimeOutInMillis;
        while (coordinationResponse instanceof SchemaChangeProcessingResponse) {
            if (System.currentTimeMillis() < nextRpcTimeOutMillis) {
                Thread.sleep(1000);
                coordinationResponse = sendRequestToCoordinator(new SchemaChangeResultRequest());
            } else {
                throw new TimeoutException("TimeOut when requesting release upstream");
            }
        }
        return ((SchemaChangeResultResponse) coordinationResponse);
    }

    private <REQUEST extends CoordinationRequest, RESPONSE extends CoordinationResponse>
            RESPONSE sendRequestToCoordinator(REQUEST request) {
        try {
            CompletableFuture<CoordinationResponse> responseFuture =
                    toCoordinator.sendRequestToCoordinator(
                            getOperatorID(), new SerializedValue<>(request));
            return CoordinationResponseUtils.unwrap(responseFuture.get());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to send request to coordinator: " + request.toString(), e);
        }
    }

    private Schema getLatestEvolvedSchema(TableId tableId) {
        try {
            Optional<Schema> optionalSchema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
            if (!optionalSchema.isPresent()) {
                throw new IllegalStateException(
                        String.format("Schema doesn't exist for table \"%s\"", tableId));
            }
            return optionalSchema.get();
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format("Unable to get latest schema for table \"%s\"", tableId), e);
        }
    }

    private Schema getLatestOriginalSchema(TableId tableId) {
        try {
            Optional<Schema> optionalSchema =
                    schemaEvolutionClient.getLatestOriginalSchema(tableId);
            if (!optionalSchema.isPresent()) {
                throw new IllegalStateException(
                        String.format("Schema doesn't exist for table \"%s\"", tableId));
            }
            return optionalSchema.get();
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format("Unable to get latest schema for table \"%s\"", tableId), e);
        }
    }

    private Boolean checkSchemaDiverges(TableId tableId) {
        try {
            return getLatestEvolvedSchema(tableId).equals(getLatestOriginalSchema(tableId));
        } catch (IllegalStateException e) {
            // schema fetch failed, regard it as diverged
            return true;
        }
    }

    private static class NullFieldGetter implements RecordData.FieldGetter {
        @Nullable
        @Override
        public Object getFieldOrNull(RecordData recordData) {
            return null;
        }
    }

    private static class TypeCoercionFieldGetter implements RecordData.FieldGetter {
        private final DataType originalType;
        private final DataType destinationType;
        private final RecordData.FieldGetter originalFieldGetter;
        private final boolean tolerantMode;
        private final String timezone;

        public TypeCoercionFieldGetter(
                DataType originalType,
                DataType destinationType,
                RecordData.FieldGetter originalFieldGetter,
                boolean tolerantMode,
                String timezone) {
            this.originalType = originalType;
            this.destinationType = destinationType;
            this.originalFieldGetter = originalFieldGetter;
            this.tolerantMode = tolerantMode;
            this.timezone = timezone;
        }

        private Object fail(IllegalArgumentException e) throws IllegalArgumentException {
            if (tolerantMode) {
                return null;
            }
            throw e;
        }

        @Nullable
        @Override
        public Object getFieldOrNull(RecordData recordData) {
            Object originalField = originalFieldGetter.getFieldOrNull(recordData);
            if (originalField == null) {
                return null;
            }
            if (destinationType.is(DataTypeRoot.BIGINT)) {
                if (originalField instanceof Byte) {
                    // TINYINT
                    return ((Byte) originalField).longValue();
                } else if (originalField instanceof Short) {
                    // SMALLINT
                    return ((Short) originalField).longValue();
                } else if (originalField instanceof Integer) {
                    // INT
                    return ((Integer) originalField).longValue();
                } else if (originalField instanceof Long) {
                    // BIGINT
                    return originalField;
                } else {
                    return fail(
                            new IllegalArgumentException(
                                    String.format(
                                            "Cannot fit type \"%s\" into a BIGINT column. "
                                                    + "Currently only TINYINT / SMALLINT / INT / LONG can be accepted by a BIGINT column",
                                            originalField.getClass())));
                }
            } else if (destinationType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) destinationType;
                BigDecimal decimalValue;
                if (originalField instanceof Byte) {
                    decimalValue = BigDecimal.valueOf(((Byte) originalField).longValue(), 0);
                } else if (originalField instanceof Short) {
                    decimalValue = BigDecimal.valueOf(((Short) originalField).longValue(), 0);
                } else if (originalField instanceof Integer) {
                    decimalValue = BigDecimal.valueOf(((Integer) originalField).longValue(), 0);
                } else if (originalField instanceof Long) {
                    decimalValue = BigDecimal.valueOf((Long) originalField, 0);
                } else if (originalField instanceof DecimalData) {
                    decimalValue = ((DecimalData) originalField).toBigDecimal();
                } else {
                    return fail(
                            new IllegalArgumentException(
                                    String.format(
                                            "Cannot fit type \"%s\" into a DECIMAL column. "
                                                    + "Currently only BYTE / SHORT / INT / LONG / DECIMAL can be accepted by a DECIMAL column",
                                            originalField.getClass())));
                }
                return decimalValue != null
                        ? DecimalData.fromBigDecimal(
                                decimalValue, decimalType.getPrecision(), decimalType.getScale())
                        : null;
            } else if (destinationType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
                if (originalField instanceof Float) {
                    // FLOAT
                    return ((Float) originalField).doubleValue();
                } else {
                    return fail(
                            new IllegalArgumentException(
                                    String.format(
                                            "Cannot fit type \"%s\" into a DOUBLE column. "
                                                    + "Currently only FLOAT can be accepted by a DOUBLE column",
                                            originalField.getClass())));
                }
            } else if (destinationType.is(DataTypeRoot.VARCHAR)) {
                if (originalField instanceof StringData) {
                    return originalField;
                } else {
                    return fail(
                            new IllegalArgumentException(
                                    String.format(
                                            "Cannot fit type \"%s\" into a STRING column. "
                                                    + "Currently only CHAR / VARCHAR can be accepted by a STRING column",
                                            originalField.getClass())));
                }
            } else if (destinationType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                    && originalType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
                // For now, TimestampData / ZonedTimestampData / LocalZonedTimestampData has no
                // difference in its internal representation, so there's no need to do any precision
                // conversion.
                return originalField;
            } else if (destinationType.is(DataTypeRoot.TIMESTAMP_WITH_TIME_ZONE)
                    && originalType.is(DataTypeRoot.TIMESTAMP_WITH_TIME_ZONE)) {
                return originalField;
            } else if (destinationType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                    && originalType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
                return originalField;
            } else if (destinationType.is(DataTypeFamily.TIMESTAMP)
                    && originalType.is(DataTypeFamily.TIMESTAMP)) {
                return castToTimestamp(originalField, timezone);
            } else {
                return fail(
                        new IllegalArgumentException(
                                String.format(
                                        "Column type \"%s\" doesn't support type coercion",
                                        destinationType)));
            }
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // Needless to do anything, since AbstractStreamOperator#snapshotState and #processElement
        // is guaranteed not to be mixed together.
    }

    private static TimestampData castToTimestamp(Object object, String timezone) {
        if (object == null) {
            return null;
        }
        if (object instanceof LocalZonedTimestampData) {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.ofInstant(
                            ((LocalZonedTimestampData) object).toInstant(), ZoneId.of(timezone)));
        } else if (object instanceof ZonedTimestampData) {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.ofInstant(
                            ((ZonedTimestampData) object).toInstant(), ZoneId.of(timezone)));
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unable to implicitly coerce object `%s` as a TIMESTAMP.", object));
        }
    }
}
