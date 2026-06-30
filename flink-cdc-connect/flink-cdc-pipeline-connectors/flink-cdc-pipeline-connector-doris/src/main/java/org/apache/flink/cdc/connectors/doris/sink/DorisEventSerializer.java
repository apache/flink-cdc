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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.doris.utils.DorisSchemaUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.Field;
import org.apache.doris.flink.sink.EscapeHandler;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.doris.flink.sink.util.DeleteOperation.addDeleteSign;
import static org.apache.doris.flink.sink.writer.LoadConstants.COLUMNS_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.DORIS_DELETE_SIGN;
import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.FORMAT_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.JSON;
import static org.apache.doris.flink.sink.writer.LoadConstants.NULL_VALUE;

/** A serializer for Event to DorisRecord. */
public class DorisEventSerializer implements DorisRecordSerializer<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisEventSerializer.class);

    /** Format TIME type data without precision. */
    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    /** Format TIME type data with millisecond precision. */
    public static final DateTimeFormatter TIME_WITH_MILLISECOND_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    @FunctionalInterface
    interface CsvOutputSchemaResolver extends Serializable {
        Schema resolve(TableId tableId, Schema inputSchema);
    }

    @FunctionalInterface
    interface JsonFieldMappingResolver extends Serializable {
        LinkedHashMap<String, String> resolve(
                TableId tableId, Schema inputSchema, List<String> oldColumns);
    }

    @FunctionalInterface
    interface DorisSchemaFetcher extends Serializable {
        org.apache.doris.flink.rest.models.Schema fetch(DorisOptions dorisOptions, TableId tableId);
    }

    /** Format DATE type data. */
    public static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Format timestamp-related type data. */
    public static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final int MAX_SCHEMA_HISTORY_SIZE = 16;
    private static final long DORIS_SCHEMA_CATCH_UP_MAX_WAIT_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final long DORIS_SCHEMA_FETCH_RETRY_MILLIS = 500L;
    private static final AtomicBoolean JSON_FALLBACK_WARNING_LOGGED = new AtomicBoolean(false);
    private static final DorisSchemaFetcher DEFAULT_DORIS_SCHEMA_FETCHER =
            (dorisOptions, tableId) ->
                    RestService.getSchema(
                            dorisOptions, tableId.getSchemaName(), tableId.getTableName(), LOG);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<TableId, Schema> inputSchemaMaps = new HashMap<>();
    private final Map<TableId, List<Schema>> inputSchemaHistoryMaps = new HashMap<>();
    private final Map<TableId, List<String>> oldInputColumnMaps = new HashMap<>();
    private final Map<TableId, Schema> csvOutputSchemaMaps = new HashMap<>();
    private final Map<TableId, Map<Schema, LinkedHashMap<String, String>>> jsonFieldMappings =
            new HashMap<>();
    private final CsvOutputSchemaResolver csvOutputSchemaResolver;
    private final JsonFieldMappingResolver jsonFieldMappingResolver;

    /** ZoneId from pipeline config to support timestamp with local time zone. */
    public final ZoneId pipelineZoneId;

    public final Configuration dorisConfig;
    private final String streamLoadFormat;
    private final String fieldDelimiter;

    public DorisEventSerializer(ZoneId zoneId, Configuration config) {
        this(zoneId, config, DorisExecutionOptions.defaults(), (CsvOutputSchemaResolver) null);
    }

    public DorisEventSerializer(
            ZoneId zoneId, Configuration config, DorisExecutionOptions executionOptions) {
        this(
                zoneId,
                config,
                executionOptions,
                createCsvOutputSchemaResolver(getStreamLoadProperties(executionOptions), null));
    }

    public DorisEventSerializer(
            ZoneId zoneId,
            Configuration config,
            DorisExecutionOptions executionOptions,
            DorisOptions dorisOptions) {
        this(
                zoneId,
                config,
                executionOptions,
                createCsvOutputSchemaResolver(
                        getStreamLoadProperties(executionOptions), dorisOptions),
                createJsonFieldMappingResolver(dorisOptions));
    }

    DorisEventSerializer(
            ZoneId zoneId,
            Configuration config,
            DorisExecutionOptions executionOptions,
            CsvOutputSchemaResolver csvOutputSchemaResolver) {
        this(zoneId, config, executionOptions, csvOutputSchemaResolver, null);
    }

    DorisEventSerializer(
            ZoneId zoneId,
            Configuration config,
            DorisExecutionOptions executionOptions,
            CsvOutputSchemaResolver csvOutputSchemaResolver,
            JsonFieldMappingResolver jsonFieldMappingResolver) {
        pipelineZoneId = zoneId;
        dorisConfig = config;
        Properties streamLoadProperties =
                executionOptions == null
                        ? DorisExecutionOptions.defaultsProperties()
                        : executionOptions.getStreamLoadProp();
        streamLoadFormat = streamLoadProperties.getProperty(FORMAT_KEY, JSON);
        fieldDelimiter =
                EscapeHandler.escapeString(
                        streamLoadProperties.getProperty(
                                FIELD_DELIMITER_KEY, FIELD_DELIMITER_DEFAULT));
        this.csvOutputSchemaResolver = csvOutputSchemaResolver;
        this.jsonFieldMappingResolver = jsonFieldMappingResolver;
        if (JSON.equals(streamLoadFormat)
                && jsonFieldMappingResolver == null
                && JSON_FALLBACK_WARNING_LOGGED.compareAndSet(false, true)) {
            LOG.warn(
                    "DorisEventSerializer is running in JSON mode without Doris physical schema resolution. "
                            + "Field names will fall back to lower-case input schema names. "
                            + "This path is intended only for tests or serializer-only scenarios.");
        }
    }

    @Override
    public DorisRecord serialize(Event event) throws IOException {
        if (event instanceof DataChangeEvent) {
            return applyDataChangeEvent((DataChangeEvent) event);
        }
        if (event instanceof SchemaChangeEvent) {
            cacheSchema((SchemaChangeEvent) event);
        }
        return null;
    }

    private void cacheSchema(SchemaChangeEvent schemaChangeEvent) {
        TableId tableId = schemaChangeEvent.tableId();
        if (schemaChangeEvent instanceof DropTableEvent) {
            inputSchemaMaps.remove(tableId);
            inputSchemaHistoryMaps.remove(tableId);
            oldInputColumnMaps.remove(tableId);
            csvOutputSchemaMaps.remove(tableId);
            jsonFieldMappings.remove(tableId);
            return;
        }

        Schema inputSchema;
        if (schemaChangeEvent instanceof CreateTableEvent) {
            inputSchema = ((CreateTableEvent) schemaChangeEvent).getSchema();
            oldInputColumnMaps.remove(tableId);
        } else {
            Preconditions.checkState(
                    inputSchemaMaps.containsKey(tableId),
                    "schema of " + tableId + " is not existed.");
            rememberOldColumns(tableId, inputSchemaMaps.get(tableId), schemaChangeEvent);
            inputSchema =
                    SchemaUtils.applySchemaChangeEvent(
                            inputSchemaMaps.get(tableId), schemaChangeEvent);
        }
        inputSchemaMaps.put(tableId, inputSchema);
        pruneOldColumns(tableId, inputSchema);
        rememberInputSchema(tableId, inputSchema);

        if (CSV.equals(streamLoadFormat)) {
            csvOutputSchemaMaps.put(tableId, resolveCsvOutputSchema(tableId, inputSchema));
        } else {
            csvOutputSchemaMaps.remove(tableId);
            jsonFieldMappings.remove(tableId);
        }
    }

    private DorisRecord applyDataChangeEvent(DataChangeEvent event) throws JsonProcessingException {
        TableId tableId = event.tableId();
        Schema currentInputSchema = inputSchemaMaps.get(tableId);
        Preconditions.checkNotNull(currentInputSchema, event.tableId() + " is not existed");
        Schema outputSchema =
                CSV.equals(streamLoadFormat)
                        ? getCsvOutputSchema(tableId, currentInputSchema)
                        : currentInputSchema;
        RecordData recordData;
        OperationType op = event.op();
        switch (op) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                recordData = event.after();
                break;
            case DELETE:
                recordData = event.before();
                break;
            default:
                throw new UnsupportedOperationException("Unsupport Operation " + op);
        }
        Schema inputSchema = resolveInputSchema(tableId, currentInputSchema, recordData);
        Map<String, Object> valueMap = serializerRecord(recordData, inputSchema);

        // get partition info from config
        Tuple2<String, String> partitionInfo =
                DorisSchemaUtils.getPartitionInfo(dorisConfig, outputSchema, tableId);
        if (!Objects.isNull(partitionInfo)) {
            String partitionKey = partitionInfo.f0;
            Object partitionValue = valueMap.get(partitionKey);
            // fill partition column by default value if null
            if (Objects.isNull(partitionValue)) {
                outputSchema
                        .getColumn(partitionKey)
                        .ifPresent(
                                column -> {
                                    DataType dataType = column.getType();
                                    if (dataType instanceof DateType) {
                                        valueMap.put(partitionKey, DorisSchemaUtils.DEFAULT_DATE);
                                    } else if (dataType instanceof LocalZonedTimestampType
                                            || dataType instanceof TimestampType
                                            || dataType instanceof ZonedTimestampType) {
                                        valueMap.put(
                                                partitionKey, DorisSchemaUtils.DEFAULT_DATETIME);
                                    }
                                });
            }
        }

        byte[] serializedRow =
                serializeValue(tableId, valueMap, outputSchema, op == OperationType.DELETE);
        return DorisRecord.of(tableId.getSchemaName(), tableId.getTableName(), serializedRow);
    }

    /** serializer RecordData to Doris Value. */
    public Map<String, Object> serializerRecord(RecordData recordData, Schema schema) {
        List<Column> columns = schema.getColumns();
        Map<String, Object> record = new HashMap<>();
        Preconditions.checkState(
                columns.size() == recordData.getArity(),
                "Column size does not match the data size");
        for (int i = 0; i < recordData.getArity(); i++) {
            DorisRowConverter.SerializationConverter converter =
                    DorisRowConverter.createNullableExternalConverter(
                            columns.get(i).getType(), pipelineZoneId);
            Object field = converter.serialize(i, recordData);
            record.put(columns.get(i).getName(), field);
        }
        return record;
    }

    private byte[] serializeValue(
            TableId tableId, Map<String, Object> valueMap, Schema schema, boolean deleted)
            throws JsonProcessingException {
        if (CSV.equals(streamLoadFormat)) {
            return serializeCsvRecord(valueMap, schema, deleted);
        }
        if (JSON.equals(streamLoadFormat)) {
            Map<String, Object> outputRecord = projectRecord(tableId, valueMap, schema);
            addDeleteSign(outputRecord, deleted);
            return objectMapper.writeValueAsString(outputRecord).getBytes(StandardCharsets.UTF_8);
        }
        throw new UnsupportedOperationException(
                "Unsupported stream load format for Doris pipeline sink: " + streamLoadFormat);
    }

    private byte[] serializeCsvRecord(
            Map<String, Object> valueMap, Schema schema, boolean deleted) {
        StringJoiner joiner = new StringJoiner(fieldDelimiter);
        for (Column column : schema.getColumns()) {
            joiner.add(toCsvField(valueMap.get(column.getName())));
        }
        joiner.add(deleted ? "1" : "0");
        return joiner.toString().getBytes(StandardCharsets.UTF_8);
    }

    private String toCsvField(Object value) {
        return value == null ? NULL_VALUE : value.toString();
    }

    private Schema resolveInputSchema(
            TableId tableId, Schema currentInputSchema, RecordData recordData) {
        Preconditions.checkNotNull(recordData, "Record data of " + tableId + " is null");
        if (currentInputSchema.getColumnCount() == recordData.getArity()) {
            return currentInputSchema;
        }

        List<Schema> schemaHistory = inputSchemaHistoryMaps.get(tableId);
        if (schemaHistory != null) {
            for (int i = schemaHistory.size() - 1; i >= 0; i--) {
                Schema schema = schemaHistory.get(i);
                if (schema.getColumnCount() == recordData.getArity()) {
                    return schema;
                }
            }
        }

        throw new IllegalStateException(
                String.format(
                        "Unable to resolve input schema for %s. record arity=%s, current schema columns=%s",
                        tableId, recordData.getArity(), currentInputSchema.getColumnCount()));
    }

    private Map<String, Object> projectRecord(
            TableId tableId, Map<String, Object> valueMap, Schema schema) {
        LinkedHashMap<String, String> jsonFieldMapping = getJsonFieldMapping(tableId, schema);
        if (jsonFieldMapping != null && !jsonFieldMapping.isEmpty()) {
            Map<String, Object> outputRecord = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : jsonFieldMapping.entrySet()) {
                String inputFieldName = entry.getValue();
                if (inputFieldName == null) {
                    continue;
                }
                outputRecord.put(entry.getKey(), valueMap.get(inputFieldName));
            }
            return outputRecord;
        }

        Map<String, Object> outputRecord = new LinkedHashMap<>();
        for (Column column : schema.getColumns()) {
            // Fallback for tests / serializers without Doris connection details.
            outputRecord.put(
                    normalizeJsonFieldName(column.getName()), valueMap.get(column.getName()));
        }
        return outputRecord;
    }

    private static String normalizeJsonFieldName(String fieldName) {
        return fieldName.toLowerCase(Locale.ROOT);
    }

    private void rememberInputSchema(TableId tableId, Schema inputSchema) {
        List<Schema> schemaHistory =
                inputSchemaHistoryMaps.computeIfAbsent(tableId, ignored -> new ArrayList<>());
        if (!schemaHistory.isEmpty()
                && schemaHistory.get(schemaHistory.size() - 1).equals(inputSchema)) {
            return;
        }
        schemaHistory.add(inputSchema);
        if (schemaHistory.size() > MAX_SCHEMA_HISTORY_SIZE) {
            schemaHistory.remove(0);
        }
    }

    private void rememberOldColumns(
            TableId tableId, Schema previousInputSchema, SchemaChangeEvent schemaChangeEvent) {
        if (schemaChangeEvent instanceof DropColumnEvent) {
            DropColumnEvent dropColumnEvent = (DropColumnEvent) schemaChangeEvent;
            for (String droppedColumn : dropColumnEvent.getDroppedColumnNames()) {
                addOldColumn(
                        tableId,
                        SchemaUtils.resolveExistingColumnName(previousInputSchema, droppedColumn));
            }
            return;
        }

        if (schemaChangeEvent instanceof RenameColumnEvent) {
            RenameColumnEvent renameColumnEvent = (RenameColumnEvent) schemaChangeEvent;
            renameColumnEvent
                    .getNameMapping()
                    .forEach(
                            (oldColumnName, newColumnName) -> {
                                if (oldColumnName.equalsIgnoreCase(newColumnName)) {
                                    return;
                                }
                                addOldColumn(
                                        tableId,
                                        SchemaUtils.resolveExistingColumnName(
                                                previousInputSchema, oldColumnName));
                            });
        }
    }

    private void addOldColumn(TableId tableId, String columnName) {
        List<String> oldColumns =
                oldInputColumnMaps.computeIfAbsent(tableId, ignored -> new ArrayList<>());
        for (String oldColumn : oldColumns) {
            if (oldColumn.equalsIgnoreCase(columnName)) {
                return;
            }
        }
        oldColumns.add(columnName);
    }

    private void pruneOldColumns(TableId tableId, Schema inputSchema) {
        List<String> oldColumns = oldInputColumnMaps.get(tableId);
        if (oldColumns == null) {
            return;
        }
        oldColumns.removeIf(columnName -> hasInputColumn(inputSchema, columnName));
        if (oldColumns.isEmpty()) {
            oldInputColumnMaps.remove(tableId);
        }
    }

    private boolean hasInputColumn(Schema inputSchema, String columnName) {
        for (Column column : inputSchema.getColumns()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }

    private Schema getCsvOutputSchema(TableId tableId, Schema inputSchema) {
        return csvOutputSchemaMaps.computeIfAbsent(
                tableId, ignored -> resolveCsvOutputSchema(tableId, inputSchema));
    }

    private LinkedHashMap<String, String> getJsonFieldMapping(TableId tableId, Schema inputSchema) {
        if (jsonFieldMappingResolver == null) {
            return null;
        }
        Map<Schema, LinkedHashMap<String, String>> tableMappings =
                jsonFieldMappings.computeIfAbsent(tableId, ignored -> new HashMap<>());
        return tableMappings.computeIfAbsent(
                inputSchema,
                ignored -> {
                    List<String> oldColumns =
                            oldInputColumnMaps.getOrDefault(tableId, new ArrayList<>());
                    LinkedHashMap<String, String> mapping =
                            jsonFieldMappingResolver.resolve(
                                    tableId, inputSchema, new ArrayList<>(oldColumns));
                    Preconditions.checkNotNull(
                            mapping, "JSON field mapping resolver returned null for " + tableId);
                    return mapping;
                });
    }

    private Schema resolveCsvOutputSchema(TableId tableId, Schema inputSchema) {
        Preconditions.checkNotNull(
                csvOutputSchemaResolver,
                "CSV format requires Doris physical schema resolution for " + tableId);
        Schema outputSchema = csvOutputSchemaResolver.resolve(tableId, inputSchema);
        Preconditions.checkNotNull(
                outputSchema, "CSV output schema resolver returned null for " + tableId);
        return outputSchema;
    }

    private static Properties getStreamLoadProperties(DorisExecutionOptions executionOptions) {
        return executionOptions == null ? null : executionOptions.getStreamLoadProp();
    }

    private static CsvOutputSchemaResolver createCsvOutputSchemaResolver(
            Properties streamLoadProperties, DorisOptions dorisOptions) {
        if (streamLoadProperties != null) {
            String columns = streamLoadProperties.getProperty(COLUMNS_KEY);
            if (columns != null && !columns.trim().isEmpty()) {
                return createColumnsPropertyResolver(columns);
            }
        }
        return createDorisPhysicalSchemaResolver(dorisOptions);
    }

    private static CsvOutputSchemaResolver createColumnsPropertyResolver(String columnsProperty) {
        return (tableId, inputSchema) -> {
            List<Column> outputColumns = new ArrayList<>();
            for (String rawColumn : columnsProperty.split(",")) {
                String normalizedColumn = normalizeColumnToken(rawColumn);
                if (normalizedColumn.isEmpty() || DORIS_DELETE_SIGN.equals(normalizedColumn)) {
                    continue;
                }
                Preconditions.checkState(
                        !normalizedColumn.contains("="),
                        "CSV format in Flink CDC Doris pipeline does not support expressions in sink.properties.columns: "
                                + columnsProperty);
                outputColumns.add(
                        DorisSchemaUtils.getColumnCaseInsensitive(inputSchema, normalizedColumn)
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        String.format(
                                                                "Column %s from sink.properties.columns is missing in Flink CDC schema for %s",
                                                                normalizedColumn, tableId))));
            }
            Preconditions.checkState(
                    !outputColumns.isEmpty(),
                    "sink.properties.columns did not contain any physical columns for " + tableId);
            return inputSchema.copy(outputColumns);
        };
    }

    private static String normalizeColumnToken(String rawColumn) {
        String normalizedColumn = rawColumn.trim();
        if (normalizedColumn.startsWith("`") && normalizedColumn.endsWith("`")) {
            normalizedColumn = normalizedColumn.substring(1, normalizedColumn.length() - 1).trim();
        }
        return normalizedColumn;
    }

    private static CsvOutputSchemaResolver createDorisPhysicalSchemaResolver(
            DorisOptions dorisOptions) {
        return createDorisPhysicalSchemaResolver(dorisOptions, DEFAULT_DORIS_SCHEMA_FETCHER);
    }

    static CsvOutputSchemaResolver createDorisPhysicalSchemaResolver(
            DorisOptions dorisOptions, DorisSchemaFetcher dorisSchemaFetcher) {
        if (dorisOptions == null) {
            return null;
        }
        return (tableId, inputSchema) -> {
            org.apache.doris.flink.rest.models.Schema dorisSchema =
                    waitDorisSchemaMatches(
                            dorisOptions,
                            tableId,
                            inputSchema,
                            dorisSchemaFetcher,
                            DORIS_SCHEMA_CATCH_UP_MAX_WAIT_MILLIS,
                            DORIS_SCHEMA_FETCH_RETRY_MILLIS,
                            false);

            List<Column> outputColumns = new ArrayList<>(dorisSchema.getProperties().size());
            for (Field field : dorisSchema.getProperties()) {
                if (DORIS_DELETE_SIGN.equals(field.getName())) {
                    continue;
                }
                outputColumns.add(
                        DorisSchemaUtils.getColumnCaseInsensitive(inputSchema, field.getName())
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        String.format(
                                                                "Column %s from Doris physical schema is missing in Flink CDC schema for %s",
                                                                field.getName(), tableId))));
            }
            return inputSchema.copy(outputColumns);
        };
    }

    private static JsonFieldMappingResolver createJsonFieldMappingResolver(
            DorisOptions dorisOptions, DorisSchemaFetcher dorisSchemaFetcher) {
        return createJsonFieldMappingResolver(
                dorisOptions,
                dorisSchemaFetcher,
                DORIS_SCHEMA_CATCH_UP_MAX_WAIT_MILLIS,
                DORIS_SCHEMA_FETCH_RETRY_MILLIS);
    }

    private static JsonFieldMappingResolver createJsonFieldMappingResolver(
            DorisOptions dorisOptions) {
        return createJsonFieldMappingResolver(dorisOptions, DEFAULT_DORIS_SCHEMA_FETCHER);
    }

    static JsonFieldMappingResolver createJsonFieldMappingResolver(
            DorisOptions dorisOptions,
            DorisSchemaFetcher dorisSchemaFetcher,
            long maxWaitMillis,
            long schemaFetchRetryMillis) {
        if (dorisOptions == null) {
            return null;
        }
        return (tableId, inputSchema, oldColumns) -> {
            org.apache.doris.flink.rest.models.Schema dorisSchema =
                    waitDorisSchemaMatches(
                            dorisOptions,
                            tableId,
                            inputSchema,
                            dorisSchemaFetcher,
                            maxWaitMillis,
                            schemaFetchRetryMillis,
                            true,
                            oldColumns);

            LinkedHashMap<String, String> mapping = new LinkedHashMap<>();
            for (Field field : dorisSchema.getProperties()) {
                if (DORIS_DELETE_SIGN.equals(field.getName())) {
                    continue;
                }
                Column inputColumn =
                        DorisSchemaUtils.getColumnCaseInsensitive(inputSchema, field.getName())
                                .orElse(null);
                if (inputColumn != null) {
                    mapping.put(field.getName(), inputColumn.getName());
                }
            }
            return mapping;
        };
    }

    /**
     * Waits for Doris FE physical schema to cover the input schema before serializing JSON rows.
     *
     * <p>This runs on the sink writer data path, so waiting here can temporarily block {@code
     * processElement} and delay downstream checkpoint alignment when Doris metadata is catching up.
     * The sink keeps this strict wait instead of degrading to stale schema writes, because
     * correctness is more important than short-term throughput during schema transitions.
     */
    static org.apache.doris.flink.rest.models.Schema waitDorisSchemaCovers(
            DorisOptions dorisOptions,
            TableId tableId,
            Schema inputSchema,
            DorisSchemaFetcher dorisSchemaFetcher,
            long maxWaitMillis,
            long schemaFetchRetryMillis) {
        return waitDorisSchemaMatches(
                dorisOptions,
                tableId,
                inputSchema,
                dorisSchemaFetcher,
                maxWaitMillis,
                schemaFetchRetryMillis,
                true,
                new ArrayList<>());
    }

    static org.apache.doris.flink.rest.models.Schema waitDorisSchemaMatches(
            DorisOptions dorisOptions,
            TableId tableId,
            Schema inputSchema,
            DorisSchemaFetcher dorisSchemaFetcher,
            long maxWaitMillis,
            long schemaFetchRetryMillis,
            boolean allowDorisOnlyColumns) {
        return waitDorisSchemaMatches(
                dorisOptions,
                tableId,
                inputSchema,
                dorisSchemaFetcher,
                maxWaitMillis,
                schemaFetchRetryMillis,
                allowDorisOnlyColumns,
                new ArrayList<>());
    }

    static org.apache.doris.flink.rest.models.Schema waitDorisSchemaMatches(
            DorisOptions dorisOptions,
            TableId tableId,
            Schema inputSchema,
            DorisSchemaFetcher dorisSchemaFetcher,
            long maxWaitMillis,
            long schemaFetchRetryMillis,
            boolean allowDorisOnlyColumns,
            List<String> disallowedColumns) {
        Preconditions.checkArgument(maxWaitMillis >= 0, "maxWaitMillis >= 0");
        Preconditions.checkArgument(schemaFetchRetryMillis > 0, "schemaFetchRetryMillis > 0");

        org.apache.doris.flink.rest.models.Schema dorisSchema = null;
        RuntimeException lastFetchException = null;
        boolean waitingLogged = false;
        long waitStartNanos = System.nanoTime();
        long deadlineNanos = waitStartNanos + TimeUnit.MILLISECONDS.toNanos(maxWaitMillis);
        int attempt = 0;

        while (true) {
            attempt++;
            try {
                dorisSchema = dorisSchemaFetcher.fetch(dorisOptions, tableId);
                Preconditions.checkNotNull(
                        dorisSchema, "Failed to resolve Doris physical schema for " + tableId);
                Preconditions.checkNotNull(
                        dorisSchema.getProperties(),
                        "Failed to resolve Doris physical schema properties for " + tableId);
                lastFetchException = null;
            } catch (RuntimeException e) {
                lastFetchException = e;
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - waitStartNanos);
                LOG.warn(
                        "Failed to resolve Doris physical schema for {} on attempt {} after {} ms. Will keep retrying until the {} ms schema catch-up budget is exhausted.",
                        tableId,
                        attempt,
                        elapsedMs,
                        maxWaitMillis,
                        e);
                if (!waitingLogged) {
                    waitingLogged = true;
                    LOG.info(
                            "Waiting for Doris physical schema of {} to catch up after schema change. inputColumns={}, maxWaitMs={}, retryIntervalMs={}",
                            tableId,
                            getInputColumnNames(inputSchema),
                            maxWaitMillis,
                            schemaFetchRetryMillis);
                }
                if (System.nanoTime() >= deadlineNanos) {
                    break;
                }
                sleepBeforeRetry(tableId, deadlineNanos, schemaFetchRetryMillis);
                continue;
            }

            if (dorisSchemaMatches(
                    dorisSchema, inputSchema, allowDorisOnlyColumns, disallowedColumns)) {
                if (waitingLogged) {
                    long elapsedMs =
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - waitStartNanos);
                    LOG.info(
                            "Doris physical schema for {} caught up after {} attempt(s) and {} ms. inputColumns={}, dorisColumns={}",
                            tableId,
                            attempt,
                            elapsedMs,
                            getInputColumnNames(inputSchema),
                            getDorisPhysicalColumnNames(dorisSchema));
                }
                return dorisSchema;
            }

            if (!waitingLogged) {
                waitingLogged = true;
                LOG.info(
                        "Waiting for Doris physical schema of {} to catch up. inputColumns={}, dorisColumns={}, maxWaitMs={}, retryIntervalMs={}",
                        tableId,
                        getInputColumnNames(inputSchema),
                        getDorisPhysicalColumnNames(dorisSchema),
                        maxWaitMillis,
                        schemaFetchRetryMillis);
            }

            if (System.nanoTime() >= deadlineNanos) {
                break;
            }

            sleepBeforeRetry(tableId, deadlineNanos, schemaFetchRetryMillis);
        }

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - waitStartNanos);
        if (lastFetchException != null) {
            LOG.warn(
                    "Failed to resolve Doris physical schema for {} after {} attempt(s) and {} ms while waiting for schema catch-up. inputColumns={}, latestDorisColumns={}",
                    tableId,
                    attempt,
                    elapsedMs,
                    getInputColumnNames(inputSchema),
                    getDorisPhysicalColumnNames(dorisSchema),
                    lastFetchException);
            throw new IllegalStateException(
                    String.format(
                            "Failed to resolve Doris physical schema for %s while waiting for schema catch-up. inputColumns=%s, latestDorisColumns=%s",
                            tableId,
                            getInputColumnNames(inputSchema),
                            getDorisPhysicalColumnNames(dorisSchema)),
                    lastFetchException);
        }

        LOG.warn(
                "Doris physical schema for {} did not catch up after {} attempt(s) and {} ms. inputColumns={}, dorisColumns={}",
                tableId,
                attempt,
                elapsedMs,
                getInputColumnNames(inputSchema),
                getDorisPhysicalColumnNames(dorisSchema));

        throw new IllegalStateException(
                String.format(
                        "Doris physical schema for %s did not catch up with input schema. inputColumns=%s, dorisColumns=%s",
                        tableId,
                        getInputColumnNames(inputSchema),
                        getDorisPhysicalColumnNames(dorisSchema)));
    }

    private static void sleepBeforeRetry(
            TableId tableId, long deadlineNanos, long schemaFetchRetryMillis) {
        long remainingMillis =
                TimeUnit.NANOSECONDS.toMillis(Math.max(0L, deadlineNanos - System.nanoTime()));
        long sleepMillis = Math.min(schemaFetchRetryMillis, Math.max(1L, remainingMillis));
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlinkRuntimeException(
                    "Interrupted while waiting for Doris physical schema of " + tableId, e);
        }
    }

    private static boolean dorisSchemaMatches(
            org.apache.doris.flink.rest.models.Schema dorisSchema,
            Schema inputSchema,
            boolean allowDorisOnlyColumns,
            List<String> disallowedColumns) {
        List<String> dorisColumns = getDorisPhysicalColumnNames(dorisSchema);
        for (Column inputColumn : inputSchema.getColumns()) {
            boolean matched = false;
            for (String dorisColumn : dorisColumns) {
                if (dorisColumn.equalsIgnoreCase(inputColumn.getName())) {
                    matched = true;
                    break;
                }
            }
            if (!matched && !hasColumnIgnoreCase(disallowedColumns, inputColumn.getName())) {
                return false;
            }
        }
        for (String dorisColumn : dorisColumns) {
            if (hasColumnIgnoreCase(disallowedColumns, dorisColumn)) {
                return false;
            }
            boolean matched = false;
            for (Column inputColumn : inputSchema.getColumns()) {
                if (dorisColumn.equalsIgnoreCase(inputColumn.getName())) {
                    matched = true;
                    break;
                }
            }
            if (matched) {
                continue;
            }
            if (!allowDorisOnlyColumns || hasColumnIgnoreCase(disallowedColumns, dorisColumn)) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasColumnIgnoreCase(List<String> columns, String columnName) {
        if (columns == null) {
            return false;
        }
        for (String column : columns) {
            if (column.equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> getInputColumnNames(Schema inputSchema) {
        List<String> inputColumns = new ArrayList<>();
        for (Column column : inputSchema.getColumns()) {
            inputColumns.add(column.getName());
        }
        return inputColumns;
    }

    private static List<String> getDorisPhysicalColumnNames(
            org.apache.doris.flink.rest.models.Schema dorisSchema) {
        List<String> dorisColumns = new ArrayList<>();
        if (dorisSchema == null || dorisSchema.getProperties() == null) {
            return dorisColumns;
        }
        for (Field field : dorisSchema.getProperties()) {
            if (!DORIS_DELETE_SIGN.equals(field.getName())) {
                dorisColumns.add(field.getName());
            }
        }
        return dorisColumns;
    }
}
