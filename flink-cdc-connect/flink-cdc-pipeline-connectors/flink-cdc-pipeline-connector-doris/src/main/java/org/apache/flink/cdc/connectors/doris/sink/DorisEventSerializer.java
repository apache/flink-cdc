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
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
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
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;

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

    @FunctionalInterface
    interface CsvOutputSchemaResolver extends Serializable {
        Schema resolve(TableId tableId, Schema inputSchema);
    }

    /** Format DATE type data. */
    public static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Format timestamp-related type data. */
    public static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final int MAX_SCHEMA_HISTORY_SIZE = 16;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<TableId, Schema> inputSchemaMaps = new HashMap<>();
    private final Map<TableId, List<Schema>> inputSchemaHistoryMaps = new HashMap<>();
    private final Map<TableId, Schema> csvOutputSchemaMaps = new HashMap<>();
    private final CsvOutputSchemaResolver csvOutputSchemaResolver;

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
                        getStreamLoadProperties(executionOptions), dorisOptions));
    }

    DorisEventSerializer(
            ZoneId zoneId,
            Configuration config,
            DorisExecutionOptions executionOptions,
            CsvOutputSchemaResolver csvOutputSchemaResolver) {
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
            csvOutputSchemaMaps.remove(tableId);
            return;
        }

        Schema inputSchema;
        if (schemaChangeEvent instanceof CreateTableEvent) {
            inputSchema = ((CreateTableEvent) schemaChangeEvent).getSchema();
        } else {
            Preconditions.checkState(
                    inputSchemaMaps.containsKey(tableId),
                    "schema of " + tableId + " is not existed.");
            inputSchema =
                    SchemaUtils.applySchemaChangeEvent(
                            inputSchemaMaps.get(tableId), schemaChangeEvent);
        }
        inputSchemaMaps.put(tableId, inputSchema);
        rememberInputSchema(tableId, inputSchema);

        if (CSV.equals(streamLoadFormat)) {
            csvOutputSchemaMaps.put(tableId, resolveCsvOutputSchema(tableId, inputSchema));
        } else {
            csvOutputSchemaMaps.remove(tableId);
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

        byte[] serializedRow = serializeValue(valueMap, outputSchema, op == OperationType.DELETE);
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

    private byte[] serializeValue(Map<String, Object> valueMap, Schema schema, boolean deleted)
            throws JsonProcessingException {
        if (CSV.equals(streamLoadFormat)) {
            return serializeCsvRecord(valueMap, schema, deleted);
        }
        if (JSON.equals(streamLoadFormat)) {
            Map<String, Object> outputRecord = projectRecord(valueMap, schema);
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

    private Map<String, Object> projectRecord(Map<String, Object> valueMap, Schema schema) {
        Map<String, Object> outputRecord = new LinkedHashMap<>();
        for (Column column : schema.getColumns()) {
            outputRecord.put(column.getName(), valueMap.get(column.getName()));
        }
        return outputRecord;
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

    private Schema getCsvOutputSchema(TableId tableId, Schema inputSchema) {
        return csvOutputSchemaMaps.computeIfAbsent(
                tableId, ignored -> resolveCsvOutputSchema(tableId, inputSchema));
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
                        inputSchema
                                .getColumn(normalizedColumn)
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
        if (dorisOptions == null) {
            return null;
        }
        return (tableId, inputSchema) -> {
            org.apache.doris.flink.rest.models.Schema dorisSchema =
                    RestService.getSchema(
                            dorisOptions, tableId.getSchemaName(), tableId.getTableName(), LOG);
            Preconditions.checkNotNull(
                    dorisSchema, "Failed to resolve Doris physical schema for " + tableId);
            Preconditions.checkNotNull(
                    dorisSchema.getProperties(),
                    "Failed to resolve Doris physical schema properties for " + tableId);

            List<Column> outputColumns = new ArrayList<>(dorisSchema.getProperties().size());
            for (Field field : dorisSchema.getProperties()) {
                if (DORIS_DELETE_SIGN.equals(field.getName())) {
                    continue;
                }
                outputColumns.add(
                        inputSchema
                                .getColumn(field.getName())
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
}
