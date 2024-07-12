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

package org.apache.flink.cdc.connectors.elasticsearch.serializer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.*;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.*;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.types.DataTypeChecks.*;

/** A serializer for Event to BulkOperationVariant. */
public class ElasticsearchEventSerializer implements ElementConverter<Event, BulkOperationVariant> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<TableId, Schema> schemaMaps = new HashMap<>();
    private final ConcurrentHashMap<TableId, List<ElasticsearchRowConverter.SerializationConverter>>
            converterCache = new ConcurrentHashMap<>();

    /** Format DATE type data. */
    public static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Format timestamp-related type data. */
    public static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    /** ZoneId from pipeline config to support timestamp with local time zone. */
    private final ZoneId pipelineZoneId;

    public ElasticsearchEventSerializer(ZoneId zoneId) {
        this.pipelineZoneId = zoneId;
    }

    @Override
    public BulkOperationVariant apply(Event event, SinkWriter.Context context) {
        try {
            if (event instanceof DataChangeEvent) {
                return applyDataChangeEvent((DataChangeEvent) event);
            } else if (event instanceof SchemaChangeEvent) {
                IndexOperation<Map<String, Object>> indexOperation =
                        applySchemaChangeEvent((SchemaChangeEvent) event);
                if (indexOperation != null) {
                    return indexOperation;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
        return null;
    }

    private IndexOperation<Map<String, Object>> applySchemaChangeEvent(
            SchemaChangeEvent schemaChangeEvent) throws IOException {
        TableId tableId = schemaChangeEvent.tableId();

        if (schemaChangeEvent instanceof CreateTableEvent) {
            Schema schema = ((CreateTableEvent) schemaChangeEvent).getSchema();
            schemaMaps.put(tableId, schema);
            return createSchemaIndexOperation(tableId, schema);
        } else if (schemaChangeEvent instanceof AddColumnEvent
                || schemaChangeEvent instanceof DropColumnEvent
                || schemaChangeEvent instanceof RenameColumnEvent) {
            if (!schemaMaps.containsKey(tableId)) {
                throw new RuntimeException("Schema of " + tableId + " does not exist.");
            }
            Schema updatedSchema =
                    SchemaUtils.applySchemaChangeEvent(schemaMaps.get(tableId), schemaChangeEvent);
            schemaMaps.put(tableId, updatedSchema);
            return createSchemaIndexOperation(tableId, updatedSchema);
        } else {
            if (!schemaMaps.containsKey(tableId)) {
                throw new RuntimeException("Schema of " + tableId + " does not exist.");
            }
            Schema updatedSchema =
                    SchemaUtils.applySchemaChangeEvent(schemaMaps.get(tableId), schemaChangeEvent);
            schemaMaps.put(tableId, updatedSchema);
        }
        return null;
    }

    private IndexOperation<Map<String, Object>> createSchemaIndexOperation(
            TableId tableId, Schema schema) {
        Map<String, Object> schemaMap = new HashMap<>();
        schemaMap.put(
                "columns",
                schema.getColumns().stream()
                        .map(Column::asSummaryString)
                        .collect(Collectors.toList()));
        schemaMap.put("primaryKeys", schema.primaryKeys());
        schemaMap.put("options", schema.options());

        return new IndexOperation.Builder<Map<String, Object>>()
                .index(tableId.toString())
                .id(tableId.getTableName())
                .document(schemaMap)
                .build();
    }

    private BulkOperationVariant applyDataChangeEvent(DataChangeEvent event)
            throws JsonProcessingException {
        TableId tableId = event.tableId();
        Schema schema = schemaMaps.get(tableId);
        Preconditions.checkNotNull(schema, event.tableId() + " does not exist.");
        Map<String, Object> valueMap;
        OperationType op = event.op();

        Object[] uniqueId =
                generateUniqueId(
                        op == OperationType.DELETE ? event.before() : event.after(), schema);
        String id = Arrays.stream(uniqueId).map(Object::toString).collect(Collectors.joining("_"));

        switch (op) {
            case INSERT:
            case REPLACE:
            case UPDATE:
                valueMap = serializeRecord(tableId, event.after(), schema, pipelineZoneId);
                return new IndexOperation.Builder<>()
                        .index(tableId.toString())
                        .id(id)
                        .document(valueMap)
                        .build();
            case DELETE:
                return new DeleteOperation.Builder().index(tableId.toString()).id(id).build();
            default:
                throw new UnsupportedOperationException("Unsupported Operation " + op);
        }
    }

    private Object[] generateUniqueId(RecordData recordData, Schema schema) {
        List<String> primaryKeys = schema.primaryKeys();
        return primaryKeys.stream()
                .map(
                        primaryKey -> {
                            Column column =
                                    schema.getColumns().stream()
                                            .filter(col -> col.getName().equals(primaryKey))
                                            .findFirst()
                                            .orElseThrow(
                                                    () ->
                                                            new IllegalStateException(
                                                                    "Primary key column not found: "
                                                                            + primaryKey));
                            int index = schema.getColumns().indexOf(column);
                            return getFieldValue(recordData, column.getType(), index);
                        })
                .toArray();
    }

    private Object getFieldValue(RecordData recordData, DataType dataType, int index) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return recordData.getBoolean(index);
            case TINYINT:
                return recordData.getByte(index);
            case SMALLINT:
                return recordData.getShort(index);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return recordData.getInt(index);
            case BIGINT:
                return recordData.getLong(index);
            case FLOAT:
                return recordData.getFloat(index);
            case DOUBLE:
                return recordData.getDouble(index);
            case CHAR:
            case VARCHAR:
                return recordData.getString(index);
            case DECIMAL:
                return recordData.getDecimal(index, getPrecision(dataType), getScale(dataType));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return recordData.getTimestamp(index, getPrecision(dataType));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return recordData.getLocalZonedTimestampData(index, getPrecision(dataType));
            case TIMESTAMP_WITH_TIME_ZONE:
                return recordData.getZonedTimestamp(index, getPrecision(dataType));
            case BINARY:
            case VARBINARY:
                return recordData.getBinary(index);
            case ARRAY:
                return recordData.getArray(index);
            case MAP:
                return recordData.getMap(index);
            case ROW:
                return recordData.getRow(index, getFieldCount(dataType));
            default:
                throw new IllegalArgumentException("Unsupported type: " + dataType);
        }
    }

    public Map<String, Object> serializeRecord(
            TableId tableId, RecordData recordData, Schema schema, ZoneId pipelineZoneId) {
        List<Column> columns = schema.getColumns();
        Map<String, Object> record = new HashMap<>();
        Preconditions.checkState(
                columns.size() == recordData.getArity(),
                "Column size does not match the data size.");

        List<ElasticsearchRowConverter.SerializationConverter> converters =
                getOrCreateConverters(tableId, schema);

        for (int i = 0; i < recordData.getArity(); i++) {
            Column column = columns.get(i);
            Object field = converters.get(i).serialize(i, recordData);
            record.put(column.getName(), field);
        }

        return record;
    }

    private List<ElasticsearchRowConverter.SerializationConverter> getOrCreateConverters(
            TableId tableId, Schema schema) {
        return converterCache.computeIfAbsent(
                tableId,
                id -> {
                    List<ElasticsearchRowConverter.SerializationConverter> converters =
                            new ArrayList<>();
                    for (Column column : schema.getColumns()) {
                        ColumnType columnType =
                                ColumnType.valueOf(column.getType().getTypeRoot().name());
                        converters.add(
                                ElasticsearchRowConverter.createNullableExternalConverter(
                                        columnType, pipelineZoneId));
                    }
                    return converters;
                });
    }

    @Override
    public void open(Sink.InitContext context) {
        ElementConverter.super.open(context);
    }
}
