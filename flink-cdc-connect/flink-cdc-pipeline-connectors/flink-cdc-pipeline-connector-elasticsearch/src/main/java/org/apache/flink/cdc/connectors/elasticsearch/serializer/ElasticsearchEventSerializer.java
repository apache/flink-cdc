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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.SHARDING_SUFFIX_SEPARATOR;

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

    private final Map<TableId, String> shardingKey;
    private final String shardingSeparator;

    public ElasticsearchEventSerializer(ZoneId zoneId) {
        this(zoneId, Collections.emptyMap(), SHARDING_SUFFIX_SEPARATOR.defaultValue());
    }

    public ElasticsearchEventSerializer(
            ZoneId zoneId, Map<TableId, String> shardingKey, String shardingSeparator) {
        this.pipelineZoneId = zoneId;
        this.shardingKey = shardingKey;
        this.shardingSeparator = shardingSeparator;
    }

    @Override
    public BulkOperationVariant apply(Event event, SinkWriter.Context context) {
        try {
            if (event instanceof DataChangeEvent) {
                return createBulkOperationVariant((DataChangeEvent) event);
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
            // Cache new converters
            getOrCreateConverters(tableId, schema);
        } else if (schemaChangeEvent instanceof AddColumnEvent
                || schemaChangeEvent instanceof DropColumnEvent
                || schemaChangeEvent instanceof RenameColumnEvent) {
            if (!schemaMaps.containsKey(tableId)) {
                throw new RuntimeException("Schema of " + tableId + " does not exist.");
            }
            Schema updatedSchema =
                    SchemaUtils.applySchemaChangeEvent(schemaMaps.get(tableId), schemaChangeEvent);
            schemaMaps.put(tableId, updatedSchema);
            // Update cached converters
            getOrCreateConverters(tableId, updatedSchema);
        } else {
            if (!schemaMaps.containsKey(tableId)) {
                throw new RuntimeException("Schema of " + tableId + " does not exist.");
            }
            Schema updatedSchema =
                    SchemaUtils.applySchemaChangeEvent(schemaMaps.get(tableId), schemaChangeEvent);
            schemaMaps.put(tableId, updatedSchema);
            // Update cached converters
            getOrCreateConverters(tableId, updatedSchema);
        }
        return null;
    }

    private BulkOperationVariant createBulkOperationVariant(DataChangeEvent event)
            throws JsonProcessingException {
        TableId tableId = event.tableId();
        Schema schema = schemaMaps.get(tableId);
        Preconditions.checkNotNull(schema, event.tableId() + " does not exist.");
        // Ensure converters are cached
        getOrCreateConverters(tableId, schema);
        Map<String, Object> valueMap;
        OperationType op = event.op();
        Object[] uniqueId =
                generateUniqueId(
                        op == OperationType.DELETE ? event.before() : event.after(),
                        schema,
                        tableId);
        String id = Arrays.stream(uniqueId).map(Object::toString).collect(Collectors.joining("_"));
        switch (op) {
            case INSERT:
            case REPLACE:
            case UPDATE:
                valueMap = serializeRecord(tableId, event.after(), schema, pipelineZoneId);
                return new IndexOperation.Builder<>()
                        .index(tableSharding(tableId, schema, valueMap))
                        .id(id)
                        .document(valueMap)
                        .build();
            case DELETE:
                return new DeleteOperation.Builder().index(tableId.toString()).id(id).build();
            default:
                throw new UnsupportedOperationException("Unsupported Operation " + op);
        }
    }

    public String tableSharding(TableId tableId, Schema schema, Map<String, Object> valueMap) {
        Object value = null;
        if (shardingKey.containsKey(tableId)) {
            value = valueMap.get(shardingKey.get(tableId));
        } else if (!schema.partitionKeys().isEmpty()) {
            value = valueMap.get(schema.partitionKeys().get(0));
        }
        return value != null ? tableId.toString() + shardingSeparator + value : tableId.toString();
    }

    private Object[] generateUniqueId(RecordData recordData, Schema schema, TableId tableId) {
        List<String> primaryKeys = schema.primaryKeys();
        List<ElasticsearchRowConverter.SerializationConverter> converters =
                converterCache.get(tableId);
        Preconditions.checkNotNull(converters, "No converters found for table: " + tableId);

        Object[] uniqueId = new Object[primaryKeys.size()];
        List<Column> columns = schema.getColumns();

        for (int i = 0; i < primaryKeys.size(); i++) {
            String primaryKey = primaryKeys.get(i);
            Column column = null;
            int index = -1;

            for (int j = 0; j < columns.size(); j++) {
                if (columns.get(j).getName().equals(primaryKey)) {
                    column = columns.get(j);
                    index = j;
                    break;
                }
            }

            if (column == null) {
                throw new IllegalStateException("Primary key column not found: " + primaryKey);
            }

            checkIndex(index, converters.size());
            uniqueId[i] = converters.get(index).serialize(index, recordData);
        }

        return uniqueId;
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
            checkIndex(i, converters.size());
            Object field = converters.get(i).serialize(i, recordData);
            record.put(column.getName(), field);
        }
        return record;
    }

    private List<ElasticsearchRowConverter.SerializationConverter> getOrCreateConverters(
            TableId tableId, Schema schema) {
        return converterCache.compute(
                tableId,
                (id, existingConverters) -> {
                    List<ElasticsearchRowConverter.SerializationConverter> converters =
                            new ArrayList<>();
                    for (Column column : schema.getColumns()) {
                        converters.add(
                                ElasticsearchRowConverter.createNullableExternalConverter(
                                        column.getType(), pipelineZoneId));
                    }
                    return converters;
                });
    }

    private void checkIndex(int index, int size) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }

    @Override
    public void open(Sink.InitContext context) {
        ElementConverter.super.open(context);
    }
}
