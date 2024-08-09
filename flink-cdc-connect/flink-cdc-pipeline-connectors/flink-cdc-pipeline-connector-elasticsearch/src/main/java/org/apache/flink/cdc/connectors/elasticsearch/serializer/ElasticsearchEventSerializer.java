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
            // 缓存新的转换器
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
            // 更新缓存的转换器
            getOrCreateConverters(tableId, updatedSchema);
        } else {
            if (!schemaMaps.containsKey(tableId)) {
                throw new RuntimeException("Schema of " + tableId + " does not exist.");
            }
            Schema updatedSchema =
                    SchemaUtils.applySchemaChangeEvent(schemaMaps.get(tableId), schemaChangeEvent);
            schemaMaps.put(tableId, updatedSchema);
            // 更新缓存的转换器
            getOrCreateConverters(tableId, updatedSchema);
        }
        return null;
    }

    private BulkOperationVariant createBulkOperationVariant(DataChangeEvent event)
            throws JsonProcessingException {
        TableId tableId = event.tableId();
        Schema schema = schemaMaps.get(tableId);
        Preconditions.checkNotNull(schema, event.tableId() + " does not exist。");
        // 确保转换器已缓存
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

    private Object[] generateUniqueId(RecordData recordData, Schema schema, TableId tableId) {
        List<String> primaryKeys = schema.primaryKeys();
        List<ElasticsearchRowConverter.SerializationConverter> converters =
                converterCache.get(tableId);
        Preconditions.checkNotNull(converters, "No converters found for table: " + tableId);

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
                            // 手动进行索引检查
                            checkIndex(index, converters.size());
                            return converters.get(index).serialize(index, recordData);
                        })
                .toArray();
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
            // 手动进行索引检查
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
                        ColumnType columnType =
                                ColumnType.valueOf(column.getType().getTypeRoot().name());
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
