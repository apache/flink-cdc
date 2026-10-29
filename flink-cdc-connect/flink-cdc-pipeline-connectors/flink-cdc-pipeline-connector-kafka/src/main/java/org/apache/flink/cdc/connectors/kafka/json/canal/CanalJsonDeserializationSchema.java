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

package org.apache.flink.cdc.connectors.kafka.json.canal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Deserialization schema that parses Canal JSON bytes from Kafka into Flink CDC {@link Event}
 * objects.
 *
 * <p>Canal JSON format has the following structure:
 *
 * <pre>{@code
 * {
 *   "old": [{"col1": "old_value"}],
 *   "data": [{"col1": "new_value"}],
 *   "type": "INSERT",
 *   "database": "mydb",
 *   "table": "mytable",
 *   "pkNames": ["col1"]
 * }
 * }</pre>
 *
 * <p>The deserializer infers the table schema from the first message for each table and emits a
 * {@link CreateTableEvent} followed by {@link DataChangeEvent}s.
 *
 * @see <a href="https://github.com/alibaba/canal">Alibaba Canal</a>
 */
public class CanalJsonDeserializationSchema implements KafkaRecordDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CanalJsonDeserializationSchema.class);

    // Canal JSON field names
    private static final String FIELD_DATA = "data";
    private static final String FIELD_OLD = "old";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_DATABASE = "database";
    private static final String FIELD_TABLE = "table";
    private static final String FIELD_PK_NAMES = "pkNames";
    private static final String FIELD_IS_DDL = "isDdl";
    private static final String FIELD_SQL = "sql";

    // Canal operation types
    private static final String OP_INSERT = "INSERT";
    private static final String OP_UPDATE = "UPDATE";
    private static final String OP_DELETE = "DELETE";

    private final boolean schemaInferEnabled;
    @Nullable private final String defaultDatabaseName;
    @Nullable private final String defaultSchemaName;
    @Nullable private final String defaultTableName;

    private transient ObjectMapper objectMapper;

    // Track seen tables to emit CreateTableEvent only once
    private transient Map<TableId, TableSchema> tableSchemas;

    // DDL parser for parsing schema change events
    private transient CanalDdlParser ddlParser;

    /** Holds the inferred schema and column order for a table. */
    static class TableSchema implements Serializable {
        private static final long serialVersionUID = 1L;
        final List<String> columnNames;
        final List<DataType> columnTypes;
        final List<String> primaryKeys;

        TableSchema(
                List<String> columnNames, List<DataType> columnTypes, List<String> primaryKeys) {
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
            this.primaryKeys = primaryKeys;
        }
    }

    public CanalJsonDeserializationSchema(
            boolean schemaInferEnabled,
            @Nullable String defaultDatabaseName,
            @Nullable String defaultSchemaName,
            @Nullable String defaultTableName) {
        this.schemaInferEnabled = schemaInferEnabled;
        this.defaultDatabaseName = defaultDatabaseName;
        this.defaultSchemaName = defaultSchemaName;
        this.defaultTableName = defaultTableName;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.objectMapper = new ObjectMapper();
        this.tableSchemas = new HashMap<>();
        this.ddlParser = new CanalDdlParser();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> out)
            throws IOException {
        if (record.value() == null) {
            LOG.debug("Received null value from Kafka, skipping.");
            return;
        }

        JsonNode root = objectMapper.readTree(record.value());
        if (root == null || root.isNull()) {
            LOG.debug("Received empty JSON from Kafka, skipping.");
            return;
        }

        boolean isDdl = root.has(FIELD_IS_DDL) && root.get(FIELD_IS_DDL).asBoolean();

        if (isDdl) {
            processDdlEvent(root, out);
            return;
        }

        processDmlEvent(root, out);
    }

    private void processDdlEvent(JsonNode root, Collector<Event> out) {
        String database =
                root.has(FIELD_DATABASE) && !root.get(FIELD_DATABASE).isNull()
                        ? root.get(FIELD_DATABASE).asText()
                        : defaultDatabaseName;
        String table =
                root.has(FIELD_TABLE) && !root.get(FIELD_TABLE).isNull()
                        ? root.get(FIELD_TABLE).asText()
                        : defaultTableName;
        String sql =
                root.has(FIELD_SQL) && !root.get(FIELD_SQL).isNull()
                        ? root.get(FIELD_SQL).asText()
                        : null;

        if (sql == null || sql.trim().isEmpty()) {
            LOG.warn("DDL message without SQL statement, skipping.");
            return;
        }

        List<SchemaChangeEvent> events = ddlParser.parse(sql, database, table, defaultSchemaName);
        for (SchemaChangeEvent event : events) {
            out.collect(event);
            if (event instanceof CreateTableEvent) {
                tableSchemas.put(event.tableId(), null);
            } else if (event instanceof org.apache.flink.cdc.common.event.DropTableEvent) {
                tableSchemas.remove(event.tableId());
            }
        }
    }

    private void processDmlEvent(JsonNode root, Collector<Event> out) {
        TableId tableId = extractTableId(root);
        String type = root.has(FIELD_TYPE) ? root.get(FIELD_TYPE).asText() : OP_INSERT;

        List<String> primaryKeys = extractPrimaryKeys(root);

        TableSchema tableSchema = tableSchemas.get(tableId);
        if (tableSchema == null) {
            JsonNode dataNode = root.get(FIELD_DATA);
            if (dataNode != null && dataNode.isArray() && dataNode.size() > 0) {
                JsonNode firstRow = dataNode.get(0);
                tableSchema = inferSchema(firstRow, primaryKeys);
            } else {
                JsonNode oldNode = root.get(FIELD_OLD);
                if (oldNode != null && oldNode.isArray() && oldNode.size() > 0) {
                    JsonNode firstRow = oldNode.get(0);
                    tableSchema = inferSchema(firstRow, primaryKeys);
                } else {
                    LOG.warn("Cannot infer schema for table {} from record, skipping.", tableId);
                    return;
                }
            }
            tableSchemas.put(tableId, tableSchema);

            Schema.Builder schemaBuilder = Schema.newBuilder();
            for (int i = 0; i < tableSchema.columnNames.size(); i++) {
                schemaBuilder.physicalColumn(
                        tableSchema.columnNames.get(i), tableSchema.columnTypes.get(i));
            }
            if (!tableSchema.primaryKeys.isEmpty()) {
                schemaBuilder.primaryKey(tableSchema.primaryKeys);
            }
            out.collect(new CreateTableEvent(tableId, schemaBuilder.build()));
        }

        JsonNode dataNode = root.get(FIELD_DATA);
        JsonNode oldNode = root.get(FIELD_OLD);

        switch (type.toUpperCase()) {
            case OP_INSERT:
                emitInsertEvents(tableId, tableSchema, dataNode, out);
                break;
            case OP_UPDATE:
                emitUpdateEvents(tableId, tableSchema, dataNode, oldNode, out);
                break;
            case OP_DELETE:
                emitDeleteEvents(tableId, tableSchema, dataNode, oldNode, out);
                break;
            default:
                LOG.warn("Unsupported Canal operation type '{}', skipping record.", type);
        }
    }

    private TableId extractTableId(JsonNode root) {
        String database =
                root.has(FIELD_DATABASE) && !root.get(FIELD_DATABASE).isNull()
                        ? root.get(FIELD_DATABASE).asText()
                        : (defaultDatabaseName != null ? defaultDatabaseName : "");
        String table =
                root.has(FIELD_TABLE) && !root.get(FIELD_TABLE).isNull()
                        ? root.get(FIELD_TABLE).asText()
                        : (defaultTableName != null ? defaultTableName : "unknown");

        if (defaultSchemaName != null) {
            return TableId.tableId(database, defaultSchemaName, table);
        } else if (!database.isEmpty()) {
            return TableId.tableId(database, table);
        } else {
            return TableId.tableId(table);
        }
    }

    private List<String> extractPrimaryKeys(JsonNode root) {
        if (!root.has(FIELD_PK_NAMES) || root.get(FIELD_PK_NAMES).isNull()) {
            return Collections.emptyList();
        }
        JsonNode pkNode = root.get(FIELD_PK_NAMES);
        if (!pkNode.isArray()) {
            return Collections.emptyList();
        }
        List<String> primaryKeys = new ArrayList<>();
        for (JsonNode pk : pkNode) {
            primaryKeys.add(pk.asText());
        }
        return primaryKeys;
    }

    private TableSchema inferSchema(JsonNode rowNode, List<String> primaryKeys) {
        List<String> columnNames = new ArrayList<>();
        List<DataType> columnTypes = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> fields = rowNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            columnNames.add(field.getKey());
            if (schemaInferEnabled) {
                columnTypes.add(inferDataType(field.getValue()));
            } else {
                columnTypes.add(DataTypes.STRING());
            }
        }

        return new TableSchema(columnNames, columnTypes, new ArrayList<>(primaryKeys));
    }

    private DataType inferDataType(JsonNode value) {
        if (value == null || value.isNull()) {
            return DataTypes.STRING();
        }
        if (value.isBoolean()) {
            return DataTypes.BOOLEAN();
        }
        if (value.isInt()) {
            return DataTypes.INT();
        }
        if (value.isLong()) {
            return DataTypes.BIGINT();
        }
        if (value.isDouble() || value.isFloat()) {
            return DataTypes.DOUBLE();
        }
        if (value.isBigDecimal()) {
            return DataTypes.DECIMAL(
                    value.decimalValue().precision(), value.decimalValue().scale());
        }
        // Default to STRING for text, objects, arrays
        return DataTypes.STRING();
    }

    private void emitInsertEvents(
            TableId tableId, TableSchema tableSchema, JsonNode dataNode, Collector<Event> out) {
        if (dataNode == null || !dataNode.isArray()) {
            return;
        }
        for (JsonNode row : dataNode) {
            GenericRecordData recordData = convertToRecordData(row, tableSchema);
            out.collect(DataChangeEvent.insertEvent(tableId, recordData));
        }
    }

    private void emitUpdateEvents(
            TableId tableId,
            TableSchema tableSchema,
            JsonNode dataNode,
            JsonNode oldNode,
            Collector<Event> out) {
        if (dataNode == null || !dataNode.isArray()) {
            return;
        }
        int size = dataNode.size();
        for (int i = 0; i < size; i++) {
            GenericRecordData after = convertToRecordData(dataNode.get(i), tableSchema);
            GenericRecordData before = null;
            if (oldNode != null && oldNode.isArray() && i < oldNode.size()) {
                before = convertToRecordData(oldNode.get(i), tableSchema);
            }
            out.collect(DataChangeEvent.updateEvent(tableId, before, after));
        }
    }

    private void emitDeleteEvents(
            TableId tableId,
            TableSchema tableSchema,
            JsonNode dataNode,
            JsonNode oldNode,
            Collector<Event> out) {
        // For DELETE, Canal puts the deleted data in 'data' field
        JsonNode sourceNode = (dataNode != null && dataNode.isArray()) ? dataNode : oldNode;
        if (sourceNode == null || !sourceNode.isArray()) {
            return;
        }
        for (JsonNode row : sourceNode) {
            GenericRecordData recordData = convertToRecordData(row, tableSchema);
            out.collect(DataChangeEvent.deleteEvent(tableId, recordData));
        }
    }

    private GenericRecordData convertToRecordData(JsonNode rowNode, TableSchema tableSchema) {
        int arity = tableSchema.columnNames.size();
        GenericRecordData recordData = new GenericRecordData(arity);
        for (int i = 0; i < arity; i++) {
            String columnName = tableSchema.columnNames.get(i);
            JsonNode value = rowNode.get(columnName);
            recordData.setField(i, convertValue(value, tableSchema.columnTypes.get(i)));
        }
        return recordData;
    }

    private Object convertValue(JsonNode value, DataType dataType) {
        if (value == null || value.isNull()) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryStringData.fromString(value.asText());
            case BOOLEAN:
                return value.asBoolean();
            case TINYINT:
                return (byte) value.asInt();
            case SMALLINT:
                return (short) value.asInt();
            case INTEGER:
                return value.asInt();
            case BIGINT:
                return value.asLong();
            case FLOAT:
                return (float) value.asDouble();
            case DOUBLE:
                return value.asDouble();
            case DECIMAL:
                return DecimalData.fromBigDecimal(
                        value.decimalValue(),
                        ((DecimalType) dataType).getPrecision(),
                        ((DecimalType) dataType).getScale());
            default:
                // Fallback: store as string
                return BinaryStringData.fromString(value.asText());
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }

    @VisibleForTesting
    Map<TableId, TableSchema> getTableSchemas() {
        return tableSchemas;
    }
}
