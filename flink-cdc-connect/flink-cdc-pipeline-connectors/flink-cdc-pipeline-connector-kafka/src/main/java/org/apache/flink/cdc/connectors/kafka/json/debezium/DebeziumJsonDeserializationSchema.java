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

package org.apache.flink.cdc.connectors.kafka.json.debezium;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumPayload.AFTER;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumPayload.BEFORE;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumPayload.OPERATION;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumPayload.SOURCE;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumSource.DATABASE;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumSource.TABLE;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumStruct.PAYLOAD;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumStruct.SCHEMA;

/**
 * Deserialization schema from Debezium JSON to Flink CDC pipeline internal data structure {@link
 * Event}.
 */
public class DebeziumJsonDeserializationSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String tables;
    private final String tablesExclude;

    private transient ObjectMapper mapper;
    private transient DebeziumJsonSchemaParser schemaParser;
    private transient DebeziumJsonRecordDataConverter recordConverter;
    private transient Selectors includeSelectors;
    private transient Selectors excludeSelectors;
    private transient Map<TableId, TableSchemaState> globalTableSchemas;
    private transient Map<PartitionTableKey, Schema> partitionTableSchemas;

    public DebeziumJsonDeserializationSchema() {
        this(null, null);
    }

    public DebeziumJsonDeserializationSchema(String tables, String tablesExclude) {
        this.tables = tables;
        this.tablesExclude = tablesExclude;
    }

    public void open() {
        initialize();
    }

    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> out)
            throws IOException {
        initialize();
        JsonNode root = mapper.readTree(record.value());
        JsonNode payload = root.path(PAYLOAD.getFieldName());
        JsonNode source = payload.path(SOURCE.getFieldName());
        JsonNode opNode = payload.path(OPERATION.getFieldName());
        if (payload.isMissingNode()
                || payload.isNull()
                || opNode.isMissingNode()
                || opNode.isNull()
                || source.path(DATABASE.getFieldName()).isMissingNode()
                || source.path(TABLE.getFieldName()).isMissingNode()) {
            return;
        }

        DebeziumJsonStruct.DebeziumOperation operation =
                DebeziumJsonStruct.DebeziumOperation.fromCode(opNode.asText());
        if (operation == null) {
            return;
        }
        TableId tableId =
                TableId.tableId(
                        source.path(DATABASE.getFieldName()).asText(),
                        source.path(TABLE.getFieldName()).asText());
        if (!acceptTable(tableId)) {
            return;
        }
        JsonNode rowSchemaNode =
                schemaParser.findFieldSchema(
                        root.path(SCHEMA.getFieldName()), AFTER.getFieldName());
        if (rowSchemaNode == null) {
            rowSchemaNode =
                    schemaParser.findFieldSchema(
                            root.path(SCHEMA.getFieldName()), BEFORE.getFieldName());
        }
        if (rowSchemaNode == null || !rowSchemaNode.path("fields").isArray()) {
            throw failure(record, "Debezium value does not contain a before/after row schema.");
        }

        Schema incomingSchema = schemaParser.parseSchema(rowSchemaNode);
        PartitionTableKey partitionTableKey =
                new PartitionTableKey(record.topic(), record.partition(), tableId);
        Schema partitionSchema = partitionTableSchemas.get(partitionTableKey);
        if (partitionSchema != null) {
            validatePartitionEvolution(record, partitionSchema, incomingSchema);
        }

        TableSchemaState state = globalTableSchemas.get(tableId);
        List<Event> schemaEvents = new ArrayList<>();
        if (state == null) {
            state = new TableSchemaState(incomingSchema);
            globalTableSchemas.put(tableId, state);
            schemaEvents.add(new CreateTableEvent(tableId, incomingSchema));
        } else {
            evolveGlobalSchema(record, tableId, state, incomingSchema, schemaEvents);
        }
        partitionTableSchemas.put(partitionTableKey, incomingSchema);
        for (Event schemaEvent : schemaEvents) {
            out.collect(schemaEvent);
        }

        Map<String, String> meta = new LinkedHashMap<>();
        meta.put("topic", record.topic());
        meta.put("partition", String.valueOf(record.partition()));
        meta.put("offset", String.valueOf(record.offset()));
        RecordData before =
                recordConverter.convertRecord(payload.get(BEFORE.getFieldName()), state.schema);
        RecordData after =
                recordConverter.convertRecord(payload.get(AFTER.getFieldName()), state.schema);
        switch (operation) {
            case READ:
            case CREATE:
                out.collect(DataChangeEvent.insertEvent(tableId, require(after, "after"), meta));
                break;
            case UPDATE:
                out.collect(
                        DataChangeEvent.updateEvent(
                                tableId, require(before, "before"), require(after, "after"), meta));
                break;
            case DELETE:
                out.collect(DataChangeEvent.deleteEvent(tableId, require(before, "before"), meta));
                break;
            default:
                throw new IllegalStateException("Unexpected Debezium operation " + operation);
        }
    }

    private void initialize() {
        if (mapper != null) {
            return;
        }
        mapper = new ObjectMapper();
        schemaParser = new DebeziumJsonSchemaParser();
        recordConverter = new DebeziumJsonRecordDataConverter();
        globalTableSchemas = new HashMap<>();
        partitionTableSchemas = new HashMap<>();
        if (tables != null) {
            includeSelectors = new Selectors.SelectorsBuilder().includeTables(tables).build();
        }
        if (tablesExclude != null) {
            excludeSelectors =
                    new Selectors.SelectorsBuilder().includeTables(tablesExclude).build();
        }
    }

    private boolean acceptTable(TableId tableId) {
        if (includeSelectors != null && !includeSelectors.isMatch(tableId)) {
            return false;
        }
        return excludeSelectors == null || !excludeSelectors.isMatch(tableId);
    }

    private void validatePartitionEvolution(
            ConsumerRecord<byte[], byte[]> record, Schema previous, Schema incoming) {
        Map<String, Column> incomingColumns = columnsByName(incoming);
        for (Column previousColumn : previous.getColumns()) {
            Column incomingColumn = incomingColumns.get(previousColumn.getName());
            if (incomingColumn == null) {
                // Dropped or renamed source columns stay in the widest schema. New names are
                // added later; missing values are coerced to null.
                continue;
            }
            DataType merged = mergeType(previousColumn.getType(), incomingColumn.getType());
            if (merged == null || !merged.equals(incomingColumn.getType())) {
                throw failure(
                        record,
                        "Incompatible or narrowing type change for column '"
                                + previousColumn.getName()
                                + "' within Kafka partition: "
                                + previousColumn.getType()
                                + " -> "
                                + incomingColumn.getType()
                                + ".");
            }
        }
    }

    private void evolveGlobalSchema(
            ConsumerRecord<byte[], byte[]> record,
            TableId tableId,
            TableSchemaState state,
            Schema incoming,
            List<Event> events) {
        List<Column> widestColumns = new ArrayList<>(state.schema.getColumns());
        List<AddColumnEvent.ColumnWithPosition> additions = new ArrayList<>();
        Map<String, DataType> alteredTypes = new LinkedHashMap<>();
        Map<String, DataType> oldTypes = new LinkedHashMap<>();
        Map<String, Integer> currentPositions = new HashMap<>();
        for (int i = 0; i < widestColumns.size(); i++) {
            currentPositions.put(widestColumns.get(i).getName(), i);
        }
        Map<String, Column> incomingColumns = columnsByName(incoming);
        for (int i = 0; i < widestColumns.size(); i++) {
            Column currentColumn = widestColumns.get(i);
            if (!incomingColumns.containsKey(currentColumn.getName())
                    && !currentColumn.getType().isNullable()) {
                DataType nullableType = currentColumn.getType().nullable();
                widestColumns.set(i, Column.physicalColumn(currentColumn.getName(), nullableType));
                alteredTypes.put(currentColumn.getName(), nullableType);
                oldTypes.put(currentColumn.getName(), currentColumn.getType());
            }
        }
        for (Column incomingColumn : incoming.getColumns()) {
            Integer position = currentPositions.get(incomingColumn.getName());
            if (position == null) {
                Column nullableColumn =
                        Column.physicalColumn(
                                incomingColumn.getName(), incomingColumn.getType().nullable());
                currentPositions.put(nullableColumn.getName(), widestColumns.size());
                widestColumns.add(nullableColumn);
                additions.add(AddColumnEvent.last(nullableColumn));
                continue;
            }
            Column currentColumn = widestColumns.get(position);
            DataType merged = mergeType(currentColumn.getType(), incomingColumn.getType());
            if (merged == null) {
                DataType reverseMerged =
                        mergeType(incomingColumn.getType(), currentColumn.getType());
                if (reverseMerged != null && reverseMerged.equals(currentColumn.getType())) {
                    continue;
                }
                throw failure(
                        record,
                        "Incompatible type change for column '"
                                + incomingColumn.getName()
                                + "': "
                                + currentColumn.getType()
                                + " versus "
                                + incomingColumn.getType()
                                + ".");
            }
            if (!merged.equals(currentColumn.getType())) {
                widestColumns.set(position, Column.physicalColumn(currentColumn.getName(), merged));
                alteredTypes.put(currentColumn.getName(), merged);
                oldTypes.put(currentColumn.getName(), currentColumn.getType());
            }
        }
        if (!additions.isEmpty()) {
            events.add(new AddColumnEvent(tableId, additions));
        }
        if (!alteredTypes.isEmpty()) {
            events.add(new AlterColumnTypeEvent(tableId, alteredTypes, oldTypes));
        }
        if (!additions.isEmpty() || !alteredTypes.isEmpty()) {
            state.schema = state.schema.copy(widestColumns);
        }
    }

    private DataType mergeType(DataType current, DataType incoming) {
        boolean nullable = current.isNullable() || incoming.isNullable();
        DataType currentNullable = current.copy(nullable);
        DataType incomingNullable = incoming.copy(nullable);
        if (currentNullable.equals(incomingNullable)) {
            return currentNullable;
        }
        // STRING is the universal widening target used by SchemaMergingUtils. Replay of an
        // INT → STRING change (MySQL ALTER to VARCHAR) must follow the same rule.
        if (incoming.is(DataTypeRoot.VARCHAR)) {
            return DataTypes.STRING().copy(nullable);
        }
        int currentRank = integerRank(current.getTypeRoot());
        int incomingRank = integerRank(incoming.getTypeRoot());
        if (currentRank > 0 && incomingRank > currentRank) {
            return incomingNullable;
        }
        if (current.is(DataTypeRoot.FLOAT) && incoming.is(DataTypeRoot.DOUBLE)) {
            return incomingNullable;
        }
        if (current.is(DataTypeRoot.VARCHAR)
                && incoming.is(DataTypeRoot.VARCHAR)
                && DataTypes.getLength(incoming).orElse(0)
                        > DataTypes.getLength(current).orElse(0)) {
            return DataTypes.VARCHAR(DataTypes.getLength(incoming).getAsInt()).copy(nullable);
        }
        if (current.is(DataTypeRoot.VARBINARY)
                && incoming.is(DataTypeRoot.VARBINARY)
                && DataTypes.getLength(incoming).orElse(0)
                        > DataTypes.getLength(current).orElse(0)) {
            return DataTypes.VARBINARY(DataTypes.getLength(incoming).getAsInt()).copy(nullable);
        }
        if (current.getTypeRoot() == incoming.getTypeRoot()
                && DataTypes.getPrecision(current).isPresent()
                && DataTypes.getPrecision(incoming).isPresent()
                && DataTypes.getPrecision(incoming).getAsInt()
                        > DataTypes.getPrecision(current).getAsInt()) {
            return incomingNullable;
        }
        if (current.is(DataTypeRoot.DECIMAL) && incoming.is(DataTypeRoot.DECIMAL)) {
            DecimalType left = (DecimalType) current;
            DecimalType right = (DecimalType) incoming;
            int scale = Math.max(left.getScale(), right.getScale());
            int integerDigits =
                    Math.max(
                            left.getPrecision() - left.getScale(),
                            right.getPrecision() - right.getScale());
            int precision = integerDigits + scale;
            if (precision <= 38 && (precision > left.getPrecision() || scale > left.getScale())) {
                return DataTypes.DECIMAL(precision, scale).copy(nullable);
            }
        }
        return null;
    }

    private int integerRank(DataTypeRoot root) {
        switch (root) {
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
                return 3;
            case BIGINT:
                return 4;
            default:
                return 0;
        }
    }

    private Map<String, Column> columnsByName(Schema schema) {
        Map<String, Column> result = new HashMap<>();
        for (Column column : schema.getColumns()) {
            result.put(column.getName(), column);
        }
        return result;
    }

    private RecordData require(RecordData record, String name) {
        return Objects.requireNonNull(record, "Debezium operation requires non-null " + name + ".");
    }

    private IllegalArgumentException failure(
            ConsumerRecord<byte[], byte[]> record, String message) {
        return new IllegalArgumentException(
                message
                        + " Kafka position "
                        + record.topic()
                        + "-"
                        + record.partition()
                        + "@"
                        + record.offset());
    }

    private static class TableSchemaState {
        private Schema schema;

        private TableSchemaState(Schema schema) {
            this.schema = schema;
        }
    }

    private static class PartitionTableKey {
        private final String topic;
        private final int partition;
        private final TableId tableId;

        private PartitionTableKey(String topic, int partition, TableId tableId) {
            this.topic = topic;
            this.partition = partition;
            this.tableId = tableId;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof PartitionTableKey)) {
                return false;
            }
            PartitionTableKey that = (PartitionTableKey) object;
            return partition == that.partition
                    && Objects.equals(topic, that.topic)
                    && Objects.equals(tableId, that.tableId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition, tableId);
        }
    }
}
