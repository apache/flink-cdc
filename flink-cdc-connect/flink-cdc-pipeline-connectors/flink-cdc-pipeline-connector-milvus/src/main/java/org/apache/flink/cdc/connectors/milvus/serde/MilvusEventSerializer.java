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

package org.apache.flink.cdc.connectors.milvus.serde;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkConfig;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusCollectionUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusTypeUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_TABLE_COMMENT;

/** Serializes Flink CDC events into Milvus write operations. */
public class MilvusEventSerializer implements Serializable {

    private static final long serialVersionUID = 1L;

    private final MilvusDataSinkConfig config;
    private final Map<TableId, Schema> schemas;
    private final Map<TableId, MilvusRowConverter> converters;
    private final Map<String, TableId> serializerCollectionOwners;

    public MilvusEventSerializer(MilvusDataSinkConfig config) {
        this.config = config;
        this.schemas = new HashMap<>();
        this.converters = new HashMap<>();
        this.serializerCollectionOwners = new HashMap<>();
    }

    public List<MilvusOperation> serialize(Event event) {
        if (event instanceof SchemaChangeEvent) {
            applySchemaChange((SchemaChangeEvent) event);
            return new ArrayList<>();
        }
        if (event instanceof DataChangeEvent) {
            return applyDataChange((DataChangeEvent) event);
        }
        throw new UnsupportedOperationException("Unsupported event: " + event);
    }

    private void applySchemaChange(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        SchemaChangeEventType type = event.getType();
        if (event instanceof CreateTableEvent) {
            MilvusCollectionUtils.registerCollectionName(
                    tableId, config, serializerCollectionOwners);
            schemas.put(tableId, ((CreateTableEvent) event).getSchema());
            refreshConverter(tableId);
            return;
        }
        if (type == ALTER_TABLE_COMMENT) {
            return;
        }
        if (type != ADD_COLUMN) {
            throw new UnsupportedOperationException(
                    "Milvus sink writer does not support schema evolution type "
                            + type
                            + " for table "
                            + tableId
                            + ".");
        }
        validateAddColumn((AddColumnEvent) event);
        Schema oldSchema = schemas.get(tableId);
        if (oldSchema == null) {
            throw new IllegalStateException(
                    "Cannot apply schema change "
                            + type
                            + " for table "
                            + tableId
                            + " because cached schema is missing.");
        }
        schemas.put(tableId, applyAddColumnEventIdempotently(oldSchema, (AddColumnEvent) event));
        refreshConverter(tableId);
    }

    private void validateAddColumn(AddColumnEvent event) {
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            Column column = columnWithPosition.getAddColumn();
            if (MilvusTypeUtils.findVectorFieldSpec(
                            column.getName(), config.getVectorFields(event.tableId()))
                    .isPresent()) {
                throw new UnsupportedOperationException(
                        "Milvus sink writer does not support adding vector field "
                                + column.getName()
                                + ".");
            }
            MilvusCollectionUtils.buildAddScalarFieldReq(column, config);
        }
    }

    private List<MilvusOperation> applyDataChange(DataChangeEvent event) {
        Schema schema = schemas.get(event.tableId());
        if (schema == null) {
            throw new IllegalStateException(
                    "Schema for table " + event.tableId() + " is missing in Milvus sink writer.");
        }
        String collectionName =
                MilvusCollectionUtils.resolveCollectionName(event.tableId(), config);
        String primaryKey = MilvusCollectionUtils.resolvePrimaryKey(schema, config);
        MilvusRowConverter converter =
                converters.computeIfAbsent(event.tableId(), this::createConverter);

        List<MilvusOperation> operations = new ArrayList<>();
        OperationType op = event.op();
        switch (op) {
            case INSERT:
            case REPLACE:
                requireAfter(event);
                operations.add(
                        MilvusOperation.upsert(
                                event.tableId(),
                                collectionName,
                                converter.extractPartitionName(
                                        event.after(), config.getPartitionField()),
                                converter.convert(event.after())));
                break;
            case UPDATE:
                requireBefore(event);
                requireAfter(event);
                Object beforeKey = converter.extractPrimaryKey(event.before(), primaryKey);
                Object afterKey = converter.extractPrimaryKey(event.after(), primaryKey);
                String beforePartition =
                        converter.extractPartitionName(event.before(), config.getPartitionField());
                String afterPartition =
                        converter.extractPartitionName(event.after(), config.getPartitionField());
                boolean primaryKeyChanged = !beforeKey.equals(afterKey);
                if (primaryKeyChanged && !isPrimaryKeyChangeAllowed()) {
                    throw new IllegalStateException(
                            "Milvus sink rejects UPDATE events that change primary key by default. "
                                    + "Set sink.primary-key-change.mode=allow only when collection-level routing is acceptable.");
                }
                if (primaryKeyChanged || !samePartition(beforePartition, afterPartition)) {
                    operations.add(
                            MilvusOperation.delete(
                                    event.tableId(), collectionName, beforePartition, beforeKey));
                }
                operations.add(
                        MilvusOperation.upsert(
                                event.tableId(),
                                collectionName,
                                afterPartition,
                                converter.convert(event.after())));
                break;
            case DELETE:
                if (config.isDeleteEnabled()) {
                    requireBefore(event);
                    operations.add(
                            MilvusOperation.delete(
                                    event.tableId(),
                                    collectionName,
                                    converter.extractPartitionName(
                                            event.before(), config.getPartitionField()),
                                    converter.extractPrimaryKey(event.before(), primaryKey)));
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + op);
        }
        return operations;
    }

    private void refreshConverter(TableId tableId) {
        converters.put(tableId, createConverter(tableId));
    }

    private Schema applyAddColumnEventIdempotently(Schema oldSchema, AddColumnEvent event) {
        List<AddColumnEvent.ColumnWithPosition> columnsToAdd = new ArrayList<>();
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            Column newColumn = columnWithPosition.getAddColumn();
            java.util.Optional<Column> existingColumn = oldSchema.getColumn(newColumn.getName());
            if (!existingColumn.isPresent()) {
                columnsToAdd.add(columnWithPosition);
                continue;
            }
            validateExistingWriterColumn(existingColumn.get(), newColumn, event.tableId());
        }
        if (columnsToAdd.isEmpty()) {
            return oldSchema;
        }
        return SchemaUtils.applySchemaChangeEvent(
                oldSchema, new AddColumnEvent(event.tableId(), columnsToAdd));
    }

    private void validateExistingWriterColumn(
            Column existingColumn, Column newColumn, TableId tableId) {
        if (!existingColumn.getType().equals(newColumn.getType())
                || existingColumn.isPhysical() != newColumn.isPhysical()) {
            throw new IllegalStateException(
                    "Milvus writer schema already contains column "
                            + newColumn.getName()
                            + " for table "
                            + tableId
                            + " but it is incompatible with replayed ADD_COLUMN.");
        }
    }

    private MilvusRowConverter createConverter(TableId tableId) {
        Schema schema = schemas.get(tableId);
        if (schema == null) {
            throw new IllegalStateException("Schema for table " + tableId + " is missing.");
        }
        return new MilvusRowConverter(
                schema, config.getVectorFields(tableId), config.getVarcharMaxLengthDefault());
    }

    private void requireBefore(DataChangeEvent event) {
        if (event.before() == null) {
            throw new IllegalStateException(
                    event.op() + " event must contain before record for table " + event.tableId());
        }
    }

    private void requireAfter(DataChangeEvent event) {
        if (event.after() == null) {
            throw new IllegalStateException(
                    event.op() + " event must contain after record for table " + event.tableId());
        }
    }

    private boolean samePartition(String left, String right) {
        if (left == null) {
            return right == null;
        }
        return left.equals(right);
    }

    private boolean isPrimaryKeyChangeAllowed() {
        return "allow".equalsIgnoreCase(config.getPrimaryKeyChangeMode());
    }
}
