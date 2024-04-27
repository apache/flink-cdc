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

package org.apache.flink.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.ChangeEventUtils;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Derive schema changes based on the routing rules. */
public class SchemaDerivation {
    private final SchemaManager schemaManager;
    private final List<Tuple2<Selectors, TableId>> routes;
    private final Map<TableId, Set<TableId>> derivationMapping;

    public SchemaDerivation(
            SchemaManager schemaManager,
            List<Tuple2<Selectors, TableId>> routes,
            Map<TableId, Set<TableId>> derivationMapping) {
        this.schemaManager = schemaManager;
        this.routes = routes;
        this.derivationMapping = derivationMapping;
    }

    public List<SchemaChangeEvent> applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        for (Tuple2<Selectors, TableId> route : routes) {
            TableId originalTable = schemaChangeEvent.tableId();

            // Check routing table
            if (!route.f0.isMatch(originalTable)) {
                continue;
            }

            // Matched a routing rule
            TableId derivedTable = route.f1;
            Set<TableId> originalTables =
                    derivationMapping.computeIfAbsent(derivedTable, t -> new HashSet<>());
            originalTables.add(originalTable);

            if (originalTables.size() == 1) {
                // 1-to-1 mapping. Replace the table ID directly
                SchemaChangeEvent derivedSchemaChangeEvent =
                        ChangeEventUtils.recreateSchemaChangeEvent(schemaChangeEvent, derivedTable);
                schemaManager.applySchemaChange(derivedSchemaChangeEvent);
                return Collections.singletonList(derivedSchemaChangeEvent);
            }

            // Many-to-1 mapping (merging tables)
            Schema derivedTableSchema = schemaManager.getLatestSchema(derivedTable).get();
            if (schemaChangeEvent instanceof CreateTableEvent) {
                return handleCreateTableEvent(
                        (CreateTableEvent) schemaChangeEvent, derivedTableSchema, derivedTable);
            } else if (schemaChangeEvent instanceof AddColumnEvent) {
                return handleAddColumnEvent(
                        (AddColumnEvent) schemaChangeEvent, derivedTableSchema, derivedTable);
            } else if (schemaChangeEvent instanceof AlterColumnTypeEvent) {
                return handleAlterColumnTypeEvent(
                        (AlterColumnTypeEvent) schemaChangeEvent, derivedTableSchema, derivedTable);
            } else if (schemaChangeEvent instanceof DropColumnEvent) {
                return Collections.emptyList();
            } else if (schemaChangeEvent instanceof RenameColumnEvent) {
                return handleRenameColumnEvent(
                        (RenameColumnEvent) schemaChangeEvent, derivedTableSchema, derivedTable);
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Unrecognized SchemaChangeEvent type: %s", schemaChangeEvent));
            }
        }

        // No routes are matched
        return Collections.singletonList(schemaChangeEvent);
    }

    public Map<TableId, Set<TableId>> getDerivationMapping() {
        return derivationMapping;
    }

    public static void serializeDerivationMapping(
            SchemaDerivation schemaDerivation, DataOutputStream out) throws IOException {
        TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
        // Serialize derivation mapping in SchemaDerivation
        Map<TableId, Set<TableId>> derivationMapping = schemaDerivation.getDerivationMapping();
        out.writeInt(derivationMapping.size());
        for (Map.Entry<TableId, Set<TableId>> entry : derivationMapping.entrySet()) {
            // Routed table ID
            TableId routedTableId = entry.getKey();
            tableIdSerializer.serialize(routedTableId, new DataOutputViewStreamWrapper(out));
            // Original table IDs
            Set<TableId> originalTableIds = entry.getValue();
            out.writeInt(originalTableIds.size());
            for (TableId originalTableId : originalTableIds) {
                tableIdSerializer.serialize(originalTableId, new DataOutputViewStreamWrapper(out));
            }
        }
    }

    public static Map<TableId, Set<TableId>> deserializerDerivationMapping(DataInputStream in)
            throws IOException {
        TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
        int derivationMappingSize = in.readInt();
        Map<TableId, Set<TableId>> derivationMapping = new HashMap<>(derivationMappingSize);
        for (int i = 0; i < derivationMappingSize; i++) {
            // Routed table ID
            TableId routedTableId =
                    tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
            // Original table IDs
            int numOriginalTables = in.readInt();
            Set<TableId> originalTableIds = new HashSet<>(numOriginalTables);
            for (int j = 0; j < numOriginalTables; j++) {
                TableId originalTableId =
                        tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
                originalTableIds.add(originalTableId);
            }
            derivationMapping.put(routedTableId, originalTableIds);
        }
        return derivationMapping;
    }

    private List<SchemaChangeEvent> handleRenameColumnEvent(
            RenameColumnEvent renameColumnEvent, Schema derivedTableSchema, TableId derivedTable) {
        List<AddColumnEvent.ColumnWithPosition> newColumns = new ArrayList<>();
        renameColumnEvent
                .getNameMapping()
                .forEach(
                        (before, after) -> {
                            if (derivedTableSchema.getColumn(after).isPresent()) {
                                return;
                            }
                            Column existedColumn = derivedTableSchema.getColumn(before).get();
                            newColumns.add(
                                    new AddColumnEvent.ColumnWithPosition(
                                            new PhysicalColumn(
                                                    after,
                                                    existedColumn.getType(),
                                                    existedColumn.getComment())));
                        });
        List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>();
        if (!newColumns.isEmpty()) {
            AddColumnEvent derivedSchemaChangeEvent = new AddColumnEvent(derivedTable, newColumns);
            schemaChangeEvents.add(derivedSchemaChangeEvent);
        }
        schemaChangeEvents.forEach(schemaManager::applySchemaChange);
        return schemaChangeEvents;
    }

    private List<SchemaChangeEvent> handleAlterColumnTypeEvent(
            AlterColumnTypeEvent alterColumnTypeEvent,
            Schema derivedTableSchema,
            TableId derivedTable) {
        Map<String, DataType> typeDifference = new HashMap<>();
        alterColumnTypeEvent
                .getTypeMapping()
                .forEach(
                        (columnName, dataType) -> {
                            Column existedColumnInDerivedTable =
                                    derivedTableSchema.getColumn(columnName).get();
                            if (!existedColumnInDerivedTable.getType().equals(dataType)) {
                                // Check type compatibility
                                DataType widerType =
                                        getWiderType(
                                                existedColumnInDerivedTable.getType(), dataType);
                                if (!widerType.equals(existedColumnInDerivedTable.getType())) {
                                    typeDifference.put(
                                            existedColumnInDerivedTable.getName(), widerType);
                                }
                            }
                        });
        List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>();
        if (!typeDifference.isEmpty()) {
            AlterColumnTypeEvent derivedSchemaChangeEvent =
                    new AlterColumnTypeEvent(derivedTable, typeDifference);
            schemaChangeEvents.add(derivedSchemaChangeEvent);
        }
        schemaChangeEvents.forEach(schemaManager::applySchemaChange);
        return schemaChangeEvents;
    }

    private List<SchemaChangeEvent> handleAddColumnEvent(
            AddColumnEvent addColumnEvent, Schema derivedTableSchema, TableId derivedTable) {
        List<AddColumnEvent.ColumnWithPosition> newColumns = new ArrayList<>();
        Map<String, DataType> newTypeMapping = new HashMap<>();
        // Check if new column already existed in the derived table
        for (AddColumnEvent.ColumnWithPosition addedColumn : addColumnEvent.getAddedColumns()) {
            Optional<Column> optionalColumnInDerivedTable =
                    derivedTableSchema.getColumn(addedColumn.getAddColumn().getName());
            if (!optionalColumnInDerivedTable.isPresent()) {
                // Non-existed column. Use AddColumn
                newColumns.add(new AddColumnEvent.ColumnWithPosition(addedColumn.getAddColumn()));
            } else {
                // Existed column. Check type compatibility
                Column existedColumnInDerivedTable = optionalColumnInDerivedTable.get();
                if (!existedColumnInDerivedTable
                        .getType()
                        .equals(addedColumn.getAddColumn().getType())) {
                    DataType widerType =
                            getWiderType(
                                    existedColumnInDerivedTable.getType(),
                                    addedColumn.getAddColumn().getType());
                    if (!widerType.equals(existedColumnInDerivedTable.getType())) {
                        newTypeMapping.put(existedColumnInDerivedTable.getName(), widerType);
                    }
                }
            }
        }

        List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>();
        if (!newColumns.isEmpty()) {
            schemaChangeEvents.add(new AddColumnEvent(derivedTable, newColumns));
        }
        if (!newTypeMapping.isEmpty()) {
            schemaChangeEvents.add(new AlterColumnTypeEvent(derivedTable, newTypeMapping));
        }
        schemaChangeEvents.forEach(schemaManager::applySchemaChange);
        return schemaChangeEvents;
    }

    private List<SchemaChangeEvent> handleCreateTableEvent(
            CreateTableEvent createTableEvent, Schema derivedTableSchema, TableId derivedTable) {
        List<AddColumnEvent.ColumnWithPosition> newColumns = new ArrayList<>();
        Map<String, DataType> newTypeMapping = new HashMap<>();
        // Check if there is any columns that doesn't exist in the derived table
        // and perform add-column for non-existed columns.
        for (Column column : createTableEvent.getSchema().getColumns()) {
            Optional<Column> optionalColumnInDerivedTable =
                    derivedTableSchema.getColumn(column.getName());
            if (!optionalColumnInDerivedTable.isPresent()) {
                // Non-existed column. Use AddColumn
                newColumns.add(new AddColumnEvent.ColumnWithPosition(column));
            } else {
                // Existed column. Check type compatibility
                Column existedColumnInDerivedTable = optionalColumnInDerivedTable.get();
                if (!existedColumnInDerivedTable.getType().equals(column.getType())) {
                    DataType widerType =
                            getWiderType(existedColumnInDerivedTable.getType(), column.getType());
                    if (!widerType.equals(existedColumnInDerivedTable.getType())) {
                        newTypeMapping.put(existedColumnInDerivedTable.getName(), widerType);
                    }
                }
            }
        }

        List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>();
        if (!newColumns.isEmpty()) {
            schemaChangeEvents.add(new AddColumnEvent(derivedTable, newColumns));
        }
        if (!newTypeMapping.isEmpty()) {
            schemaChangeEvents.add(new AlterColumnTypeEvent(derivedTable, newTypeMapping));
        }
        schemaChangeEvents.forEach(schemaManager::applySchemaChange);
        return schemaChangeEvents;
    }

    private DataType getWiderType(DataType thisType, DataType thatType) {
        if (thisType.equals(thatType)) {
            return thisType;
        }
        if (thisType.is(DataTypeFamily.INTEGER_NUMERIC)
                && thatType.is(DataTypeFamily.INTEGER_NUMERIC)) {
            return DataTypes.BIGINT();
        }
        if (thisType.is(DataTypeFamily.CHARACTER_STRING)
                && thatType.is(DataTypeFamily.CHARACTER_STRING)) {
            return DataTypes.STRING();
        }
        if (thisType.is(DataTypeFamily.APPROXIMATE_NUMERIC)
                && thatType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
            return DataTypes.DOUBLE();
        }
        throw new IllegalStateException(
                String.format("Incompatible types: \"%s\" and \"%s\"", thisType, thatType));
    }
}
