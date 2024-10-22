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

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;
import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/**
 * A {@code MetadataApplier} that applies metadata changes to Paimon. Support primary key table
 * only.
 */
public class PaimonMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonMetadataApplier.class);

    // Catalog is unSerializable.
    private transient Catalog catalog;

    // currently, we set table options for all tables using the same options.
    private final Map<String, String> tableOptions;

    private final Options catalogOptions;

    private final Map<TableId, List<String>> partitionMaps;

    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    public final boolean ignoreIncompatibleOnRestart;

    public PaimonMetadataApplier(Options catalogOptions) {
        this(catalogOptions, new HashMap<>(), new HashMap<>(), true);
    }

    public PaimonMetadataApplier(
            Options catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps) {
        this(catalogOptions, tableOptions, partitionMaps, true);
    }

    public PaimonMetadataApplier(
            Options catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps,
            boolean ignoreIncompatibleOnRestart) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = tableOptions;
        this.partitionMaps = partitionMaps;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
        this.ignoreIncompatibleOnRestart = ignoreIncompatibleOnRestart;
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Sets.newHashSet(
                SchemaChangeEventType.CREATE_TABLE,
                SchemaChangeEventType.ADD_COLUMN,
                SchemaChangeEventType.DROP_COLUMN,
                SchemaChangeEventType.RENAME_COLUMN,
                SchemaChangeEventType.ALTER_COLUMN_TYPE);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (catalog == null) {
            catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        }
        SchemaChangeEventVisitor.visit(
                schemaChangeEvent,
                addColumnEvent -> {
                    applyAddColumn(addColumnEvent);
                    return null;
                },
                alterColumnTypeEvent -> {
                    applyAlterColumnType(alterColumnTypeEvent);
                    return null;
                },
                createTableEvent -> {
                    applyCreateTable(createTableEvent);
                    return null;
                },
                dropColumnEvent -> {
                    applyDropColumn(dropColumnEvent);
                    return null;
                },
                dropTableEvent -> {
                    throw new UnsupportedSchemaChangeEventException(dropTableEvent);
                },
                renameColumnEvent -> {
                    applyRenameColumn(renameColumnEvent);
                    return null;
                },
                truncateTableEvent -> {
                    throw new UnsupportedSchemaChangeEventException(truncateTableEvent);
                });
    }

    private void applyCreateTable(CreateTableEvent event) throws SchemaEvolveException {
        try {
            if (!catalog.databaseExists(event.tableId().getSchemaName())) {
                catalog.createDatabase(event.tableId().getSchemaName(), true);
            }
            Identifier identifier =
                    new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName());

            if (catalog.tableExists(identifier)) {
                applyAlterTableOptions(identifier);
                if (!ignoreIncompatibleOnRestart) {
                    checkColumnsCompatibility(event, identifier);
                }
            } else {
                Schema schema = event.getSchema();
                org.apache.paimon.schema.Schema.Builder builder =
                        new org.apache.paimon.schema.Schema.Builder();
                schema.getColumns()
                        .forEach(
                                (column) ->
                                        builder.column(
                                                column.getName(),
                                                LogicalTypeConversion.toDataType(
                                                        DataTypeUtils.toFlinkDataType(
                                                                        column.getType())
                                                                .getLogicalType())));
                builder.primaryKey(schema.primaryKeys().toArray(new String[0]));
                if (partitionMaps.containsKey(event.tableId())) {
                    builder.partitionKeys(partitionMaps.get(event.tableId()));
                } else if (schema.partitionKeys() != null && !schema.partitionKeys().isEmpty()) {
                    builder.partitionKeys(schema.partitionKeys());
                }
                builder.options(tableOptions);
                builder.options(schema.options());
                catalog.createTable(
                        new Identifier(
                                event.tableId().getSchemaName(), event.tableId().getTableName()),
                        builder.build(),
                        true);
            }
        } catch (Catalog.TableAlreadyExistException
                | Catalog.DatabaseNotExistException
                | Catalog.DatabaseAlreadyExistException
                | Catalog.TableNotExistException e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void checkColumnsCompatibility(CreateTableEvent event, Identifier identifier)
            throws Catalog.TableNotExistException {
        Table paimonTable = catalog.getTable(identifier);
        List<String> partitionKeys = paimonTable.partitionKeys();
        List<String> primaryKeys = paimonTable.primaryKeys();

        Map<String, DataField> oldColumns = new HashMap<>();
        for (DataField oldField : paimonTable.rowType().getFields()) {
            oldColumns.put(oldField.name(), oldField);
        }
        List<Column> newColumns = event.getSchema().getColumns();
        String referenceColumnName = null;

        List<AddColumnEvent.ColumnWithPosition> addColumns = new ArrayList<>();
        Map<String, DataType> columnTypeMap = new HashMap<>();
        for (int i = 0; i < newColumns.size(); i++) {
            Column newColumn = newColumns.get(i);
            String columnName = newColumn.getName();

            if (primaryKeys.contains(columnName) || partitionKeys.contains(columnName)) {
                referenceColumnName = columnName;
                continue;
            }

            if (oldColumns.containsKey(columnName)) {
                // alter column type
                if (!oldColumns
                        .get(columnName)
                        .type()
                        .equals(
                                LogicalTypeConversion.toDataType(
                                        DataTypeUtils.toFlinkDataType(newColumn.getType())
                                                .getLogicalType()))) {
                    columnTypeMap.put(columnName, newColumn.getType());
                }
            } else {
                // add column
                if (i == 0) {
                    addColumns.add(
                            AddColumnEvent.first(
                                    Column.physicalColumn(
                                            columnName,
                                            newColumn.getType(),
                                            newColumn.getComment())));
                } else {
                    addColumns.add(
                            AddColumnEvent.after(
                                    Column.physicalColumn(
                                            columnName,
                                            newColumn.getType(),
                                            newColumn.getComment()),
                                    referenceColumnName));
                }
            }
            referenceColumnName = columnName;
        }
        if (!columnTypeMap.isEmpty()) {
            applyAlterColumnType(new AlterColumnTypeEvent(event.tableId(), columnTypeMap));
        }
        if (!addColumns.isEmpty()) {
            applyAddColumn(new AddColumnEvent(event.tableId(), addColumns));
        }
    }

    private void applyAlterTableOptions(Identifier identifier) {
        try {
            Table paimonTable = catalog.getTable(identifier);
            Map<String, String> oldOptions = paimonTable.options();
            Set<String> immutableOptionKeys = CoreOptions.getImmutableOptionKeys();
            List<SchemaChange> tableOptionChanges = new ArrayList<>();

            // Remove options that are no longer present
            oldOptions.keySet().stream()
                    .filter(key -> !tableOptions.containsKey(key))
                    .forEach(key -> tableOptionChanges.add(SchemaChange.removeOption(key)));

            // Add or update options
            tableOptions.entrySet().stream()
                    .filter(entry -> !immutableOptionKeys.contains(entry.getKey()))
                    .filter(entry -> !entry.getKey().equals(CoreOptions.BUCKET.key()))
                    .filter(
                            entry ->
                                    !Objects.equals(
                                            oldOptions.get(entry.getKey()), entry.getValue()))
                    .forEach(
                            entry ->
                                    tableOptionChanges.add(
                                            SchemaChange.setOption(
                                                    entry.getKey(), entry.getValue())));

            if (!tableOptionChanges.isEmpty()) {
                catalog.alterTable(identifier, tableOptionChanges, true);
            }
        } catch (Catalog.TableNotExistException
                | Catalog.ColumnAlreadyExistException
                | Catalog.ColumnNotExistException e) {
            throw new RuntimeException("This is unexpected.", e);
        }
    }

    private void applyAddColumn(AddColumnEvent event) throws SchemaEvolveException {
        try {
            List<SchemaChange> tableChangeList = applyAddColumnEventWithPosition(event);
            catalog.alterTable(
                    new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                    tableChangeList,
                    true);
        } catch (Catalog.TableNotExistException
                | Catalog.ColumnAlreadyExistException
                | Catalog.ColumnNotExistException e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private List<SchemaChange> applyAddColumnEventWithPosition(AddColumnEvent event)
            throws SchemaEvolveException {
        try {
            List<SchemaChange> tableChangeList = new ArrayList<>();
            for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
                SchemaChange tableChange;
                switch (columnWithPosition.getPosition()) {
                    case FIRST:
                        tableChange =
                                SchemaChangeProvider.add(
                                        columnWithPosition,
                                        SchemaChange.Move.first(
                                                columnWithPosition.getAddColumn().getName()));
                        tableChangeList.add(tableChange);
                        break;
                    case LAST:
                        SchemaChange schemaChangeWithLastPosition =
                                SchemaChangeProvider.add(columnWithPosition);
                        tableChangeList.add(schemaChangeWithLastPosition);
                        break;
                    case BEFORE:
                        SchemaChange schemaChangeWithBeforePosition =
                                applyAddColumnWithBeforePosition(
                                        event.tableId().getSchemaName(),
                                        event.tableId().getTableName(),
                                        columnWithPosition);
                        tableChangeList.add(schemaChangeWithBeforePosition);
                        break;
                    case AFTER:
                        checkNotNull(
                                columnWithPosition.getExistedColumnName(),
                                "Existing column name must be provided for AFTER position");
                        SchemaChange.Move after =
                                SchemaChange.Move.after(
                                        columnWithPosition.getAddColumn().getName(),
                                        columnWithPosition.getExistedColumnName());
                        tableChange = SchemaChangeProvider.add(columnWithPosition, after);
                        tableChangeList.add(tableChange);
                        break;
                    default:
                        throw new SchemaEvolveException(
                                event,
                                "Unknown column position: " + columnWithPosition.getPosition());
                }
            }
            return tableChangeList;
        } catch (Catalog.TableNotExistException e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private SchemaChange applyAddColumnWithBeforePosition(
            String schemaName,
            String tableName,
            AddColumnEvent.ColumnWithPosition columnWithPosition)
            throws Catalog.TableNotExistException {
        String existedColumnName = columnWithPosition.getExistedColumnName();
        Table table = catalog.getTable(new Identifier(schemaName, tableName));
        List<String> columnNames = table.rowType().getFieldNames();
        int index = checkColumnPosition(existedColumnName, columnNames);
        SchemaChange.Move after =
                SchemaChange.Move.after(
                        columnWithPosition.getAddColumn().getName(), columnNames.get(index - 1));

        return SchemaChangeProvider.add(columnWithPosition, after);
    }

    private int checkColumnPosition(String existedColumnName, List<String> columnNames) {
        if (existedColumnName == null) {
            return 0;
        }
        int index = columnNames.indexOf(existedColumnName);
        checkArgument(index != -1, "Column %s not found", existedColumnName);
        return index;
    }

    private void applyDropColumn(DropColumnEvent event) throws SchemaEvolveException {
        try {
            List<SchemaChange> tableChangeList = new ArrayList<>();
            event.getDroppedColumnNames()
                    .forEach((column) -> tableChangeList.add(SchemaChangeProvider.drop(column)));
            catalog.alterTable(
                    new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                    tableChangeList,
                    true);
        } catch (Catalog.TableNotExistException
                | Catalog.ColumnAlreadyExistException
                | Catalog.ColumnNotExistException e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyRenameColumn(RenameColumnEvent event) throws SchemaEvolveException {
        try {
            List<SchemaChange> tableChangeList = new ArrayList<>();
            event.getNameMapping()
                    .forEach(
                            (oldName, newName) ->
                                    tableChangeList.add(
                                            SchemaChangeProvider.rename(oldName, newName)));
            catalog.alterTable(
                    new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                    tableChangeList,
                    true);
        } catch (Catalog.TableNotExistException
                | Catalog.ColumnAlreadyExistException
                | Catalog.ColumnNotExistException e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event) throws SchemaEvolveException {
        try {
            List<SchemaChange> tableChangeList = new ArrayList<>();
            event.getTypeMapping()
                    .forEach(
                            (oldName, newType) ->
                                    tableChangeList.add(
                                            SchemaChangeProvider.updateColumnType(
                                                    oldName, newType)));
            catalog.alterTable(
                    new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                    tableChangeList,
                    true);
        } catch (Catalog.TableNotExistException
                | Catalog.ColumnAlreadyExistException
                | Catalog.ColumnNotExistException e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }
}
