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
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;
import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/**
 * A {@code MetadataApplier} that applies metadata changes to Paimon. Support primary key table
 * only.
 */
public class PaimonMetadataApplier implements MetadataApplier {

    // Catalog is unSerializable.
    private transient Catalog catalog;

    // currently, we set table options for all tables using the same options.
    private final Map<String, String> tableOptions;

    private final Options catalogOptions;

    private final Map<TableId, List<String>> partitionMaps;

    public PaimonMetadataApplier(Options catalogOptions) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = new HashMap<>();
        this.partitionMaps = new HashMap<>();
    }

    public PaimonMetadataApplier(
            Options catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = tableOptions;
        this.partitionMaps = partitionMaps;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        if (catalog == null) {
            catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        }
        try {
            if (schemaChangeEvent instanceof CreateTableEvent) {
                applyCreateTable((CreateTableEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof AddColumnEvent) {
                applyAddColumn((AddColumnEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof DropColumnEvent) {
                applyDropColumn((DropColumnEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof RenameColumnEvent) {
                applyRenameColumn((RenameColumnEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof AlterColumnTypeEvent) {
                applyAlterColumn((AlterColumnTypeEvent) schemaChangeEvent);
            } else {
                throw new UnsupportedOperationException(
                        "PaimonDataSink doesn't support schema change event " + schemaChangeEvent);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void applyCreateTable(CreateTableEvent event)
            throws Catalog.DatabaseAlreadyExistException, Catalog.TableAlreadyExistException,
                    Catalog.DatabaseNotExistException {
        if (!catalog.databaseExists(event.tableId().getSchemaName())) {
            catalog.createDatabase(event.tableId().getSchemaName(), true);
        }
        Schema schema = event.getSchema();
        org.apache.paimon.schema.Schema.Builder builder =
                new org.apache.paimon.schema.Schema.Builder();
        schema.getColumns()
                .forEach(
                        (column) ->
                                builder.column(
                                        column.getName(),
                                        LogicalTypeConversion.toDataType(
                                                DataTypeUtils.toFlinkDataType(column.getType())
                                                        .getLogicalType())));
        builder.primaryKey(schema.primaryKeys().toArray(new String[0]));
        if (partitionMaps.containsKey(event.tableId())) {
            builder.partitionKeys(partitionMaps.get(event.tableId()));
        } else if (schema.partitionKeys() != null && !schema.partitionKeys().isEmpty()) {
            builder.partitionKeys(schema.partitionKeys());
        }
        builder.options(tableOptions);
        catalog.createTable(
                new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                builder.build(),
                true);
    }

    private void applyAddColumn(AddColumnEvent event)
            throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException,
                    Catalog.ColumnNotExistException {
        List<SchemaChange> tableChangeList = applyAddColumnEventWithPosition(event);
        catalog.alterTable(
                new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                tableChangeList,
                true);
    }

    private List<SchemaChange> applyAddColumnEventWithPosition(AddColumnEvent event)
            throws Catalog.TableNotExistException {
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
                    throw new IllegalArgumentException(
                            "Unknown column position: " + columnWithPosition.getPosition());
            }
        }
        return tableChangeList;
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

    private void applyDropColumn(DropColumnEvent event)
            throws Catalog.ColumnAlreadyExistException, Catalog.TableNotExistException,
                    Catalog.ColumnNotExistException {
        List<SchemaChange> tableChangeList = new ArrayList<>();
        event.getDroppedColumnNames()
                .forEach((column) -> tableChangeList.add(SchemaChangeProvider.drop(column)));
        catalog.alterTable(
                new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                tableChangeList,
                true);
    }

    private void applyRenameColumn(RenameColumnEvent event)
            throws Catalog.ColumnAlreadyExistException, Catalog.TableNotExistException,
                    Catalog.ColumnNotExistException {
        List<SchemaChange> tableChangeList = new ArrayList<>();
        event.getNameMapping()
                .forEach(
                        (oldName, newName) ->
                                tableChangeList.add(SchemaChangeProvider.rename(oldName, newName)));
        catalog.alterTable(
                new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                tableChangeList,
                true);
    }

    private void applyAlterColumn(AlterColumnTypeEvent event)
            throws Catalog.ColumnAlreadyExistException, Catalog.TableNotExistException,
                    Catalog.ColumnNotExistException {
        List<SchemaChange> tableChangeList = new ArrayList<>();
        event.getTypeMapping()
                .forEach(
                        (oldName, newType) ->
                                tableChangeList.add(
                                        SchemaChangeProvider.updateColumnType(oldName, newType)));
        catalog.alterTable(
                new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                tableChangeList,
                true);
    }
}
