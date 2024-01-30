/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.paimon.sink;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.common.types.utils.DataTypeUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    /** TODO support partition column. */
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
        List<SchemaChange> tableChangeList = new ArrayList<>();
        event.getAddedColumns()
                .forEach(
                        (column) -> {
                            SchemaChange tableChange =
                                    SchemaChange.addColumn(
                                            column.getAddColumn().getName(),
                                            LogicalTypeConversion.toDataType(
                                                    DataTypeUtils.toFlinkDataType(
                                                                    column.getAddColumn().getType())
                                                            .getLogicalType()));
                            tableChangeList.add(tableChange);
                        });
        catalog.alterTable(
                new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                tableChangeList,
                true);
    }

    private void applyDropColumn(DropColumnEvent event)
            throws Catalog.ColumnAlreadyExistException, Catalog.TableNotExistException,
                    Catalog.ColumnNotExistException {
        List<SchemaChange> tableChangeList = new ArrayList<>();
        event.getDroppedColumnNames()
                .forEach(
                        (column) -> {
                            SchemaChange tableChange = SchemaChange.dropColumn(column);
                            tableChangeList.add(tableChange);
                        });
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
                        (oldName, newName) -> {
                            SchemaChange tableChange = SchemaChange.renameColumn(oldName, newName);
                            tableChangeList.add(tableChange);
                        });
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
                        (oldName, newType) -> {
                            SchemaChange tableChange =
                                    SchemaChange.updateColumnType(
                                            oldName,
                                            LogicalTypeConversion.toDataType(
                                                    DataTypeUtils.toFlinkDataType(newType)
                                                            .getLogicalType()));
                            tableChangeList.add(tableChange);
                        });
        catalog.alterTable(
                new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName()),
                tableChangeList,
                true);
    }
}
