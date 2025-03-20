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

package org.apache.flink.cdc.connectors.oceanbase.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseCatalog;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseCatalogException;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseCatalogFactory;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseColumn;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseTable;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Supports {@link OceanBaseDataSink} to schema evolution. */
public class OceanBaseMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMetadataApplier.class);

    private final OceanBaseCatalog catalog;

    public OceanBaseMetadataApplier(OceanBaseConnectorOptions connectorOptions) {
        try {
            this.catalog = OceanBaseCatalogFactory.createOceanBaseCatalog(connectorOptions);
            catalog.open();
        } catch (Exception e) {
            throw new OceanBaseCatalogException("Fail to init OceanBaseMetadataApplier.", e);
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        SchemaChangeEventVisitor.<Void, SchemaEvolveException>visit(
                event,
                addColumnEvent -> {
                    applyAddColumnEvent(addColumnEvent);
                    return null;
                },
                alterColumnTypeEvent -> {
                    applyAlterColumnTypeEvent(alterColumnTypeEvent);
                    return null;
                },
                createTableEvent -> {
                    applyCreateTableEvent(createTableEvent);
                    return null;
                },
                dropColumnEvent -> {
                    applyDropColumnEvent(dropColumnEvent);
                    return null;
                },
                dropTableEvent -> {
                    applyDropTableEvent(dropTableEvent);
                    return null;
                },
                renameColumnEvent -> {
                    applyRenameColumnEvent(renameColumnEvent);
                    return null;
                },
                truncateTableEvent -> {
                    applyTruncateTableEvent(truncateTableEvent);
                    return null;
                });
    }

    private void applyCreateTableEvent(CreateTableEvent createTableEvent)
            throws IllegalArgumentException {
        Schema schema = createTableEvent.getSchema();
        TableId tableId = createTableEvent.tableId();
        OceanBaseTable oceanBaseTable = OceanBaseUtils.toOceanBaseTable(tableId, schema);

        if (!catalog.databaseExists(oceanBaseTable.getDatabaseName())) {
            catalog.createDatabase(oceanBaseTable.getDatabaseName(), true);
        }

        try {
            catalog.createTable(oceanBaseTable, true);
            LOG.info("Successful to create table, event: {}", createTableEvent);
        } catch (OceanBaseCatalogException e) {
            LOG.error("Failed to create table, event: {}", createTableEvent.tableId(), e);
            throw new RuntimeException("Failed to create table, event: " + createTableEvent, e);
        }
    }

    private void applyAddColumnEvent(AddColumnEvent addColumnEvent) {
        List<OceanBaseColumn> addColumns = new ArrayList<>();
        for (AddColumnEvent.ColumnWithPosition columnWithPosition :
                addColumnEvent.getAddedColumns()) {
            Preconditions.checkState(
                    columnWithPosition.getPosition() == AddColumnEvent.ColumnPosition.LAST,
                    "The oceanbase pipeline connector currently only supports add the column to the last.");

            // we will ignore position information, and always add the column to the last.
            // The reason is that in OceanBase, only adding columns to the last as an online DDL,
            // and this pipeline connector currently only supports online DDL.
            Column column = columnWithPosition.getAddColumn();
            OceanBaseColumn.Builder builder =
                    new OceanBaseColumn.Builder()
                            .setColumnName(column.getName())
                            .setOrdinalPosition(-1)
                            .setColumnComment(column.getComment());
            OceanBaseUtils.toOceanBaseDataType(column, false, builder);
            addColumns.add(builder.build());
        }

        TableId tableId = addColumnEvent.tableId();
        catalog.alterAddColumns(tableId.getSchemaName(), tableId.getTableName(), addColumns);
    }

    private void applyDropColumnEvent(DropColumnEvent dropColumnEvent) {
        List<String> dropColumns = dropColumnEvent.getDroppedColumnNames();
        catalog.alterDropColumns(
                dropColumnEvent.tableId().getSchemaName(),
                dropColumnEvent.tableId().getTableName(),
                dropColumns);
    }

    private void applyAlterColumnTypeEvent(AlterColumnTypeEvent alterColumnTypeEvent) {
        TableId tableId = alterColumnTypeEvent.tableId();
        Map<String, DataType> typeMapping = alterColumnTypeEvent.getTypeMapping();

        for (Map.Entry<String, DataType> entry : typeMapping.entrySet()) {
            catalog.alterColumnType(
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    entry.getKey(),
                    entry.getValue());
        }
    }

    private void applyRenameColumnEvent(RenameColumnEvent renameColumnEvent) {
        TableId tableId = renameColumnEvent.tableId();
        Map<String, String> nameMapping = renameColumnEvent.getNameMapping();
        for (Map.Entry<String, String> entry : nameMapping.entrySet()) {
            catalog.renameColumn(
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    entry.getKey(),
                    entry.getValue());
        }
    }

    private void applyDropTableEvent(DropTableEvent dropTableEvent) {
        TableId tableId = dropTableEvent.tableId();
        catalog.dropTable(tableId.getSchemaName(), tableId.getTableName());
    }

    private void applyTruncateTableEvent(TruncateTableEvent truncateTableEvent) {
        TableId tableId = truncateTableEvent.tableId();

        // check table exists
        if (catalog.tableExists(tableId.getSchemaName(), tableId.getTableName())) {
            catalog.truncateTable(tableId.getSchemaName(), tableId.getTableName());
        } else {
            throw new OceanBaseCatalogException(
                    String.format(
                            "Failed to truncate table %s.%s, because the table not exist",
                            tableId.getSchemaName(), tableId.getTableName()));
        }
    }
}
