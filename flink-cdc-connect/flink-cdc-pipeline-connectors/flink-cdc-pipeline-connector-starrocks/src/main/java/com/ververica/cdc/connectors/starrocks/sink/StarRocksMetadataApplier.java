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

package com.ververica.cdc.connectors.starrocks.sink;

import com.starrocks.connector.flink.catalog.StarRocksCatalog;
import com.starrocks.connector.flink.catalog.StarRocksCatalogException;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.sink.MetadataApplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.starrocks.sink.StarRocksUtils.toStarRocksDataType;

/** A {@code MetadataApplier} that applies metadata changes to StarRocks. */
public class StarRocksMetadataApplier implements MetadataApplier {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksMetadataApplier.class);

    private final StarRocksCatalog catalog;
    private final TableCreateConfig tableCreateConfig;
    private final SchemaChangeConfig schemaChangeConfig;
    private boolean isOpened;

    public StarRocksMetadataApplier(
            StarRocksCatalog catalog,
            TableCreateConfig tableCreateConfig,
            SchemaChangeConfig schemaChangeConfig) {
        this.catalog = catalog;
        this.tableCreateConfig = tableCreateConfig;
        this.schemaChangeConfig = schemaChangeConfig;
        this.isOpened = false;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        if (!isOpened) {
            isOpened = true;
            catalog.open();
        }

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
                    "StarRocksDataSink doesn't support schema change event " + schemaChangeEvent);
        }
    }

    private void applyCreateTable(CreateTableEvent createTableEvent) {
        StarRocksTable starRocksTable =
                StarRocksUtils.toStarRocksTable(
                        createTableEvent.tableId(),
                        createTableEvent.getSchema(),
                        tableCreateConfig);
        if (!catalog.databaseExists(starRocksTable.getDatabaseName())) {
            catalog.createDatabase(starRocksTable.getDatabaseName(), true);
        }

        try {
            catalog.createTable(starRocksTable, true);
            LOG.info("Successful to create table, event: {}", createTableEvent);
        } catch (StarRocksCatalogException e) {
            LOG.error("Failed to create table, event: {}", createTableEvent.tableId(), e);
            throw new RuntimeException("Failed to create table, event: " + createTableEvent, e);
        }
    }

    private void applyAddColumn(AddColumnEvent addColumnEvent) {
        List<StarRocksColumn> addColumns = new ArrayList<>();
        for (AddColumnEvent.ColumnWithPosition columnWithPosition :
                addColumnEvent.getAddedColumns()) {
            // we will ignore position information, and always add the column to the last.
            // The reason is that the order of columns between source table and StarRocks
            // table may be not consistent because of limitations of StarRocks table, so the
            // position may be meaningless. For example, primary keys of StarRocks table
            // must be at the front, but mysql doest not have this limitation, so the order
            // may be different, and also FIRST position is not allowed for StarRocks primary
            // key table.
            Column column = columnWithPosition.getAddColumn();
            StarRocksColumn.Builder builder =
                    new StarRocksColumn.Builder()
                            .setColumnName(column.getName())
                            .setOrdinalPosition(-1)
                            .setColumnComment(column.getComment());
            toStarRocksDataType(column, builder);
            addColumns.add(builder.build());
        }

        TableId tableId = addColumnEvent.tableId();
        StarRocksCatalogException alterException = null;
        try {
            catalog.alterAddColumns(
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    addColumns,
                    schemaChangeConfig.getTimeoutSecond());
        } catch (StarRocksCatalogException e) {
            alterException = e;
        }

        // Check whether the columns have been actually added to the table.
        // This is useful for duplicate schema change after failover. Adding
        // same columns will fail on StarRocks side, but it should be successful
        // on CDC side
        StarRocksTable table = null;
        try {
            table = catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        } catch (StarRocksCatalogException e) {
            LOG.warn("Failed to get table {}", tableId, e);
        }
        boolean allAdded = true;
        if (table != null) {
            for (StarRocksColumn column : addColumns) {
                if (table.getColumn(column.getColumnName()) == null) {
                    allAdded = false;
                    break;
                }
            }
        }

        if (allAdded) {
            if (alterException == null) {
                LOG.info("Successful to apply add column, event: {}", addColumnEvent);
            } else {
                LOG.info(
                        "Successful to apply add column, event: {}, and ignore the alter exception",
                        addColumnEvent,
                        alterException);
            }
            return;
        }

        if (alterException != null) {
            LOG.error(
                    "Failed to apply add column because of alter exception, event: {}",
                    addColumnEvent,
                    alterException);
            throw new RuntimeException(
                    "Failed to apply add column because of alter exception, event: "
                            + addColumnEvent,
                    alterException);
        } else {
            String errorMsg =
                    String.format(
                            "Failed to apply add column because of validation failure, event: %s, table: %s",
                            addColumnEvent, table);
            LOG.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
    }

    private void applyDropColumn(DropColumnEvent dropColumnEvent) {
        List<String> dropColumns =
                dropColumnEvent.getDroppedColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        TableId tableId = dropColumnEvent.tableId();
        StarRocksCatalogException alterException = null;
        try {
            catalog.alterDropColumns(
                    dropColumnEvent.tableId().getSchemaName(),
                    dropColumnEvent.tableId().getTableName(),
                    dropColumns,
                    schemaChangeConfig.getTimeoutSecond());
        } catch (StarRocksCatalogException e) {
            alterException = e;
        }

        // Check whether the columns have been actually dropped from the table.
        // This is useful for duplicate schema change after failover. Drop
        // non-existed columns will fail on StarRocks side, but it should be
        // successful on CDC side
        StarRocksTable table = null;
        try {
            table = catalog.getTable(tableId.getSchemaName(), tableId.getTableName()).orElse(null);
        } catch (StarRocksCatalogException ie) {
            LOG.warn("Failed to get table {}", tableId, ie);
        }

        boolean allDrop = true;
        if (table != null) {
            for (String columnName : dropColumns) {
                if (table.getColumn(columnName) != null) {
                    allDrop = false;
                    break;
                }
            }
        }

        if (allDrop) {
            if (alterException == null) {
                LOG.info("Successful to apply drop column, event: {}", dropColumnEvent);
            } else {
                LOG.info(
                        "Successful to apply drop column, event: {}, and ignore the alter exception",
                        dropColumnEvent,
                        alterException);
            }
            return;
        }

        if (alterException != null) {
            LOG.error(
                    "Failed to apply drop column because of alter exception, event: {}",
                    dropColumnEvent,
                    alterException);
            throw new RuntimeException(
                    "Failed to apply drop column because of alter exception, event: "
                            + dropColumnEvent,
                    alterException);
        } else {
            String errorMsg =
                    String.format(
                            "Failed to apply drop column because of validation failure, event: %s, table: %s",
                            dropColumnEvent, table);
            LOG.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
    }

    private void applyRenameColumn(RenameColumnEvent renameColumnEvent) {
        // TODO StarRocks plans to support column rename since 3.3 which has not been released.
        // Support it later.
        throw new UnsupportedOperationException("Rename column is not supported currently");
    }

    private void applyAlterColumn(AlterColumnTypeEvent alterColumnTypeEvent) {
        // TODO There are limitations for data type conversions. We should know the data types
        // before and after changing so that we can make a validation. But the event only contains
        // data
        // types after changing. One way is that the framework delivers the old schema. We can
        // support
        // the alter after a discussion.
        throw new UnsupportedOperationException("Alter column is not supported currently");
    }
}
