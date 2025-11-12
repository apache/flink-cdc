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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import com.starrocks.connector.flink.catalog.StarRocksCatalogException;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksUtils.toStarRocksDataType;

/** A {@code MetadataApplier} that applies metadata changes to StarRocks. */
public class StarRocksMetadataApplier implements MetadataApplier {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksMetadataApplier.class);

    private final StarRocksEnrichedCatalog catalog;
    private final TableCreateConfig tableCreateConfig;
    private final SchemaChangeConfig schemaChangeConfig;
    private boolean isOpened;
    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    public StarRocksMetadataApplier(
            StarRocksEnrichedCatalog catalog,
            TableCreateConfig tableCreateConfig,
            SchemaChangeConfig schemaChangeConfig) {
        this.catalog = catalog;
        this.tableCreateConfig = tableCreateConfig;
        this.schemaChangeConfig = schemaChangeConfig;
        this.isOpened = false;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
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
                SchemaChangeEventType.DROP_TABLE,
                SchemaChangeEventType.TRUNCATE_TABLE);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (!isOpened) {
            isOpened = true;
            catalog.open();
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
                    applyDropTable(dropTableEvent);
                    return null;
                },
                renameColumnEvent -> {
                    applyRenameColumn(renameColumnEvent);
                    return null;
                },
                truncateTableEvent -> {
                    applyTruncateTable(truncateTableEvent);
                    return null;
                });
    }

    private void applyCreateTable(CreateTableEvent createTableEvent) throws SchemaEvolveException {
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
            throw new SchemaEvolveException(createTableEvent, "Failed to create table", e);
        }
    }

    private void applyAddColumn(AddColumnEvent addColumnEvent) throws SchemaEvolveException {
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
                            .setColumnComment(column.getComment())
                            .setDefaultValue(
                                    StarRocksUtils.convertInvalidTimestampDefaultValue(
                                            column.getDefaultValueExpression(), column.getType()));
            toStarRocksDataType(column, false, builder);
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
            throw new SchemaEvolveException(
                    addColumnEvent,
                    "Failed to apply add column because of alter exception, event: ",
                    alterException);
        } else {
            String errorMsg =
                    String.format(
                            "Failed to apply add column because of validation failure, event: %s, table: %s",
                            addColumnEvent, table);
            LOG.error(errorMsg);
            throw new SchemaEvolveException(addColumnEvent, errorMsg, null);
        }
    }

    private void applyDropColumn(DropColumnEvent dropColumnEvent) throws SchemaEvolveException {
        List<String> dropColumns = dropColumnEvent.getDroppedColumnNames();
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
            throw new SchemaEvolveException(
                    dropColumnEvent,
                    "Failed to apply drop column because of alter exception",
                    alterException);
        } else {
            String errorMsg =
                    String.format(
                            "Failed to apply drop column because of validation failure, event: %s, table: %s",
                            dropColumnEvent, table);
            LOG.error(errorMsg);
            throw new SchemaEvolveException(dropColumnEvent, errorMsg, null);
        }
    }

    private void applyRenameColumn(RenameColumnEvent renameColumnEvent)
            throws SchemaEvolveException {
        // TODO StarRocks plans to support column rename since 3.3 which has not been released.
        // Support it later.
        throw new UnsupportedSchemaChangeEventException(renameColumnEvent);
    }

    private void applyAlterColumnType(AlterColumnTypeEvent alterColumnTypeEvent)
            throws SchemaEvolveException {
        // TODO There are limitations for data type conversions. We should know the data types
        // before and after changing so that we can make a validation. But the event only contains
        // data
        // types after changing. One way is that the framework delivers the old schema. We can
        // support
        // the alter after a discussion.
        throw new UnsupportedSchemaChangeEventException(alterColumnTypeEvent);
    }

    private void applyTruncateTable(TruncateTableEvent truncateTableEvent) {
        try {
            catalog.truncateTable(
                    truncateTableEvent.tableId().getSchemaName(),
                    truncateTableEvent.tableId().getTableName());
        } catch (StarRocksCatalogException e) {
            throw new SchemaEvolveException(truncateTableEvent, e.getMessage(), e);
        }
    }

    private void applyDropTable(DropTableEvent dropTableEvent) {
        try {
            catalog.dropTable(
                    dropTableEvent.tableId().getSchemaName(),
                    dropTableEvent.tableId().getTableName());
        } catch (StarRocksCatalogException e) {
            throw new SchemaEvolveException(dropTableEvent, e.getMessage(), e);
        }
    }
}
