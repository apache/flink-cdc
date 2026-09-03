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
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import com.starrocks.connector.flink.catalog.StarRocksCatalogException;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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
                SchemaChangeEventType.RENAME_COLUMN,
                SchemaChangeEventType.ALTER_COLUMN_TYPE,
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

        SchemaChangeEventVisitor.voidVisit(
                schemaChangeEvent,
                this::applyAddColumn,
                this::applyAlterColumnType,
                this::applyCreateTable,
                this::applyDropColumn,
                this::applyDropTable,
                this::applyRenameColumn,
                this::applyTruncateTable,
                alterTableCommentEvent -> {
                    // TODO Currently, table comments cannot be modified.
                    // See
                    // https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/#alter-table-comment-from-v31
                    LOG.warn(
                            "AlterTableCommentEvent is not supported by StarRocks connector yet. Event: {}",
                            alterTableCommentEvent);
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
            Optional<StarRocksTable> existingTable =
                    catalog.getTable(
                            starRocksTable.getDatabaseName(), starRocksTable.getTableName());
            if (existingTable.isPresent()) {
                validateExistingTable(createTableEvent, existingTable.get(), starRocksTable);
                LOG.info(
                        "Table already exists with a compatible schema, event: {}",
                        createTableEvent);
                return;
            }
            catalog.createTable(starRocksTable, true);
            LOG.info("Successful to create table, event: {}", createTableEvent);
        } catch (StarRocksCatalogException e) {
            LOG.error("Failed to create table, event: {}", createTableEvent.tableId(), e);
            throw new SchemaEvolveException(createTableEvent, "Failed to create table", e);
        }
    }

    private void applyAddColumn(AddColumnEvent addColumnEvent) throws SchemaEvolveException {
        List<StarRocksColumn> addColumns = new ArrayList<>();
        StarRocksTable existingTable = getRequiredTable(addColumnEvent);
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
            toStarRocksDataType(column, false, builder, tableCreateConfig.getUnicodeCharMaxBytes());
            StarRocksColumn targetColumn = builder.build();
            StarRocksColumn existingColumn = existingTable.getColumn(targetColumn.getColumnName());
            if (existingColumn == null) {
                addColumns.add(targetColumn);
            } else if (isSameOrWider(existingColumn, targetColumn)) {
                LOG.info(
                        "Column {} already exists with a compatible type, skipping replayed add column event.",
                        targetColumn.getColumnName());
            } else {
                throw new SchemaEvolveException(
                        addColumnEvent,
                        String.format(
                                "Existing column %s is incompatible with replayed column %s",
                                existingColumn, targetColumn),
                        null);
            }
        }
        if (addColumns.isEmpty()) {
            return;
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
        try {
            TableId tableId = renameColumnEvent.tableId();
            Map<String, String> nameMapping = renameColumnEvent.getNameMapping();
            for (Map.Entry<String, String> entry : nameMapping.entrySet()) {
                catalog.renameColumn(
                        tableId.getSchemaName(),
                        tableId.getTableName(),
                        entry.getKey(),
                        entry.getValue());
            }
        } catch (Exception e) {
            throw new SchemaEvolveException(
                    renameColumnEvent, "fail to apply rename column event", e);
        }
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event) throws SchemaEvolveException {
        try {
            TableId tableId = event.tableId();
            Map<String, DataType> typeMapping = event.getTypeMapping();
            StarRocksTable existingTable = getRequiredTable(event);

            for (Map.Entry<String, DataType> entry : typeMapping.entrySet()) {
                StarRocksColumn.Builder builder =
                        new StarRocksColumn.Builder().setColumnName(entry.getKey());
                toStarRocksDataType(
                        entry.getValue(),
                        false,
                        builder,
                        tableCreateConfig.getUnicodeCharMaxBytes());
                StarRocksColumn targetColumn = builder.build();
                StarRocksColumn existingColumn = existingTable.getColumn(entry.getKey());
                if (existingColumn == null) {
                    throw new SchemaEvolveException(
                            event, "Cannot alter non-existing column " + entry.getKey(), null);
                }
                if (isSameOrWider(existingColumn, targetColumn)
                        && isSameOrWider(targetColumn, existingColumn)) {
                    LOG.info(
                            "Column {} is already at type {}, skipping replayed alter column event.",
                            entry.getKey(),
                            existingColumn.getDataType());
                    continue;
                }
                if (!isSameOrWider(targetColumn, existingColumn)) {
                    throw new SchemaEvolveException(
                            event,
                            String.format(
                                    "Cannot safely widen column %s from %s to %s",
                                    entry.getKey(), existingColumn, targetColumn),
                            null);
                }
                catalog.alterColumnType(
                        tableId.getSchemaName(),
                        tableId.getTableName(),
                        targetColumn,
                        schemaChangeConfig.getTimeoutSecond());
            }
        } catch (Exception e) {
            if (e instanceof SchemaEvolveException) {
                throw (SchemaEvolveException) e;
            }
            throw new SchemaEvolveException(event, "fail to apply alter column type event", e);
        }
    }

    private StarRocksTable getRequiredTable(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        try {
            return catalog.getTable(tableId.getSchemaName(), tableId.getTableName())
                    .orElseThrow(
                            () ->
                                    new SchemaEvolveException(
                                            event, "Table " + tableId + " does not exist", null));
        } catch (StarRocksCatalogException e) {
            throw new SchemaEvolveException(event, "Failed to inspect table " + tableId, e);
        }
    }

    private void validateExistingTable(
            CreateTableEvent event, StarRocksTable actual, StarRocksTable expected) {
        List<String> actualKeys = actual.getTableKeys().orElse(new ArrayList<>());
        List<String> expectedKeys = expected.getTableKeys().orElse(new ArrayList<>());
        if (!actualKeys.equals(expectedKeys)) {
            throw new SchemaEvolveException(
                    event,
                    String.format(
                            "Existing table primary keys %s differ from inferred primary keys %s",
                            actualKeys, expectedKeys),
                    null);
        }

        Map<String, StarRocksColumn> expectedColumns = new HashMap<>();
        for (StarRocksColumn expectedColumn : expected.getColumns()) {
            expectedColumns.put(expectedColumn.getColumnName(), expectedColumn);
            StarRocksColumn actualColumn = actual.getColumn(expectedColumn.getColumnName());
            if (actualColumn == null || !isSameOrWider(actualColumn, expectedColumn)) {
                throw new SchemaEvolveException(
                        event,
                        String.format(
                                "Existing column %s is missing or incompatible with inferred column %s",
                                actualColumn, expectedColumn),
                        null);
            }
        }

        for (StarRocksColumn actualColumn : actual.getColumns()) {
            if (!expectedColumns.containsKey(actualColumn.getColumnName())
                    && !actualColumn.isNullable()
                    && !actualColumn.getDefaultValue().isPresent()) {
                throw new SchemaEvolveException(
                        event,
                        String.format(
                                "Existing extra column %s is non-nullable and has no default value",
                                actualColumn.getColumnName()),
                        null);
            }
        }
    }

    private boolean isSameOrWider(StarRocksColumn wider, StarRocksColumn narrower) {
        if (narrower.isNullable() && !wider.isNullable()) {
            return false;
        }

        String widerType = wider.getDataType().toUpperCase(Locale.ROOT);
        String narrowerType = narrower.getDataType().toUpperCase(Locale.ROOT);
        if (widerType.equals(narrowerType)) {
            if ("CHAR".equals(widerType) || "VARCHAR".equals(widerType)) {
                return wider.getColumnSize().orElse(0) >= narrower.getColumnSize().orElse(0);
            }
            if ("DECIMAL".equals(widerType)) {
                int widerPrecision = wider.getColumnSize().orElse(0);
                int widerScale = wider.getDecimalDigits().orElse(0);
                int narrowerPrecision = narrower.getColumnSize().orElse(0);
                int narrowerScale = narrower.getDecimalDigits().orElse(0);
                return widerScale >= narrowerScale
                        && widerPrecision - widerScale >= narrowerPrecision - narrowerScale;
            }
            return true;
        }

        if ("VARCHAR".equals(widerType) && "CHAR".equals(narrowerType)) {
            return wider.getColumnSize().orElse(0) >= narrower.getColumnSize().orElse(0);
        }
        if ("VARCHAR".equals(widerType) || "STRING".equals(widerType)) {
            return numericTypeRank(narrowerType) >= 0 || "BOOLEAN".equals(narrowerType);
        }
        int widerRank = numericTypeRank(widerType);
        int narrowerRank = numericTypeRank(narrowerType);
        return widerRank >= 0 && narrowerRank >= 0 && widerRank >= narrowerRank;
    }

    private int numericTypeRank(String type) {
        switch (type) {
            case "TINYINT":
                return 0;
            case "SMALLINT":
                return 1;
            case "INT":
                return 2;
            case "BIGINT":
                return 3;
            case "LARGEINT":
                return 4;
            case "FLOAT":
                return 5;
            case "DOUBLE":
                return 6;
            default:
                return -1;
        }
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
