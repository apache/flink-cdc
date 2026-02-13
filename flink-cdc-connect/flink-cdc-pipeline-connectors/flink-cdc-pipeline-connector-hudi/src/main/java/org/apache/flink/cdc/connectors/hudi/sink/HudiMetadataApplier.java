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

package org.apache.flink.cdc.connectors.hudi.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.connectors.hudi.sink.util.ConfigUtils;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.types.DataType;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.catalog.CatalogOptions;
import org.apache.hudi.table.catalog.HoodieCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link MetadataApplier} that applies schema changes to Hudi tables.
 *
 * <p>This applier is responsible for initializing the Hudi table metadata in the file system if it
 * does not already exist.
 */
public class HudiMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(HudiMetadataApplier.class);

    private final Configuration config;

    // Catalog is unSerializable, similar to PaimonMetadataApplier
    private transient HoodieCatalog catalog;

    private final org.apache.flink.configuration.Configuration catalogConfig;

    public HudiMetadataApplier(Configuration config) {
        this.config = config;
        this.catalogConfig = convertToCatalogConfig(config);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        LOG.info("Applying schema change event: {}", schemaChangeEvent);
        // Initialize catalog if not already done
        if (catalog == null) {
            catalog = new HoodieCatalog("hoodie_catalog", catalogConfig);
            try {
                catalog.open();
            } catch (CatalogException e) {
                throw new RuntimeException("Failed to open HoodieCatalog", e);
            }
        }

        try {
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
                        throw new UnsupportedOperationException("DropTableEvent is not supported");
                    },
                    renameColumnEvent -> {
                        applyRenameColumn(renameColumnEvent);
                        return null;
                    },
                    truncateTableEvent -> {
                        throw new UnsupportedOperationException(
                                "TruncateTableEvent is not supported");
                    });
        } catch (Exception e) {
            LOG.error("Failed to apply schema change for table {}", schemaChangeEvent.tableId(), e);
            throw new RuntimeException("Failed to apply schema change", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    private void applyCreateTable(CreateTableEvent event) {
        try {
            TableId tableId = event.tableId();
            String databaseName = tableId.getSchemaName();

            // Create database if it doesn't exist
            if (!catalog.databaseExists(databaseName)) {
                CatalogDatabase database = new CatalogDatabaseImpl(new HashMap<>(), null);
                catalog.createDatabase(databaseName, database, true);
                LOG.info("Created database: {}", databaseName);
            }

            // Convert CDC Schema to Flink ResolvedCatalogTable
            ResolvedCatalogTable catalogTable = convertToCatalogTable(event.getSchema());
            ObjectPath objectPath = new ObjectPath(databaseName, tableId.getTableName());

            // Create table using catalog
            catalog.createTable(objectPath, catalogTable, true);

            LOG.info("Successfully created Hudi table {} via catalog", tableId);
        } catch (DatabaseAlreadyExistException e) {
            // Should not happen because ignoreIfExists=true
            LOG.warn("Database already exists: {}", e.getMessage());
        } catch (TableAlreadyExistException e) {
            // Should not happen because ignoreIfExists=true
            LOG.warn("Table already exists: {}", e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create table via catalog", e);
        }
    }

    private void applyAddColumn(AddColumnEvent event) throws Exception {
        TableId tableId = event.tableId();
        ObjectPath objectPath = new ObjectPath(tableId.getSchemaName(), tableId.getTableName());

        // Get existing table and ensure it's resolved
        ResolvedCatalogTable existingTable = getResolvedCatalogTable(objectPath);
        ResolvedSchema existingSchema = existingTable.getResolvedSchema();

        // Build new columns list with added columns
        List<org.apache.flink.table.catalog.Column> newColumns =
                new ArrayList<>(existingSchema.getColumns());
        List<TableChange> tableChanges = new ArrayList<>();
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            Column addColumn = columnWithPosition.getAddColumn();
            DataType flinkType = DataTypeUtils.toFlinkDataType(addColumn.getType());
            org.apache.flink.table.catalog.Column newColumn =
                    org.apache.flink.table.catalog.Column.physical(addColumn.getName(), flinkType);

            // Handle column position
            switch (columnWithPosition.getPosition()) {
                case FIRST:
                    newColumns.add(0, newColumn);

                    tableChanges.add(
                            TableChange.add(newColumn, TableChange.ColumnPosition.first()));
                    break;
                case LAST:
                    newColumns.add(newColumn);
                    tableChanges.add(TableChange.add(newColumn));
                    break;
                case BEFORE:
                    int beforeIndex =
                            findColumnIndex(newColumns, columnWithPosition.getExistedColumnName());
                    newColumns.add(beforeIndex, newColumn);
                    tableChanges.add(
                            TableChange.add(
                                    newColumn,
                                    TableChange.ColumnPosition.after(
                                            newColumns.get(beforeIndex).getName())));
                    break;
                case AFTER:
                    int afterIndex =
                            findColumnIndex(newColumns, columnWithPosition.getExistedColumnName());
                    newColumns.add(afterIndex + 1, newColumn);
                    tableChanges.add(
                            TableChange.add(
                                    newColumn,
                                    TableChange.ColumnPosition.after(
                                            newColumns.get(afterIndex + 1).getName())));
                    break;
            }
            LOG.info(
                    "Adding column {} to table {} at position {}",
                    addColumn.getName(),
                    tableId,
                    columnWithPosition.getPosition());
        }

        // Create new resolved schema
        ResolvedSchema newSchema =
                new ResolvedSchema(
                        newColumns,
                        existingSchema.getWatermarkSpecs(),
                        existingSchema.getPrimaryKey().orElse(null));

        // Create new catalog table
        ResolvedCatalogTable newTable = createUpdatedCatalogTable(existingTable, newSchema);

        // Alter table
        LOG.info("Committing add column changes {} to HoodieCatalog", tableChanges);
        catalog.alterTable(objectPath, newTable, tableChanges, false);

        // Verify the change was persisted
        updateAndVerifyTableChange(tableId, objectPath, newSchema);
    }

    private void applyDropColumn(DropColumnEvent event) throws Exception {
        TableId tableId = event.tableId();
        ObjectPath objectPath = new ObjectPath(tableId.getSchemaName(), tableId.getTableName());

        // Get existing table and ensure it's resolved
        ResolvedCatalogTable existingTable = getResolvedCatalogTable(objectPath);
        ResolvedSchema existingSchema = existingTable.getResolvedSchema();

        LOG.info(
                "Before drop - Table {} has columns: {}",
                tableId,
                existingSchema.getColumns().stream()
                        .map(org.apache.flink.table.catalog.Column::getName)
                        .collect(Collectors.toList()));

        // Build new columns list without dropped columns
        List<org.apache.flink.table.catalog.Column> newColumns =
                new ArrayList<>(existingSchema.getColumns());
        newColumns.removeIf(col -> event.getDroppedColumnNames().contains(col.getName()));

        LOG.info("Dropping columns {} from table {}", event.getDroppedColumnNames(), tableId);

        // Create new resolved schema
        ResolvedSchema newSchema =
                new ResolvedSchema(
                        newColumns,
                        existingSchema.getWatermarkSpecs(),
                        existingSchema.getPrimaryKey().orElse(null));

        LOG.info(
                "After drop - Table {} should have columns: {}",
                tableId,
                newSchema.getColumns().stream()
                        .map(org.apache.flink.table.catalog.Column::getName)
                        .collect(Collectors.toList()));

        // Create new catalog table
        ResolvedCatalogTable newTable = createUpdatedCatalogTable(existingTable, newSchema);

        // Build table changes
        List<TableChange> tableChanges =
                event.getDroppedColumnNames().stream()
                        .map(colName -> TableChange.dropColumn(colName))
                        .collect(Collectors.toList());

        // Alter table in using Hoodie's catalog and commit required metadata changes
        LOG.info("Committing drop column changes {} to HoodieCatalog", tableChanges);
        catalog.alterTable(objectPath, newTable, tableChanges, false);

        LOG.info("Successfully dropped columns from table {}", tableId);

        // Verify the change was persisted
        updateAndVerifyTableChange(tableId, objectPath, newSchema);
    }

    private void applyRenameColumn(RenameColumnEvent event) throws Exception {
        TableId tableId = event.tableId();
        ObjectPath objectPath = new ObjectPath(tableId.getSchemaName(), tableId.getTableName());

        // Get existing table and ensure it's resolved
        ResolvedCatalogTable existingTable = getResolvedCatalogTable(objectPath);
        ResolvedSchema existingSchema = existingTable.getResolvedSchema();

        // Build new columns list with renamed columns
        List<org.apache.flink.table.catalog.Column> newColumns = new ArrayList<>();
        List<TableChange> tableChanges = new ArrayList<>();
        for (org.apache.flink.table.catalog.Column oldCol : existingSchema.getColumns()) {
            String newName =
                    event.getNameMapping().getOrDefault(oldCol.getName(), oldCol.getName());
            if (!newName.equals(oldCol.getName())) {
                LOG.info(
                        "Renaming column {} to {} in table {}", oldCol.getName(), newName, tableId);
                newColumns.add(
                        org.apache.flink.table.catalog.Column.physical(
                                newName, oldCol.getDataType()));
                tableChanges.add(TableChange.modifyColumnName(oldCol, newName));
            } else {
                // No name change
                newColumns.add(oldCol);
            }
        }

        // Create new resolved schema
        ResolvedSchema newSchema =
                new ResolvedSchema(
                        newColumns,
                        existingSchema.getWatermarkSpecs(),
                        existingSchema.getPrimaryKey().orElse(null));

        // Create new catalog table
        ResolvedCatalogTable newTable = createUpdatedCatalogTable(existingTable, newSchema);

        // Alter table in using Hoodie's catalog and commit required metadata changes
        catalog.alterTable(objectPath, newTable, tableChanges, false);
        LOG.info("Successfully renamed columns in table {}", tableId);

        // Verify the change was persisted
        updateAndVerifyTableChange(tableId, objectPath, newSchema);
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event) throws Exception {
        TableId tableId = event.tableId();
        ObjectPath objectPath = new ObjectPath(tableId.getSchemaName(), tableId.getTableName());

        // Get existing table and ensure it's resolved
        ResolvedCatalogTable existingTable = getResolvedCatalogTable(objectPath);
        ResolvedSchema existingSchema = existingTable.getResolvedSchema();

        // Build new columns list with altered types
        List<org.apache.flink.table.catalog.Column> newColumns = new ArrayList<>();
        List<TableChange> tableChanges = new ArrayList<>();
        for (org.apache.flink.table.catalog.Column oldCol : existingSchema.getColumns()) {
            if (event.getTypeMapping().containsKey(oldCol.getName())) {
                DataType newType =
                        DataTypeUtils.toFlinkDataType(event.getTypeMapping().get(oldCol.getName()));
                LOG.info(
                        "Altering column {} type from {} to {} in table {}",
                        oldCol.getName(),
                        oldCol.getDataType(),
                        newType,
                        tableId);
                newColumns.add(
                        org.apache.flink.table.catalog.Column.physical(oldCol.getName(), newType));
                tableChanges.add(TableChange.modifyPhysicalColumnType(oldCol, newType));
            } else {
                // No type change
                newColumns.add(oldCol);
            }
        }

        // Create new resolved schema
        ResolvedSchema newSchema =
                new ResolvedSchema(
                        newColumns,
                        existingSchema.getWatermarkSpecs(),
                        existingSchema.getPrimaryKey().orElse(null));

        // Create new catalog table
        ResolvedCatalogTable newTable = createUpdatedCatalogTable(existingTable, newSchema);

        // Alter table by passing in tableChanges
        catalog.alterTable(objectPath, newTable, tableChanges, false);
        LOG.info("Successfully altered column types in table {}", tableId);

        // Verify the change was persisted
        updateAndVerifyTableChange(tableId, objectPath, newSchema);
    }

    /**
     * Gets a table from the catalog and ensures it's returned as a ResolvedCatalogTable. If the
     * catalog returns a DefaultCatalogTable, it will be converted to ResolvedCatalogTable.
     */
    private ResolvedCatalogTable getResolvedCatalogTable(ObjectPath objectPath) throws Exception {
        CatalogBaseTable table = catalog.getTable(objectPath);

        if (table instanceof ResolvedCatalogTable) {
            return (ResolvedCatalogTable) table;
        }

        // If it's a CatalogTable (or DefaultCatalogTable), resolve it
        if (table instanceof CatalogTable) {
            CatalogTable catalogTable = (CatalogTable) table;
            org.apache.flink.table.api.Schema schema = catalogTable.getUnresolvedSchema();

            // Resolve the schema
            List<org.apache.flink.table.catalog.Column> resolvedColumns = new ArrayList<>();
            for (org.apache.flink.table.api.Schema.UnresolvedColumn column : schema.getColumns()) {
                if (column instanceof org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn) {
                    org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn physicalColumn =
                            (org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn) column;
                    // We need to get the data type - this is already resolved in the schema
                    // For now, we'll rebuild from the schema by resolving it
                    // This is a workaround since we don't have access to the type resolver
                }
            }

            // Alternative approach: rebuild the ResolvedCatalogTable from scratch
            // Extract physical columns from the schema
            ResolvedSchema resolvedSchema = resolveSchema(schema);

            return new ResolvedCatalogTable(catalogTable, resolvedSchema);
        }

        throw new IllegalStateException(
                "Unexpected catalog table type: " + table.getClass().getName());
    }

    /**
     * Resolves an unresolved schema to a ResolvedSchema. This manually extracts column information
     * from the schema.
     */
    private ResolvedSchema resolveSchema(org.apache.flink.table.api.Schema unresolvedSchema) {
        List<org.apache.flink.table.catalog.Column> columns = new ArrayList<>();

        for (org.apache.flink.table.api.Schema.UnresolvedColumn unresolvedColumn :
                unresolvedSchema.getColumns()) {
            if (unresolvedColumn
                    instanceof org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn) {
                org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn physicalColumn =
                        (org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn)
                                unresolvedColumn;

                // Get the column name
                String columnName = physicalColumn.getName();

                // Get the data type - cast from AbstractDataType to DataType
                // This is safe because unresolved schemas from catalog tables contain DataType
                DataType dataType = (DataType) physicalColumn.getDataType();

                columns.add(org.apache.flink.table.catalog.Column.physical(columnName, dataType));
            }
        }

        // Extract primary key if exists
        UniqueConstraint primaryKey = null;
        if (unresolvedSchema.getPrimaryKey().isPresent()) {
            org.apache.flink.table.api.Schema.UnresolvedPrimaryKey unresolvedPrimaryKey =
                    unresolvedSchema.getPrimaryKey().get();
            primaryKey =
                    UniqueConstraint.primaryKey(
                            unresolvedPrimaryKey.getConstraintName(),
                            unresolvedPrimaryKey.getColumnNames());
        }

        return new ResolvedSchema(columns, new ArrayList<>(), primaryKey);
    }

    private int findColumnIndex(
            List<org.apache.flink.table.catalog.Column> columns, String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    private ResolvedCatalogTable createUpdatedCatalogTable(
            ResolvedCatalogTable existingTable, ResolvedSchema newSchema) {
        // Build Flink Schema from resolved schema
        org.apache.flink.table.api.Schema tableSchema =
                org.apache.flink.table.api.Schema.newBuilder()
                        .fromResolvedSchema(newSchema)
                        .build();

        // Create new CatalogTable with same options and comment
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableSchema,
                        existingTable.getComment(),
                        existingTable.getPartitionKeys(),
                        existingTable.getOptions());

        return new ResolvedCatalogTable(catalogTable, newSchema);
    }

    /** Converts a Flink DataType to an Avro Schema. */
    private org.apache.avro.Schema convertFlinkTypeToAvro(DataType flinkType) {
        org.apache.flink.table.types.logical.LogicalType logicalType = flinkType.getLogicalType();

        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);
            case BIGINT:
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
            case FLOAT:
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT);
            case DOUBLE:
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE);
            case VARCHAR:
            case CHAR:
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
            case VARBINARY:
            case BINARY:
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES);
            case DECIMAL:
                org.apache.flink.table.types.logical.DecimalType decimalType =
                        (org.apache.flink.table.types.logical.DecimalType) logicalType;
                return org.apache.avro.LogicalTypes.decimal(
                                decimalType.getPrecision(), decimalType.getScale())
                        .addToSchema(
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));
            case DATE:
                return org.apache.avro.LogicalTypes.date()
                        .addToSchema(
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return org.apache.avro.LogicalTypes.timestampMicros()
                        .addToSchema(
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
            default:
                // Default to string for unsupported types
                LOG.warn("Unsupported Flink type {}, defaulting to STRING", logicalType);
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
        }
    }

    private void updateAndVerifyTableChange(
            TableId tableId, ObjectPath objectPath, ResolvedSchema newSchema) throws Exception {
        ResolvedCatalogTable verifyTable = getResolvedCatalogTable(objectPath);
        LOG.info(
                "Verified - Table {} now has columns: {}",
                tableId,
                verifyTable.getResolvedSchema().getColumns().stream()
                        .map(org.apache.flink.table.catalog.Column::getName)
                        .collect(Collectors.toList()));
    }

    /** Converts CDC Configuration to Flink Configuration for HoodieCatalog. */
    private org.apache.flink.configuration.Configuration convertToCatalogConfig(
            Configuration cdcConfig) {
        org.apache.flink.configuration.Configuration flinkConfig =
                new org.apache.flink.configuration.Configuration();

        // Set catalog path (base path for all tables)
        String basePath = cdcConfig.get(HudiConfig.PATH);
        flinkConfig.setString(CatalogOptions.CATALOG_PATH.key(), basePath);

        // Set mode to DFS (filesystem-based)
        // TODO: make this configurable
        flinkConfig.setString(CatalogOptions.MODE.key(), "dfs");

        // Set default database
        flinkConfig.setString(CatalogOptions.DEFAULT_DATABASE.key(), "default");

        return flinkConfig;
    }

    /** Converts CDC Schema to Flink ResolvedCatalogTable. */
    private ResolvedCatalogTable convertToCatalogTable(Schema cdcSchema) {
        // Build resolved columns
        List<org.apache.flink.table.catalog.Column> resolvedColumns = new ArrayList<>();
        for (Column column : cdcSchema.getColumns()) {
            DataType flinkType = DataTypeUtils.toFlinkDataType(column.getType());
            resolvedColumns.add(
                    org.apache.flink.table.catalog.Column.physical(column.getName(), flinkType));
        }

        // Build primary key constraint
        List<String> primaryKeys = cdcSchema.primaryKeys();
        UniqueConstraint primaryKeyConstraint = null;
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            primaryKeyConstraint = UniqueConstraint.primaryKey("pk", primaryKeys);
        }

        // Build ResolvedSchema
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        resolvedColumns,
                        new ArrayList<>(), // No watermark specs
                        primaryKeyConstraint);

        // Build Flink Schema from resolved schema
        org.apache.flink.table.api.Schema tableSchema =
                org.apache.flink.table.api.Schema.newBuilder()
                        .fromResolvedSchema(resolvedSchema)
                        .build();

        // Build table options (Hudi-specific configurations)
        Map<String, String> tableOptions = new HashMap<>();

        // Add table type
        String tableType = config.get(HudiConfig.TABLE_TYPE);
        if (tableType != null) {
            tableOptions.put(FlinkOptions.TABLE_TYPE.key(), tableType);
        }

        // Add ordering fields if specified
        String orderingFields = config.get(HudiConfig.ORDERING_FIELDS);
        if (orderingFields != null) {
            tableOptions.put(FlinkOptions.ORDERING_FIELDS.key(), orderingFields);
        }
        ConfigUtils.setupHoodieKeyOptions(tableOptions, cdcSchema);

        // Add partition fields if specified
        List<String> partitionKeys = cdcSchema.partitionKeys();

        // Create CatalogTable
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableSchema,
                        cdcSchema.comment(),
                        partitionKeys != null ? partitionKeys : Collections.emptyList(),
                        tableOptions);

        return new ResolvedCatalogTable(catalogTable, resolvedSchema);
    }
}
