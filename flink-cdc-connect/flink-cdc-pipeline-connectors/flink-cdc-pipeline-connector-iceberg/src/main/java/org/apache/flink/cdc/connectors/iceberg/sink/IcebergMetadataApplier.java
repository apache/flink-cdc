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

package org.apache.flink.cdc.connectors.iceberg.sink;

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
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.IcebergTypeUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/** A {@link MetadataApplier} for Apache Iceberg. */
public class IcebergMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataApplier.class);

    private transient Catalog catalog;

    private final Map<String, String> catalogOptions;

    // currently, we set table options for all tables using the same options.
    private final Map<String, String> tableOptions;

    private final Map<TableId, List<String>> partitionMaps;

    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    public IcebergMetadataApplier(Map<String, String> catalogOptions) {
        this(catalogOptions, new HashMap<>(), new HashMap<>());
    }

    public IcebergMetadataApplier(
            Map<String, String> catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = tableOptions;
        this.partitionMaps = partitionMaps;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (catalog == null) {
            catalog =
                    CatalogUtil.buildIcebergCatalog(
                            this.getClass().getSimpleName(), catalogOptions, new Configuration());
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

    private void applyCreateTable(CreateTableEvent event) {
        try {
            long startTimestamp = System.currentTimeMillis();
            TableIdentifier tableIdentifier = TableIdentifier.parse(event.tableId().identifier());
            // Step 0: Create namespace if not exists.
            if (catalog instanceof SupportsNamespaces) {
                SupportsNamespaces namespaceCatalog = (SupportsNamespaces) catalog;
                Namespace namespace = Namespace.of(tableIdentifier.namespace().levels());
                if (!namespaceCatalog.namespaceExists(namespace)) {
                    namespaceCatalog.createNamespace(namespace);
                }
            }

            // Step1: Build Schema.
            org.apache.flink.cdc.common.schema.Schema cdcSchema = event.getSchema();
            List<Types.NestedField> columns = new ArrayList<>();
            Set<Integer> identifierFieldIds = new HashSet<>();
            for (int index = 0; index < event.getSchema().getColumnCount(); index++) {
                columns.add(
                        IcebergTypeUtils.convertCdcColumnToIcebergField(
                                index, (PhysicalColumn) cdcSchema.getColumns().get(index)));
                if (cdcSchema.primaryKeys().contains(cdcSchema.getColumns().get(index).getName())) {
                    identifierFieldIds.add(index);
                }
            }

            // Step2: Build partition spec.
            Schema icebergSchema = new Schema(columns, identifierFieldIds);
            List<String> partitionColumns = cdcSchema.partitionKeys();
            if (partitionMaps.containsKey(event.tableId())) {
                partitionColumns = partitionMaps.get(event.tableId());
            }
            PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
            for (String name : partitionColumns) {
                // TODO Add more partition transforms, see
                // https://iceberg.apache.org/spec/#partition-transforms.
                builder.identity(name);
            }
            PartitionSpec partitionSpec = builder.build();
            if (!catalog.tableExists(tableIdentifier)) {
                catalog.createTable(tableIdentifier, icebergSchema, partitionSpec, tableOptions);
                LOG.info(
                        "Spend {} ms to create iceberg table {}",
                        System.currentTimeMillis() - startTimestamp,
                        tableIdentifier);
            }
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAddColumn(AddColumnEvent event) {
        TableIdentifier tableIdentifier = TableIdentifier.parse(event.tableId().identifier());
        try {
            Table table = catalog.loadTable(tableIdentifier);
            applyAddColumnEventWithPosition(table, event);
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAddColumnEventWithPosition(Table table, AddColumnEvent event)
            throws SchemaEvolveException {

        try {
            UpdateSchema updateSchema = table.updateSchema();
            for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
                Column addColumn = columnWithPosition.getAddColumn();
                String columnName = addColumn.getName();
                String columnComment = addColumn.getComment();
                Type icebergType =
                        FlinkSchemaUtil.convert(
                                DataTypeUtils.toFlinkDataType(addColumn.getType())
                                        .getLogicalType());
                switch (columnWithPosition.getPosition()) {
                    case FIRST:
                        updateSchema.addColumn(columnName, icebergType, columnComment);
                        table.updateSchema().moveFirst(columnName);
                        break;
                    case LAST:
                        updateSchema.addColumn(columnName, icebergType, columnComment);
                        break;
                    case BEFORE:
                        updateSchema.addColumn(columnName, icebergType, columnComment);
                        updateSchema.moveBefore(
                                columnName, columnWithPosition.getExistedColumnName());
                        break;
                    case AFTER:
                        checkNotNull(
                                columnWithPosition.getExistedColumnName(),
                                "Existing column name must be provided for AFTER position");
                        updateSchema.addColumn(columnName, icebergType, columnComment);
                        updateSchema.moveAfter(
                                columnName, columnWithPosition.getExistedColumnName());
                        break;
                    default:
                        throw new SchemaEvolveException(
                                event,
                                "Unknown column position: " + columnWithPosition.getPosition());
                }
            }
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyDropColumn(DropColumnEvent event) {
        try {
            UpdateSchema updateSchema =
                    catalog.loadTable(TableIdentifier.parse(event.tableId().identifier()))
                            .updateSchema();
            event.getDroppedColumnNames().forEach(updateSchema::deleteColumn);
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyRenameColumn(RenameColumnEvent event) {
        try {
            UpdateSchema updateSchema =
                    catalog.loadTable(TableIdentifier.parse(event.tableId().identifier()))
                            .updateSchema();
            event.getNameMapping().forEach(updateSchema::renameColumn);
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event) {
        try {
            UpdateSchema updateSchema =
                    catalog.loadTable(TableIdentifier.parse(event.tableId().identifier()))
                            .updateSchema();
            event.getTypeMapping()
                    .forEach(
                            (name, newType) -> {
                                Type.PrimitiveType type =
                                        FlinkSchemaUtil.convert(
                                                        DataTypeUtils.toFlinkDataType(newType)
                                                                .getLogicalType())
                                                .asPrimitiveType();
                                updateSchema.updateColumn(name, type);
                            });
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
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
    public void close() {
        catalog = null;
    }
}
