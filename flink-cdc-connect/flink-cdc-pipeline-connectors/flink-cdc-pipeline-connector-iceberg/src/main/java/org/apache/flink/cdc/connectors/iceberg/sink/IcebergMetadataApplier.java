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
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;

import org.apache.flink.shaded.guava32.com.google.common.collect.Sets;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

public class IcebergMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataApplier.class);

    // Catalog is unSerializable.
    private transient Catalog catalog;

    private final Map<String, String> catalogOptions;

    private final Map<TableId, List<String>> partitionMaps;

    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    // currently, we set table options for all tables using the same options.
    private final Map<String, String> tableOptions;

    public IcebergMetadataApplier(Map<String, String> catalogOptions) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = new HashMap<>();
        this.partitionMaps = new HashMap<>();
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
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
            catalog = CatalogUtil.buildIcebergCatalog("cdc-iceberg-catalog", catalogOptions, null);
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
            Namespace namespace = Namespace.of(event.tableId().getNamespace());
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(namespace, event.tableId().getSchemaName());
            org.apache.flink.cdc.common.schema.Schema cdcSchema = event.getSchema();
            Table table = catalog.loadTable(tableIdentifier);
            Schema icebergSchema = new Schema();
            for (Column column : cdcSchema.getColumns()) {
                table.updateSchema()
                        .addColumn(
                                column.getName(),
                                FlinkSchemaUtil.convert(
                                        DataTypeUtils.toFlinkDataType(column.getType())
                                                .getLogicalType()),
                                column.getComment());
            }
            table.updateSchema().setIdentifierFields(cdcSchema.partitionKeys());
            PartitionSpec spec = null;
            if (partitionMaps.containsKey(event.tableId())) {
                spec =
                        PartitionSpec.builderFor(icebergSchema)
                                .identity(String.join(",", partitionMaps.get(event.tableId())))
                                .build();
            } else if (cdcSchema.partitionKeys() != null && !cdcSchema.partitionKeys().isEmpty()) {
                spec =
                        PartitionSpec.builderFor(icebergSchema)
                                .identity(String.join(",", cdcSchema.partitionKeys()))
                                .build();
            }
            catalog.createTable(tableIdentifier, icebergSchema, spec, catalogOptions);
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAddColumn(AddColumnEvent event) {
        try {
            Namespace namespace = Namespace.of(event.tableId().getNamespace());
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(namespace, event.tableId().getSchemaName());
            Table table = catalog.loadTable(tableIdentifier);
            applyAddColumnEventWithPosition(table, event);

        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAddColumnEventWithPosition(Table table, AddColumnEvent event)
            throws SchemaEvolveException {
        try {
            for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
                Column addColumn = columnWithPosition.getAddColumn();
                switch (columnWithPosition.getPosition()) {
                    case FIRST:
                        table.updateSchema()
                                .addColumn(
                                        addColumn.getName(),
                                        FlinkSchemaUtil.convert(
                                                DataTypeUtils.toFlinkDataType(addColumn.getType())
                                                        .getLogicalType()),
                                        addColumn.getComment());
                        table.updateSchema().moveFirst(addColumn.getName());
                        break;
                    case LAST:
                        table.updateSchema()
                                .addColumn(
                                        addColumn.getName(),
                                        FlinkSchemaUtil.convert(
                                                DataTypeUtils.toFlinkDataType(addColumn.getType())
                                                        .getLogicalType()),
                                        addColumn.getComment());
                        break;
                    case BEFORE:
                        table.updateSchema()
                                .addColumn(
                                        addColumn.getName(),
                                        FlinkSchemaUtil.convert(
                                                DataTypeUtils.toFlinkDataType(addColumn.getType())
                                                        .getLogicalType()),
                                        addColumn.getComment());
                        table.updateSchema()
                                .moveBefore(
                                        addColumn.getName(),
                                        columnWithPosition.getExistedColumnName());
                        break;
                    case AFTER:
                        checkNotNull(
                                columnWithPosition.getExistedColumnName(),
                                "Existing column name must be provided for AFTER position");
                        table.updateSchema()
                                .addColumn(
                                        addColumn.getName(),
                                        FlinkSchemaUtil.convert(
                                                DataTypeUtils.toFlinkDataType(addColumn.getType())
                                                        .getLogicalType()),
                                        addColumn.getComment());
                        table.updateSchema()
                                .moveAfter(
                                        columnWithPosition.getAddColumn().getName(),
                                        columnWithPosition.getExistedColumnName());
                        break;
                    default:
                        throw new SchemaEvolveException(
                                event,
                                "Unknown column position: " + columnWithPosition.getPosition());
                }
            }
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyDropColumn(DropColumnEvent event) {
        try {
            Namespace namespace = Namespace.of(event.tableId().getNamespace());
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(namespace, event.tableId().getSchemaName());
            Table table = catalog.loadTable(tableIdentifier);
            event.getDroppedColumnNames()
                    .forEach((column) -> table.updateSchema().deleteColumn(column));
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyRenameColumn(RenameColumnEvent event) {
        try {
            Namespace namespace = Namespace.of(event.tableId().getNamespace());
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(namespace, event.tableId().getSchemaName());
            Table table = catalog.loadTable(tableIdentifier);
            event.getNameMapping()
                    .forEach(
                            (oldName, newName) ->
                                    table.updateSchema().renameColumn(oldName, newName));
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event) {
        try {
            Namespace namespace = Namespace.of(event.tableId().getNamespace());
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(namespace, event.tableId().getSchemaName());
            Table table = catalog.loadTable(tableIdentifier);
            event.getTypeMapping()
                    .forEach(
                            (oldName, newType) -> {
                                Type.PrimitiveType type =
                                        FlinkSchemaUtil.convert(
                                                        DataTypeUtils.toFlinkDataType(newType)
                                                                .getLogicalType())
                                                .asPrimitiveType();
                                table.updateSchema().updateColumn(oldName, type);
                            });
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
}
