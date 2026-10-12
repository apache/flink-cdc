/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/*
 * Vendored from Debezium 3.4.2.Final with one change: buildAndRegisterSchema() guards the
 * DebeziumOpenLineageEmitter call with a null check on taskContext.  In Flink CDC's incremental
 * snapshot path the taskContext field can legitimately be null (OracleTaskContext is created
 * in configure() but a Debezium 3.4.2 API change causes RelationalDatabaseSchema to require
 * it during schema recovery via HistorizedRelationalDatabaseSchema.recover()), leading to NPE.
 */
package io.debezium.relational;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.relational.Key.KeyMapper;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.topic.TopicNamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.debezium.openlineage.dataset.DatasetMetadata.DatasetKind.INPUT;

/**
 * Vendored from Debezium 3.4.2.Final. The only change is adding a null guard on {@code taskContext}
 * inside {@link #buildAndRegisterSchema(Table)} so that OpenLineage telemetry is skipped when no
 * task context is available (Flink CDC incremental snapshot path).
 */
public abstract class RelationalDatabaseSchema implements DatabaseSchema<TableId> {

    private static final Logger LOG = LoggerFactory.getLogger(RelationalDatabaseSchema.class);

    private final RelationalDatabaseConnectorConfig config;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;
    private final TableSchemaBuilder schemaBuilder;
    private final TableFilter tableFilter;
    private final ColumnNameFilter columnFilter;
    private final ColumnMappers columnMappers;
    private final KeyMapper customKeysMapper;
    private final CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext;

    private final SchemasByTableId schemasByTableId;
    private final Tables tables;

    protected RelationalDatabaseSchema(
            RelationalDatabaseConnectorConfig config,
            TopicNamingStrategy<TableId> topicNamingStrategy,
            TableFilter tableFilter,
            ColumnNameFilter columnFilter,
            TableSchemaBuilder schemaBuilder,
            boolean tableIdCaseInsensitive,
            KeyMapper customKeysMapper,
            CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext) {
        this.config = config;
        this.topicNamingStrategy = topicNamingStrategy;
        this.schemaBuilder = schemaBuilder;
        this.tableFilter = tableFilter;
        this.columnFilter = columnFilter;
        this.columnMappers = ColumnMappers.create(config);
        this.customKeysMapper = customKeysMapper;
        this.schemasByTableId = new SchemasByTableId(tableIdCaseInsensitive);
        this.tables = new Tables(tableIdCaseInsensitive);
        this.taskContext = taskContext;
    }

    @Override
    public void close() {}

    public Set<TableId> tableIds() {
        return tables.subset(tableFilter).tableIds();
    }

    @Override
    public Collection<TableId> dataCollectionIds() {
        return tableIds();
    }

    @Override
    public void assureNonEmptySchema() {
        if (tableIds().isEmpty()) {
            LOG.warn(NO_CAPTURED_DATA_COLLECTIONS_WARNING);
        }
    }

    @Override
    public TableSchema schemaFor(TableId id) {
        return schemasByTableId.get(id);
    }

    public Table tableFor(TableId id) {
        return tableFilter.isIncluded(id) ? tables.forTable(id) : null;
    }

    @Override
    public boolean isHistorized() {
        return false;
    }

    protected Tables tables() {
        return tables;
    }

    protected void clearSchemas() {
        schemasByTableId.clear();
    }

    /**
     * Builds the CDC event schema for the given table and registers it in this schema. The
     * OpenLineage telemetry call is skipped when {@code taskContext} is null to avoid NPE in the
     * Flink CDC incremental snapshot path.
     */
    protected void buildAndRegisterSchema(Table table) {
        if (tableFilter.isIncluded(table.id())) {
            TableSchema schema =
                    schemaBuilder.create(
                            topicNamingStrategy,
                            table,
                            columnFilter,
                            columnMappers,
                            customKeysMapper);
            schemasByTableId.put(table.id(), schema);
            if (taskContext != null) {
                DebeziumOpenLineageEmitter.emit(
                        DebeziumOpenLineageEmitter.connectorContext(
                                taskContext.getRawConfig().asMap(),
                                config.getConnectorName(),
                                taskContext.getRunId()),
                        DebeziumTaskState.RUNNING,
                        List.of(extractDatasetMetadata(table)));
            }
        }
    }

    private DatasetMetadata extractDatasetMetadata(Table table) {
        List<DatasetMetadata.FieldDefinition> fieldDefinitions =
                table.columns().stream()
                        .map(
                                c ->
                                        new DatasetMetadata.FieldDefinition(
                                                c.name(), c.typeName(), c.comment()))
                        .toList();
        return new DatasetMetadata(
                getIdentifier(table),
                INPUT,
                DatasetMetadata.TABLE_DATASET_TYPE,
                DatasetMetadata.DataStore.DATABASE,
                fieldDefinitions);
    }

    private String getIdentifier(Table table) {
        String dbName =
                config.getJdbcConfig().getDatabase() == null
                        ? ""
                        : config.getJdbcConfig().getDatabase();
        if (table.id().catalog() == null) {
            return dbName + "." + table.id().identifier();
        }
        return table.id().identifier();
    }

    protected void removeSchema(TableId id) {
        schemasByTableId.remove(id);
    }

    protected TableFilter getTableFilter() {
        return tableFilter;
    }

    @Override
    public boolean tableInformationComplete() {
        return false;
    }

    public void refresh(Table table) {
        tables().overwriteTable(table);
        refreshSchema(table.id());
    }

    protected void refreshSchema(TableId id) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("refreshing DB schema for table '{}'", id);
        }
        Table table = tableFor(id);
        buildAndRegisterSchema(table);
    }

    private static class SchemasByTableId {

        private final boolean tableIdCaseInsensitive;
        private final ConcurrentMap<TableId, TableSchema> values;

        SchemasByTableId(boolean tableIdCaseInsensitive) {
            this.tableIdCaseInsensitive = tableIdCaseInsensitive;
            this.values = new ConcurrentHashMap<>();
        }

        public void clear() {
            values.clear();
        }

        public TableSchema remove(TableId tableId) {
            return values.remove(toLowerCaseIfNeeded(tableId));
        }

        public TableSchema get(TableId tableId) {
            return values.get(toLowerCaseIfNeeded(tableId));
        }

        public TableSchema put(TableId tableId, TableSchema updated) {
            return values.put(toLowerCaseIfNeeded(tableId), updated);
        }

        private TableId toLowerCaseIfNeeded(TableId tableId) {
            return tableIdCaseInsensitive ? tableId.toLowercase() : tableId;
        }
    }
}
