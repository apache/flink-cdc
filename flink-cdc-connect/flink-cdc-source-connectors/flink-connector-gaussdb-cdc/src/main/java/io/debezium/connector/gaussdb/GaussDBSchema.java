/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.gaussdb;

import org.apache.flink.cdc.connectors.gaussdb.source.utils.CustomGaussDBSchema;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;

import javax.annotation.Nullable;

import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjuster;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link DatabaseSchema} implementation for GaussDB.
 *
 * <p>This class provides access to table metadata and maintains a cache of derived {@link
 * TableSchema}s, backed by {@link CustomGaussDBSchema} which queries pg_catalog system tables.
 */
public class GaussDBSchema extends RelationalDatabaseSchema {

    private final CustomGaussDBSchema schemaCache;
    private final ConcurrentMap<TableId, io.debezium.relational.history.TableChanges.TableChange>
            schemaHistory;
    private final ConcurrentMap<TableId, io.debezium.relational.TableSchema> tableSchemas;
    private final TableSchemaBuilder tableSchemaBuilder;
    private final TopicSelector<TableId> topicSelector;

    public GaussDBSchema(GaussDBConnectorConfig config, JdbcConnection jdbcConnection) {
        super(
                config,
                createTopicSelector(config),
                config.getTableFilters().dataCollectionFilter(),
                config.getColumnFilter(),
                createTableSchemaBuilder(config),
                false,
                config.getKeyMapper());
        this.schemaCache = new CustomGaussDBSchema(jdbcConnection);
        this.schemaHistory = new ConcurrentHashMap<>();
        this.tableSchemas = new ConcurrentHashMap<>();
        this.tableSchemaBuilder = createTableSchemaBuilder(config);
        this.topicSelector = createTopicSelector(config);
    }

    /**
     * Override schemaFor to bypass the tableFilter check that was causing tables to be incorrectly
     * filtered due to 3-part TableId format mismatch.
     */
    @Override
    @Nullable
    public io.debezium.relational.TableSchema schemaFor(TableId tableId) {
        if (tableId == null) {
            return null;
        }

        // Check cache first
        io.debezium.relational.TableSchema cached = tableSchemas.get(tableId);
        if (cached != null) {
            return cached;
        }

        // Get the table definition
        Table table = tableFor(tableId);
        if (table == null) {
            return null;
        }

        // Build the TableSchema directly, bypassing the parent's filter
        // Use the topic selector to generate schema prefix and envelope schema name
        String topicName = topicSelector.topicNameFor(tableId);
        io.debezium.relational.TableSchema schema =
                tableSchemaBuilder.create(
                        topicName, // schemaPrefix
                        topicName, // envelopeSchemaName
                        table, null, // columnFilter - include all columns
                        null, // columnMappers - no column mapping
                        null); // keyMapper - use default

        if (schema != null) {
            tableSchemas.put(tableId, schema);
        }
        return schema;
    }

    @Override
    @Nullable
    public Table tableFor(TableId tableId) {
        Table cached = super.tableFor(tableId);
        if (cached != null) {
            return cached;
        }

        io.debezium.relational.history.TableChanges.TableChange change =
                schemaCache.getTableSchema(tableId);
        if (change == null) {
            return null;
        }

        schemaHistory.put(tableId, change);
        Table table = change.getTable();
        if (table == null) {
            return null;
        }

        super.refresh(table);
        Table result = super.tableFor(tableId);
        return result;
    }

    /** Returns the last observed schema change per table. */
    public Map<TableId, io.debezium.relational.history.TableChanges.TableChange> schemaHistory() {
        return Collections.unmodifiableMap(schemaHistory);
    }

    /** Invalidates derived schemas and cached table definition for the given table. */
    public void refreshTable(TableId tableId) {
        if (tableId == null) {
            return;
        }
        schemaCache.invalidateTableSchema(tableId);
        schemaHistory.remove(tableId);
        super.refreshSchema(tableId);
    }

    @Override
    public void close() {
        schemaCache.clear();
        schemaHistory.clear();
        super.close();
    }

    private static TopicSelector<TableId> createTopicSelector(GaussDBConnectorConfig config) {
        return TopicSelector.defaultSelector(
                config,
                (tableId, prefix, delimiter) ->
                        prefix + delimiter + config.getTableIdMapper().toString(tableId));
    }

    private static TableSchemaBuilder createTableSchemaBuilder(GaussDBConnectorConfig config) {
        TemporalAdjuster adjuster = temporal -> temporal;
        JdbcValueConverters converters =
                new JdbcValueConverters(
                        config.getDecimalMode(),
                        config.getTemporalPrecisionMode(),
                        ZoneOffset.UTC,
                        adjuster,
                        JdbcValueConverters.BigIntUnsignedMode.LONG,
                        config.binaryHandlingMode());

        return new TableSchemaBuilder(
                converters,
                new GaussDBDefaultValueConverter(),
                config.schemaNameAdjustmentMode().createAdjuster(),
                config.customConverterRegistry(),
                config.getSourceInfoStructMaker().schema(),
                config.getSanitizeFieldNames(),
                false);
    }
}
