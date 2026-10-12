/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import io.debezium.config.Configuration;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Selectors.TableSelectionPredicateBuilder;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.schema.DataCollectionFilters;

import java.util.function.Predicate;

/**
 * Vendored from Debezium, updated for Debezium 3.4.2 API. Adds 4-param constructor
 * (useCatalogBeforeSchema) and schemaFilter() method. Retains setDataCollectionFilters() for MySQL
 * connector compatibility.
 */
public class RelationalTableFilters implements DataCollectionFilters {

    private final TableFilter eligibleTableFilter;
    private TableFilter tableFilter;
    private final Predicate<String> databaseFilter;
    private final Predicate<String> schemaFilter;
    private final String excludeColumns;
    private final TableFilter schemaSnapshotFilter;

    /** Debezium 3.4.2 constructor with useCatalogBeforeSchema. */
    public RelationalTableFilters(
            Configuration config,
            TableFilter systemTablesFilter,
            TableIdToStringMapper tableIdMapper,
            boolean useCatalogBeforeSchema) {
        this(config, systemTablesFilter, tableIdMapper);
    }

    /** Legacy 3-param constructor (used by MySQL connector path). */
    public RelationalTableFilters(
            Configuration config,
            TableFilter systemTablesFilter,
            TableIdToStringMapper tableIdMapper) {

        final TableSelectionPredicateBuilder eligibleTables =
                Selectors.tableSelector()
                        .includeDatabases(
                                config.getString(
                                        RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST))
                        .excludeDatabases(
                                config.getString(
                                        RelationalDatabaseConnectorConfig.DATABASE_EXCLUDE_LIST))
                        .includeSchemas(
                                config.getString(
                                        RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST))
                        .excludeSchemas(
                                config.getString(
                                        RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST));

        final Predicate<TableId> eligibleTablePredicate = eligibleTables.build();

        Predicate<TableId> finalEligibleTablePredicate =
                config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                        ? eligibleTablePredicate.and(systemTablesFilter::isIncluded)
                        : eligibleTablePredicate;

        this.eligibleTableFilter = finalEligibleTablePredicate::test;

        Predicate<TableId> tablePredicate =
                eligibleTables
                        .includeTables(
                                config.getString(
                                        RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST),
                                tableIdMapper)
                        .excludeTables(
                                config.getString(
                                        RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST),
                                tableIdMapper)
                        .build();

        Predicate<TableId> finalTablePredicate =
                config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                        ? tablePredicate.and(systemTablesFilter::isIncluded)
                        : tablePredicate;

        this.tableFilter = finalTablePredicate::test;

        this.databaseFilter =
                Selectors.databaseSelector()
                        .includeDatabases(
                                config.getString(
                                        RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST))
                        .excludeDatabases(
                                config.getString(
                                        RelationalDatabaseConnectorConfig.DATABASE_EXCLUDE_LIST))
                        .build();

        this.schemaFilter = s -> true;

        Predicate<TableId> eligibleSchemaPredicate =
                config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                        ? systemTablesFilter::isIncluded
                        : x -> true;

        this.schemaSnapshotFilter =
                config.getBoolean(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL)
                        ? eligibleSchemaPredicate.and(tableFilter::isIncluded)::test
                        : eligibleSchemaPredicate::test;

        this.excludeColumns =
                config.getString(RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST);
    }

    @Override
    public TableFilter dataCollectionFilter() {
        return tableFilter;
    }

    public TableFilter eligibleDataCollectionFilter() {
        return eligibleTableFilter;
    }

    public TableFilter eligibleForSchemaDataCollectionFilter() {
        return schemaSnapshotFilter;
    }

    public Predicate<String> databaseFilter() {
        return databaseFilter;
    }

    public Predicate<String> schemaFilter() {
        return schemaFilter;
    }

    public String getExcludeColumns() {
        return excludeColumns;
    }

    public void setDataCollectionFilters(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }
}
