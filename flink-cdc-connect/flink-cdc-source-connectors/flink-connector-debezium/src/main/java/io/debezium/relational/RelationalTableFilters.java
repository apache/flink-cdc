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
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.schema.DataCollectionFilters;

import java.util.function.Predicate;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.COLUMN_BLACKLIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST;

/**
 * Copied from Debezium 1.9.8.Final.
 *
 * <p>Line 146: add a method to update the tableFilter variable.
 */
public class RelationalTableFilters implements DataCollectionFilters {

    // Filter that filters tables based only on datbase/schema/system table filters
    // but not table
    // filters
    // Represents the list of tables whose schema needs to be captured
    private final TableFilter eligibleTableFilter;
    // Filter that filters tables based on table filters
    private TableFilter tableFilter;
    private final Predicate<String> databaseFilter;
    private final String excludeColumns;

    /**
     * Evaluate whether the table is eligible for schema snapshotting or not. This closely relates
     * to fact whether only captured tables schema should be stored in database history or all
     * tables schema.
     */
    private final TableFilter schemaSnapshotFilter;

    public RelationalTableFilters(
            Configuration config,
            TableFilter systemTablesFilter,
            TableIdToStringMapper tableIdMapper) {
        // Define the filter that provides the list of tables that could be captured if
        // configured
        String dbInclude =
                config.getFallbackStringProperty(
                        RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST,
                        RelationalDatabaseConnectorConfig.DATABASE_WHITELIST);
        String dbExclude =
                config.getFallbackStringProperty(
                        RelationalDatabaseConnectorConfig.DATABASE_EXCLUDE_LIST,
                        RelationalDatabaseConnectorConfig.DATABASE_BLACKLIST);
        String schemaInclude =
                config.getFallbackStringProperty(
                        RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST,
                        RelationalDatabaseConnectorConfig.SCHEMA_WHITELIST);
        String schemaExclude =
                config.getFallbackStringProperty(
                        RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST,
                        RelationalDatabaseConnectorConfig.SCHEMA_BLACKLIST);
        String tableInclude =
                config.getFallbackStringProperty(
                        RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                        RelationalDatabaseConnectorConfig.TABLE_WHITELIST);
        String tableExclude =
                config.getFallbackStringProperty(
                        RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST,
                        RelationalDatabaseConnectorConfig.TABLE_BLACKLIST);

        final TableSelectionPredicateBuilder eligibleTables =
                Selectors.tableSelector()
                        .includeDatabases(dbInclude)
                        .excludeDatabases(dbExclude)
                        .includeSchemas(schemaInclude)
                        .excludeSchemas(schemaExclude);
        final Predicate<TableId> eligibleTablePredicate = eligibleTables.build();

        Predicate<TableId> finalEligibleTablePredicate =
                config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                        ? eligibleTablePredicate.and(systemTablesFilter::isIncluded)
                        : eligibleTablePredicate;

        this.eligibleTableFilter = finalEligibleTablePredicate::test;

        // Define the filter using the include and exclude lists for tables and database
        // names ...
        Predicate<TableId> tablePredicate =
                eligibleTables
                        .includeTables(tableInclude, tableIdMapper)
                        .excludeTables(tableExclude, tableIdMapper)
                        .build();

        Predicate<TableId> combinedPredicate = eligibleTablePredicate.and(tablePredicate);
        final Predicate<TableId> finalTablePredicate =
                (systemTablesFilter != null)
                        ? combinedPredicate.and(systemTablesFilter::isIncluded)
                        : combinedPredicate;

        this.tableFilter = finalTablePredicate::test;

        // Define the database filter using the include and exclude lists for database
        // names
        // ...
        this.databaseFilter =
                Selectors.databaseSelector()
                        .includeDatabases(dbInclude)
                        .excludeDatabases(dbExclude)
                        .build();

        Predicate<TableId> eligibleSchemaPredicate =
                config.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)
                        ? systemTablesFilter::isIncluded
                        : x -> true;

        this.schemaSnapshotFilter =
                config.getBoolean(DatabaseHistory.STORE_ONLY_CAPTURED_TABLES_DDL)
                        ? eligibleSchemaPredicate.and(tableFilter::isIncluded)::test
                        : eligibleSchemaPredicate::test;

        this.excludeColumns =
                config.getFallbackStringProperty(COLUMN_EXCLUDE_LIST, COLUMN_BLACKLIST);
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

    public String getExcludeColumns() {
        return excludeColumns;
    }

    public void setDataCollectionFilters(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }
}
