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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Extended PostgresConnectorConfig that wraps TableFilter to include partition parent tables.
 *
 * <p>This ensures that partition parent tables pass the filter check in
 * RelationalDatabaseSchema.buildAndRegisterSchema(), even when they are not explicitly listed in
 * table.include.list.
 */
public class PostgresPartitionConnectorConfig extends PostgresConnectorConfig {

    private final RelationalTableFilters wrappedTableFilters;

    /**
     * Creates a PostgresPartitionConnectorConfig with a pre-configured router.
     *
     * <p>This constructor reuses the router's parsed rules, avoiding duplicate parsing.
     *
     * @param config the Debezium configuration
     * @param router the partition router (nullable)
     */
    public PostgresPartitionConnectorConfig(
            Configuration config, @Nullable PostgresPartitionRouter router) {
        super(config);
        this.wrappedTableFilters = createWrappedTableFilters(config, router);
    }

    public PostgresPartitionConnectorConfig(Configuration config) {
        this(config, null);
    }

    @Override
    public RelationalTableFilters getTableFilters() {
        return wrappedTableFilters;
    }

    private RelationalTableFilters createWrappedTableFilters(
            Configuration config, @Nullable PostgresPartitionRouter router) {
        RelationalTableFilters baseFilters = super.getTableFilters();

        // Apply partition-aware filtering if router is provided and routing is enabled
        if (router != null && router.isRoutingEnabled()) {
            PostgresPartitionRules rules = router.getRules();
            Set<TableId> configuredParents = rules.getConfiguredParents();
            List<Pattern> childPatterns = rules.getChildTablePatterns();
            return new PartitionAwareTableFilters(baseFilters, configuredParents, childPatterns);
        }

        // If routing is not enabled, return base filters without partition filtering
        return baseFilters;
    }

    /**
     * Wrapper for RelationalTableFilters that includes partition parent tables and excludes
     * partition child tables from the filter.
     *
     * <p>This ensures that:
     *
     * <ul>
     *   <li>Partition parent tables pass the filter even if not in table.include.list
     *   <li>Partition child tables are excluded so super.refresh() only loads main tables
     * </ul>
     */
    private static class PartitionAwareTableFilters extends RelationalTableFilters {

        private final RelationalTableFilters delegate;
        private final Set<TableId> configuredParents;
        private final List<Pattern> childPatterns;
        private final Tables.TableFilter wrappedDataCollectionFilter;

        PartitionAwareTableFilters(
                RelationalTableFilters delegate,
                Set<TableId> configuredParents,
                List<Pattern> childPatterns) {
            // Call super with dummy config - we'll delegate all methods
            super(
                    Configuration.empty(),
                    tableId -> true,
                    tableId -> tableId.catalog() + "." + tableId.schema() + "." + tableId.table());
            this.delegate = delegate;
            this.configuredParents = configuredParents;
            this.childPatterns = childPatterns;
            this.wrappedDataCollectionFilter = createWrappedFilter();
        }

        private Tables.TableFilter createWrappedFilter() {
            Tables.TableFilter baseFilter = delegate.dataCollectionFilter();
            return tableId -> {
                // Exclude partition child tables (they will be loaded on-demand)
                if (isPartitionChild(tableId)) {
                    return false;
                }
                // Include configured partition parents
                if (configuredParents.contains(tableId)) {
                    return true;
                }
                // Delegate to base filter for other tables
                return baseFilter.isIncluded(tableId);
            };
        }

        /** Checks if the table matches any child partition pattern. */
        private boolean isPartitionChild(TableId tableId) {
            if (childPatterns.isEmpty()) {
                return false;
            }
            String fullName = tableId.schema() + "." + tableId.table();
            for (Pattern pattern : childPatterns) {
                if (pattern.matcher(fullName).matches()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Tables.TableFilter dataCollectionFilter() {
            return wrappedDataCollectionFilter;
        }

        @Override
        public Tables.TableFilter eligibleDataCollectionFilter() {
            return delegate.eligibleDataCollectionFilter();
        }

        @Override
        public Tables.TableFilter eligibleForSchemaDataCollectionFilter() {
            return delegate.eligibleForSchemaDataCollectionFilter();
        }

        @Override
        public Predicate<String> databaseFilter() {
            return delegate.databaseFilter();
        }

        @Override
        public String getExcludeColumns() {
            return delegate.getExcludeColumns();
        }
    }
}
