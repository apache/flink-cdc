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

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;

import io.debezium.relational.TableId;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * PostgresPartitionRouter handles routing of PostgreSQL partitioned tables.
 *
 * <p>This class is responsible for mapping child partition tables to their parent tables using
 * pattern-based matching configured via {@code partition.tables}.
 *
 * <p><b>Architecture:</b> This class is part of a layered architecture:
 *
 * <ul>
 *   <li>Layer 1: {@link PostgresPartitionRules} - Configuration parsing
 *   <li>Layer 2: PostgresPartitionRouter - Routing mechanism (this class)
 *   <li>Layer 3: {@link PostgresPartitionInclusionDecider} - Filtering decisions
 *   <li>Layer 4: Integration classes (Schema, Dialect, etc.)
 * </ul>
 */
public class PostgresPartitionRouter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Whether partition routing is enabled. */
    private final boolean routingEnabled;

    /** Parsed partition routing rules. */
    private final PostgresPartitionRules rules;

    /**
     * Creates a new PostgresPartitionRouter with pre-parsed rules.
     *
     * <p>This is the primary constructor that accepts already-parsed rules, enabling reuse of
     * parsing results across multiple components (e.g., Router, ConnectorConfig, RoutingSchema).
     *
     * @param routingEnabled whether partition routing is enabled
     * @param rules the pre-parsed partition rules
     */
    public PostgresPartitionRouter(boolean routingEnabled, PostgresPartitionRules rules) {
        this.routingEnabled = routingEnabled;
        this.rules = rules;
    }

    /**
     * Creates a new PostgresPartitionRouter by parsing configuration strings.
     *
     * <p>This is a convenience constructor that parses the configuration internally. For better
     * reuse of parsed rules, consider using {@link #PostgresPartitionRouter(boolean,
     * PostgresPartitionRules)} or the factory method {@link #fromConfig}.
     *
     * @param routingEnabled whether partition routing is enabled
     * @param tables comma-separated list of parent table patterns
     * @param partitionTables comma-separated list of partition table patterns
     */
    public PostgresPartitionRouter(boolean routingEnabled, String tables, String partitionTables) {
        this(routingEnabled, PostgresPartitionRules.parse(tables, partitionTables, null));
    }

    /**
     * Creates a PostgresPartitionRouter from a PostgresSourceConfig.
     *
     * <p>This factory method extracts the necessary configuration and creates a router with
     * properly parsed rules. This is the recommended way to create a router as it ensures
     * consistent configuration parsing.
     *
     * @param sourceConfig the Postgres source configuration
     * @return a new PostgresPartitionRouter instance
     */
    public static PostgresPartitionRouter fromConfig(PostgresSourceConfig sourceConfig) {
        boolean routingEnabled = sourceConfig.includePartitionedTables();
        String tablesPattern = null;
        if (sourceConfig.getTableList() != null && !sourceConfig.getTableList().isEmpty()) {
            tablesPattern = String.join(",", sourceConfig.getTableList());
        }
        String partitionTables = sourceConfig.getPartitionTables();

        PostgresPartitionRules rules =
                PostgresPartitionRules.parse(tablesPattern, partitionTables, null);
        return new PostgresPartitionRouter(routingEnabled, rules);
    }

    /** Returns the parsed partition rules. */
    public PostgresPartitionRules getRules() {
        return rules;
    }

    /** Returns whether routing is enabled. */
    public boolean isRoutingEnabled() {
        return routingEnabled;
    }

    /**
     * Routes child partition tables to their parent tables.
     *
     * <p>This method returns only parent tables and non-partitioned tables in a deduplicated and
     * order-preserving manner.
     *
     * @param capturedTableIds list of captured table IDs
     * @return iterable of routed table IDs (parents and non-partitioned tables)
     */
    public Iterable<TableId> routeRepresentativeTables(List<TableId> capturedTableIds) {
        // Short-circuit if routing is disabled - return original list as-is
        if (!routingEnabled) {
            return new LinkedHashSet<>(capturedTableIds);
        }
        LinkedHashSet<TableId> routedTables = new LinkedHashSet<>();
        for (TableId tableId : capturedTableIds) {
            routedTables.add(getPartitionParent(tableId).orElse(tableId));
        }
        return routedTables;
    }

    /**
     * Gets the parent table for a given partition table using pattern matching.
     *
     * <p>Note: This is a low-level query method that does NOT check routingEnabled. Callers should
     * use {@link #route(TableId)} or {@link #isChildTable(TableId)} for routing-aware behavior.
     *
     * @param tableId the partition table ID
     * @return Optional containing the parent TableId if this is a child partition, empty otherwise
     */
    public Optional<TableId> getPartitionParent(TableId tableId) {
        String schemaTable =
                (tableId.schema() != null ? tableId.schema() : "") + "." + tableId.table();
        String tableOnly = tableId.table();

        for (Map.Entry<Pattern, TableId> entry : rules.getChildToParentMappings().entrySet()) {
            Pattern pattern = entry.getKey();
            if (pattern.matcher(schemaTable).matches()
                    || (tableOnly != null && pattern.matcher(tableOnly).matches())) {
                TableId mappedParent = entry.getValue();
                if (mappedParent != null
                        && mappedParent.schema() == null
                        && tableId.schema() != null) {
                    mappedParent =
                            PostgresPartitionRules.createTableId(
                                    tableId.schema(), mappedParent.table());
                }
                return Optional.ofNullable(mappedParent);
            }
        }
        return Optional.empty();
    }

    /**
     * Checks if the given table is a child partition table.
     *
     * <p>Note: This method respects the routingEnabled flag. When routing is disabled, it always
     * returns false to treat partitions as regular tables.
     *
     * @param tableId the table ID to check
     * @return true if routing is enabled and the table is a child partition, false otherwise
     */
    public boolean isChildTable(TableId tableId) {
        if (!routingEnabled) {
            return false;
        }
        return getPartitionParent(tableId).isPresent();
    }

    /**
     * Routes a table to its parent if routing is enabled and a parent exists. Otherwise returns
     * itself.
     *
     * @param tableId the table ID to route
     * @return the parent table ID if this is a child partition, otherwise the original table ID
     */
    public TableId route(TableId tableId) {
        if (!routingEnabled) {
            return tableId;
        }
        return getPartitionParent(tableId).orElse(tableId);
    }

    /**
     * Checks whether the given tableId is one of the configured parent tables.
     *
     * @param tableId the table ID to check
     * @return true if this is a configured parent table
     */
    public boolean isConfiguredParent(TableId tableId) {
        Set<TableId> parents = rules.getConfiguredParents();
        return parents != null
                && parents.contains(PostgresPartitionRules.toComparableTableId(tableId));
    }
}
