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

import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.utils.Predicates;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Policy layer for partition table inclusion decisions and selector building.
 *
 * <p>This class centralizes the inclusion semantics for PostgreSQL partitioned tables:
 *
 * <ul>
 *   <li><b>Rule 1:</b> Parent included/configured → include all partitions
 *   <li><b>Rule 2:</b> Partitions are included based on parent, not direct child match
 * </ul>
 *
 * <p><b>Architecture:</b> This class is part of a layered architecture:
 *
 * <ul>
 *   <li>Layer 1: {@link PostgresPartitionRules} - Configuration parsing
 *   <li>Layer 2: {@link PostgresPartitionRouter} - Routing mechanism
 *   <li>Layer 3: PostgresPartitionInclusionDecider - Filtering decisions (this class)
 *   <li>Layer 4: Integration classes (Schema, Dialect, etc.)
 * </ul>
 */
public class PostgresPartitionInclusionDecider implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Tables.TableFilter filters;
    @Nullable private final PostgresPartitionRouter router;

    /** Cached selectors for table matching (thread-safe lazy initialization). */
    private transient AtomicReference<Selectors> selectorsRef;

    /**
     * Creates a new PostgresPartitionInclusionDecider.
     *
     * @param filters the table filters for inclusion decisions
     * @param router the partition router for physical→logical mapping
     */
    public PostgresPartitionInclusionDecider(
            Tables.TableFilter filters, PostgresPartitionRouter router) {
        this.filters = filters;
        this.router = router;
    }

    /**
     * Determines if a table should be included based on partition semantics.
     *
     * <p>Implementation of semantic rules:
     *
     * <ul>
     *   <li>Rule 1: If parent is included, all partitions are included
     *   <li>Rule 2: If partition is explicitly included but parent is excluded, do not capture
     *   <li>Rule 3: Always route to parent table name
     *   <li>Rule 4: Exclude configured partition parent tables (data resides in child partitions)
     * </ul>
     *
     * @param tableId the table to check
     * @return true if the table should be included
     */
    public boolean isIncluded(TableId tableId) {
        if (tableId == null) {
            return false;
        }

        // Exclude configured partition parent tables when routing is enabled
        // Parent tables don't store data - all data is in child partitions
        if (router != null && router.isRoutingEnabled() && router.isConfiguredParent(tableId)) {
            return false;
        }

        // Child partitions are included based on their parent.
        if (router != null && router.isChildTable(tableId)) {
            TableId parent = router.getPartitionParent(tableId).orElse(null);
            if (parent == null) {
                return false;
            }
            return isParentIncluded(parent);
        }

        // Non-partition tables fall back to direct filter match.
        return filters.isIncluded(tableId);
    }

    private boolean isParentIncluded(TableId parent) {
        if (filters.isIncluded(parent)) {
            return true;
        }
        return router != null && router.isConfiguredParent(parent);
    }

    /**
     * Gets the selectors for table matching in CDC pipeline.
     *
     * <p>This method uses thread-safe lazy initialization via AtomicReference.
     *
     * @return the Selectors instance for matching TableIds
     */
    public Selectors getSelectors() {
        // Handle deserialization: transient field may be null
        if (selectorsRef == null) {
            selectorsRef = new AtomicReference<>();
        }
        Selectors cached = selectorsRef.get();
        if (cached != null) {
            return cached;
        }
        // Build new selectors
        String pattern = buildPartitionAwareSelectorsPattern();
        // Use ".*" wildcard for "capture all" mode when no table pattern is configured
        if (pattern == null || pattern.trim().isEmpty()) {
            pattern = ".*";
        }
        Selectors newSelectors = new Selectors.SelectorsBuilder().includeTables(pattern).build();
        // CAS to ensure only one thread wins
        if (selectorsRef.compareAndSet(null, newSelectors)) {
            return newSelectors;
        }
        // Another thread won, return their result
        return selectorsRef.get();
    }

    /**
     * Builds a partition-aware selector pattern string.
     *
     * <p>This method generates patterns that include both child partition tables and non-partition
     * parent tables, while excluding partition parents that have child patterns defined.
     */
    private String buildPartitionAwareSelectorsPattern() {
        if (router == null) {
            throw new IllegalStateException("Router is required for building selectors");
        }

        PostgresPartitionRules rules = router.getRules();
        String tablesPattern = rules.getTablesPattern();
        String partitionTablesPattern = rules.getPartitionTablesPattern();

        // If no partition routing, return original tables pattern (may be null for "capture all")
        if (!router.isRoutingEnabled()
                || partitionTablesPattern == null
                || partitionTablesPattern.trim().isEmpty()) {
            return tablesPattern;
        }

        LinkedHashSet<String> selectorPatterns = new LinkedHashSet<>();
        LinkedHashSet<ParentKey> excludedParents = new LinkedHashSet<>();

        // Step 1: Add child patterns from partition.tables
        addChildPartitionPatterns(partitionTablesPattern, selectorPatterns, excludedParents);

        // Step 2: Add leftover tables from 'tables' that are not partition parents
        addNonPartitionParentPatterns(tablesPattern, excludedParents, selectorPatterns);

        if (selectorPatterns.isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid table inclusion pattern cannot be null or empty");
        }
        return String.join(",", selectorPatterns);
    }

    /**
     * Validates and returns the tables pattern.
     *
     * @param tablesPattern the tables pattern to validate
     * @return the validated tables pattern
     * @throws IllegalArgumentException if the pattern is null or empty
     */
    private String validateTablesPattern(String tablesPattern) {
        if (tablesPattern == null || tablesPattern.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid table inclusion pattern cannot be null or empty");
        }
        return tablesPattern;
    }

    /**
     * Adds child partition patterns to the selector patterns set.
     *
     * @param partitionTablesPattern the partition tables pattern string
     * @param selectorPatterns the set to add selector patterns to
     * @param excludedParents the set to track excluded parent tables
     */
    private void addChildPartitionPatterns(
            String partitionTablesPattern,
            LinkedHashSet<String> selectorPatterns,
            LinkedHashSet<ParentKey> excludedParents) {
        for (String entry : Predicates.RegExSplitterByComma.split(partitionTablesPattern)) {
            if (entry == null || entry.trim().isEmpty()) {
                continue;
            }

            String childPart = extractChildPartFromEntry(entry.trim());
            if (childPart == null || childPart.isEmpty()) {
                continue;
            }

            String normalized = PostgresPartitionRules.normalizePatternIgnoreCatalog(childPart);
            PostgresPartitionRules.SchemaAndTable schemaAndTable =
                    PostgresPartitionRules.splitSchemaAndTable(normalized);

            String schema = schemaAndTable.schema;
            String tableRegex = schemaAndTable.tableOrRegex;
            if (tableRegex == null || tableRegex.isEmpty()) {
                continue;
            }

            // Add selector patterns for this child partition
            addSelectorPatternsForChild(schema, tableRegex, selectorPatterns);

            // Track the parent table to exclude it later
            String derivedParentTable =
                    PostgresPartitionRules.deriveParentTableNameFromChildRegex(tableRegex);
            if (!derivedParentTable.isEmpty()) {
                excludedParents.add(new ParentKey(schema, derivedParentTable));
            }
        }
    }

    /**
     * Extracts the child part from a partition table entry.
     *
     * @param entry the entry string (e.g., "public.orders:public.orders_\\d+")
     * @return the child part, or null if invalid
     */
    private String extractChildPartFromEntry(String entry) {
        int colonIndex = entry.indexOf(':');
        if (colonIndex >= 0) {
            return entry.substring(colonIndex + 1).trim();
        }
        return entry;
    }

    /**
     * Adds selector patterns for a child partition table.
     *
     * @param schema the schema name (may be null)
     * @param tableRegex the table regex pattern
     * @param selectorPatterns the set to add patterns to
     */
    private void addSelectorPatternsForChild(
            String schema, String tableRegex, LinkedHashSet<String> selectorPatterns) {
        if (schema == null || schema.isEmpty()) {
            // Schema-less patterns: match any catalog
            selectorPatterns.add("\\.*." + tableRegex);
            selectorPatterns.add("\\.*.\\.*." + tableRegex);
        } else {
            // Schema-specific patterns
            selectorPatterns.add(schema + "." + tableRegex);
            selectorPatterns.add("\\.*." + schema + "." + tableRegex);
        }
    }

    /**
     * Adds non-partition parent patterns to the selector patterns set.
     *
     * @param tablesPattern the tables pattern string
     * @param excludedParents the set of excluded parent tables
     * @param selectorPatterns the set to add selector patterns to
     */
    private void addNonPartitionParentPatterns(
            String tablesPattern,
            LinkedHashSet<ParentKey> excludedParents,
            LinkedHashSet<String> selectorPatterns) {
        if (tablesPattern == null || tablesPattern.trim().isEmpty()) {
            return;
        }

        for (String entry : Predicates.RegExSplitterByComma.split(tablesPattern)) {
            if (entry == null || entry.trim().isEmpty()) {
                continue;
            }

            String trimmed = entry.trim();
            String[] parts = Predicates.RegExSplitterByDot.split(trimmed);
            ParentKey key = ParentKey.from(parts);

            // Skip if this is an excluded partition parent
            if (key != null && isExcludedParent(excludedParents, key)) {
                continue;
            }

            // Add the original pattern
            selectorPatterns.add(trimmed);

            // Add variations for different catalog formats
            addSelectorPatternVariations(parts, selectorPatterns);
        }
    }

    /**
     * Adds selector pattern variations for different catalog formats.
     *
     * @param parts the split parts of the table pattern
     * @param selectorPatterns the set to add patterns to
     */
    private void addSelectorPatternVariations(
            String[] parts, LinkedHashSet<String> selectorPatterns) {
        if (parts.length == 2) {
            // schema.table format: add catalog.schema.table variation
            selectorPatterns.add("\\.*." + String.join(".", parts));
        } else if (parts.length == 3) {
            // catalog.schema.table format: add schema.table variation
            selectorPatterns.add(parts[1] + "." + parts[2]);
        }
    }

    private static boolean isExcludedParent(
            LinkedHashSet<ParentKey> excludedParents, ParentKey candidate) {
        for (ParentKey excluded : excludedParents) {
            if (excluded.matches(candidate)) {
                return true;
            }
        }
        return false;
    }
}
