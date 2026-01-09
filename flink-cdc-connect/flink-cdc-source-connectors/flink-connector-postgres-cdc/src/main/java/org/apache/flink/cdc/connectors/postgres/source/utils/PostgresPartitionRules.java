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

import org.apache.flink.cdc.common.utils.PatternCache;
import org.apache.flink.cdc.common.utils.Predicates;

import io.debezium.relational.TableId;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Immutable configuration object that encapsulates parsed partition routing rules.
 *
 * <p>This class serves as the single source of truth for partition table configuration parsing,
 * eliminating duplication across PostgresPartitionRouter, PostgresPartitionConnectorConfig, and
 * PostgresPartitionRoutingSchema.
 *
 * <p><b>Design Principles:</b>
 *
 * <ul>
 *   <li><b>Immutability:</b> All fields are final and collections are unmodifiable
 *   <li><b>Single Responsibility:</b> Only handles configuration parsing, not routing decisions
 *   <li><b>Thread-Safe:</b> Can be safely shared across multiple components
 * </ul>
 *
 * <p><b>Usage Example:</b>
 *
 * <pre>{@code
 * PostgresPartitionRules rules = PostgresPartitionRules.parse(
 *     "parent1:child1_.*,parent2:child2_.*",
 *     "public"
 * );
 * Set<TableId> parents = rules.getConfiguredParents();
 * Map<Pattern, TableId> mappings = rules.getChildToParentMappings();
 * }</pre>
 */
public final class PostgresPartitionRules implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_SCHEMA = "public";

    private static final char[] PARENT_DERIVE_STOP_CHARS =
            new char[] {'\\', '[', '(', '{', '*', '+', '?', '|', '.'};

    /** Set of explicitly configured parent table IDs. */
    private transient Set<TableId> configuredParents;

    /** Mapping from child partition patterns to their parent TableId. */
    private transient Map<Pattern, TableId> childToParentMappings;

    /** Child table patterns used for excluding partition children in table filters. */
    private transient List<Pattern> childTablePatterns;

    /** Original tables pattern string (for backward compatibility). */
    private final String tablesPattern;

    /** Original partition tables pattern string (for backward compatibility). */
    private final String partitionTablesPattern;

    /** Private constructor. Use {@link #parse(String, String, String)} to create instances. */
    private PostgresPartitionRules(
            Set<TableId> configuredParents,
            Map<Pattern, TableId> childToParentMappings,
            List<Pattern> childTablePatterns,
            String tablesPattern,
            String partitionTablesPattern) {
        this.configuredParents = Collections.unmodifiableSet(configuredParents);
        this.childToParentMappings = Collections.unmodifiableMap(childToParentMappings);
        this.childTablePatterns = Collections.unmodifiableList(childTablePatterns);
        this.tablesPattern = tablesPattern;
        this.partitionTablesPattern = partitionTablesPattern;
    }

    /**
     * Gets the set of configured parent table IDs.
     *
     * @return unmodifiable set of parent tables
     */
    public Set<TableId> getConfiguredParents() {
        return configuredParents;
    }

    /**
     * Gets the mapping from child partition patterns to parent table IDs.
     *
     * @return unmodifiable map of pattern to parent mappings
     */
    public Map<Pattern, TableId> getChildToParentMappings() {
        return childToParentMappings;
    }

    /**
     * Gets the compiled child table patterns for filtering.
     *
     * @return unmodifiable list of patterns
     */
    public List<Pattern> getChildTablePatterns() {
        return childTablePatterns;
    }

    /**
     * Gets the original tables pattern string.
     *
     * @return tables pattern, may be null
     */
    public String getTablesPattern() {
        return tablesPattern;
    }

    /**
     * Gets the original partition tables pattern string.
     *
     * @return partition tables pattern, may be null
     */
    public String getPartitionTablesPattern() {
        return partitionTablesPattern;
    }

    /**
     * Checks if there are any configured partition rules.
     *
     * @return true if partition rules exist
     */
    public boolean hasPartitionRules() {
        return !childToParentMappings.isEmpty();
    }

    /**
     * Normalizes a regex pattern to ignore catalog prefix during matching.
     *
     * @param pattern the original pattern
     * @return normalized pattern without catalog prefix
     */
    static String normalizePatternIgnoreCatalog(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return pattern;
        }

        String trimmedPattern = pattern.trim();
        if (!trimmedPattern.contains(".")) {
            return trimmedPattern;
        }

        // Try to handle escaped dots first (catalog\.schema\.table)
        String escapedResult = handleEscapedDots(trimmedPattern);
        if (escapedResult != null) {
            return escapedResult;
        }

        // Handle unescaped dots (catalog.schema.table)
        return handleUnescapedDots(trimmedPattern);
    }

    /**
     * Handles patterns with escaped dots (catalog\.schema\.table).
     *
     * @param pattern the pattern to process
     * @return normalized pattern, or null if no escaped dots found
     */
    private static String handleEscapedDots(String pattern) {
        int firstEscapedDot = pattern.indexOf("\\.");
        if (firstEscapedDot < 0) {
            return null;
        }

        int secondEscapedDot = pattern.indexOf("\\.", firstEscapedDot + 2);
        if (secondEscapedDot > firstEscapedDot) {
            // Found two "\." occurrences - this is a three-segment pattern
            // Remove the leading "catalog\." part
            return pattern.substring(firstEscapedDot + 2);
        }

        // Only one "\." found - this is a two-segment pattern, return as-is
        return pattern;
    }

    /**
     * Handles patterns with unescaped dots (catalog.schema.table).
     *
     * <p>This method counts dots that are NOT part of regex patterns (like .*) and strips the
     * catalog prefix if there are 2 or more separator dots.
     *
     * @param pattern the pattern to process
     * @return normalized pattern without catalog prefix
     */
    private static String handleUnescapedDots(String pattern) {
        int dotCount = 0;
        int firstDotIndex = -1;

        for (int i = 0; i < pattern.length(); i++) {
            if (pattern.charAt(i) == '.') {
                // Skip regex quantifier dots (followed by * + ? {)
                if (i + 1 < pattern.length()) {
                    char next = pattern.charAt(i + 1);
                    if (next == '*' || next == '+' || next == '?' || next == '{') {
                        continue;
                    }
                }
                dotCount++;
                if (firstDotIndex < 0) {
                    firstDotIndex = i;
                }
            }
        }

        // If we have 2 or more separator dots, strip the first segment (catalog)
        if (dotCount >= 2 && firstDotIndex > 0) {
            return pattern.substring(firstDotIndex + 1);
        }
        return pattern;
    }

    /**
     * Parses partition configuration and creates an immutable PostgresPartitionRules instance.
     *
     * @param tables comma-separated list of parent table patterns
     * @param partitionTables comma-separated list of partition table patterns
     * @param defaultSchema default schema name (e.g., "public")
     * @return parsed PostgresPartitionRules instance
     */
    public static PostgresPartitionRules parse(
            String tables, String partitionTables, String defaultSchema) {
        String resolvedDefaultSchema =
                (defaultSchema == null || defaultSchema.trim().isEmpty())
                        ? DEFAULT_SCHEMA
                        : defaultSchema.trim();
        Map<Pattern, TableId> childToParentMap = new LinkedHashMap<>();
        Set<TableId> configuredParents = new LinkedHashSet<>();
        List<Pattern> childTablePatterns = new ArrayList<>();

        if (partitionTables == null || partitionTables.trim().isEmpty()) {
            return new PostgresPartitionRules(
                    configuredParents,
                    childToParentMap,
                    childTablePatterns,
                    tables,
                    partitionTables);
        }

        String[] partitionEntries = Predicates.RegExSplitterByComma.split(partitionTables);

        for (int i = 0; i < partitionEntries.length; i++) {
            String raw = partitionEntries[i];
            if (raw == null) {
                continue;
            }
            String entry = raw.trim();
            if (entry.isEmpty()) {
                continue;
            }

            int colonIndex = entry.indexOf(':');
            if (colonIndex <= 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid partition.tables entry '%s': must use colon format 'parent:child_regex' "
                                        + "(e.g., 'public.orders:public.orders_\\d{6}')",
                                entry));
            }
            String parentPart = entry.substring(0, colonIndex).trim();
            String childPart = entry.substring(colonIndex + 1).trim();

            if (childPart == null || childPart.isEmpty()) {
                continue;
            }

            String fallbackSchemaFromChild =
                    splitSchemaAndTable(normalizePatternIgnoreCatalog(childPart)).schema;

            // Configured parents: for schema refresh and parent PK fixups.
            TableId configuredParent =
                    parseParentTableId(
                            parentPart, resolvedDefaultSchema, fallbackSchemaFromChild, false);
            if (configuredParent != null) {
                configuredParents.add(configuredParent);
            }

            TableId derivedConfiguredParent =
                    deriveParentTableIdFromChildPattern(childPart, resolvedDefaultSchema, false);
            if (derivedConfiguredParent != null) {
                configuredParents.add(derivedConfiguredParent);
            }

            // Mapping parent: for routing. Use explicit parent from colon format.
            TableId mappingParent =
                    parseParentTableId(
                            parentPart, resolvedDefaultSchema, fallbackSchemaFromChild, true);

            String routerPatternStr = buildChildPatternString(childPart, null, false);
            if (routerPatternStr != null) {
                childToParentMap.put(PatternCache.getPattern(routerPatternStr), mappingParent);
            }

            String filterPatternStr =
                    buildChildPatternString(childPart, resolvedDefaultSchema, true);
            if (filterPatternStr != null) {
                childTablePatterns.add(PatternCache.getPattern(filterPatternStr));
            }
        }

        return new PostgresPartitionRules(
                configuredParents, childToParentMap, childTablePatterns, tables, partitionTables);
    }

    /**
     * Extracts the default schema from a schema include list configuration.
     *
     * <p>If the schema list is null or empty, returns "public". Otherwise, returns the first schema
     * from the comma-separated list.
     *
     * <p>This method is used to extract the default schema from Debezium configuration.
     *
     * @param schemaIncludeList comma-separated list of schemas (e.g., "public,my_schema")
     * @return the first schema from the list, or "public" if the list is null or empty
     */
    public static String extractDefaultSchemaFromConfig(String schemaIncludeList) {
        if (schemaIncludeList == null || schemaIncludeList.isEmpty()) {
            return DEFAULT_SCHEMA;
        }

        String[] schemas = schemaIncludeList.split(",");
        if (schemas.length > 0 && !schemas[0].trim().isEmpty()) {
            return schemas[0].trim();
        }

        return DEFAULT_SCHEMA;
    }

    /**
     * Creates a TableId with null catalog, using the provided schema and table.
     *
     * <p>This is a convenience method for creating TableId objects without catalog, which is the
     * common pattern in PostgreSQL CDC.
     *
     * @param schema the schema name (may be null)
     * @param table the table name (must not be null)
     * @return a new TableId with null catalog
     */
    public static TableId createTableId(String schema, String table) {
        return new TableId(null, schema, table);
    }

    /**
     * Gets the schema.table string representation of a TableId.
     *
     * <p>This returns "schema.table" format, ignoring the catalog component.
     *
     * @param tableId the table ID
     * @return the schema.table string, or null if tableId is null
     */
    public static String getSchemaTableName(TableId tableId) {
        if (tableId == null) {
            return null;
        }
        return tableId.schema() + "." + tableId.table();
    }

    /**
     * Creates a comparison TableId with null catalog for matching purposes.
     *
     * <p>This is useful when comparing TableId objects where the catalog should be ignored.
     *
     * @param tableId the original table ID
     * @return a new TableId with null catalog, same schema and table
     */
    public static TableId toComparableTableId(TableId tableId) {
        if (tableId == null) {
            return null;
        }
        return new TableId(null, tableId.schema(), tableId.table());
    }

    /**
     * Builds a child table pattern string.
     *
     * @param childPattern the child table pattern
     * @param defaultSchema default schema to use when schema is not specified
     * @param requireSchema whether to require a schema
     * @return pattern string, or null if table part is empty
     */
    private static String buildChildPatternString(
            String childPattern, String defaultSchema, boolean requireSchema) {
        String normalized = normalizePatternIgnoreCatalog(childPattern);
        SchemaAndTable schemaAndTable = splitSchemaAndTable(normalized);
        String tablePart = schemaAndTable.tableOrRegex;
        if (tablePart == null || tablePart.isEmpty()) {
            return null;
        }

        String cleanedTableRegex = stripAnchors(tablePart);
        String schema = schemaAndTable.schema;

        if (schema == null || schema.isEmpty()) {
            if (requireSchema) {
                schema = defaultSchema;
            } else {
                return "^" + cleanedTableRegex + "$";
            }
        }

        return "^" + Pattern.quote(schema) + "\\." + cleanedTableRegex + "$";
    }

    private static String stripAnchors(String tablePart) {
        if (tablePart == null || tablePart.isEmpty()) {
            return tablePart;
        }

        // Only strip if they are actual anchors, not escaped literals (e.g., \$ or \^)
        boolean hasStartAnchor = tablePart.startsWith("^") && !tablePart.startsWith("\\^");
        boolean hasEndAnchor =
                tablePart.endsWith("$")
                        && !tablePart.endsWith("\\$")
                        && !tablePart.endsWith("\\\\$");
        String tableRegex = tablePart;
        if (hasStartAnchor) {
            tableRegex = tableRegex.substring(1);
        }
        if (hasEndAnchor && !tableRegex.isEmpty()) {
            tableRegex = tableRegex.substring(0, tableRegex.length() - 1);
        }
        return tableRegex;
    }

    /**
     * Derives parent table ID from child pattern by removing regex suffix.
     *
     * @param childPattern the child table pattern
     * @param defaultSchema default schema to use when schema is not specified
     * @param allowNullSchema whether to allow null schema in the result
     * @return derived parent TableId, or null if parent table name is empty
     */
    private static TableId deriveParentTableIdFromChildPattern(
            String childPattern, String defaultSchema, boolean allowNullSchema) {
        String normalized = normalizePatternIgnoreCatalog(childPattern);
        SchemaAndTable schemaAndTable = splitSchemaAndTable(normalized);
        String parentTable = deriveParentTableNameFromChildRegex(schemaAndTable.tableOrRegex);
        if (parentTable.isEmpty()) {
            return null;
        }

        String schema = schemaAndTable.schema;
        if (schema == null || schema.isEmpty()) {
            if (allowNullSchema) {
                return createTableId(null, parentTable);
            }
            schema = defaultSchema;
        }
        return createTableId(schema, parentTable);
    }

    /**
     * Parses a schema.table pattern into a TableId with null catalog.
     *
     * @param schemaTablePattern the schema.table pattern to parse
     * @param defaultSchema default schema to use when schema is not specified
     * @param fallbackSchema fallback schema to use (may be null)
     * @param allowNullSchema whether to allow null schema in the result
     * @return parsed TableId, or null if table part is empty
     */
    private static TableId parseParentTableId(
            String schemaTablePattern,
            String defaultSchema,
            String fallbackSchema,
            boolean allowNullSchema) {
        String normalized = normalizePatternIgnoreCatalog(schemaTablePattern);
        SchemaAndTable schemaAndTable = splitSchemaAndTable(normalized);
        String schema = schemaAndTable.schema;
        String table = schemaAndTable.tableOrRegex;
        if (table == null || table.isEmpty()) {
            return null;
        }
        if (schema == null || schema.isEmpty()) {
            schema = fallbackSchema != null ? fallbackSchema : defaultSchema;
        }
        if (allowNullSchema && (schema == null || schema.isEmpty())) {
            return createTableId(null, table);
        }
        return createTableId(schema, table);
    }

    static String deriveParentTableNameFromChildRegex(String childTableRegex) {
        if (childTableRegex == null || childTableRegex.isEmpty()) {
            return "";
        }

        // Find first occurrence of any stop character
        int cut = childTableRegex.length();
        for (char stop : PARENT_DERIVE_STOP_CHARS) {
            int idx = childTableRegex.indexOf(stop);
            if (idx >= 0 && idx < cut) {
                cut = idx;
            }
        }

        // Trim trailing underscores and hyphens
        while (cut > 0
                && (childTableRegex.charAt(cut - 1) == '_'
                        || childTableRegex.charAt(cut - 1) == '-')) {
            cut--;
        }
        return cut == childTableRegex.length()
                ? childTableRegex
                : childTableRegex.substring(0, cut);
    }

    static SchemaAndTable splitSchemaAndTable(String normalized) {
        if (normalized == null) {
            return new SchemaAndTable(null, null);
        }

        String trimmed = normalized.trim();
        if (trimmed.isEmpty()) {
            return new SchemaAndTable(null, "");
        }

        // Support both "schema.tableRegex" and "schema\\.tableRegex"
        int escapedDot = trimmed.indexOf("\\.");
        if (escapedDot >= 0) {
            String schema = trimmed.substring(0, escapedDot);
            String table = trimmed.substring(escapedDot + 2);
            return new SchemaAndTable(schema.isEmpty() ? null : schema, table);
        }

        int dot = indexOfSeparatorDot(trimmed);
        if (dot >= 0) {
            String schema = trimmed.substring(0, dot);
            String table = trimmed.substring(dot + 1);
            return new SchemaAndTable(schema.isEmpty() ? null : schema, table);
        }

        return new SchemaAndTable(null, trimmed);
    }

    private static int indexOfSeparatorDot(String s) {
        boolean escaped = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c != '.') {
                continue;
            }
            if (i + 1 < s.length()) {
                char next = s.charAt(i + 1);
                if (next == '*' || next == '+' || next == '?' || next == '{') {
                    continue;
                }
            }
            return i;
        }
        return -1;
    }

    // ==================== Custom Serialization ====================

    /** Custom serialization: write Pattern and TableId as strings. */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        // Write configuredParents as strings
        out.writeInt(configuredParents.size());
        for (TableId tableId : configuredParents) {
            out.writeObject(tableId.schema());
            out.writeObject(tableId.table());
        }

        // Write childToParentMappings as strings
        out.writeInt(childToParentMappings.size());
        for (Map.Entry<Pattern, TableId> entry : childToParentMappings.entrySet()) {
            out.writeObject(entry.getKey().pattern());
            TableId parent = entry.getValue();
            out.writeObject(parent != null ? parent.schema() : null);
            out.writeObject(parent != null ? parent.table() : null);
        }

        // Write childTablePatterns as strings
        out.writeInt(childTablePatterns.size());
        for (Pattern pattern : childTablePatterns) {
            out.writeObject(pattern.pattern());
        }
    }

    /** Custom deserialization: rebuild Pattern and TableId from strings. */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // Read configuredParents
        int parentsSize = in.readInt();
        Set<TableId> parents = new LinkedHashSet<>();
        for (int i = 0; i < parentsSize; i++) {
            String schema = (String) in.readObject();
            String table = (String) in.readObject();
            parents.add(createTableId(schema, table));
        }
        this.configuredParents = Collections.unmodifiableSet(parents);

        // Read childToParentMappings
        int mappingsSize = in.readInt();
        Map<Pattern, TableId> mappings = new LinkedHashMap<>();
        for (int i = 0; i < mappingsSize; i++) {
            String patternStr = (String) in.readObject();
            String parentSchema = (String) in.readObject();
            String parentTable = (String) in.readObject();
            Pattern pattern = PatternCache.getPattern(patternStr);
            TableId parent = parentTable != null ? createTableId(parentSchema, parentTable) : null;
            mappings.put(pattern, parent);
        }
        this.childToParentMappings = Collections.unmodifiableMap(mappings);

        // Read childTablePatterns
        int patternsSize = in.readInt();
        List<Pattern> patterns = new ArrayList<>();
        for (int i = 0; i < patternsSize; i++) {
            String patternStr = (String) in.readObject();
            patterns.add(PatternCache.getPattern(patternStr));
        }
        this.childTablePatterns = Collections.unmodifiableList(patterns);
    }

    static class SchemaAndTable {
        final String schema;
        final String tableOrRegex;

        SchemaAndTable(String schema, String tableOrRegex) {
            this.schema = schema;
            this.tableOrRegex = tableOrRegex;
        }
    }
}
