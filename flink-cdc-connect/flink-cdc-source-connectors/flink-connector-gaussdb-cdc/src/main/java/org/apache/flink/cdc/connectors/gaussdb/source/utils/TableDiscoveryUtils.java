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

package org.apache.flink.cdc.connectors.gaussdb.source.utils;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** A utility class for GaussDB table discovery. */
public class TableDiscoveryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    private static final Set<String> SYSTEM_SCHEMAS;

    private static final String LIST_TABLES_IN_SCHEMA_SQL =
            "SELECT n.nspname, c.relname "
                    + "FROM pg_catalog.pg_class c "
                    + "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE n.nspname = ? "
                    + "  AND c.relkind IN ('r', 'p')";

    private static final String READ_COLUMNS_SQL =
            "SELECT a.attnum, a.attname, "
                    + "pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_expr, "
                    + "NOT a.attnotnull AS is_nullable, "
                    + "pg_get_expr(ad.adbin, ad.adrelid) AS column_default "
                    + "FROM pg_catalog.pg_attribute a "
                    + "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
                    + "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
                    + "LEFT JOIN pg_catalog.pg_attrdef ad "
                    + "  ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum "
                    + "WHERE n.nspname = ? AND c.relname = ? "
                    + "  AND a.attnum > 0 AND NOT a.attisdropped "
                    + "ORDER BY a.attnum";

    private static final String READ_CONSTRAINTS_SQL =
            "SELECT con.contype, con.conkey "
                    + "FROM pg_catalog.pg_constraint con "
                    + "JOIN pg_catalog.pg_class c ON con.conrelid = c.oid "
                    + "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
                    + "WHERE n.nspname = ? AND c.relname = ? AND con.contype IN ('p', 'u')";

    static {
        Set<String> systemSchemas = new HashSet<>();
        systemSchemas.add("pg_catalog");
        systemSchemas.add("information_schema");
        systemSchemas.add("pg_toast");
        SYSTEM_SCHEMAS = Collections.unmodifiableSet(systemSchemas);
    }

    public static List<TableId> listTables(
            String database,
            JdbcConnection jdbc,
            RelationalTableFilters tableFilters,
            String schema)
            throws SQLException {
        return listTables(database, jdbc, tableFilters, Collections.singletonList(schema));
    }

    public static List<TableId> listTables(
            String database,
            JdbcConnection jdbc,
            RelationalTableFilters tableFilters,
            List<String> schemas)
            throws SQLException {
        List<DiscoveredTable> tables = discoverTables(database, jdbc, tableFilters, schemas);
        List<TableId> discovered =
                tables.stream().map(DiscoveredTable::getTableId).collect(Collectors.toList());

        LOG.info(
                "GaussDB captured tables: {}.",
                discovered.stream().map(TableId::toString).collect(Collectors.joining(",")));
        return discovered;
    }

    /**
     * Discovers tables in the given schemas, returning lightweight table metadata including primary
     * key and column definitions.
     */
    public static List<DiscoveredTable> discoverTables(
            String database,
            JdbcConnection jdbc,
            RelationalTableFilters tableFilters,
            List<String> schemas)
            throws SQLException {
        if (schemas == null || schemas.isEmpty()) {
            return Collections.emptyList();
        }

        final List<DiscoveredTable> discovered = new ArrayList<>();
        for (String schema : schemas) {
            if (schema == null || isSystemSchema(schema)) {
                continue;
            }

            jdbc.prepareQuery(
                    LIST_TABLES_IN_SCHEMA_SQL,
                    stmt -> stmt.setString(1, schema),
                    rs -> {
                        while (rs.next()) {
                            String schemaName = rs.getString(1);
                            String tableName = rs.getString(2);
                            if (schemaName == null
                                    || tableName == null
                                    || isSystemSchema(schemaName)) {
                                continue;
                            }

                            TableId tableId = new TableId(database, schemaName, tableName);
                            if (!tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                                continue;
                            }

                            DiscoveredTable table = readTable(jdbc, tableId);
                            if (table == null) {
                                continue;
                            }
                            if (table.getPrimaryKeyColumnNames().isEmpty()) {
                                LOG.warn(
                                        "Skipping table {} because it has no primary key; GaussDB incremental snapshot requires primary key.",
                                        tableId);
                                continue;
                            }
                            discovered.add(table);
                        }
                    });
        }

        return discovered;
    }

    @Nullable
    private static DiscoveredTable readTable(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        List<DiscoveredColumn> columns = readColumns(jdbc, tableId);
        if (columns.isEmpty()) {
            return null;
        }

        Map<Integer, String> attnumToName =
                columns.stream()
                        .collect(
                                Collectors.toMap(
                                        DiscoveredColumn::getAttnum,
                                        DiscoveredColumn::getName,
                                        (a, b) -> a));

        ConstraintInfo constraints = readConstraints(jdbc, tableId);
        List<String> primaryKeys =
                resolveKeyColumnNames(constraints.primaryKeyAttnums, attnumToName);
        List<List<String>> uniqueKeys = new ArrayList<>();
        for (List<Integer> attnums : constraints.uniqueKeyAttnums) {
            List<String> names = resolveKeyColumnNames(attnums, attnumToName);
            if (!names.isEmpty()) {
                uniqueKeys.add(names);
            }
        }

        return new DiscoveredTable(tableId, columns, primaryKeys, uniqueKeys);
    }

    private static List<DiscoveredColumn> readColumns(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        List<DiscoveredColumn> columns = new ArrayList<>();
        jdbc.prepareQuery(
                READ_COLUMNS_SQL,
                stmt -> {
                    stmt.setString(1, tableId.schema());
                    stmt.setString(2, tableId.table());
                },
                rs -> {
                    while (rs.next()) {
                        columns.add(
                                new DiscoveredColumn(
                                        rs.getInt(1),
                                        rs.getString(2),
                                        rs.getString(3),
                                        rs.getBoolean(4),
                                        rs.getString(5)));
                    }
                });
        return columns;
    }

    private static ConstraintInfo readConstraints(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        ConstraintInfo info = new ConstraintInfo();
        jdbc.prepareQuery(
                READ_CONSTRAINTS_SQL,
                stmt -> {
                    stmt.setString(1, tableId.schema());
                    stmt.setString(2, tableId.table());
                },
                rs -> {
                    while (rs.next()) {
                        String type = rs.getString(1);
                        if (type == null || type.isEmpty()) {
                            continue;
                        }

                        if (type.charAt(0) == 'p' && info.primaryKeyAttnums.isEmpty()) {
                            info.primaryKeyAttnums = parseAttnumArray(rs);
                        } else if (type.charAt(0) == 'u') {
                            List<Integer> attnums = parseAttnumArray(rs);
                            if (!attnums.isEmpty()) {
                                info.uniqueKeyAttnums.add(attnums);
                            }
                        }
                    }
                });
        return info;
    }

    private static List<Integer> parseAttnumArray(ResultSet rs) throws SQLException {
        try {
            java.sql.Array array = rs.getArray(2);
            if (array != null) {
                Object raw = array.getArray();
                if (raw instanceof Object[]) {
                    Object[] items = (Object[]) raw;
                    List<Integer> attnums = new ArrayList<>(items.length);
                    for (Object item : items) {
                        if (item instanceof Number) {
                            attnums.add(((Number) item).intValue());
                        } else if (item != null) {
                            attnums.add(Integer.parseInt(item.toString()));
                        }
                    }
                    return attnums;
                }
            }
        } catch (SQLException ignored) {
            // fall through
        }

        String str = rs.getString(2);
        if (str == null || str.isEmpty()) {
            return Collections.emptyList();
        }
        String trimmed = str.trim();
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        if (trimmed.isEmpty()) {
            return Collections.emptyList();
        }
        String[] parts = trimmed.split(",");
        List<Integer> attnums = new ArrayList<>(parts.length);
        for (String part : parts) {
            if (part != null && !part.trim().isEmpty()) {
                attnums.add(Integer.parseInt(part.trim()));
            }
        }
        return attnums;
    }

    private static List<String> resolveKeyColumnNames(
            List<Integer> attnums, Map<Integer, String> attnumToName) {
        if (attnums == null || attnums.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> names = new ArrayList<>(attnums.size());
        for (Integer attnum : attnums) {
            String name = attnumToName.get(attnum);
            if (name != null) {
                names.add(name);
            }
        }
        return names;
    }

    private static boolean isSystemSchema(String schemaName) {
        return SYSTEM_SCHEMAS.contains(schemaName.toLowerCase(Locale.ROOT));
    }

    /** Lightweight metadata for a discovered table. */
    public static class DiscoveredTable {
        private final TableId tableId;
        private final List<DiscoveredColumn> columns;
        private final List<String> primaryKeyColumnNames;
        private final List<List<String>> uniqueKeyColumnNames;

        private DiscoveredTable(
                TableId tableId,
                List<DiscoveredColumn> columns,
                List<String> primaryKeyColumnNames,
                List<List<String>> uniqueKeyColumnNames) {
            this.tableId = tableId;
            this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
            this.primaryKeyColumnNames =
                    Collections.unmodifiableList(new ArrayList<>(primaryKeyColumnNames));
            this.uniqueKeyColumnNames =
                    Collections.unmodifiableList(new ArrayList<>(uniqueKeyColumnNames));
        }

        public TableId getTableId() {
            return tableId;
        }

        public List<DiscoveredColumn> getColumns() {
            return columns;
        }

        public List<String> getPrimaryKeyColumnNames() {
            return primaryKeyColumnNames;
        }

        public List<List<String>> getUniqueKeyColumnNames() {
            return uniqueKeyColumnNames;
        }
    }

    /** Lightweight metadata for a discovered column. */
    public static class DiscoveredColumn {
        private final int attnum;
        private final String name;
        private final String typeExpression;
        private final boolean nullable;
        @Nullable private final String defaultExpression;

        private DiscoveredColumn(
                int attnum,
                String name,
                String typeExpression,
                boolean nullable,
                @Nullable String defaultExpression) {
            this.attnum = attnum;
            this.name = name;
            this.typeExpression = typeExpression;
            this.nullable = nullable;
            this.defaultExpression = defaultExpression;
        }

        public int getAttnum() {
            return attnum;
        }

        public String getName() {
            return name;
        }

        public String getTypeExpression() {
            return typeExpression;
        }

        public boolean isNullable() {
            return nullable;
        }

        @Nullable
        public String getDefaultExpression() {
            return defaultExpression;
        }
    }

    private static class ConstraintInfo {
        private List<Integer> primaryKeyAttnums = Collections.emptyList();
        private final List<List<Integer>> uniqueKeyAttnums = new ArrayList<>();
    }
}
