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

import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import io.debezium.schema.SchemaChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A lightweight schema cache for GaussDB tables. */
public class CustomGaussDBSchema {

    private static final Logger LOG = LoggerFactory.getLogger(CustomGaussDBSchema.class);

    private static final String DEFAULT_SCHEMA = "public";

    private static final String READ_COLUMNS_SQL =
            "SELECT a.attnum, a.attname, "
                    + "pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_expr, "
                    + "NOT a.attnotnull AS is_nullable, "
                    + "pg_get_expr(ad.adbin, ad.adrelid) AS column_default, "
                    + "a.atttypid AS type_oid "
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

    private final Map<TableId, TableChange> schemasByTableId = new HashMap<>();
    private final Set<TableId> tablesWithoutPrimaryKey = new HashSet<>();
    private final JdbcConnection jdbcConnection;

    public CustomGaussDBSchema(JdbcConnection jdbcConnection) {
        this.jdbcConnection = jdbcConnection;
    }

    /** Clears all cached table schemas and bookkeeping state. */
    public void clear() {
        schemasByTableId.clear();
        tablesWithoutPrimaryKey.clear();
    }

    /** Invalidates the cached schema for the given table, forcing a re-read on the next access. */
    public void invalidateTableSchema(TableId tableId) {
        if (tableId == null) {
            return;
        }
        schemasByTableId.remove(tableId);
        tablesWithoutPrimaryKey.remove(tableId);
    }

    /**
     * Refreshes the cached schema for the given table by re-reading it from the database.
     *
     * <p>If the table has no primary key, it is tracked as such and the cache entry remains empty.
     */
    public void refreshTableSchema(TableId tableId) {
        invalidateTableSchema(tableId);
        getTableSchema(tableId);
    }

    /**
     * Applies a schema change event by invalidating or updating the in-memory cache.
     *
     * <p>Schema change event handling is out of scope for v1.0; this method provides a safe
     * extension point for future streaming DDL support.
     */
    public void applySchemaChangeEvent(@Nullable SchemaChangeEvent event) {
        if (event == null || event.getTableChanges() == null) {
            return;
        }
        for (TableChange change : event.getTableChanges()) {
            applyTableChange(change);
        }
    }

    /**
     * Applies a Debezium {@link TableChange} to the cache.
     *
     * <p>This is a lightweight implementation intended for future schema change integration; it is
     * safe to call with changes coming from a snapshot or DDL stream.
     */
    public void applyTableChange(@Nullable TableChange tableChange) {
        if (tableChange == null) {
            return;
        }

        Table table = tableChange.getTable();
        if (table == null || table.id() == null) {
            return;
        }

        TableId tableId = table.id();
        TableChangeType type = tableChange.getType();
        if (type == TableChangeType.DROP) {
            invalidateTableSchema(tableId);
            return;
        }

        schemasByTableId.put(tableId, tableChange);
        tablesWithoutPrimaryKey.remove(tableId);
    }

    /** Returns a cached schema, loading it from database if needed. */
    @Nullable
    public TableChange getTableSchema(TableId tableId) {
        if (schemasByTableId.containsKey(tableId)) {
            return schemasByTableId.get(tableId);
        }
        if (tablesWithoutPrimaryKey.contains(tableId)) {
            return null;
        }

        try {
            TableChange schema = readTableSchema(tableId);
            if (schema != null) {
                schemasByTableId.put(tableId, schema);
            }
            return schema;
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to read GaussDB table schema", e);
        }
    }

    /** Returns schemas for the given tables, skipping tables without primary key. */
    public Map<TableId, TableChange> getTableSchema(List<TableId> tableIds) {
        if (tableIds == null || tableIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<TableId, TableChange> result = new HashMap<>();
        for (TableId tableId : tableIds) {
            TableChange change = getTableSchema(tableId);
            if (change != null) {
                result.put(tableId, change);
            }
        }
        return result;
    }

    @Nullable
    private TableChange readTableSchema(TableId tableId) throws SQLException {
        final String schema = normalizeSchema(tableId.schema());
        final String table = tableId.table();

        final Map<String, JdbcColumnMetadata> jdbcMetadataByName =
                readJdbcColumnMetadata(schema, table);

        final List<ColumnInfo> columns = readColumns(schema, table);
        if (columns.isEmpty()) {
            throw new FlinkRuntimeException("No columns found for table " + tableId);
        }

        final Map<Integer, String> attnumToName =
                columns.stream().collect(Collectors.toMap(c -> c.attnum, c -> c.name, (a, b) -> a));

        ConstraintInfo constraints = readConstraints(schema, table);
        List<String> primaryKeys =
                resolveKeyColumnNames(constraints.primaryKeyAttnums, attnumToName);
        if (primaryKeys.isEmpty()) {
            LOG.warn(
                    "Skipping table {} because it has no primary key; GaussDB incremental snapshot requires primary key.",
                    tableId);
            tablesWithoutPrimaryKey.add(tableId);
            return null;
        }

        TableEditor editor = Table.editor().tableId(tableId);
        List<Column> dbzColumns = new ArrayList<>(columns.size());
        for (ColumnInfo column : columns) {
            JdbcColumnMetadata jdbcMeta =
                    jdbcMetadataByName.getOrDefault(column.name, JdbcColumnMetadata.empty());
            Column dbzColumn = toDbzColumn(column, jdbcMeta);
            dbzColumns.add(dbzColumn);
        }

        editor.setColumns(dbzColumns);
        editor.setPrimaryKeyNames(primaryKeys);
        if (constraints.hasUniqueConstraint) {
            editor.setUniqueValues();
        }

        Table tableDef = editor.create();
        return new TableChange(TableChangeType.CREATE, tableDef);
    }

    private static Column toDbzColumn(ColumnInfo column, JdbcColumnMetadata jdbcMeta) {
        final String typeName = jdbcMeta.typeName != null ? jdbcMeta.typeName : column.typeExpr;
        final int jdbcType = jdbcMeta.jdbcType != null ? jdbcMeta.jdbcType : Types.OTHER;
        final int length = jdbcMeta.length != null ? jdbcMeta.length : Column.UNSET_INT_VALUE;
        final Integer scale = jdbcMeta.scale.orElse(null);

        io.debezium.relational.ColumnEditor editor =
                Column.editor()
                        .name(column.name)
                        .type(typeName, column.typeExpr)
                        .nativeType(column.typeOid)
                        .jdbcType(jdbcType)
                        .position(column.attnum)
                        .optional(column.nullable)
                        .length(length)
                        .autoIncremented(isAutoIncrement(column.defaultExpression))
                        .generated(false);

        if (scale != null) {
            editor.scale(scale);
        }

        if (column.defaultExpression != null) {
            editor.defaultValueExpression(column.defaultExpression);
        }

        return editor.create();
    }

    private static boolean isAutoIncrement(@Nullable String defaultExpression) {
        if (defaultExpression == null) {
            return false;
        }
        String expr = defaultExpression.toLowerCase();
        return expr.contains("nextval(") || expr.contains("identity");
    }

    private List<ColumnInfo> readColumns(String schema, String table) throws SQLException {
        final List<ColumnInfo> columns = new ArrayList<>();
        jdbcConnection.prepareQuery(
                READ_COLUMNS_SQL,
                stmt -> {
                    stmt.setString(1, schema);
                    stmt.setString(2, table);
                },
                rs -> {
                    while (rs.next()) {
                        columns.add(
                                new ColumnInfo(
                                        rs.getInt(1),
                                        rs.getString(2),
                                        rs.getString(3),
                                        rs.getBoolean(4),
                                        rs.getString(5),
                                        rs.getInt(6)));
                    }
                });
        return columns;
    }

    private ConstraintInfo readConstraints(String schema, String table) throws SQLException {
        final ConstraintInfo info = new ConstraintInfo();
        jdbcConnection.prepareQuery(
                READ_CONSTRAINTS_SQL,
                stmt -> {
                    stmt.setString(1, schema);
                    stmt.setString(2, table);
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
                            info.hasUniqueConstraint = true;
                        }
                    }
                });
        return info;
    }

    private static List<Integer> parseAttnumArray(ResultSet rs) throws SQLException {
        try {
            java.sql.Array array = rs.getArray(2);
            if (array == null) {
                return Collections.emptyList();
            }
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
        } catch (SQLException ignored) {
            // fall through
        }

        String str = rs.getString(2);
        if (str == null || str.isEmpty()) {
            return Collections.emptyList();
        }
        // JDBC driver may return int2vector as "{1,2}"
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

    private Map<String, JdbcColumnMetadata> readJdbcColumnMetadata(String schema, String table)
            throws SQLException {
        DatabaseMetaData meta = jdbcConnection.connection().getMetaData();
        Map<String, JdbcColumnMetadata> result = new HashMap<>();
        try (ResultSet rs = meta.getColumns(null, schema, table, "%")) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                if (columnName == null) {
                    continue;
                }
                int jdbcType = rs.getInt("DATA_TYPE");
                String typeName = rs.getString("TYPE_NAME");
                int columnSize = rs.getInt("COLUMN_SIZE");
                int scale = rs.getInt("DECIMAL_DIGITS");
                boolean scaleIsNull = rs.wasNull();

                result.put(
                        columnName,
                        new JdbcColumnMetadata(
                                jdbcType,
                                typeName,
                                columnSize > 0 ? columnSize : Column.UNSET_INT_VALUE,
                                scaleIsNull ? Optional.empty() : Optional.of(scale)));
            }
        }
        return result;
    }

    private static String normalizeSchema(@Nullable String schema) {
        return schema == null || schema.trim().isEmpty() ? DEFAULT_SCHEMA : schema;
    }

    private static class ColumnInfo {
        private final int attnum;
        private final String name;
        private final String typeExpr;
        private final boolean nullable;
        @Nullable private final String defaultExpression;
        private final int typeOid;

        private ColumnInfo(
                int attnum,
                String name,
                String typeExpr,
                boolean nullable,
                @Nullable String defaultExpression,
                int typeOid) {
            this.attnum = attnum;
            this.name = name;
            this.typeExpr = typeExpr;
            this.nullable = nullable;
            this.defaultExpression = defaultExpression;
            this.typeOid = typeOid;
        }
    }

    private static class ConstraintInfo {
        private List<Integer> primaryKeyAttnums = Collections.emptyList();
        private boolean hasUniqueConstraint = false;
    }

    private static class JdbcColumnMetadata {
        @Nullable private final Integer jdbcType;
        @Nullable private final String typeName;
        @Nullable private final Integer length;
        private final Optional<Integer> scale;

        private JdbcColumnMetadata(
                @Nullable Integer jdbcType,
                @Nullable String typeName,
                @Nullable Integer length,
                Optional<Integer> scale) {
            this.jdbcType = jdbcType;
            this.typeName = typeName;
            this.length = length;
            this.scale = scale == null ? Optional.empty() : scale;
        }

        private static JdbcColumnMetadata empty() {
            return new JdbcColumnMetadata(null, null, null, Optional.empty());
        }
    }
}
