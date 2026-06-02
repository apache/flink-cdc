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

package org.apache.flink.cdc.connectors.pgvector.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** PostgreSQL SQL builders for pgvector sink. */
public class PgVectorSqlUtils {

    private static final Pattern STORAGE_PARAMETER_PATTERN =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

    private PgVectorSqlUtils() {}

    public static PgVectorTableInfo resolveTableInfo(TableId tableId, String defaultSchema) {
        String schemaName = tableId.getSchemaName();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            schemaName = defaultSchema;
        }
        return new PgVectorTableInfo(schemaName, tableId.getTableName());
    }

    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    public static String quoteTable(PgVectorTableInfo tableInfo) {
        return quoteIdentifier(tableInfo.getSchemaName())
                + "."
                + quoteIdentifier(tableInfo.getTableName());
    }

    public static String createSchemaSql(PgVectorTableInfo tableInfo) {
        return "CREATE SCHEMA IF NOT EXISTS " + quoteIdentifier(tableInfo.getSchemaName());
    }

    public static String createTableSql(
            PgVectorTableInfo tableInfo,
            Schema schema,
            Map<String, PgVectorColumnSpec> vectorColumns,
            Map<String, String> tableCreateProperties) {
        String columns =
                schema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .map(
                                column ->
                                        quoteIdentifier(column.getName())
                                                + " "
                                                + PgVectorTypeUtils.toPostgresType(
                                                        tableInfo, column, vectorColumns)
                                                + nullableClause(column))
                        .collect(Collectors.joining(", "));
        String primaryKey = "";
        if (!schema.primaryKeys().isEmpty()) {
            primaryKey =
                    ", PRIMARY KEY ("
                            + schema.primaryKeys().stream()
                                    .map(PgVectorSqlUtils::quoteIdentifier)
                                    .collect(Collectors.joining(", "))
                            + ")";
        }
        return "CREATE TABLE IF NOT EXISTS "
                + quoteTable(tableInfo)
                + " ("
                + columns
                + primaryKey
                + ")"
                + tablePropertiesClause(tableCreateProperties);
    }

    public static String addColumnSql(
            PgVectorTableInfo tableInfo,
            Column column,
            Map<String, PgVectorColumnSpec> vectorColumns) {
        return "ALTER TABLE "
                + quoteTable(tableInfo)
                + " ADD COLUMN IF NOT EXISTS "
                + quoteIdentifier(column.getName())
                + " "
                + PgVectorTypeUtils.toPostgresType(tableInfo, column, vectorColumns)
                + nullableClause(column);
    }

    public static String alterColumnTypeSql(
            PgVectorTableInfo tableInfo,
            String columnName,
            Column column,
            Map<String, PgVectorColumnSpec> vectorColumns) {
        return "ALTER TABLE "
                + quoteTable(tableInfo)
                + " ALTER COLUMN "
                + quoteIdentifier(columnName)
                + " TYPE "
                + PgVectorTypeUtils.toPostgresType(tableInfo, column, vectorColumns)
                + " USING "
                + quoteIdentifier(columnName)
                + "::"
                + PgVectorTypeUtils.toPostgresType(tableInfo, column, vectorColumns);
    }

    public static String alterColumnTypeSql(
            PgVectorTableInfo tableInfo, String columnName, String postgresType) {
        return "ALTER TABLE "
                + quoteTable(tableInfo)
                + " ALTER COLUMN "
                + quoteIdentifier(columnName)
                + " TYPE "
                + postgresType
                + " USING "
                + quoteIdentifier(columnName)
                + "::"
                + postgresType;
    }

    public static String dropColumnSql(PgVectorTableInfo tableInfo, String columnName) {
        return "ALTER TABLE "
                + quoteTable(tableInfo)
                + " DROP COLUMN IF EXISTS "
                + quoteIdentifier(columnName);
    }

    public static String renameColumnSql(
            PgVectorTableInfo tableInfo, String oldColumnName, String newColumnName) {
        return "ALTER TABLE "
                + quoteTable(tableInfo)
                + " RENAME COLUMN "
                + quoteIdentifier(oldColumnName)
                + " TO "
                + quoteIdentifier(newColumnName);
    }

    public static String truncateTableSql(PgVectorTableInfo tableInfo) {
        return "TRUNCATE TABLE " + quoteTable(tableInfo);
    }

    public static String dropTableSql(PgVectorTableInfo tableInfo) {
        return "DROP TABLE IF EXISTS " + quoteTable(tableInfo);
    }

    public static String upsertSql(PgVectorTableInfo tableInfo, Schema schema) {
        List<String> columnNames =
                schema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .map(Column::getName)
                        .collect(Collectors.toList());
        String columns =
                columnNames.stream()
                        .map(PgVectorSqlUtils::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                columnNames.stream().map(ignored -> "?").collect(Collectors.joining(", "));

        if (schema.primaryKeys().isEmpty()) {
            return "INSERT INTO "
                    + quoteTable(tableInfo)
                    + " ("
                    + columns
                    + ") VALUES ("
                    + placeholders
                    + ")";
        }

        String conflictColumns =
                schema.primaryKeys().stream()
                        .map(PgVectorSqlUtils::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String updateAssignments =
                columnNames.stream()
                        .filter(columnName -> !schema.primaryKeys().contains(columnName))
                        .map(
                                columnName ->
                                        quoteIdentifier(columnName)
                                                + " = EXCLUDED."
                                                + quoteIdentifier(columnName))
                        .collect(Collectors.joining(", "));
        if (updateAssignments.isEmpty()) {
            return "INSERT INTO "
                    + quoteTable(tableInfo)
                    + " ("
                    + columns
                    + ") VALUES ("
                    + placeholders
                    + ") ON CONFLICT ("
                    + conflictColumns
                    + ") DO NOTHING";
        }

        return "INSERT INTO "
                + quoteTable(tableInfo)
                + " ("
                + columns
                + ") VALUES ("
                + placeholders
                + ") ON CONFLICT ("
                + conflictColumns
                + ") DO UPDATE SET "
                + updateAssignments;
    }

    public static String deleteSql(PgVectorTableInfo tableInfo, Schema schema) {
        String where =
                schema.primaryKeys().stream()
                        .map(pk -> quoteIdentifier(pk) + " = ?")
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + quoteTable(tableInfo) + " WHERE " + where;
    }

    public static String vectorColumnKey(PgVectorTableInfo tableInfo, String columnName) {
        return tableInfo.identifier() + "." + columnName;
    }

    public static String tableColumnKey(TableId tableId, String columnName) {
        return tableId.identifier() + "." + columnName;
    }

    private static String nullableClause(Column column) {
        return column.getType().isNullable() ? "" : " NOT NULL";
    }

    private static String tablePropertiesClause(Map<String, String> tableCreateProperties) {
        if (tableCreateProperties.isEmpty()) {
            return "";
        }
        String withProperties =
                tableCreateProperties.entrySet().stream()
                        .map(
                                entry ->
                                        quoteStorageParameter(entry.getKey())
                                                + " = "
                                                + quoteStorageParameterValue(entry.getValue()))
                        .collect(Collectors.joining(", "));
        return " WITH (" + withProperties + ")";
    }

    private static String quoteStorageParameter(String value) {
        if (!STORAGE_PARAMETER_PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid PostgreSQL storage parameter: " + value);
        }
        return value;
    }

    private static String quoteStorageParameterValue(String value) {
        if (value.matches("(?i)true|false|[-+]?\\d+(\\.\\d+)?")) {
            return value;
        }
        return "'" + value.replace("'", "''") + "'";
    }
}
