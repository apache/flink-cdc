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

package org.apache.flink.cdc.connectors.db2.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.source.utils.Db2ConnectionUtils;

import io.debezium.connector.db2.Db2Connection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for converting from Debezium {@link Table} types to {@link Schema}. */
public class Db2SchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(Db2SchemaUtils.class);

    public static List<String> listSchemas(Db2SourceConfig sourceConfig, String namespace) {
        try (JdbcConnection jdbc = getDb2Dialect(sourceConfig).openJdbcConnection(sourceConfig)) {
            return listSchemas(jdbc, namespace);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list schemas: " + e.getMessage(), e);
        }
    }

    public static List<String> listNamespaces(Db2SourceConfig sourceConfig) {
        if (sourceConfig.getDatabaseList() != null && !sourceConfig.getDatabaseList().isEmpty()) {
            return new ArrayList<>(sourceConfig.getDatabaseList());
        }
        try (JdbcConnection jdbc = getDb2Dialect(sourceConfig).openJdbcConnection(sourceConfig)) {
            return listNamespaces(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list namespaces: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(
            Db2SourceConfig sourceConfig, @Nullable String dbName, @Nullable String schemaName) {
        try (JdbcConnection jdbc = getDb2Dialect(sourceConfig).openJdbcConnection(sourceConfig)) {
            List<io.debezium.relational.TableId> dbzTableIds =
                    Db2ConnectionUtils.listTables(jdbc, sourceConfig.getTableFilters());

            return dbzTableIds.stream()
                    .filter(tableId -> dbName == null || dbName.equalsIgnoreCase(tableId.catalog()))
                    .filter(
                            tableId ->
                                    schemaName == null
                                            || schemaName.equalsIgnoreCase(tableId.schema()))
                    .map(Db2SchemaUtils::toCdcTableId)
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            throw new RuntimeException("Error to list tables: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(Db2SourceConfig sourceConfig, TableId tableId) {
        Db2Dialect dialect = getDb2Dialect(sourceConfig);
        try (JdbcConnection jdbc = dialect.openJdbcConnection(sourceConfig)) {
            return getTableSchema(tableId, (Db2Connection) jdbc, dialect);
        } catch (SQLException e) {
            throw new RuntimeException("Error to get table schema: " + e.getMessage(), e);
        }
    }

    public static Db2Dialect getDb2Dialect(Db2SourceConfig sourceConfig) {
        return new Db2Dialect(sourceConfig);
    }

    public static List<String> listSchemas(JdbcConnection jdbc, String namespace)
            throws SQLException {
        LOG.info("Read list of available schemas");
        final List<String> schemaNames = new ArrayList<>();

        jdbc.query(
                "SELECT DISTINCT TABSCHEMA FROM SYSCAT.TABLES WHERE TYPE = 'T' ORDER BY TABSCHEMA",
                rs -> {
                    while (rs.next()) {
                        String schemaName = rs.getString(1);
                        if (schemaName != null) {
                            schemaNames.add(schemaName.trim());
                        }
                    }
                });
        LOG.info("\t list of available schemas are: {}", schemaNames);
        return schemaNames;
    }

    public static List<String> listNamespaces(JdbcConnection jdbc) throws SQLException {
        LOG.info("Read list of available namespaces (databases)");
        final List<String> namespaceNames = new ArrayList<>();
        namespaceNames.add(((Db2Connection) jdbc).getRealDatabaseName());
        LOG.info("\t list of available namespaces are: {}", namespaceNames);
        return namespaceNames;
    }

    public static String quote(String dbOrTableName) {
        return "\"" + dbOrTableName.replace("\"", "\"\"") + "\"";
    }

    public static Schema getTableSchema(TableId tableId, Db2Connection jdbc, Db2Dialect dialect) {
        io.debezium.relational.TableId dbzTableId = toDbzTableId(tableId);
        return getTableSchema(dbzTableId, jdbc, dialect);
    }

    public static Schema getTableSchema(
            io.debezium.relational.TableId tableId, Db2Connection jdbc, Db2Dialect dialect) {
        try {
            TableChange tableChange = dialect.queryTableSchema(jdbc, tableId);
            if (tableChange == null || tableChange.getTable() == null) {
                throw new RuntimeException("Cannot find table schema for " + tableId);
            }
            return toSchema(tableChange.getTable());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get table schema for " + tableId, e);
        }
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream().map(Db2SchemaUtils::toColumn).collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        if (column.defaultValueExpression().isPresent()) {
            String defaultValueExpression =
                    normalizeDefaultValueExpression(column.defaultValueExpression().get());
            return Column.physicalColumn(
                    column.name(),
                    Db2TypeUtils.fromDbzColumn(column),
                    column.comment(),
                    defaultValueExpression);
        } else {
            return Column.physicalColumn(
                    column.name(), Db2TypeUtils.fromDbzColumn(column), column.comment());
        }
    }

    private static String normalizeDefaultValueExpression(String defaultValueExpression) {
        if (defaultValueExpression == null) {
            return null;
        }
        String trimmed = defaultValueExpression.trim();
        if (trimmed.isEmpty()) {
            return trimmed;
        }
        String unwrapped = stripOuterParentheses(trimmed);
        return unquoteDb2StringLiteral(unwrapped);
    }

    private static String stripOuterParentheses(String expression) {
        String current = expression;
        while (isWrappedByParentheses(current)) {
            current = current.substring(1, current.length() - 1).trim();
        }
        return current;
    }

    private static boolean isWrappedByParentheses(String expression) {
        if (expression.length() < 2
                || expression.charAt(0) != '('
                || expression.charAt(expression.length() - 1) != ')') {
            return false;
        }
        int depth = 0;
        boolean inSingleQuote = false;
        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);
            if (c == '\'') {
                if (inSingleQuote
                        && i + 1 < expression.length()
                        && expression.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inSingleQuote = !inSingleQuote;
                continue;
            }
            if (inSingleQuote) {
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0 && i < expression.length() - 1) {
                    return false;
                }
            }
        }
        return depth == 0 && !inSingleQuote;
    }

    private static String unquoteDb2StringLiteral(String expression) {
        String trimmed = expression.trim();
        if (trimmed.isEmpty()) {
            return trimmed;
        }
        int quoteIndex = -1;
        if (trimmed.startsWith("N'") || trimmed.startsWith("n'")) {
            quoteIndex = 1;
        } else if (trimmed.charAt(0) == '\'') {
            quoteIndex = 0;
        }
        if (quoteIndex < 0 || trimmed.charAt(quoteIndex) != '\'') {
            return expression;
        }
        StringBuilder literal = new StringBuilder();
        for (int i = quoteIndex + 1; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (c == '\'') {
                if (i + 1 < trimmed.length() && trimmed.charAt(i + 1) == '\'') {
                    literal.append('\'');
                    i++;
                    continue;
                }
                if (i == trimmed.length() - 1) {
                    return literal.toString();
                }
                return expression;
            }
            literal.append(c);
        }
        return expression;
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        // DB2 TableId format: database.schema.table
        // CDC TableId: namespace (database), schemaName (schema), tableName (table)
        return new io.debezium.relational.TableId(
                tableId.getNamespace(), tableId.getSchemaName(), tableId.getTableName());
    }

    public static TableId toCdcTableId(io.debezium.relational.TableId dbzTableId) {
        // DB2 uses database.schema.table structure
        // Debezium TableId: catalog (database), schema, table
        // CDC TableId: namespace (database), schemaName (schema), tableName (table)
        String catalog = dbzTableId.catalog();
        String schema = dbzTableId.schema();
        String table = dbzTableId.table();

        LOG.debug(
                "Converting Debezium TableId to CDC TableId - catalog: {}, schema: {}, table: {}",
                catalog,
                schema,
                table);

        return TableId.tableId(catalog, schema, table);
    }
}
