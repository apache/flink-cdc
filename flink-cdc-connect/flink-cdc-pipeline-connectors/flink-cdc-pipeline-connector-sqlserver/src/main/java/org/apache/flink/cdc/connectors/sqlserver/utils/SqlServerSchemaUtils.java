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

package org.apache.flink.cdc.connectors.sqlserver.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils;
import org.apache.flink.table.api.ValidationException;

import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class SqlServerSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerSchemaUtils.class);

    private static final String SQL_SERVER_AGENT_STATUS_QUERY =
            "SELECT TOP(1) status_desc FROM sys.dm_server_services "
                    + "WHERE servicename LIKE 'SQL Server Agent (%'";

    public static List<String> listSchemas(SqlServerSourceConfig sourceConfig, String namespace) {
        try (JdbcConnection jdbc =
                getSqlServerDialect(sourceConfig).openJdbcConnection(sourceConfig)) {
            return listSchemas(jdbc, namespace);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list schemas: " + e.getMessage(), e);
        }
    }

    public static List<String> listNamespaces(SqlServerSourceConfig sourceConfig) {
        try (JdbcConnection jdbc =
                getSqlServerDialect(sourceConfig).openJdbcConnection(sourceConfig)) {
            return listNamespaces(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list namespaces: " + e.getMessage(), e);
        }
    }

    public static void validateSqlServerAgentRunning(SqlServerSourceConfig sourceConfig) {
        try (JdbcConnection jdbc =
                getSqlServerDialect(sourceConfig).openJdbcConnection(sourceConfig)) {
            String agentStatus =
                    jdbc.queryAndMap(
                            SQL_SERVER_AGENT_STATUS_QUERY,
                            rs -> rs.next() ? rs.getString(1) : null);
            if (!"Running".equalsIgnoreCase(agentStatus)) {
                throw new ValidationException(
                        "SQL Server Agent is not running. Please start SQL Server Agent before"
                                + " creating the SQL Server pipeline.");
            }
        } catch (SQLException e) {
            throw new ValidationException(
                    "Failed to validate SQL Server Agent status from sys.dm_server_services.", e);
        }
    }

    public static List<TableId> listTables(
            SqlServerSourceConfig sourceConfig,
            @Nullable String dbName,
            @Nullable String schemaName) {
        try (JdbcConnection jdbc =
                getSqlServerDialect(sourceConfig).openJdbcConnection(sourceConfig)) {
            List<String> databases =
                    dbName != null
                            ? Collections.singletonList(dbName)
                            : sourceConfig.getDatabaseList();

            List<io.debezium.relational.TableId> dbzTableIds =
                    SqlServerConnectionUtils.listTables(
                            jdbc, sourceConfig.getTableFilters(), databases);

            return dbzTableIds.stream()
                    .filter(
                            tableId ->
                                    schemaName == null
                                            || schemaName.equalsIgnoreCase(tableId.schema()))
                    .map(SqlServerSchemaUtils::toCdcTableId)
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            throw new RuntimeException("Error to list tables: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(SqlServerSourceConfig sourceConfig, TableId tableId) {
        SqlServerDialect dialect = getSqlServerDialect(sourceConfig);
        try (JdbcConnection jdbc = dialect.openJdbcConnection(sourceConfig)) {
            return getTableSchema(tableId, (SqlServerConnection) jdbc, dialect);
        } catch (SQLException e) {
            throw new RuntimeException("Error to get table schema: " + e.getMessage(), e);
        }
    }

    public static SqlServerDialect getSqlServerDialect(SqlServerSourceConfig sourceConfig) {
        return new SqlServerDialect(sourceConfig);
    }

    public static List<String> listSchemas(JdbcConnection jdbc, String namespace)
            throws SQLException {
        LOG.info("Read list of available schemas");
        final List<String> schemaNames = new ArrayList<>();

        String querySql =
                String.format(
                        "SELECT SCHEMA_NAME FROM %s.INFORMATION_SCHEMA.SCHEMATA", quote(namespace));

        jdbc.query(
                querySql,
                rs -> {
                    while (rs.next()) {
                        schemaNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available schemas are: {}", schemaNames);
        return schemaNames;
    }

    public static List<String> listNamespaces(JdbcConnection jdbc) throws SQLException {
        LOG.info("Read list of available namespaces (databases)");
        final List<String> namespaceNames = new ArrayList<>();
        jdbc.query(
                "SELECT name FROM sys.databases",
                rs -> {
                    while (rs.next()) {
                        namespaceNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available namespaces are: {}", namespaceNames);
        return namespaceNames;
    }

    public static String quote(String dbOrTableName) {
        return "[" + dbOrTableName.replace("]", "]]") + "]";
    }

    public static Schema getTableSchema(
            TableId tableId, SqlServerConnection jdbc, SqlServerDialect dialect) {
        io.debezium.relational.TableId dbzTableId = toDbzTableId(tableId);
        return getTableSchema(dbzTableId, jdbc, dialect);
    }

    public static Schema getTableSchema(
            io.debezium.relational.TableId tableId,
            SqlServerConnection jdbc,
            SqlServerDialect dialect) {
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
                table.columns().stream()
                        .map(SqlServerSchemaUtils::toColumn)
                        .collect(Collectors.toList());

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
                    SqlServerTypeUtils.fromDbzColumn(column),
                    column.comment(),
                    defaultValueExpression);
        } else {
            return Column.physicalColumn(
                    column.name(), SqlServerTypeUtils.fromDbzColumn(column), column.comment());
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
        return unquoteSqlServerStringLiteral(unwrapped);
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

    private static String unquoteSqlServerStringLiteral(String expression) {
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
        // SQL Server TableId format: database.schema.table
        // CDC TableId: namespace (database), schemaName (schema), tableName (table)
        return new io.debezium.relational.TableId(
                tableId.getNamespace(), tableId.getSchemaName(), tableId.getTableName());
    }

    public static TableId toCdcTableId(io.debezium.relational.TableId dbzTableId) {
        // SQL Server uses database.schema.table structure
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
