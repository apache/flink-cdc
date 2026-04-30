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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.starrocks.connector.flink.catalog.StarRocksCatalog;
import com.starrocks.connector.flink.catalog.StarRocksCatalogException;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** An enriched {@code StarRocksCatalog} with more schema evolution abilities. */
public class StarRocksEnrichedCatalog extends StarRocksCatalog {
    public StarRocksEnrichedCatalog(String jdbcUrl, String username, String password) {
        super(jdbcUrl, username, password);
    }

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksEnrichedCatalog.class);

    @Override
    public void createTable(StarRocksTable table, boolean ignoreIfExists)
            throws StarRocksCatalogException {
        String createTableSql = buildCreateTableSql(table, ignoreIfExists);
        try {
            executeUpdateStatement(createTableSql);
            LOG.info(
                    "Success to create table {}.{}, sql: {}",
                    table.getDatabaseName(),
                    table.getDatabaseName(),
                    createTableSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to create table {}.{}, sql: {}",
                    table.getDatabaseName(),
                    table.getDatabaseName(),
                    createTableSql,
                    e);
            throw new StarRocksCatalogException(
                    String.format(
                            "Failed to create table %s.%s",
                            table.getDatabaseName(), table.getDatabaseName()),
                    e);
        }
    }

    @Override
    public void alterAddColumns(
            String databaseName,
            String tableName,
            List<StarRocksColumn> addColumns,
            long timeoutSecond)
            throws StarRocksCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "table name cannot be null or empty.");
        Preconditions.checkArgument(!addColumns.isEmpty(), "Added columns should not be empty.");

        String alterSql =
                buildAlterAddColumnsSql(databaseName, tableName, addColumns, timeoutSecond);
        try {
            long startTimeMillis = System.currentTimeMillis();
            executeAlter(databaseName, tableName, alterSql, timeoutSecond);
            LOG.info(
                    "Success to add columns to {}.{}, duration: {}ms, sql: {}",
                    databaseName,
                    tableName,
                    System.currentTimeMillis() - startTimeMillis,
                    alterSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to add columns to {}.{}, sql: {}",
                    databaseName,
                    tableName,
                    alterSql,
                    e);
            throw new StarRocksCatalogException(
                    String.format("Failed to add columns to %s.%s ", databaseName, tableName), e);
        }
    }

    public void truncateTable(String databaseName, String tableName)
            throws StarRocksCatalogException {
        checkTableArgument(databaseName, tableName);
        String alterSql = this.buildTruncateTableSql(databaseName, tableName);
        try {
            // TRUNCATE TABLE is not regarded as a column-based schema change for StarRocks, so
            // there's no need to check the evolution state.
            executeUpdateStatement(alterSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to truncate table `{}`.`{}`. SQL executed: {}",
                    databaseName,
                    tableName,
                    alterSql);
            throw new StarRocksCatalogException(
                    String.format("Failed to truncate table `%s`.`%s`.", databaseName, tableName),
                    e);
        }
    }

    public void dropTable(String databaseName, String tableName) throws StarRocksCatalogException {
        checkTableArgument(databaseName, tableName);
        String alterSql = this.buildDropTableSql(databaseName, tableName);
        try {
            // like TRUNCATE TABLE, DROP TABLE isn't a column-affecting operation and `executeAlter`
            // method isn't appropriate.
            executeUpdateStatement(alterSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to drop table `{}`.`{}`. SQL executed: {}",
                    databaseName,
                    tableName,
                    alterSql);
            throw new StarRocksCatalogException(
                    String.format("Failed to drop table `%s`.`%s`.", databaseName, tableName), e);
        }
    }

    public void renameColumn(
            String databaseName, String tableName, String oldColumnName, String newColumnName)
            throws StarRocksCatalogException {
        checkTableArgument(databaseName, tableName);
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(oldColumnName),
                "old column name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(newColumnName),
                "new column name cannot be null or empty.");
        String alterSql =
                this.buildRenameColumnSql(databaseName, tableName, oldColumnName, newColumnName);
        try {
            executeUpdateStatement(alterSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to alter table `{}`.`{}` rename column {} to {}. SQL executed: {}",
                    databaseName,
                    tableName,
                    oldColumnName,
                    newColumnName,
                    alterSql);
            throw new StarRocksCatalogException(
                    String.format(
                            "Failed to alter table `%s`.`%s` rename column %s to %s.",
                            databaseName, tableName, oldColumnName, newColumnName),
                    e);
        }
    }

    public void alterColumnType(String databaseName, String tableName, StarRocksColumn column)
            throws StarRocksCatalogException {
        checkTableArgument(databaseName, tableName);
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(column.getColumnName()),
                "column name cannot be null or empty.");
        String alterSql = buildAlterColumnTypeSql(databaseName, tableName, buildColumnStmt(column));
        try {
            long startTimeMillis = System.currentTimeMillis();
            executeUpdateStatement(alterSql);
            LOG.info(
                    "Success to alter table {}.{} modify column type, duration: {}ms, sql: {}",
                    databaseName,
                    tableName,
                    System.currentTimeMillis() - startTimeMillis,
                    alterSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to alter table {}.{} modify column type, sql: {}",
                    databaseName,
                    tableName,
                    alterSql,
                    e);
            throw new StarRocksCatalogException(
                    String.format(
                            "Failed to alter table %s.%s modify column type",
                            databaseName, tableName),
                    e);
        }
    }

    private String buildAlterAddColumnsSql(
            String databaseName,
            String tableName,
            List<StarRocksColumn> addColumns,
            long timeoutSecond) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("ALTER TABLE `%s`.`%s` ", databaseName, tableName));
        String columnsStmt =
                addColumns.stream()
                        .map(col -> "ADD COLUMN " + buildColumnStmt(col))
                        .collect(Collectors.joining(", "));
        builder.append(columnsStmt);
        builder.append(String.format(" PROPERTIES (\"timeout\" = \"%s\")", timeoutSecond));
        builder.append(";");
        return builder.toString();
    }

    private String buildCreateTableSql(StarRocksTable table, boolean ignoreIfExists) {
        StringBuilder builder = new StringBuilder();
        builder.append(
                String.format(
                        "CREATE TABLE %s`%s`.`%s`",
                        ignoreIfExists ? "IF NOT EXISTS " : "",
                        table.getDatabaseName(),
                        table.getTableName()));
        builder.append(" (\n");
        String columnsStmt =
                table.getColumns().stream()
                        .map(this::buildColumnStmt)
                        .collect(Collectors.joining(",\n"));
        builder.append(columnsStmt);
        builder.append("\n) ");

        Preconditions.checkArgument(
                table.getTableType() == StarRocksTable.TableType.PRIMARY_KEY,
                "Not support to build create table sql for table type " + table.getTableType());
        Preconditions.checkArgument(
                table.getTableKeys().isPresent(),
                "Can't build create table sql because there is no table keys");
        String tableKeys =
                table.getTableKeys().get().stream()
                        .map(key -> "`" + key + "`")
                        .collect(Collectors.joining(", "));
        builder.append(String.format("PRIMARY KEY (%s)\n", tableKeys));

        Preconditions.checkArgument(
                table.getDistributionKeys().isPresent(),
                "Can't build create table sql because there is no distribution keys");
        String distributionKeys =
                table.getDistributionKeys().get().stream()
                        .map(key -> "`" + key + "`")
                        .collect(Collectors.joining(", "));
        builder.append(String.format("DISTRIBUTED BY HASH (%s)", distributionKeys));
        if (table.getNumBuckets().isPresent()) {
            builder.append(" BUCKETS ");
            builder.append(table.getNumBuckets().get());
        }
        if (!table.getProperties().isEmpty()) {
            builder.append("\nPROPERTIES (\n");
            String properties =
                    table.getProperties().entrySet().stream()
                            .map(
                                    entry ->
                                            String.format(
                                                    "\"%s\" = \"%s\"",
                                                    entry.getKey(), entry.getValue()))
                            .collect(Collectors.joining(",\n"));
            builder.append(properties);
            builder.append("\n)");
        }
        builder.append(";");
        return builder.toString();
    }

    private String buildTruncateTableSql(String databaseName, String tableName) {
        return String.format("TRUNCATE TABLE `%s`.`%s`;", databaseName, tableName);
    }

    private String buildDropTableSql(String databaseName, String tableName) {
        return String.format("DROP TABLE `%s`.`%s`;", databaseName, tableName);
    }

    private String buildRenameColumnSql(
            String databaseName, String tableName, String oldColumnName, String newColumnName) {
        return String.format(
                "ALTER TABLE `%s`.`%s` RENAME COLUMN %s TO %s;",
                databaseName, tableName, oldColumnName, newColumnName);
    }

    private String buildAlterColumnTypeSql(
            String databaseName, String tableName, String columnStmt) {
        return String.format(
                "ALTER TABLE `%s`.`%s` MODIFY COLUMN %s", databaseName, tableName, columnStmt);
    }

    private void executeUpdateStatement(String sql) throws StarRocksCatalogException {
        try {
            Method m =
                    getClass()
                            .getSuperclass()
                            .getDeclaredMethod("executeUpdateStatement", String.class);
            m.setAccessible(true);
            m.invoke(this, sql);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeAlter(
            String databaseName, String tableName, String alterSql, long timeoutSecond)
            throws StarRocksCatalogException {
        try {
            Method m =
                    getClass()
                            .getSuperclass()
                            .getDeclaredMethod(
                                    "executeAlter",
                                    String.class,
                                    String.class,
                                    String.class,
                                    long.class);
            m.setAccessible(true);
            m.invoke(this, databaseName, tableName, alterSql, timeoutSecond);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkTableArgument(String databaseName, String tableName) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "Table name cannot be null or empty.");
    }

    private String buildColumnStmt(StarRocksColumn column) {
        StringBuilder builder = new StringBuilder();
        builder.append("`");
        builder.append(column.getColumnName());
        builder.append("` ");
        builder.append(
                getFullColumnType(
                        column.getDataType(), column.getColumnSize(), column.getDecimalDigits()));
        builder.append(" ");
        builder.append(column.isNullable() ? "NULL" : "NOT NULL");
        if (column.getDefaultValue().isPresent()) {
            builder.append(
                    String.format(
                            " DEFAULT \"%s\"",
                            escapeForDoubleQuotedSqlString(column.getDefaultValue().get())));
        }

        if (column.getColumnComment().isPresent()) {
            builder.append(
                    String.format(
                            " COMMENT \"%s\"",
                            escapeForDoubleQuotedSqlString(column.getColumnComment().get())));
        }
        return builder.toString();
    }

    private String escapeForDoubleQuotedSqlString(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private String getFullColumnType(
            String type, Optional<Integer> columnSize, Optional<Integer> decimalDigits) {
        String dataType = type.toUpperCase();
        switch (dataType) {
            case "DECIMAL":
                Preconditions.checkArgument(
                        columnSize.isPresent(), "DECIMAL type must have column size");
                Preconditions.checkArgument(
                        decimalDigits.isPresent(), "DECIMAL type must have decimal digits");
                return String.format("DECIMAL(%d, %s)", columnSize.get(), decimalDigits.get());
            case "CHAR":
            case "VARCHAR":
                Preconditions.checkArgument(
                        columnSize.isPresent(), type + " type must have column size");
                return String.format("%s(%d)", dataType, columnSize.get());
            default:
                return dataType;
        }
    }
}
