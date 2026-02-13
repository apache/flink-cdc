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

package org.apache.flink.cdc.connectors.oceanbase.catalog;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.oceanbase.sink.OceanBaseUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A {@link OceanBaseCatalog} for OceanBase connector that supports schema evolution under MySQL
 * mode.
 */
public class OceanBaseMySQLCatalog extends OceanBaseCatalog {

    private static final String RENAME_DDL = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s";
    private static final String ALTER_COLUMN_TYPE_DDL = "ALTER TABLE %s.%s MODIFY COLUMN %s %s;";

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMySQLCatalog.class);

    public OceanBaseMySQLCatalog(OceanBaseConnectorOptions connectorOptions) {
        super(connectorOptions);
    }

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     * @throws OceanBaseCatalogException in case of any runtime exception
     */
    @Override
    public boolean databaseExists(String databaseName) throws OceanBaseCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        String querySql =
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA` WHERE SCHEMA_NAME = ?;";
        try {
            List<String> dbList =
                    executeSingleColumnStatement(querySql, escapeSingleQuote(databaseName));
            return !dbList.isEmpty();
        } catch (Exception e) {
            LOG.error(
                    "Failed to check database exist, database: {}, sql: {}",
                    databaseName,
                    querySql,
                    e);
            throw new OceanBaseCatalogException(
                    String.format("Failed to check database exist, database: %s", databaseName), e);
        }
    }

    public static String escapeSingleQuote(String dbOrTableName) {
        return dbOrTableName.replace("'", "\\'");
    }

    public static String quote(String dbOrTableName) {
        return "`" + dbOrTableName.replace("`", "``") + "`";
    }

    /**
     * Create a database.
     *
     * @param databaseName Name of the database
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *     exists.
     * @throws OceanBaseCatalogException in case of any runtime exception
     */
    @Override
    public void createDatabase(String databaseName, boolean ignoreIfExists)
            throws OceanBaseCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        String sql = buildCreateDatabaseSql(quote(databaseName), ignoreIfExists);
        try {
            executeUpdateStatement(sql);
            LOG.info("Successful to create database {}, sql: {}", databaseName, sql);
        } catch (Exception e) {
            LOG.info("Failed to create database {}, sql: {}", databaseName, sql, e);
            throw new OceanBaseCatalogException(
                    String.format(
                            "Failed to create database %s, ignoreIfExists: %s",
                            databaseName, ignoreIfExists),
                    e);
        }
    }

    @Override
    public boolean tableExists(String databaseName, String tableName)
            throws OceanBaseCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "table name cannot be null or empty.");
        String querySql =
                "SELECT `TABLE_NAME` FROM `INFORMATION_SCHEMA`.`TABLES` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;";
        try {
            List<String> dbList =
                    executeSingleColumnStatement(
                            querySql,
                            escapeSingleQuote(databaseName),
                            escapeSingleQuote(tableName));
            return !dbList.isEmpty();
        } catch (Exception e) {
            LOG.error(
                    "Failed to check table exist, table: {}.{}, sql: {}",
                    databaseName,
                    tableName,
                    querySql,
                    e);
            throw new OceanBaseCatalogException(
                    String.format(
                            "Failed to check table exist, table: %s.%s", databaseName, tableName),
                    e);
        }
    }

    /**
     * Creates a table.
     *
     * @param table the table definition
     * @param ignoreIfExists flag to specify behavior when a table already exists.
     * @throws OceanBaseCatalogException in case of any runtime exception
     */
    @Override
    public void createTable(OceanBaseTable table, boolean ignoreIfExists)
            throws OceanBaseCatalogException {
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
            throw new OceanBaseCatalogException(
                    String.format(
                            "Failed to create table %s.%s",
                            table.getDatabaseName(), table.getDatabaseName()),
                    e);
        }
    }

    @Override
    public void alterAddColumns(
            String databaseName, String tableName, List<OceanBaseColumn> addColumns) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "table name cannot be null or empty.");
        Preconditions.checkArgument(!addColumns.isEmpty(), "Added columns should not be empty.");

        String alterSql = buildAlterAddColumnsSql(databaseName, tableName, addColumns);
        try {
            long startTimeMillis = System.currentTimeMillis();
            executeUpdateStatement(alterSql);
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
            throw new OceanBaseCatalogException(
                    String.format("Failed to add columns to %s.%s ", databaseName, tableName), e);
        }
    }

    @Override
    public void alterDropColumns(String databaseName, String tableName, List<String> dropColumns) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "table name cannot be null or empty.");
        Preconditions.checkArgument(!dropColumns.isEmpty(), "Drop columns should not be empty.");

        String alterSql = buildAlterDropColumnsSql(databaseName, tableName, dropColumns);
        try {
            long startTimeMillis = System.currentTimeMillis();
            executeUpdateStatement(alterSql);
            LOG.info(
                    "Success to drop columns from {}.{}, duration: {}ms, sql: {}",
                    databaseName,
                    tableName,
                    System.currentTimeMillis() - startTimeMillis,
                    alterSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to drop columns from {}.{}, sql: {}",
                    databaseName,
                    tableName,
                    alterSql);
            throw new OceanBaseCatalogException(
                    String.format("Failed to drop columns from %s.%s ", databaseName, tableName),
                    e);
        }
    }

    @Override
    public void alterColumnType(
            String databaseName, String tableName, String columnName, DataType dataType) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "table name cannot be null or empty.");

        OceanBaseUtils.CdcDataTypeTransformer cdcDataTypeTransformer =
                new OceanBaseUtils.CdcDataTypeTransformer(false, new OceanBaseColumn.Builder());
        OceanBaseColumn oceanBaseColumn =
                dataType.accept(cdcDataTypeTransformer).setColumnName(columnName).build();
        String alterTypeSql =
                String.format(
                        ALTER_COLUMN_TYPE_DDL,
                        quote(databaseName),
                        quote(tableName),
                        quote(columnName),
                        oceanBaseColumn.getDataType());

        try {
            long startTimeMillis = System.currentTimeMillis();
            executeUpdateStatement(alterTypeSql);
            LOG.info(
                    "Success to alter table {}.{} column {} to type {}, duration: {}ms, sql: {}",
                    databaseName,
                    tableName,
                    columnName,
                    oceanBaseColumn.getDataType(),
                    System.currentTimeMillis() - startTimeMillis,
                    alterTypeSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to alter table {}.{} column {} to type {}, sql: {}",
                    databaseName,
                    tableName,
                    columnName,
                    oceanBaseColumn.getDataType(),
                    alterTypeSql,
                    e);
            throw new OceanBaseCatalogException(
                    String.format(
                            "Failed to alter table %s.%s column %s to type %s ",
                            databaseName, tableName, columnName, oceanBaseColumn.getDataType()),
                    e);
        }
    }

    @Override
    public void renameColumn(
            String schemaName, String tableName, String oldColumnName, String newColumnName) {
        String renameColumnSql =
                buildRenameColumnSql(schemaName, tableName, oldColumnName, newColumnName);
        try {
            long startTimeMillis = System.currentTimeMillis();
            executeUpdateStatement(renameColumnSql);
            LOG.info(
                    "Success to rename {} column from {} to {}, duration: {}ms, sql: {}",
                    String.format("%s.%s", schemaName, tableName),
                    oldColumnName,
                    newColumnName,
                    System.currentTimeMillis() - startTimeMillis,
                    renameColumnSql);
        } catch (Exception e) {
            LOG.error(
                    "Fail to rename {} column from {} to {}, duration: {}ms, sql: {}",
                    String.format("%s.%s", schemaName, tableName),
                    oldColumnName,
                    newColumnName,
                    renameColumnSql,
                    e);
            throw new OceanBaseCatalogException(
                    String.format(
                            "Failed to rename %s column from %s to %s ",
                            String.format("%s.%s", schemaName, tableName), schemaName, tableName),
                    e);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "table name cannot be null or empty.");

        String dropTableDDL =
                String.format("DROP TABLE IF EXISTS %s.%s", quote(databaseName), quote(tableName));
        try {
            long startTimeMillis = System.currentTimeMillis();
            executeUpdateStatement(dropTableDDL);
            LOG.info(
                    "Success to drop table {}.{}, duration: {}ms, sql: {}",
                    databaseName,
                    tableName,
                    System.currentTimeMillis() - startTimeMillis,
                    dropTableDDL);
        } catch (Exception e) {
            LOG.error(
                    "Failed to drop table {}.{}, sql: {}",
                    databaseName,
                    tableName,
                    dropTableDDL,
                    e);
            throw new OceanBaseCatalogException(
                    String.format("Failed to drop table %s.%s ", databaseName, tableName), e);
        }
    }

    @Override
    public void truncateTable(String databaseName, String tableName) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "table name cannot be null or empty.");

        String dropTableDDL =
                String.format("TRUNCATE TABLE %s.%s", quote(databaseName), quote(tableName));
        try {
            long startTimeMillis = System.currentTimeMillis();
            executeUpdateStatement(dropTableDDL);
            LOG.info(
                    "Success to truncate table {}.{}, duration: {}ms, sql: {}",
                    databaseName,
                    tableName,
                    System.currentTimeMillis() - startTimeMillis,
                    dropTableDDL);
        } catch (Exception e) {
            LOG.error(
                    "Failed to truncate table {}.{}, sql: {}",
                    databaseName,
                    tableName,
                    dropTableDDL,
                    e);
            throw new OceanBaseCatalogException(
                    String.format("Failed to truncate table %s.%s ", databaseName, tableName), e);
        }
    }

    // ------------------------------------------------------------------------------------------
    // OceanBase DDL SQL
    // ------------------------------------------------------------------------------------------

    protected String buildCreateDatabaseSql(String databaseName, boolean ignoreIfExists) {
        return String.format(
                "CREATE DATABASE %s%s;", ignoreIfExists ? "IF NOT EXISTS " : "", databaseName);
    }

    protected String buildCreateTableSql(OceanBaseTable table, boolean ignoreIfExists) {
        StringBuilder builder = new StringBuilder();
        builder.append(
                String.format(
                        "CREATE TABLE %s%s.%s",
                        ignoreIfExists ? "IF NOT EXISTS " : "",
                        quote(table.getDatabaseName()),
                        quote(table.getTableName())));
        builder.append(" (\n");
        String columnsStmt =
                table.getColumns().stream()
                        .map(this::buildColumnStmt)
                        .collect(Collectors.joining(",\n"));
        builder.append(columnsStmt);
        builder.append(",\n");

        String tableKeys =
                table.getTableKeys().get().stream()
                        .map(key -> "`" + key + "`")
                        .collect(Collectors.joining(", "));
        builder.append(String.format("PRIMARY KEY (%s)", tableKeys));
        builder.append("\n) ");

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

    private String buildAlterDropColumnsSql(
            String databaseName, String tableName, List<String> dropColumns) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("ALTER TABLE %s.%s ", quote(databaseName), quote(tableName)));
        String columnsStmt =
                dropColumns.stream()
                        .map(col -> String.format("DROP COLUMN `%s`", col))
                        .collect(Collectors.joining(", "));
        builder.append(columnsStmt);
        builder.append(";");
        return builder.toString();
    }

    protected String buildColumnStmt(OceanBaseColumn column) {
        StringBuilder builder = new StringBuilder();
        builder.append("`");
        builder.append(column.getColumnName());
        builder.append("` ");
        builder.append(
                getFullColumnType(
                        column.getDataType(), column.getColumnSize(), column.getNumericScale()));
        builder.append(" ");
        builder.append(column.isNullable() ? "NULL" : "NOT NULL");
        if (column.getDefaultValue().isPresent()) {
            builder.append(String.format(" DEFAULT \"%s\"", column.getDefaultValue().get()));
        }

        if (column.getColumnComment().isPresent()) {
            builder.append(String.format(" COMMENT \"%s\"", column.getColumnComment().get()));
        }
        return builder.toString();
    }

    protected String getFullColumnType(
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
            case "VARBINARY":
                Preconditions.checkArgument(
                        columnSize.isPresent(), type + " type must have column size");
                return String.format("%s(%d)", dataType, columnSize.get());
            case "DATETIME":
            case "TIMESTAMP":
                return columnSize
                        .map(size -> String.format("%s(%d)", dataType, size))
                        .orElse(dataType);
            default:
                return dataType;
        }
    }

    protected String buildAlterAddColumnsSql(
            String databaseName, String tableName, List<OceanBaseColumn> addColumns) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("ALTER TABLE %s.%s ", quote(databaseName), quote(tableName)));
        String columnsStmt =
                addColumns.stream()
                        .map(col -> "ADD COLUMN " + buildColumnStmt(col))
                        .collect(Collectors.joining(", "));
        builder.append(columnsStmt);
        builder.append(";");
        return builder.toString();
    }

    private static String buildRenameColumnSql(
            String schemaName, String tableName, String oldColumnName, String newColumnName) {
        return String.format(
                RENAME_DDL,
                quote(schemaName),
                quote(tableName),
                quote(oldColumnName),
                quote(newColumnName));
    }
}
