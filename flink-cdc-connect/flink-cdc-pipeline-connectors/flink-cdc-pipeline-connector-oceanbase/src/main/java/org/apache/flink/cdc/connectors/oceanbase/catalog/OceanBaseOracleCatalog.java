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

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** A {@link OceanBaseCatalog} for OceanBase connector that supports schema evolution under Oracle mode. */
public class OceanBaseOracleCatalog extends OceanBaseCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseOracleCatalog.class);

    public OceanBaseOracleCatalog(OceanBaseConnectorOptions connectorOptions) {
        super(connectorOptions);
    }

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     * @throws OceanBaseCatalogException in case of any runtime exception
     */
    public boolean databaseExists(String databaseName) throws OceanBaseCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        String querySql =
                String.format(
                        "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA` WHERE SCHEMA_NAME = '%s';",
                        databaseName);
        try {
            List<String> dbList = executeSingleColumnStatement(querySql);
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

    /**
     * Create a database.
     *
     * @param databaseName Name of the database
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *     exists: if set to false, throw a StarRocksCatalogException, if set to true, do nothing.
     * @throws OceanBaseCatalogException in case of any runtime exception
     */
    public void createDatabase(String databaseName, boolean ignoreIfExists)
            throws OceanBaseCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        String sql = buildCreateDatabaseSql(databaseName, ignoreIfExists);
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

    /**
     * Creates a table.
     *
     * @param table the table definition
     * @param ignoreIfExists flag to specify behavior when a table already exists. if set to false,
     *     it throws a TableAlreadyExistException, if set to true, do nothing.
     * @throws OceanBaseCatalogException in case of any runtime exception
     */
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

    // ------------------------------------------------------------------------------------------
    // OceanBase DDL SQL
    // ------------------------------------------------------------------------------------------

    private String buildCreateDatabaseSql(String databaseName, boolean ignoreIfExists) {
        return String.format(
                "CREATE DATABASE %s%s;", ignoreIfExists ? "IF NOT EXISTS " : "", databaseName);
    }

    private String buildCreateTableSql(OceanBaseTable table, boolean ignoreIfExists) {
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
                table.getTableType() == OceanBaseTable.TableType.PRIMARY_KEY,
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

    private String buildAlterAddColumnsSql(
            String databaseName,
            String tableName,
            List<OceanBaseColumn> addColumns,
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

    private String buildAlterDropColumnsSql(
            String databaseName, String tableName, List<String> dropColumns, long timeoutSecond) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("ALTER TABLE `%s`.`%s` ", databaseName, tableName));
        String columnsStmt =
                dropColumns.stream()
                        .map(col -> String.format("DROP COLUMN `%s`", col))
                        .collect(Collectors.joining(", "));
        builder.append(columnsStmt);
        builder.append(String.format(" PROPERTIES (\"timeout\" = \"%s\")", timeoutSecond));
        builder.append(";");
        return builder.toString();
    }

    private String buildColumnStmt(OceanBaseColumn column) {
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
            builder.append(String.format(" DEFAULT \"%s\"", column.getDefaultValue().get()));
        }

        if (column.getColumnComment().isPresent()) {
            builder.append(String.format(" COMMENT \"%s\"", column.getColumnComment().get()));
        }
        return builder.toString();
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
