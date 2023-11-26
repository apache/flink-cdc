/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.starrocks.sink.catalog;

import com.ververica.cdc.common.utils.Preconditions;
import com.ververica.cdc.common.utils.StringUtils;
import com.ververica.cdc.connectors.starrocks.sink.JdbcConfig;
import com.ververica.cdc.connectors.starrocks.sink.StarRocksColumn;
import com.ververica.cdc.connectors.starrocks.sink.StarRocksMetadataApplier;
import com.ververica.cdc.connectors.starrocks.sink.StarRocksTable;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Responsible for reading and writing metadata such as database/table from StarRocks. */
public class StarRocksCatalog implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksMetadataApplier.class);

    private final JdbcConfig jdbcConfig;

    public StarRocksCatalog(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
    }

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     * @throws StarRocksCatalogException in case of any runtime exception
     */
    public boolean databaseExists(String databaseName) throws StarRocksCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        List<String> dbList =
                executeSingleColumnStatement(
                        "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA` WHERE SCHEMA_NAME = ?;",
                        databaseName);
        return !dbList.isEmpty();
    }

    /**
     * Create a database.
     *
     * @param databaseName Name of the database
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *     exists: if set to false, throw a StarRocksCatalogException, if set to true, do nothing.
     * @throws StarRocksCatalogException in case of any runtime exception
     */
    public void createDatabase(String databaseName, boolean ignoreIfExists)
            throws StarRocksCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        try {
            String sql = buildCreateDatabaseSql(databaseName, ignoreIfExists);
            executeUpdateStatement(sql);
        } catch (SQLException e) {
            throw new StarRocksCatalogException(
                    String.format(
                            "Failed to create database %s, ignoreIfExists: %s",
                            databaseName, ignoreIfExists),
                    e);
        }
    }

    /**
     * Returns a {@link StarRocksTable} identified by the given databaseName and tableName.
     *
     * @param databaseName Name of the database
     * @param tableName Name of the table
     * @return an optional of the requested table. null if the table does not exist.
     * @throws StarRocksCatalogException in case of any runtime exception
     */
    public Optional<StarRocksTable> getTable(String databaseName, String tableName)
            throws StarRocksCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "table name cannot be null or empty.");

        final String tableSchemaQuery =
                "SELECT `COLUMN_NAME`, `DATA_TYPE`, `ORDINAL_POSITION`, `COLUMN_SIZE`, `DECIMAL_DIGITS`, "
                        + "`IS_NULLABLE`, `COLUMN_KEY`, `COLUMN_COMMENT` FROM `information_schema`.`COLUMNS` "
                        + "WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=?;";

        StarRocksTable.TableType tableType = StarRocksTable.TableType.UNKNOWN;
        List<StarRocksColumn> columns = new ArrayList<>();
        List<String> tableKeys = new ArrayList<>();
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(tableSchemaQuery)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String name = resultSet.getString("COLUMN_NAME");
                        String type = resultSet.getString("DATA_TYPE");
                        int position = resultSet.getInt("ORDINAL_POSITION");
                        Integer size = resultSet.getInt("COLUMN_SIZE");
                        if (resultSet.wasNull()) {
                            size = null;
                        }
                        Integer scale = resultSet.getInt("DECIMAL_DIGITS");
                        if (resultSet.wasNull()) {
                            scale = null;
                        }
                        String isNullable = resultSet.getString("IS_NULLABLE");
                        String comment = resultSet.getString("COLUMN_COMMENT");
                        StarRocksColumn column =
                                new StarRocksColumn.Builder()
                                        .setColumnName(name)
                                        .setOrdinalPosition(position - 1)
                                        .setDataType(type)
                                        .setColumnSize(size)
                                        .setDecimalDigits(scale)
                                        .setNullable(
                                                isNullable == null
                                                        || !isNullable.equalsIgnoreCase("NO"))
                                        .setColumnComment(comment)
                                        .build();
                        columns.add(column);

                        // Only primary key table has value in this field. and the value is "PRI"
                        String columnKey = resultSet.getString("COLUMN_KEY");
                        if (columnKey != null) {
                            tableType =
                                    columnKey.equalsIgnoreCase("PRI")
                                            ? StarRocksTable.TableType.PRIMARY_KEY
                                            : StarRocksTable.TableType.UNKNOWN;
                            tableKeys.add(columnKey);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new StarRocksCatalogException(
                    String.format("Failed to get table %s.%s", databaseName, tableName), e);
        }

        StarRocksTable starRocksTable = null;
        if (!columns.isEmpty()) {
            starRocksTable =
                    new StarRocksTable.Builder()
                            .setDatabaseName(databaseName)
                            .setTableName(tableName)
                            .setTableType(tableType)
                            .setColumns(columns)
                            .setTableKeys(tableKeys)
                            .build();
        }
        return Optional.ofNullable(starRocksTable);
    }

    /**
     * Creates a table.
     *
     * @param table the table definition
     * @param ignoreIfExists flag to specify behavior when a table already exists. if set to false,
     *     it throws a TableAlreadyExistException, if set to true, do nothing.
     * @throws StarRocksCatalogException in case of any runtime exception
     */
    public void createTable(StarRocksTable table, boolean ignoreIfExists)
            throws StarRocksCatalogException {
        String createTableSql = buildCreateTableSql(table, ignoreIfExists);
        try {
            executeUpdateStatement(createTableSql);
            LOG.info(
                    "Success to create table {}.{}",
                    table.getDatabaseName(),
                    table.getDatabaseName());
            LOG.info("The create table sql:\n{}", createTableSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to create table {}.{}",
                    table.getDatabaseName(),
                    table.getDatabaseName());
            LOG.error("The failed create table sql:\n{}", createTableSql);
            throw new StarRocksCatalogException(
                    String.format(
                            "Failed to create table %s.%s ",
                            table.getDatabaseName(), table.getDatabaseName()),
                    e);
        }
    }

    /**
     * Add columns to a table. Note that those columns will be added to the last position.
     *
     * @param databaseName Name of the database
     * @param tableName Name of the table
     * @param addColumns Columns to add
     * @param timeoutSecond Timeout for a schema change on StarRocks side
     * @throws StarRocksCatalogException in case of any runtime exception
     */
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
            executeAlter(databaseName, tableName, alterSql, timeoutSecond);
            LOG.info("Success to add columns {}.{}", databaseName, tableName);
            LOG.info("The add columns sql:\n{}", alterSql);
        } catch (Exception e) {
            LOG.info("Failed to add columns {}.{}", databaseName, tableName);
            LOG.error("The failed add columns sql:\n{}", alterSql);
            throw new StarRocksCatalogException(
                    String.format("Failed to add columns %s.%s ", databaseName, tableName), e);
        }
    }

    /**
     * Drop columns of a table.
     *
     * @param databaseName Name of the database
     * @param tableName Name of the table
     * @param dropColumns Columns to drop
     * @param timeoutSecond Timeout for a schema change on StarRocks side
     * @throws StarRocksCatalogException in case of any runtime exception
     */
    public void alterDropColumns(
            String databaseName, String tableName, List<String> dropColumns, long timeoutSecond)
            throws StarRocksCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "table name cannot be null or empty.");
        Preconditions.checkArgument(!dropColumns.isEmpty(), "Drop columns should not be empty.");

        String alterSql =
                buildAlterDropColumnsSql(databaseName, tableName, dropColumns, timeoutSecond);
        try {
            executeAlter(databaseName, tableName, alterSql, timeoutSecond);
            LOG.info("Success to drop columns {}.{}", databaseName, tableName);
            LOG.info("The drop columns sql:\n{}", alterSql);
        } catch (Exception e) {
            LOG.info("Failed to drop columns {}.{}", databaseName, tableName);
            LOG.error("The failed drop columns sql:\n{}", alterSql);
            throw new StarRocksCatalogException(
                    String.format("Failed to drop columns %s.%s ", databaseName, tableName), e);
        }
    }

    private void executeAlter(
            String databaseName, String tableName, String alterSql, long timeoutSecond)
            throws StarRocksCatalogException {
        try {
            executeUpdateStatement(alterSql);
        } catch (SQLException e) {
            throw new StarRocksCatalogException(
                    String.format("Failed to execute alter sql for %s.%s", databaseName, tableName),
                    e);
        }

        // Alter job may be executed asynchronously, so check the job state periodically before
        // timeout
        long startTime = System.currentTimeMillis();
        int retries = 0;
        while (System.currentTimeMillis() - startTime < timeoutSecond * 1000) {
            try {
                AlterJobState jobState = getAlterJobState(databaseName, tableName);
                retries = 0;
                if ("CANCELLED".equalsIgnoreCase(jobState.state)) {
                    throw new StarRocksCatalogException(
                            "Alter job is cancelled, error msg: " + jobState.msg);
                }
                LOG.info("Get alter job state for {}.{}, {}", databaseName, tableName, jobState);
            } catch (Exception e) {
                LOG.warn("Failed to get alter job state for {}.{}", databaseName, tableName, e);
                retries += 1;
                if (retries > 3) {
                    throw new StarRocksCatalogException(
                            String.format(
                                    "Failed to get alter job state for %s.%s",
                                    databaseName, tableName),
                            e);
                }
            }

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new StarRocksCatalogException(
                        String.format(
                                "Failed to get alter job state for %s.%s because of interruption ",
                                databaseName, tableName));
            }
        }
    }

    private static class AlterJobState {
        String jobId;
        String tableName;
        String createTime;
        String finishTime;
        String transactionId;
        String state;
        String msg;

        @Override
        public String toString() {
            return "AlterJobState{"
                    + "jobId='"
                    + jobId
                    + '\''
                    + ", tableName='"
                    + tableName
                    + '\''
                    + ", createTime='"
                    + createTime
                    + '\''
                    + ", finishTime='"
                    + finishTime
                    + '\''
                    + ", transactionId='"
                    + transactionId
                    + '\''
                    + ", state='"
                    + state
                    + '\''
                    + ", msg='"
                    + msg
                    + '\''
                    + '}';
        }

        public AlterJobState(
                String jobId,
                String tableName,
                String createTime,
                String finishTime,
                String transactionId,
                String state,
                String msg) {
            this.jobId = jobId;
            this.tableName = tableName;
            this.createTime = createTime;
            this.finishTime = finishTime;
            this.transactionId = transactionId;
            this.state = state;
            this.msg = msg;
        }
    }

    private AlterJobState getAlterJobState(String databaseName, String tableName)
            throws SQLException {
        String showAlterSql =
                "SHOW ALTER TABLE COLUMN FROM ? WHERE TableName = ? ORDER BY JobId DESC LIMIT 1;";
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(showAlterSql)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        return new AlterJobState(
                                resultSet.getString("JobId"),
                                resultSet.getString("TableName"),
                                resultSet.getString("CreateTime"),
                                resultSet.getString("FinishTime"),
                                resultSet.getString("TransactionId"),
                                resultSet.getString("State"),
                                resultSet.getString("Msg"));
                    }
                }
            }
        }
        throw new SQLException(
                String.format("Alter job state for %s.%s does not exsit", databaseName, tableName));
    }

    private List<String> executeSingleColumnStatement(String sql, Object... params) {
        try (Connection conn = getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            List<String> columnValues = Lists.newArrayList();
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String columnValue = rs.getString(1);
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new StarRocksCatalogException(String.format("Failed to execute sql: %s", sql), e);
        }
    }

    private void executeUpdateStatement(String sql) throws SQLException {
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                jdbcConfig.getJdbcUrl(), jdbcConfig.getUsername(), jdbcConfig.getPassword());
    }

    // ------------------------------------------------------------------------------------------
    // StarRocks DDL SQL
    // ------------------------------------------------------------------------------------------

    private String buildCreateDatabaseSql(String databaseName, boolean ignoreIfExists) {
        return String.format(
                "CREATE DATABASE %s%s;", ignoreIfExists ? "IF NOT EXISTS " : "", databaseName);
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
                table.getTableType() != StarRocksTable.TableType.PRIMARY_KEY,
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

        if (column.getColumnComment().isPresent()) {
            builder.append(" COMMENT \"");
            builder.append(column.getColumnComment().get());
            builder.append("\"");
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
