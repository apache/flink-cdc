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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** An enriched {@code StarRocksCatalog} with more schema evolution abilities. */
public class StarRocksEnrichedCatalog extends StarRocksCatalog {
    public StarRocksEnrichedCatalog(String jdbcUrl, String username, String password) {
        super(jdbcUrl, username, password);
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksEnrichedCatalog.class);

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
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new StarRocksCatalogException("Failed to execute SQL statement", e);
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

    public void alterColumnPosition(
            String databaseName,
            String tableName,
            StarRocksColumn column,
            boolean isKey,
            String precedingColumn,
            long timeoutSecond) {
        checkTableArgument(databaseName, tableName);
        // The upstream catalog does not load COLUMN_DEFAULT. Preserve it when restating the column.
        StarRocksColumn completeColumn = loadColumnDefault(databaseName, tableName, column);
        String sql =
                buildAlterColumnPositionSql(
                        databaseName, tableName, completeColumn, isKey, precedingColumn);
        String previousJobId = getLastColumnAlterJobId(databaseName, tableName);
        executeUpdateStatement(sql);
        waitForColumnReorder(databaseName, tableName, previousJobId, timeoutSecond);
    }

    private void waitForColumnReorder(
            String databaseName, String tableName, String previousJobId, long timeoutSecond) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSecond);
        String sql =
                "SHOW ALTER TABLE COLUMN FROM "
                        + quoteIdentifier(databaseName)
                        + " WHERE TableName = '"
                        + escapeLiteral(tableName)
                        + "' ORDER BY JobId DESC LIMIT 1";
        while (System.nanoTime() < deadline) {
            try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                    Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery(sql)) {
                if (result.next() && !result.getString("JobId").equals(previousJobId)) {
                    String state = result.getString("State");
                    if ("FINISHED".equalsIgnoreCase(state)) {
                        return;
                    }
                    if ("CANCELLED".equalsIgnoreCase(state)) {
                        throw new StarRocksCatalogException(
                                "Column reorder was cancelled: " + result.getString("Msg"));
                    }
                }
            } catch (SQLException e) {
                throw new StarRocksCatalogException("Failed to query column reorder state", e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StarRocksCatalogException(
                        "Interrupted while waiting for column reorder", e);
            }
        }
        throw new StarRocksCatalogException("Column reorder did not finish before timeout");
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static String escapeLiteral(String value) {
        return value.replace("\\", "\\\\").replace("'", "''");
    }

    private static String formatDefaultValue(String value, String dataType) {
        if ("DATETIME".equalsIgnoreCase(dataType)
                && ("CURRENT_TIMESTAMP".equalsIgnoreCase(value)
                        || "CURRENT_TIMESTAMP()".equalsIgnoreCase(value))) {
            return value;
        }
        return "'" + escapeLiteral(value) + "'";
    }

    private String getLastColumnAlterJobId(String databaseName, String tableName) {
        String sql =
                "SHOW ALTER TABLE COLUMN FROM "
                        + quoteIdentifier(databaseName)
                        + " WHERE TableName = '"
                        + escapeLiteral(tableName)
                        + "' ORDER BY JobId DESC LIMIT 1";
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            return result.next() ? result.getString("JobId") : null;
        } catch (SQLException e) {
            throw new StarRocksCatalogException("Failed to query last column alter job", e);
        }
    }

    private StarRocksColumn loadColumnDefault(
            String databaseName, String tableName, StarRocksColumn column) {
        String sql =
                "SELECT COLUMN_DEFAULT FROM information_schema.COLUMNS "
                        + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, databaseName);
            statement.setString(2, tableName);
            statement.setString(3, column.getColumnName());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new StarRocksCatalogException("Column to reorder no longer exists");
                }
                return new StarRocksColumn.Builder()
                        .setColumnName(column.getColumnName())
                        .setOrdinalPosition(column.getOrdinalPosition())
                        .setDataType(column.getDataType())
                        .setColumnSize(column.getColumnSize().orElse(null))
                        .setDecimalDigits(column.getDecimalDigits().orElse(null))
                        .setNullable(column.isNullable())
                        .setColumnComment(column.getColumnComment().orElse(null))
                        .setDefaultValue(result.getString("COLUMN_DEFAULT"))
                        .build();
            }
        } catch (SQLException e) {
            throw new StarRocksCatalogException(
                    "Failed to load column default before reordering", e);
        }
    }

    String buildAlterColumnPositionSql(
            String databaseName,
            String tableName,
            StarRocksColumn column,
            boolean isKey,
            String precedingColumn) {
        String type =
                getFullColumnType(
                        column.getDataType(), column.getColumnSize(), column.getDecimalDigits());
        StringBuilder definition = new StringBuilder(type);
        if (isKey) {
            definition.append(" KEY");
        }
        definition.append(column.isNullable() ? " NULL" : " NOT NULL");
        column.getDefaultValue()
                .ifPresent(
                        value ->
                                definition
                                        .append(" DEFAULT ")
                                        .append(formatDefaultValue(value, column.getDataType())));
        column.getColumnComment()
                .ifPresent(
                        comment ->
                                definition
                                        .append(" COMMENT '")
                                        .append(escapeLiteral(comment))
                                        .append("'"));
        return "ALTER TABLE "
                + quoteIdentifier(databaseName)
                + "."
                + quoteIdentifier(tableName)
                + " MODIFY COLUMN "
                + quoteIdentifier(column.getColumnName())
                + " "
                + definition
                + (precedingColumn == null
                        ? " FIRST"
                        : " AFTER " + quoteIdentifier(precedingColumn));
    }
}
