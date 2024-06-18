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

package org.apache.flink.cdc.connectors.jdbc.catalog;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.jdbc.conn.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.jdbc.conn.JdbcConnectionPoolFactory;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** AbstractJdbcCatalog. */
public abstract class AbstractJdbcCatalog extends AbstractCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcCatalog.class);
    protected final JdbcConnectionFactory connectionFactory;

    public AbstractJdbcCatalog(
            String name, JdbcSinkConfig sinkConfig, JdbcConnectionPoolFactory poolFactory) {
        super(name);
        connectionFactory = new JdbcConnectionFactory(sinkConfig, poolFactory);
    }

    @Override
    public void close() throws CatalogException {}

    @Override
    public void createTable(TableId tableId, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkTableIdArguments(tableId);

        String createTableSql = buildCreateTableSql(tableId, schema, ignoreIfExists);

        try {
            executeUpdate(createTableSql);
            LOG.info(
                    "Success to create table {}.{}, sql: {}",
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    createTableSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to create table {}.{}, sql: {},reason:{}",
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    createTableSql,
                    e.getMessage());
        }
    }

    @Override
    public void addColumn(TableId tableId, AddColumnEvent addedColumns)
            throws TableNotExistException, CatalogException {
        checkTableIdArguments(tableId);

        Preconditions.checkArgument(
                !addedColumns.getAddedColumns().isEmpty(), "Added columns should not be empty.");

        String addColumnSql = buildAlterAddColumnsSql(tableId, addedColumns.getAddedColumns());
        try {
            executeUpdate(addColumnSql);
            LOG.info(
                    "Success to add column on table {}.{}, sql: {}",
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    addColumnSql);
        } catch (Exception e) {
            LOG.error(
                    "Success to add column on table {}.{}, sql: {},reason:{}",
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    addColumnSql,
                    e.getMessage());
        }
    }

    @Override
    public void dropColumn(TableId tableId, DropColumnEvent addedColumns)
            throws TableNotExistException, CatalogException {
        checkTableIdArguments(tableId);

        List<String> droppedColumnNames = addedColumns.getDroppedColumnNames();
        Preconditions.checkArgument(
                !droppedColumnNames.isEmpty(), "Dropped columns should not be empty.");

        droppedColumnNames.forEach(
                column -> {
                    String dropColumnSql = buildDropColumnSql(tableId, column);
                    try {
                        executeUpdate(dropColumnSql);
                        LOG.info(
                                "Success to drop column on table {}.{}, sql: {}",
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                dropColumnSql);
                    } catch (Exception e) {
                        LOG.error(
                                "Failed to drop column on table {}.{}, sql: {},reason:{}",
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                dropColumnSql,
                                e.getMessage());
                    }
                });
    }

    @Override
    public void renameColumn(TableId tableId, RenameColumnEvent addedColumns)
            throws TableNotExistException, CatalogException {
        checkTableIdArguments(tableId);

        Map<String, String> nameMapping = addedColumns.getNameMapping();
        for (Map.Entry<String, String> entry : nameMapping.entrySet()) {
            String renameColumnSql =
                    buildRenameColumnSql(tableId, entry.getKey(), entry.getValue());
            try {
                executeUpdate(renameColumnSql);
                LOG.info(
                        "Success to rename column table {}.{}, sql: {}",
                        tableId.getSchemaName(),
                        tableId.getTableName(),
                        renameColumnSql);
            } catch (Exception e) {
                LOG.error(
                        "Failed to rename column table {}.{}, sql: {},reason:{}",
                        tableId.getSchemaName(),
                        tableId.getTableName(),
                        renameColumnSql,
                        e.getMessage());
            }
        }
    }

    @Override
    public void alterColumnTyp(TableId tableId, AlterColumnTypeEvent alterColumns)
            throws TableNotExistException, CatalogException {
        checkTableIdArguments(tableId);

        Map<String, DataType> typeMapping = alterColumns.getTypeMapping();
        for (Map.Entry<String, DataType> entry : typeMapping.entrySet()) {
            String alterColumnTypeSql =
                    buildAlterColumnTypeSql(tableId, entry.getKey(), entry.getValue());

            try {
                executeUpdate(alterColumnTypeSql);
                LOG.info(
                        "Success to alert column table {}.{}, sql: {}",
                        tableId.getSchemaName(),
                        tableId.getTableName(),
                        alterColumnTypeSql);
            } catch (Exception e) {
                LOG.error(
                        "Failed to alert column table {}.{}, sql: {},reason:{}",
                        tableId.getSchemaName(),
                        tableId.getTableName(),
                        alterColumnTypeSql,
                        e.getMessage());
            }
        }
    }

    @Override
    public String getUpsertStatement(TableId tableId, Schema schema) {
        checkTableIdArguments(tableId);

        return buildUpsertSql(tableId, schema);
    }

    protected void checkTableIdArguments(TableId tableId) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableId.getSchemaName()),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableId.getTableName()),
                "table name cannot be null or empty.");
    }

    protected abstract String buildUpsertSql(TableId tableId, Schema schema);

    protected abstract String buildCreateTableSql(
            TableId tableId, Schema schema, boolean ignoreIfExists);

    protected abstract String buildAlterAddColumnsSql(
            TableId tableId, List<AddColumnEvent.ColumnWithPosition> addedColumns);

    protected abstract String buildRenameColumnSql(TableId tableId, String oldName, String newName);

    protected abstract String buildDropColumnSql(TableId tableId, String column);

    protected abstract String buildAlterColumnTypeSql(
            TableId tableId, String columnName, DataType columnType);

    protected void executeUpdate(String sql) throws SQLException {
        try (Connection connection = connectionFactory.connect();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    /** Process ResultSet objects. */
    @FunctionalInterface
    public interface ResultSetConsumer<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    public <T> Optional<List<T>> executeQuery(String sql, ResultSetConsumer<T> consumer)
            throws SQLException {
        try (Connection connection = connectionFactory.connect();
                PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {

            List<T> results = new ArrayList<>();
            while (rs.next()) {
                if (consumer != null) {
                    T value = consumer.apply(rs);
                    if (value != null) {
                        results.add(value);
                    }
                }
            }
            return results.isEmpty() ? Optional.empty() : Optional.of(results);
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed executeSql error %s", sql), e);
        }
    }
}
