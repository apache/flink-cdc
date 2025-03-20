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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.jdbc.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.jdbc.connection.JdbcConnectionPoolFactory;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/** An abstract base class for various JDBC-like sinks. */
public abstract class JdbcCatalog implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);

    private final String catalogName;

    protected final JdbcConnectionFactory connectionFactory;

    public JdbcCatalog(
            String catalogName, JdbcSinkConfig sinkConfig, JdbcConnectionPoolFactory poolFactory) {
        this.catalogName = catalogName;
        this.connectionFactory = new JdbcConnectionFactory(sinkConfig, poolFactory);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public abstract void close() throws CatalogException;

    public void createTable(CreateTableEvent createTableEvent, boolean ignoreIfExists)
            throws CatalogException {
        TableId tableId = createTableEvent.tableId();
        checkTableIdArguments(tableId);

        Schema schema = createTableEvent.getSchema();
        String createTableSql = buildCreateTableSql(tableId, schema, ignoreIfExists);

        try {
            executeUpdate(createTableSql);
            LOG.info(
                    "Successfully created table `{}`.`{}`. Raw SQL: {}",
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    createTableSql);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to create table `%s`.`%s`. Raw SQL: %s",
                            tableId.getSchemaName(), tableId.getTableName(), createTableSql),
                    e);
        }
    }

    public void addColumn(AddColumnEvent addColumnEvent) throws CatalogException {
        TableId tableId = addColumnEvent.tableId();
        checkTableIdArguments(tableId);

        Preconditions.checkArgument(
                !addColumnEvent.getAddedColumns().isEmpty(), "Added columns should not be empty.");

        String addColumnSql = buildAlterAddColumnsSql(tableId, addColumnEvent.getAddedColumns());
        try {
            executeUpdate(addColumnSql);
            LOG.info(
                    "Successfully added column in table `{}`.`{}`. Raw SQL: {}",
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    addColumnSql);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to add column in table `%s`.`%s`. Raw SQL: %s",
                            tableId.getSchemaName(), tableId.getTableName(), addColumnSql),
                    e);
        }
    }

    public void dropColumn(DropColumnEvent dropColumnEvent) throws CatalogException {
        TableId tableId = dropColumnEvent.tableId();
        checkTableIdArguments(tableId);

        List<String> droppedColumnNames = dropColumnEvent.getDroppedColumnNames();
        Preconditions.checkArgument(
                !droppedColumnNames.isEmpty(), "Dropped columns should not be empty.");

        droppedColumnNames.forEach(
                column -> {
                    String dropColumnSql = buildDropColumnSql(tableId, column);
                    try {
                        executeUpdate(dropColumnSql);
                        LOG.info(
                                "Successfully dropped column in table `{}`.`{}`. Raw SQL: {}",
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                dropColumnSql);
                    } catch (Exception e) {
                        throw new CatalogException(
                                String.format(
                                        "Failed to drop column in table `%s`.`%s`. Raw SQL: %s",
                                        tableId.getSchemaName(),
                                        tableId.getTableName(),
                                        dropColumnSql),
                                e);
                    }
                });
    }

    public void renameColumn(RenameColumnEvent renameColumnEvent) throws CatalogException {
        TableId tableId = renameColumnEvent.tableId();
        checkTableIdArguments(tableId);

        Map<String, String> nameMapping = renameColumnEvent.getNameMapping();
        for (Map.Entry<String, String> entry : nameMapping.entrySet()) {
            String renameColumnSql =
                    buildRenameColumnSql(tableId, entry.getKey(), entry.getValue());
            try {
                executeUpdate(renameColumnSql);
                LOG.info(
                        "Successfully renamed column in table `{}`.`{}`. Raw SQL: {}",
                        tableId.getSchemaName(),
                        tableId.getTableName(),
                        renameColumnSql);
            } catch (Exception e) {
                throw new CatalogException(
                        String.format(
                                "Failed to rename column in table `%s`.`%s`. Raw SQL: %s",
                                tableId.getSchemaName(), tableId.getTableName(), renameColumnSql),
                        e);
            }
        }
    }

    public void alterColumnType(AlterColumnTypeEvent alterColumnTypeEvent) throws CatalogException {
        TableId tableId = alterColumnTypeEvent.tableId();
        checkTableIdArguments(tableId);

        Map<String, DataType> typeMapping = alterColumnTypeEvent.getTypeMapping();
        for (Map.Entry<String, DataType> entry : typeMapping.entrySet()) {
            String alterColumnTypeSql =
                    buildAlterColumnTypeSql(tableId, entry.getKey(), entry.getValue());

            try {
                executeUpdate(alterColumnTypeSql);
                LOG.info(
                        "Successfully altered column type in table `{}`.`{}`. Raw SQL: {}",
                        tableId.getSchemaName(),
                        tableId.getTableName(),
                        alterColumnTypeSql);
            } catch (Exception e) {
                throw new CatalogException(
                        String.format(
                                "Failed to alter column type in table `%s`.`%s`. Raw SQL: %s",
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                alterColumnTypeSql),
                        e);
            }
        }
    }

    public void truncateTable(TruncateTableEvent truncateTableEvent) throws CatalogException {
        TableId tableId = truncateTableEvent.tableId();
        checkTableIdArguments(tableId);

        String truncateTableSql = buildTruncateTableSql(tableId);

        try {
            executeUpdate(truncateTableSql);
            LOG.info(
                    "Successfully truncated table `{}`.`{}`. Raw SQL: {}",
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    truncateTableSql);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to truncate table `%s`.`%s`. Raw SQL: %s",
                            tableId.getSchemaName(), tableId.getTableName(), truncateTableSql),
                    e);
        }
    }

    public void dropTable(DropTableEvent dropTableEvent, boolean ignoreIfNotExist)
            throws CatalogException {
        TableId tableId = dropTableEvent.tableId();
        checkTableIdArguments(tableId);

        String dropTableSql = buildDropTableSql(tableId, ignoreIfNotExist);

        try {
            executeUpdate(dropTableSql);
            LOG.info(
                    "Successfully dropped table `{}`.`{}`. Raw SQL: {}",
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    dropTableSql);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format(
                            "Failed to drop table `%s`.`%s`. Raw SQL: %s",
                            tableId.getSchemaName(), tableId.getTableName(), dropTableSql),
                    e);
        }
    }

    public String getUpsertStatement(TableId tableId, Schema schema) {
        checkTableIdArguments(tableId);
        return buildUpsertSql(tableId, schema);
    }

    public String getDeleteStatement(TableId tableId, List<String> primaryKeys) {
        checkTableIdArguments(tableId);
        return buildDeleteSql(tableId, primaryKeys);
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

    protected abstract String buildDeleteSql(TableId tableId, List<String> primaryKeys);

    protected abstract String buildCreateTableSql(
            TableId tableId, Schema schema, boolean ignoreIfExists);

    protected abstract String buildAlterAddColumnsSql(
            TableId tableId, List<AddColumnEvent.ColumnWithPosition> addedColumns);

    protected abstract String buildRenameColumnSql(TableId tableId, String oldName, String newName);

    protected abstract String buildDropColumnSql(TableId tableId, String column);

    protected abstract String buildAlterColumnTypeSql(
            TableId tableId, String columnName, DataType columnType);

    protected abstract String buildTruncateTableSql(TableId tableId);

    protected abstract String buildDropTableSql(TableId tableId, boolean ignoreIfNotExist);

    protected void executeUpdate(String sql) throws SQLException {
        try (Connection connection = connectionFactory.connect();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }
}
