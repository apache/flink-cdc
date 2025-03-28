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

package org.apache.flink.cdc.connectors.jdbc.dialect;

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
import org.apache.flink.cdc.connectors.jdbc.exception.JdbcSinkDialectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/** An abstract base class for various JDBC-like sinks. */
public abstract class JdbcSinkDialect implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSinkDialect.class);

    private final String dialectName;
    protected final JdbcConnectionFactory connectionFactory;

    public JdbcSinkDialect(String dialectName, JdbcSinkConfig sinkConfig) {
        this.dialectName = dialectName;
        this.connectionFactory = new JdbcConnectionFactory(sinkConfig);
    }

    /** Returns a unique identifier of this dialect. */
    public String getDialectName() {
        return dialectName;
    }

    // Rewritable methods that could be overwritten by subclasses.

    /** Creates a single row of SQL that upserts into {@code tableId} with given {@code schema}. */
    protected abstract String buildUpsertSql(TableId tableId, Schema schema);

    /** Creates a single row of SQL that deletes from {@code tableId} with given {@code schema}. */
    protected abstract String buildDeleteSql(TableId tableId, Schema schema);

    /**
     * Creates a single row of SQL that creates a table with {@code tableId} with given {@code
     * schema}.
     */
    protected abstract String buildCreateTableSql(
            TableId tableId, Schema schema, boolean ignoreIfExists);

    /**
     * Creates a single row of SQL that adds multiple columns like {@code addedColumns} into table
     * {@code tableId}.
     */
    protected abstract String buildAlterAddColumnsSql(
            TableId tableId, List<AddColumnEvent.ColumnWithPosition> addedColumns);

    /**
     * Creates a single row of SQL that renames column {@code oldName} to {@code newName} in table
     * {@code tableId}.
     */
    protected abstract String buildRenameColumnSql(TableId tableId, String oldName, String newName);

    /**
     * Creates a single row of SQL that drops specific column with name {@code column} in table
     * {@code tableId}.
     */
    protected abstract String buildDropColumnSql(TableId tableId, String column);

    /**
     * Creates a single row of SQL that alters a specific column {@code columnName}'type to {@code
     * columnType} in table {@code tableId}.
     */
    protected abstract String buildAlterColumnTypeSql(
            TableId tableId, String columnName, DataType columnType);

    /** Creates a single row of SQL that truncates given table {@code tableId}. */
    protected abstract String buildTruncateTableSql(TableId tableId);

    /** Creates a single row of SQL that drops given table {@code tableId}. */
    protected abstract String buildDropTableSql(TableId tableId, boolean ignoreIfNotExist);

    // Final methods that are not meant to be overwritten.
    public final void createTable(CreateTableEvent createTableEvent, boolean ignoreIfExists)
            throws JdbcSinkDialectException {
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
            throw new JdbcSinkDialectException(
                    String.format(
                            "Failed to create table `%s`.`%s`. Raw SQL: %s",
                            tableId.getSchemaName(), tableId.getTableName(), createTableSql),
                    e);
        }
    }

    public final void addColumn(AddColumnEvent addColumnEvent) throws JdbcSinkDialectException {
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
            throw new JdbcSinkDialectException(
                    String.format(
                            "Failed to add column in table `%s`.`%s`. Raw SQL: %s",
                            tableId.getSchemaName(), tableId.getTableName(), addColumnSql),
                    e);
        }
    }

    public final void dropColumn(DropColumnEvent dropColumnEvent) throws JdbcSinkDialectException {
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
                        throw new JdbcSinkDialectException(
                                String.format(
                                        "Failed to drop column in table `%s`.`%s`. Raw SQL: %s",
                                        tableId.getSchemaName(),
                                        tableId.getTableName(),
                                        dropColumnSql),
                                e);
                    }
                });
    }

    public final void renameColumn(RenameColumnEvent renameColumnEvent)
            throws JdbcSinkDialectException {
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
                throw new JdbcSinkDialectException(
                        String.format(
                                "Failed to rename column in table `%s`.`%s`. Raw SQL: %s",
                                tableId.getSchemaName(), tableId.getTableName(), renameColumnSql),
                        e);
            }
        }
    }

    public final void alterColumnType(AlterColumnTypeEvent alterColumnTypeEvent)
            throws JdbcSinkDialectException {
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
                throw new JdbcSinkDialectException(
                        String.format(
                                "Failed to alter column type in table `%s`.`%s`. Raw SQL: %s",
                                tableId.getSchemaName(),
                                tableId.getTableName(),
                                alterColumnTypeSql),
                        e);
            }
        }
    }

    public final void truncateTable(TruncateTableEvent truncateTableEvent)
            throws JdbcSinkDialectException {
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
            throw new JdbcSinkDialectException(
                    String.format(
                            "Failed to truncate table `%s`.`%s`. Raw SQL: %s",
                            tableId.getSchemaName(), tableId.getTableName(), truncateTableSql),
                    e);
        }
    }

    public final void dropTable(DropTableEvent dropTableEvent, boolean ignoreIfNotExist)
            throws JdbcSinkDialectException {
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
            throw new JdbcSinkDialectException(
                    String.format(
                            "Failed to drop table `%s`.`%s`. Raw SQL: %s",
                            tableId.getSchemaName(), tableId.getTableName(), dropTableSql),
                    e);
        }
    }

    public final String getUpsertStatement(TableId tableId, Schema schema) {
        checkTableIdArguments(tableId);
        return buildUpsertSql(tableId, schema);
    }

    public final String getDeleteStatement(TableId tableId, Schema schema) {
        checkTableIdArguments(tableId);
        return buildDeleteSql(tableId, schema);
    }

    protected final void checkTableIdArguments(TableId tableId) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableId.getSchemaName()),
                "database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableId.getTableName()),
                "table name cannot be null or empty.");
    }

    protected final void executeUpdate(String sql) throws SQLException {
        try (Connection connection = connectionFactory.connect();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }
}
