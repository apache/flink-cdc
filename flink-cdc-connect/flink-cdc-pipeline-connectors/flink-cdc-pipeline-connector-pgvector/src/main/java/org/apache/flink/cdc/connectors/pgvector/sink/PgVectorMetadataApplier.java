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

package org.apache.flink.cdc.connectors.pgvector.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.AlterTableCommentEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.pgvector.utils.PgVectorSqlUtils;
import org.apache.flink.cdc.connectors.pgvector.utils.PgVectorTableInfo;
import org.apache.flink.cdc.connectors.pgvector.utils.PgVectorTypeUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.TRUNCATE_TABLE;

/** Applies schema changes to PostgreSQL pgvector tables. */
public class PgVectorMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(PgVectorMetadataApplier.class);

    private final PgVectorDataSinkConfig config;
    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    public PgVectorMetadataApplier(PgVectorDataSinkConfig config) {
        this.config = config;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Sets.newHashSet(
                CREATE_TABLE,
                ADD_COLUMN,
                ALTER_COLUMN_TYPE,
                DROP_COLUMN,
                DROP_TABLE,
                RENAME_COLUMN,
                TRUNCATE_TABLE);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (!acceptsSchemaEvolutionType(schemaChangeEvent.getType())) {
            return;
        }
        try {
            SchemaChangeEventVisitor.<Void, Exception>visit(
                    schemaChangeEvent,
                    addColumnEvent -> {
                        applyAddColumn(addColumnEvent);
                        return null;
                    },
                    alterColumnTypeEvent -> {
                        applyAlterColumnType(alterColumnTypeEvent);
                        return null;
                    },
                    createTableEvent -> {
                        applyCreateTable(createTableEvent);
                        return null;
                    },
                    dropColumnEvent -> {
                        applyDropColumn(dropColumnEvent);
                        return null;
                    },
                    dropTableEvent -> {
                        applyDropTable(dropTableEvent);
                        return null;
                    },
                    renameColumnEvent -> {
                        applyRenameColumn(renameColumnEvent);
                        return null;
                    },
                    truncateTableEvent -> {
                        applyTruncateTable(truncateTableEvent);
                        return null;
                    },
                    alterTableCommentEvent -> {
                        applyAlterTableComment(alterTableCommentEvent);
                        return null;
                    });
        } catch (Exception e) {
            throw new SchemaEvolveException(schemaChangeEvent, e.getMessage(), e);
        }
    }

    private void applyCreateTable(CreateTableEvent event) throws SQLException {
        PgVectorTableInfo tableInfo =
                PgVectorSqlUtils.resolveTableInfo(event.tableId(), config.getDefaultSchema());
        executeInTransaction(
                connection -> {
                    try (Statement statement = connection.createStatement()) {
                        if (config.isCreateExtensionEnabled()) {
                            statement.execute("CREATE EXTENSION IF NOT EXISTS vector");
                        }
                        if (config.isCreateSchemaEnabled()) {
                            statement.execute(PgVectorSqlUtils.createSchemaSql(tableInfo));
                        }
                        if (config.isCreateTableEnabled()) {
                            statement.execute(
                                    PgVectorSqlUtils.createTableSql(
                                            tableInfo,
                                            event.getSchema(),
                                            config.getVectorColumns(),
                                            config.getTableCreateProperties()));
                        }
                    }
                });
    }

    private void applyAddColumn(AddColumnEvent event) throws SQLException {
        PgVectorTableInfo tableInfo =
                PgVectorSqlUtils.resolveTableInfo(event.tableId(), config.getDefaultSchema());
        executeInTransaction(
                connection -> {
                    try (Statement statement = connection.createStatement()) {
                        for (AddColumnEvent.ColumnWithPosition columnWithPosition :
                                event.getAddedColumns()) {
                            statement.execute(
                                    PgVectorSqlUtils.addColumnSql(
                                            tableInfo,
                                            columnWithPosition.getAddColumn(),
                                            config.getVectorColumns()));
                        }
                    }
                });
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event) throws SQLException {
        PgVectorTableInfo tableInfo =
                PgVectorSqlUtils.resolveTableInfo(event.tableId(), config.getDefaultSchema());
        executeInTransaction(
                connection -> {
                    try (Statement statement = connection.createStatement()) {
                        for (Map.Entry<String, DataType> typeChange :
                                event.getTypeMapping().entrySet()) {
                            String postgresType =
                                    PgVectorTypeUtils.findVectorColumnSpec(
                                                    tableInfo,
                                                    typeChange.getKey(),
                                                    config.getVectorColumns())
                                            .map(spec -> spec.toSqlType())
                                            .orElseGet(
                                                    () ->
                                                            PgVectorTypeUtils.toPostgresType(
                                                                    typeChange.getValue()));
                            statement.execute(
                                    PgVectorSqlUtils.alterColumnTypeSql(
                                            tableInfo, typeChange.getKey(), postgresType));
                        }
                    }
                });
    }

    private void applyDropColumn(DropColumnEvent event) throws SQLException {
        PgVectorTableInfo tableInfo =
                PgVectorSqlUtils.resolveTableInfo(event.tableId(), config.getDefaultSchema());
        executeInTransaction(
                connection -> {
                    try (Statement statement = connection.createStatement()) {
                        for (String columnName : event.getDroppedColumnNames()) {
                            statement.execute(
                                    PgVectorSqlUtils.dropColumnSql(tableInfo, columnName));
                        }
                    }
                });
    }

    private void applyRenameColumn(RenameColumnEvent event) throws SQLException {
        PgVectorTableInfo tableInfo =
                PgVectorSqlUtils.resolveTableInfo(event.tableId(), config.getDefaultSchema());
        executeInTransaction(
                connection -> {
                    try (Statement statement = connection.createStatement()) {
                        for (Map.Entry<String, String> rename : event.getNameMapping().entrySet()) {
                            statement.execute(
                                    PgVectorSqlUtils.renameColumnSql(
                                            tableInfo, rename.getKey(), rename.getValue()));
                        }
                    }
                });
    }

    private void applyTruncateTable(TruncateTableEvent event) throws SQLException {
        executeTableSql(PgVectorSqlUtils.truncateTableSql(resolve(event)));
    }

    private void applyDropTable(DropTableEvent event) throws SQLException {
        executeTableSql(PgVectorSqlUtils.dropTableSql(resolve(event)));
    }

    private void applyAlterTableComment(AlterTableCommentEvent event) {
        LOG.debug("Ignoring table comment change for pgvector sink: {}", event);
    }

    private void executeTableSql(String sql) throws SQLException {
        executeInTransaction(
                connection -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.execute(sql);
                    }
                });
    }

    private void executeInTransaction(SqlCallback callback) throws SQLException {
        try (Connection connection = openConnection()) {
            connection.setAutoCommit(false);
            try {
                callback.run(connection);
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        }
    }

    private PgVectorTableInfo resolve(SchemaChangeEvent event) {
        return PgVectorSqlUtils.resolveTableInfo(event.tableId(), config.getDefaultSchema());
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(
                config.getJdbcUrl(), config.getUsername(), config.getPassword());
    }

    @FunctionalInterface
    private interface SqlCallback {
        void run(Connection connection) throws SQLException;
    }
}
