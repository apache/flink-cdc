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

package org.apache.flink.cdc.connectors.db2.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class Db2SchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(Db2SchemaUtils.class);

    private static final String LIST_SCHEMAS_SQL = "SELECT SCHEMANAME FROM SYSCAT.SCHEMATA";

    private static final String LIST_TABLES_SQL =
            "SELECT TABSCHEMA, TABNAME FROM SYSCAT.TABLES WHERE TYPE = 'T'";

    /** List all schemas of the database in the given {@link Db2SourceConfig}. */
    public static List<String> listSchemas(Db2SourceConfig sourceConfig) {
        try (JdbcConnection jdbc = createDb2Connection(sourceConfig)) {
            return listSchemas(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list schemas: " + e.getMessage(), e);
        }
    }

    public static List<String> listSchemas(JdbcConnection jdbc) throws SQLException {
        LOG.info("Read list of available schemas");
        final List<String> schemaNames = new ArrayList<>();
        jdbc.query(
                LIST_SCHEMAS_SQL,
                rs -> {
                    while (rs.next()) {
                        schemaNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available schemas are: {}", schemaNames);
        return schemaNames;
    }

    /**
     * List all tables of the database in the given {@link Db2SourceConfig}.
     *
     * @param sourceConfig The source configuration.
     * @param schemaName The schema to list tables from. If null, list tables from all schemas.
     * @return The list of {@link TableId}s in the format of "schema.table".
     */
    public static List<TableId> listTables(
            Db2SourceConfig sourceConfig, @Nullable String schemaName) {
        try (JdbcConnection jdbc = createDb2Connection(sourceConfig)) {
            return listTables(jdbc, schemaName);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list tables: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(JdbcConnection jdbc, @Nullable String schemaName)
            throws SQLException {
        LOG.info("Read list of available tables");
        final List<TableId> tableIds = new ArrayList<>();
        String querySql =
                schemaName == null
                        ? LIST_TABLES_SQL
                        : LIST_TABLES_SQL + " AND TABSCHEMA = '" + schemaName + "'";
        jdbc.query(
                querySql,
                rs -> {
                    while (rs.next()) {
                        tableIds.add(TableId.tableId(rs.getString(1), rs.getString(2)));
                    }
                });
        LOG.info("\t list of available tables are: {}", tableIds);
        return tableIds;
    }

    /** Get the {@link Schema} of the given table. */
    public static Schema getTableSchema(Db2SourceConfig sourceConfig, TableId tableId) {
        Db2Dialect dialect = new Db2Dialect(sourceConfig);
        try (JdbcConnection jdbc = createDb2Connection(sourceConfig)) {
            return getTableSchema(tableId, jdbc, dialect);
        } catch (SQLException e) {
            throw new RuntimeException("Error to get table schema: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(TableId tableId, JdbcConnection jdbc, Db2Dialect dialect) {
        try {
            TableChange tableChange = dialect.queryTableSchema(jdbc, toDbzTableId(tableId, jdbc));
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
                table.columns().stream().map(Db2SchemaUtils::toColumn).collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        if (column.defaultValueExpression().isPresent()) {
            return Column.physicalColumn(
                    column.name(),
                    Db2TypeUtils.fromDbzColumn(column),
                    column.comment(),
                    column.defaultValueExpression().get());
        } else {
            return Column.physicalColumn(
                    column.name(), Db2TypeUtils.fromDbzColumn(column), column.comment());
        }
    }

    /**
     * Converts a pipeline {@link TableId} to a debezium {@link io.debezium.relational.TableId}. The
     * pipeline TableId of Db2 is in the format of "schema.table", while the catalog of the debezium
     * TableId is the database name.
     */
    public static io.debezium.relational.TableId toDbzTableId(
            TableId tableId, String databaseName) {
        return new io.debezium.relational.TableId(
                databaseName, tableId.getSchemaName(), tableId.getTableName());
    }

    private static io.debezium.relational.TableId toDbzTableId(
            TableId tableId, JdbcConnection jdbc) {
        String databaseName =
                ((io.debezium.connector.db2.Db2Connection) jdbc).getRealDatabaseName();
        return toDbzTableId(tableId, databaseName);
    }

    /** Converts a debezium {@link io.debezium.relational.TableId} to a pipeline {@link TableId}. */
    public static TableId toCdcTableId(io.debezium.relational.TableId dbzTableId) {
        return TableId.tableId(dbzTableId.schema(), dbzTableId.table());
    }

    public static JdbcConnection createDb2Connection(Db2SourceConfig sourceConfig) {
        Db2Dialect dialect = new Db2Dialect(sourceConfig);
        return dialect.openJdbcConnection(sourceConfig);
    }
}
