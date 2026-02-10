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

package org.apache.flink.cdc.connectors.oracle.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.oracle.source.OracleDialect;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleSchema;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class OracleSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSchemaUtils.class);

    public static List<TableId> listTables(
            OracleSourceConfig sourceConfig, @Nullable String dbName) {
        try (JdbcConnection jdbc = createOracleConnection(sourceConfig)) {
            List<String> databases =
                    dbName != null ? Collections.singletonList(dbName) : listDatabases(jdbc);

            List<TableId> tableIds = new ArrayList<>();
            for (String database : databases) {
                tableIds.addAll(listTables(jdbc, database));
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<String> listDatabases(OracleSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = createOracleConnection(sourceConfig)) {
            return listDatabases(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<String> listDatabases(JdbcConnection jdbc) throws SQLException {
        // READ DATABASE NAMES
        LOG.info("Read list of available schemas");
        final List<String> schemaNames = new ArrayList<>();
        jdbc.query(
                "SELECT username FROM all_users",
                rs -> {
                    while (rs.next()) {
                        schemaNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available databases are: {}", schemaNames);
        return schemaNames;
    }

    public static List<TableId> listTables(JdbcConnection jdbc, String schemaName)
            throws SQLException {
        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each schema.
        LOG.info("Read list of available tables in {}", schemaName);
        final List<TableId> tableIds = new ArrayList<>();
        jdbc.query(
                "SELECT table_name FROM all_tables WHERE owner ='" + schemaName.toUpperCase() + "'",
                statementFactory -> statementFactory.createStatement(),
                rs -> {
                    while (rs.next()) {
                        tableIds.add(
                                TableId.tableId(
                                        schemaName.toLowerCase(Locale.ROOT),
                                        rs.getString(1).toLowerCase(Locale.ROOT)));
                    }
                });
        LOG.info("\t list of available tables are: {}", tableIds);
        return tableIds;
    }

    public static Schema getTableSchema(TableId tableId, OracleSourceConfig sourceConfig) {
        try {
            // fetch table schemas
            JdbcConnection jdbc = createOracleConnection(sourceConfig);
            OracleSchema oracleSchema = new OracleSchema();
            TableChanges.TableChange tableSchema =
                    oracleSchema.getTableSchema(jdbc, toDbzTableId(tableId));
            return toSchema(tableSchema.getTable());
        } catch (Exception e) {
            throw new RuntimeException("Error to get table schema: " + e.getMessage(), e);
        }
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream()
                        .map(OracleSchemaUtils::toColumn)
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        return Column.physicalColumn(
                column.name(), OracleTypeUtils.fromDbzColumn(column), column.comment());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    private OracleSchemaUtils() {}

    public static Schema getSchema(
            JdbcConnection jdbcConnection, io.debezium.relational.TableId tableId) {
        OracleDialect dialect = new OracleDialect();
        TableChanges.TableChange currentSchema = dialect.queryTableSchema(jdbcConnection, tableId);
        Table table = Objects.requireNonNull(currentSchema).getTable();
        List<io.debezium.relational.Column> columns = table.columns();
        List<String> pks = getTablePks(jdbcConnection, tableId);
        List<org.apache.flink.cdc.common.schema.Column> list = new ArrayList<>();
        for (io.debezium.relational.Column column : columns) {
            DataType dataType = OracleTypeUtils.fromDbzColumn(column);
            org.apache.flink.cdc.common.schema.Column cdcColumn =
                    org.apache.flink.cdc.common.schema.Column.physicalColumn(
                            column.name().toLowerCase(Locale.ROOT), dataType);
            list.add(cdcColumn);
        }
        return Schema.newBuilder().setColumns(list).primaryKey(pks).build();
    }

    public static List<String> getTablePks(
            JdbcConnection jdbc, io.debezium.relational.TableId tableId) {
        List<String> list = new ArrayList<>();
        final String showCreateTableQuery =
                String.format(
                        "SELECT COLUMN_NAME FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '%s' and cols.OWNER='%s' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position ",
                        tableId.table().toUpperCase(),
                        tableId.schema() == null
                                ? tableId.catalog().toUpperCase()
                                : tableId.schema().toUpperCase());
        try {
            return jdbc.queryAndMap(
                    showCreateTableQuery,
                    rs -> {
                        while (rs.next()) {
                            String columnName;
                            columnName = rs.getString(1);
                            list.add(columnName.toLowerCase(Locale.ROOT));
                        }
                        return list;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to get table pks for %s", tableId), e);
        }
    }

    public static JdbcConnection createOracleConnection(OracleSourceConfig sourceConfig) {
        OracleDialect dialect = new OracleDialect();
        return dialect.openJdbcConnection(sourceConfig);
    }
}
