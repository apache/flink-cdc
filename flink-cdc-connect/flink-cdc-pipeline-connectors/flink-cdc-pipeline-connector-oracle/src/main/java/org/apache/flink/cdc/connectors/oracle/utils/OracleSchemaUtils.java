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
import org.apache.flink.cdc.connectors.oracle.dto.ColumnInfo;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleSchema;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class OracleSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSchemaUtils.class);

    public static List<TableId> listTables(
            OracleSourceConfig sourceConfig, @Nullable String dbName) {
        try (JdbcConnection jdbc = DebeziumUtils.createOracleConnection(sourceConfig)) {
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
        try (JdbcConnection jdbc = DebeziumUtils.createOracleConnection(sourceConfig)) {
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
                statementFactory -> {
                    Statement statement = statementFactory.createStatement();
                    return statement;
                },
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
            JdbcConnection jdbc = DebeziumUtils.createOracleConnection(sourceConfig);
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

    private static io.debezium.relational.Column columnInfotoColumn(ColumnInfo columnInfo) {
        Integer length =
                columnInfo.getDataPrecision() == 0 || columnInfo.getDataPrecision() == null
                        ? columnInfo.getDataLength()
                        : columnInfo.getDataPrecision();
        ColumnEditor editor =
                io.debezium.relational.Column.editor()
                        .name(columnInfo.getColumnName())
                        .type(columnInfo.getDataType())
                        .length(length)
                        .scale(columnInfo.getDataScale());
        return editor.create();
    }

    public static Schema getSchema(JdbcConnection jdbc, io.debezium.relational.TableId tableId) {
        List<ColumnInfo> columns = showCreateTable(jdbc, tableId);
        List<String> pks = getTablePks(jdbc, tableId);
        List<org.apache.flink.cdc.common.schema.Column> list = new ArrayList<>();
        for (ColumnInfo columnInfo : columns) {
            DataType dataType = null;
            dataType = OracleTypeUtils.fromDbzColumn(columnInfotoColumn(columnInfo));
            org.apache.flink.cdc.common.schema.Column column =
                    org.apache.flink.cdc.common.schema.Column.metadataColumn(
                            columnInfo.getColumnName().toLowerCase(Locale.ROOT), dataType);
            list.add(column);
        }
        return Schema.newBuilder().setColumns(list).primaryKey(pks).build();
    }

    public static List<ColumnInfo> showCreateTable(
            JdbcConnection jdbc, io.debezium.relational.TableId tableId) {
        List<ColumnInfo> list = new ArrayList<>();
        final String showCreateTableQuery =
                String.format(
                        "select COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE from all_tab_columns where Table_Name='%s' and OWNER='%s' order by COLUMN_ID",
                        tableId.table().toUpperCase(), tableId.catalog().toUpperCase());
        try {
            return jdbc.queryAndMap(
                    showCreateTableQuery,
                    rs -> {
                        while (rs.next()) {
                            ColumnInfo columnInfo = new ColumnInfo();
                            columnInfo.setColumnName(rs.getString(1));
                            columnInfo.setDataType(rs.getString(2));
                            columnInfo.setDataLength(rs.getInt(3));
                            columnInfo.setDataPrecision(rs.getInt(4));
                            columnInfo.setDataScale(rs.getInt(5));
                            list.add(columnInfo);
                        }
                        return list;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format("Failed to show create table for %s", tableId), e);
        }
    }

    public static List<String> getTablePks(
            JdbcConnection jdbc, io.debezium.relational.TableId tableId) {
        List<String> list = new ArrayList<>();
        final String showCreateTableQuery =
                String.format(
                        "SELECT COLUMN_NAME FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '%s' and cols.OWNER='%s' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position ",
                        tableId.table().toUpperCase(), tableId.catalog().toUpperCase());
        try {
            return jdbc.queryAndMap(
                    showCreateTableQuery,
                    rs -> {
                        while (rs.next()) {
                            String columnName = null;
                            columnName = rs.getString(1);
                            list.add(columnName.toLowerCase(Locale.ROOT));
                        }
                        return list;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to get table pks for %s", tableId), e);
        }
    }
}
