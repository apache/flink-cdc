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
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleSchema;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OraclePartition;
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
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.oracle.source.utils.OracleConnectionUtils.createOracleConnection;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class OracleSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSchemaUtils.class);

    public static List<String> listDatabases(OracleSourceConfig sourceConfig) {
        try (JdbcConnection jdbc =
                createOracleConnection(sourceConfig.getDbzConnectorConfig().getJdbcConfig())) {
            return listDatabases(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(
            OracleSourceConfig sourceConfig, @Nullable String dbName) {
        try (OracleConnection jdbc =
                createOracleConnection(sourceConfig.getDbzConnectorConfig().getJdbcConfig())) {

            List<String> databases =
                    dbName != null
                            ? Collections.singletonList(dbName)
                            : Collections.singletonList(sourceConfig.getDatabaseList().get(0));

            List<TableId> tableIds = new ArrayList<>();
            for (String database : databases) {
                tableIds.addAll(listTables(jdbc, database));
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(
            OracleSourceConfig sourceConfig, OraclePartition partition, TableId tableId) {
        try (OracleConnection jdbc =
                createOracleConnection(sourceConfig.getDbzConnectorConfig().getJdbcConfig())) {
            return getTableSchema(partition, tableId, sourceConfig, jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to get table schema: " + e.getMessage(), e);
        }
    }

    public static List<String> listDatabases(JdbcConnection jdbc) throws SQLException {
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        LOG.info("Read list of available databases");
        final List<String> databaseNames = new ArrayList<>();
        jdbc.query(
                "SHOW DATABASES WHERE `database` NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')",
                rs -> {
                    while (rs.next()) {
                        databaseNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available databases are: {}", databaseNames);
        return databaseNames;
    }

    public static List<TableId> listTables(JdbcConnection jdbc, String dbName) throws SQLException {
        LOG.info("Read list of available tables in {}", dbName);
        final List<TableId> tableIds = new ArrayList<>();
        final String query =
                "select owner, table_name from all_tables "
                        +
                        // filter special spatial tables
                        "where table_name NOT LIKE 'MDRT_%' "
                        + "and table_name NOT LIKE 'MDRS_%' "
                        + "and table_name NOT LIKE 'MDXT_%' "
                        +
                        // filter index-organized-tables
                        "and (table_name NOT LIKE 'SYS_IOT_OVER_%' and IOT_NAME IS NULL) "
                        +
                        // filter nested tables
                        "and nested = 'NO'"
                        +
                        // filter parent tables of nested tables
                        "and table_name not in (select PARENT_TABLE_NAME from ALL_NESTED_TABLES)";
        jdbc.query(
                query,
                rs -> {
                    while (rs.next()) {
                        tableIds.add(TableId.tableId(rs.getString(1), rs.getString(2)));
                    }
                });
        LOG.info("\t list of available tables are: {}", tableIds);
        return tableIds;
    }

    public static Schema getTableSchema(
            OraclePartition partition,
            TableId tableId,
            OracleSourceConfig sourceConfig,
            OracleConnection jdbc) {
        // fetch table schemas
        OracleSchema oracleSchema = new OracleSchema();
        TableChanges.TableChange tableSchema =
                oracleSchema.getTableSchema(jdbc, toDbzTableId(tableId));
        return toSchema(tableSchema.getTable());
    }

    public static Schema getTableSchema(
            io.debezium.relational.TableId tableId, OracleConnection jdbc) {
        // fetch table schemas
        OracleSchema oracleSchema = new OracleSchema();
        TableChanges.TableChange tableSchema = oracleSchema.getTableSchema(jdbc, tableId);
        return toSchema(tableSchema.getTable());
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
}
