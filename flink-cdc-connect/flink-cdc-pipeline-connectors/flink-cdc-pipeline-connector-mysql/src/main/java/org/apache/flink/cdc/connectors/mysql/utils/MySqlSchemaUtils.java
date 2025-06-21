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

package org.apache.flink.cdc.connectors.mysql.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlSchema;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
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

import static org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils.createMySqlConnection;
import static org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils.quote;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class MySqlSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSchemaUtils.class);

    public static List<String> listDatabases(MySqlSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = createMySqlConnection(sourceConfig)) {
            return listDatabases(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(
            MySqlSourceConfig sourceConfig, @Nullable String dbName) {
        try (MySqlConnection jdbc = createMySqlConnection(sourceConfig)) {
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

    public static Schema getTableSchema(
            MySqlSourceConfig sourceConfig, MySqlPartition partition, TableId tableId) {
        try (MySqlConnection jdbc = createMySqlConnection(sourceConfig)) {
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
        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database. We can't use a prepared statement with
        // MySQL, so we have to build the SQL statement each time. Although in other cases this
        // might lead to SQL injection, in our case we are reading the database names from the
        // database and not taking them from the user ...
        LOG.info("Read list of available tables in {}", dbName);
        final List<TableId> tableIds = new ArrayList<>();
        jdbc.query(
                "SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'",
                rs -> {
                    while (rs.next()) {
                        tableIds.add(TableId.tableId(dbName, rs.getString(1)));
                    }
                });
        LOG.info("\t list of available tables are: {}", tableIds);
        return tableIds;
    }

    public static Schema getTableSchema(
            MySqlPartition partition,
            TableId tableId,
            MySqlSourceConfig sourceConfig,
            MySqlConnection jdbc) {
        // fetch table schemas
        try (MySqlSchema mySqlSchema =
                new MySqlSchema(sourceConfig, jdbc.isTableIdCaseSensitive())) {
            TableChanges.TableChange tableSchema =
                    mySqlSchema.getTableSchema(partition, jdbc, toDbzTableId(tableId));
            return toSchema(tableSchema.getTable(), sourceConfig.isTreatTinyInt1AsBoolean());
        }
    }

    public static Schema toSchema(Table table, boolean tinyInt1isBit) {
        List<Column> columns =
                table.columns().stream()
                        .map(column -> toColumn(column, tinyInt1isBit))
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column, boolean tinyInt1isBit) {
        return Column.physicalColumn(
                column.name(),
                MySqlTypeUtils.fromDbzColumn(column, tinyInt1isBit),
                column.comment());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    public static boolean isTableIdCaseInsensitive(MySqlSourceConfig sourceConfig) {
        try (MySqlConnection jdbc = createMySqlConnection(sourceConfig)) {
            return jdbc.isTableIdCaseSensitive();
        } catch (Exception e) {
            throw new RuntimeException("Error to get table id caseSensitive: " + e.getMessage(), e);
        }
    }

    private MySqlSchemaUtils() {}
}
