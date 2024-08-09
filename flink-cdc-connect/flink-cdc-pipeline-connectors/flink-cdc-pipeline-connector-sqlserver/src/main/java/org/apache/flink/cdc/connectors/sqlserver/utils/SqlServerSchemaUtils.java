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

package org.apache.flink.cdc.connectors.sqlserver.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerSchema;
import org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils;

import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerPartition;
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

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class SqlServerSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerSchemaUtils.class);

    public static List<String> listSchemas(SqlServerSourceConfig sourceConfig, String namespace) {
        try (SqlServerConnection jdbc =
                SqlServerConnectionUtils.createSqlServerConnection(
                        sourceConfig.getDbzConnectorConfig())) {
            return listSchemas(jdbc, namespace);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list schemas: " + e.getMessage(), e);
        }
    }

    public static List<String> listNamespaces(SqlServerSourceConfig sourceConfig) {
        try (SqlServerConnection jdbc =
                SqlServerConnectionUtils.createSqlServerConnection(
                        sourceConfig.getDbzConnectorConfig())) {
            return listNamespaces(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list namespaces: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(
            SqlServerSourceConfig sourceConfig, @Nullable String dbName) {
        try (SqlServerConnection jdbc =
                SqlServerConnectionUtils.createSqlServerConnection(
                        sourceConfig.getDbzConnectorConfig())) {
            List<String> databases =
                    dbName != null
                            ? Collections.singletonList(dbName)
                            : Collections.singletonList(sourceConfig.getDatabaseList().get(0));

            List<TableId> tableIds = new ArrayList<>();
            List<TableId> tableIdList =
                    SqlServerConnectionUtils.listTables(
                                    jdbc, sourceConfig.getTableFilters(), databases)
                            .stream()
                            .map(SqlServerSchemaUtils::toCdcTableId)
                            .collect(Collectors.toList());
            tableIds.addAll(tableIdList);
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(
            SqlServerSourceConfig sourceConfig, SqlServerPartition partition, TableId tableId) {
        try (SqlServerConnection jdbc =
                SqlServerConnectionUtils.createSqlServerConnection(
                        sourceConfig.getDbzConnectorConfig())) {
            return getTableSchema(partition, tableId, sourceConfig, jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list schema: " + e.getMessage(), e);
        }
    }

    public static List<String> listSchemas(JdbcConnection jdbc, String namespace)
            throws SQLException {
        LOG.info("Read list of available schemas");
        final List<String> schemaNames = new ArrayList<>();

        String querySql =
                String.format("SELECT NAME FROM %s.SYS.SCHEMAS WHERE SCHEMA_ID < 16384", namespace);

        jdbc.query(
                querySql,
                rs -> {
                    while (rs.next()) {
                        schemaNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available schemas are: {}", schemaNames);
        return schemaNames;
    }

    public static List<String> listNamespaces(JdbcConnection jdbc) throws SQLException {
        LOG.info("Read list of available namespaces");
        final List<String> namespaceNames = new ArrayList<>();
        jdbc.query(
                "SELECT NAME FROM SYS.DATABASES WHERE NAME NOT IN ('MASTER', 'TEMPDB', 'MODEL', 'MSDB')",
                rs -> {
                    while (rs.next()) {
                        namespaceNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available namespaces are: {}", namespaceNames);
        return namespaceNames;
    }

    public static Schema getTableSchema(
            SqlServerPartition partition,
            TableId tableId,
            SqlServerSourceConfig sourceConfig,
            SqlServerConnection jdbc) {
        // fetch table schemas
        SqlServerSchema sqlServerSchema = new SqlServerSchema();
        TableChanges.TableChange tableSchema =
                sqlServerSchema.getTableSchema(
                        jdbc,
                        toDbzTableId(tableId),
                        sourceConfig
                                .getDbzConnectorConfig()
                                .getTableFilters()
                                .dataCollectionFilter());
        return toSchema(tableSchema.getTable());
    }

    public static Schema getTableSchema(
            io.debezium.relational.TableId tableId,
            SqlServerSourceConfig sourceConfig,
            SqlServerConnection jdbc) {
        // fetch table schemas
        SqlServerSchema sqlServerSchema = new SqlServerSchema();
        TableChanges.TableChange tableSchema =
                sqlServerSchema.getTableSchema(
                        jdbc,
                        tableId,
                        sourceConfig
                                .getDbzConnectorConfig()
                                .getTableFilters()
                                .dataCollectionFilter());

        return toSchema(tableSchema.getTable());
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream()
                        .map(SqlServerSchemaUtils::toColumn)
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        return Column.physicalColumn(
                column.name(), SqlServerTypeUtils.fromDbzColumn(column), column.comment());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    public static org.apache.flink.cdc.common.event.TableId toCdcTableId(
            io.debezium.relational.TableId dbzTableId) {
        return org.apache.flink.cdc.common.event.TableId.tableId(
                dbzTableId.schema(), dbzTableId.table());
    }

    private SqlServerSchemaUtils() {}
}
