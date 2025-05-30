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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.apache.flink.cdc.connectors.mysql.schema.MySqlSchema;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Utilities to discovery matched tables. */
public class TableDiscoveryUtils {

    private static final String[] TABLE_QUERY = {"TABLE"};
    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    public static List<TableId> listTables(
            JdbcConnection jdbc, Predicate<String> databaseFilter, Predicate<TableId> tableFilter)
            throws SQLException {
        final List<TableId> capturedTableIds = new ArrayList<>();
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        try (Connection connection = jdbc.connection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            try (ResultSet tableResult = databaseMetaData.getTables(null, null, "%", TABLE_QUERY)) {
                while (tableResult.next()) {
                    String dbName = tableResult.getString("TABLE_CAT");
                    if (!databaseFilter.test(dbName)) {
                        continue;
                    }
                    String tableName = tableResult.getString("TABLE_NAME");
                    TableId tableId = new TableId(dbName, null, tableName);
                    if (tableFilter.test(tableId)) {
                        capturedTableIds.add(tableId);
                        LOG.info("\t including table '{}' for further processing", tableId);
                    } else {
                        LOG.info("\t '{}' is filtered out of table capturing", tableId);
                    }
                }

            } catch (Exception e) {
                LOG.warn("Failed to get list of tables: {}", e.getMessage());
            }
        }
        return capturedTableIds;
    }

    public static Map<TableId, TableChange> discoverSchemaForCapturedTables(
            MySqlPartition partition, MySqlSourceConfig sourceConfig, MySqlConnection jdbc) {
        final List<TableId> capturedTableIds;
        try {
            capturedTableIds =
                    listTables(
                            jdbc, sourceConfig.getDatabaseFilter(), sourceConfig.getTableFilter());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to discover captured tables", e);
        }
        return discoverSchemaForCapturedTables(partition, capturedTableIds, sourceConfig, jdbc);
    }

    public static Map<TableId, TableChange> discoverSchemaForNewAddedTables(
            MySqlPartition partition,
            List<TableId> existedTables,
            MySqlSourceConfig sourceConfig,
            MySqlConnection jdbc) {
        final List<TableId> capturedTableIds;
        try {
            capturedTableIds =
                    listTables(
                                    jdbc,
                                    sourceConfig.getDatabaseFilter(),
                                    sourceConfig.getTableFilter())
                            .stream()
                            .filter(tableId -> !existedTables.contains(tableId))
                            .collect(Collectors.toList());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to discover captured tables", e);
        }
        return capturedTableIds.isEmpty()
                ? new HashMap<>()
                : discoverSchemaForCapturedTables(partition, capturedTableIds, sourceConfig, jdbc);
    }

    public static Map<TableId, TableChange> discoverSchemaForCapturedTables(
            MySqlPartition partition,
            List<TableId> capturedTableIds,
            MySqlSourceConfig sourceConfig,
            MySqlConnection jdbc) {
        if (capturedTableIds.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can't find any matched tables, please check your configured database-name: %s and table-name: %s",
                            sourceConfig.getDatabaseList(), sourceConfig.getTableList()));
        }

        // fetch table schemas
        try (MySqlSchema mySqlSchema =
                new MySqlSchema(sourceConfig, jdbc.isTableIdCaseSensitive())) {
            Map<TableId, TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                TableChange tableSchema = mySqlSchema.getTableSchema(partition, jdbc, tableId);
                tableSchemas.put(tableId, tableSchema);
            }
            return tableSchemas;
        }
    }
}
