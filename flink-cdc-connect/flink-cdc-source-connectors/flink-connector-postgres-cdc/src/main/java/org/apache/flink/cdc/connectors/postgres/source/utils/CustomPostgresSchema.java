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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.debezium.connector.postgresql.connection.PostgresConnection.escapeJdbcSearchPattern;

/** Loads and caches the schemas of requested PostgreSQL tables. */
public class CustomPostgresSchema {

    private final Map<TableId, TableChange> schemasByTableId = new HashMap<>();
    private final PostgresConnectorConfig dbzConfig;

    public CustomPostgresSchema(PostgresSourceConfig sourceConfig) {
        this.dbzConfig = sourceConfig.getDbzConnectorConfig();
    }

    public TableChange getTableSchema(PostgresConnection jdbc, TableId tableId) {
        TableChange cached = schemasByTableId.get(tableId);
        return cached != null
                ? cached
                : getTableSchema(jdbc, Collections.singleton(tableId)).get(tableId);
    }

    public Map<TableId, TableChange> getTableSchema(
            PostgresConnection jdbc, Collection<TableId> tableIds) {
        return getTableSchema(jdbc, tableIds, false);
    }

    public Map<TableId, TableChange> getInitialSnapshotTableSchemas(
            PostgresConnection jdbc, Collection<TableId> tableIds) {
        return getTableSchema(jdbc, tableIds, true);
    }

    private Map<TableId, TableChange> getTableSchema(
            PostgresConnection jdbc, Collection<TableId> tableIds, boolean bulkReadColumns) {
        Map<TableId, TableChange> tableChanges = new HashMap<>();
        Map<String, Set<TableId>> missingBySchema = new HashMap<>();
        for (TableId tableId : tableIds) {
            TableChange cached = schemasByTableId.get(tableId);
            if (cached != null) {
                tableChanges.put(tableId, cached);
            } else {
                missingBySchema
                        .computeIfAbsent(tableId.schema(), ignored -> new HashSet<>())
                        .add(tableId);
            }
        }

        if (!missingBySchema.isEmpty()) {
            try {
                readTableSchemas(jdbc, missingBySchema, tableChanges, bulkReadColumns);
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to read table schema", e);
            }
        }
        return tableChanges;
    }

    private void readTableSchemas(
            PostgresConnection jdbc,
            Map<String, Set<TableId>> missingBySchema,
            Map<TableId, TableChange> tableChanges,
            boolean bulkReadColumns)
            throws SQLException {
        String escape = jdbc.connection().getMetaData().getSearchStringEscape();
        Tables.TableFilter capturedTables = dbzConfig.getTableFilters().dataCollectionFilter();
        for (Map.Entry<String, Set<TableId>> entry : missingBySchema.entrySet()) {
            String schema = entry.getKey();
            Set<TableId> requestedTables = entry.getValue();
            String schemaPattern = schema == null ? null : escapeJdbcSearchPattern(schema, escape);
            Tables.TableFilter requestedCapturedTables =
                    tableId ->
                            requestedTables.contains(tableId) && capturedTables.isIncluded(tableId);
            Tables tables = new Tables();
            if (bulkReadColumns && requestedTables.size() > 1) {
                jdbc.readSchemaWithBulkColumns(
                        tables, dbzConfig.databaseName(), schemaPattern, requestedCapturedTables);
            } else {
                jdbc.readSchema(
                        tables,
                        dbzConfig.databaseName(),
                        schemaPattern,
                        requestedCapturedTables,
                        null,
                        false);
            }

            for (TableId tableId : requestedTables) {
                Table table = tables.forTable(tableId);
                if (table == null) {
                    throw new FlinkRuntimeException(
                            String.format("Failed to read table schema of table %s", tableId));
                }
                TableChange change = new TableChange(TableChangeType.CREATE, table);
                schemasByTableId.put(tableId, change);
                tableChanges.put(tableId, change);
            }
        }
    }
}
