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

package org.apache.flink.cdc.connectors.sqlserver.source.dialect;

import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** A component used to get schema by table path. */
public class SqlServerSchema {

    private final Map<TableId, TableChange> schemasByTableId;

    public SqlServerSchema() {
        this.schemasByTableId = new ConcurrentHashMap<>();
    }

    public TableChange getTableSchema(
            JdbcConnection jdbc, TableId tableId, Tables.TableFilter tableFilters) {
        // read schema from cache first
        if (!schemasByTableId.containsKey(tableId)) {
            readTableSchema(jdbc, Collections.singletonList(tableId), tableFilters);
        }
        return schemasByTableId.get(tableId);
    }

    public Map<TableId, TableChange> getTableSchema(
            JdbcConnection jdbc, List<TableId> tableIds, Tables.TableFilter tableFilters) {
        // read schema from cache first
        Map<TableId, TableChange> tableChanges = new HashMap<>();

        List<TableId> unMatchTableIds = new ArrayList<>();
        for (TableId tableId : tableIds) {
            if (schemasByTableId.containsKey(tableId)) {
                tableChanges.put(tableId, schemasByTableId.get(tableId));
            } else {
                unMatchTableIds.add(tableId);
            }
        }

        if (!unMatchTableIds.isEmpty()) {
            readTableSchema(jdbc, tableIds, tableFilters);
            for (TableId tableId : unMatchTableIds) {
                if (schemasByTableId.containsKey(tableId)) {
                    tableChanges.put(tableId, schemasByTableId.get(tableId));
                } else {
                    throw new FlinkRuntimeException(
                            String.format("Failed to read table schema of table %s", tableId));
                }
            }
        }
        return tableChanges;
    }

    private List<TableChange> readTableSchema(
            JdbcConnection jdbc, List<TableId> tableIds, Tables.TableFilter tableFilters) {
        SqlServerConnection sqlServerConnection = (SqlServerConnection) jdbc;

        Tables tables = new Tables();
        for (TableId tableId : tableIds) {
            tables.overwriteTable(tables.editOrCreateTable(tableId).create());
        }

        try {
            // leave the schema pattern open so all requested tables are read in one scan, even
            // when they span multiple SQL Server schemas
            sqlServerConnection.readSchema(
                    tables, tableIds.get(0).catalog(), null, tableFilters, null, false);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to read schema", e);
        }

        // tableFilters matches every captured table, so this single scan already fetched all of
        // them; cache everything it found instead of just the tables that were asked for, so
        // later single-table lookups (e.g. from chunk splitting) hit the cache too
        for (TableId tableId : tables.tableIds()) {
            Table table = tables.forTable(tableId);
            if (table != null) {
                schemasByTableId.put(
                        tableId, new TableChange(TableChanges.TableChangeType.CREATE, table));
            }
        }

        List<TableChange> tableChanges = new ArrayList<>();
        for (TableId tableId : tableIds) {
            tableChanges.add(Objects.requireNonNull(schemasByTableId.get(tableId)));
        }
        return tableChanges;
    }
}
