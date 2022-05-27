/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.sqlserver.experimental.utils;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.sqlserver.experimental.config.SqlServerSourceConfig;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** A component used to get schema by table path. */
public class SqlServerSchema {

    private static final int CHANGE_TABLE_DATA_COLUMN_OFFSET = 5;

    private static final String GET_LIST_OF_KEY_COLUMNS =
            "SELECT * FROM cdc.index_columns WHERE object_id=?";

    private final SqlServerConnectorConfig connectorConfig;
    private final Map<TableId, TableChange> schemasByTableId;

    public SqlServerSchema(SqlServerSourceConfig sourceConfig) {
        this.connectorConfig = sourceConfig.getDbzConnectorConfig();
        this.schemasByTableId = new HashMap<>();
    }

    /**
     * Gets table schema for the given table path. It will request to Sql Server by running `SHOW
     * CREATE TABLE` if cache missed.
     */
    public TableChange getTableSchema(JdbcConnection jdbc, TableId tableId) {
        // read schema from cache first
        TableChange schema = schemasByTableId.get(tableId);
        if (schema == null) {
            schema = readTableSchema(jdbc, tableId);
            schemasByTableId.put(tableId, schema);
        }
        return schema;
    }

    private TableChange readTableSchema(JdbcConnection jdbc, TableId tableId) {

        Set<TableId> tableIdSet = new HashSet<>();
        tableIdSet.add(tableId);

        Tables tables = new Tables();
        tables.overwriteTable(tables.editOrCreateTable(tableId).create());
        try {
            jdbc.readSchema(
                    tables,
                    tableId.catalog(),
                    tableId.schema(),
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);

            Table table = tables.forTable(tableId);

            return new TableChange(TableChanges.TableChangeType.CREATE, table);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to read schema for table %s", tableId), e);
        }
    }
}
