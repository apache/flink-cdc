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

package org.apache.flink.cdc.connectors.oceanbase.source.schema;

import org.apache.flink.cdc.connectors.oceanbase.source.connection.OceanBaseConnection;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** A component used to get schema by table path. */
public class OceanBaseSchema {

    private final Map<TableId, TableChanges.TableChange> schemasByTableId;

    public OceanBaseSchema() {
        this.schemasByTableId = new HashMap<>();
    }

    public TableChanges.TableChange getTableSchema(JdbcConnection connection, TableId tableId) {
        TableChanges.TableChange schema = schemasByTableId.get(tableId);
        if (schema == null) {
            schema = readTableSchema(connection, tableId);
            schemasByTableId.put(tableId, schema);
        }
        return schema;
    }

    private TableChanges.TableChange readTableSchema(JdbcConnection jdbc, TableId tableId) {
        OceanBaseConnection connection = (OceanBaseConnection) jdbc;
        Set<TableId> tableIdSet = new HashSet<>();
        tableIdSet.add(tableId);

        final Map<TableId, TableChanges.TableChange> tableChangeMap = new HashMap<>();
        Tables tables = new Tables();
        tables.overwriteTable(tables.editOrCreateTable(tableId).create());

        try {
            connection.readSchemaForCapturedTables(
                    tables, tableId.catalog(), tableId.schema(), null, false, tableIdSet);
            Table table = tables.forTable(tableId);
            TableChanges.TableChange tableChange =
                    new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
            tableChangeMap.put(tableId, tableChange);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to read schema for table %s ", tableId), e);
        }

        if (!tableChangeMap.containsKey(tableId)) {
            throw new FlinkRuntimeException(
                    String.format("Can't obtain schema for table %s ", tableId));
        }

        return tableChangeMap.get(tableId);
    }
}
