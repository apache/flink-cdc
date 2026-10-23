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

package org.apache.flink.cdc.connectors.oracle.source.utils;

import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.oracle.OracleConnection;
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
import java.util.Objects;
import java.util.Set;

/** A component used to get schema by table path. */
public class OracleSchema {

    private final Map<TableId, TableChange> schemasByTableId;

    public OracleSchema() {
        this.schemasByTableId = new HashMap<>();
    }

    /** Gets table schema for the given table path. */
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
        OracleConnection oracleConnection = (OracleConnection) jdbc;
        Set<TableId> tableIdSet = new HashSet<>();
        tableIdSet.add(tableId);

        // Oracle JDBC in CDB mode returns the PDB name as TABLE_CAT in metadata results
        // (e.g. "FREEPDB1") even when the session is switched to that PDB. Our tableId
        // has a null catalog. Use a catalog-agnostic filter so columns and PKs are read
        // regardless of whether Oracle returns a non-null TABLE_CAT.
        Tables.TableFilter catalogAgnosticFilter =
                t ->
                        tableIdSet.stream()
                                .anyMatch(
                                        ref ->
                                                Objects.equals(ref.schema(), t.schema())
                                                        && Objects.equals(ref.table(), t.table()));

        final Map<TableId, TableChange> tableChangeMap = new HashMap<>();
        Tables tables = new Tables();
        tables.overwriteTable(tables.editOrCreateTable(tableId).create());

        try {
            oracleConnection.readSchema(
                    tables,
                    tableId.catalog(),
                    tableId.schema(),
                    catalogAgnosticFilter,
                    null,
                    false);

            // readSchema() stores the table under the key from JDBC metadata (which may have
            // "FREEPDB1" as catalog). Fall back to a schema+table-only lookup if the exact
            // tableId key (null catalog) is not present or has no columns.
            Table table = tables.forTable(tableId);
            if (table == null || table.columns().isEmpty()) {
                table =
                        tables.tableIds().stream()
                                .filter(
                                        id ->
                                                Objects.equals(id.schema(), tableId.schema())
                                                        && Objects.equals(
                                                                id.table(), tableId.table()))
                                .map(tables::forTable)
                                .filter(t -> t != null && !t.columns().isEmpty())
                                .findFirst()
                                .orElse(table);
            }

            if (table != null) {
                // If JDBC returned the table under a different catalog (e.g. "FREEPDB1" in CDB
                // mode), re-key it to the original tableId (null catalog) so that downstream
                // lookups via tableFor(tableId) succeed with the null-catalog key.
                if (!Objects.equals(table.id(), tableId)) {
                    table = table.edit().tableId(tableId).create();
                }
                TableChange tableChange =
                        new TableChange(TableChanges.TableChangeType.CREATE, table);
                tableChangeMap.put(tableId, tableChange);
            }
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
