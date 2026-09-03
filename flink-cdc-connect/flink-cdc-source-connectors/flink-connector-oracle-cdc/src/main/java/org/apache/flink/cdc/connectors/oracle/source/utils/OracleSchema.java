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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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

        // Debezium 2.0 removed OracleConnection#readSchemaForCapturedTables, whose contract was to
        // look the table up by the caller's own TableId and store the result under that same id.
        // Core's JdbcConnection#readSchema behaves differently in two ways that both matter here:
        // it rebuilds the id from the JDBC metadata (catalog, schema, table) and stores the table
        // under *that* id, and it applies the table filter to the rebuilt id rather than to the
        // requested one. Callers in Flink CDC identify an Oracle table as
        // TableId(catalog=owner, schema=null, table=name) - see OracleSchemaUtils#toDbzTableId -
        // which never equals the rebuilt TableId(catalog, owner, name), so an exact-equality
        // filter matches nothing and a lookup by the requested id finds nothing.
        //
        // The removed method was also lenient about that "owner" in a way callers rely on: it
        // passed the value straight to DatabaseMetaData#getColumns as the catalog, which Oracle
        // ignores, together with a null schema pattern - so it effectively searched every schema
        // and keyed the result by the requested id. OracleMetadataAccessorITCase depends on that,
        // asking for the container's database name (ORCLCDB) rather than the real owner
        // (DEBEZIUM). Try the owner as a schema first, which is correct and cheap when callers
        // pass a real owner, and only fall back to the schema-wide search when that finds
        // nothing.
        final String owner = tableId.schema() != null ? tableId.schema() : tableId.catalog();

        Table table = readTable(oracleConnection, tableId, owner);
        if (table == null) {
            table = readTable(oracleConnection, tableId, null);
        }
        if (table == null) {
            throw new FlinkRuntimeException(
                    String.format("Can't obtain schema for table %s ", tableId));
        }

        // Restore the pre-Debezium-2.0 behavior of returning a table carrying the requested id.
        return new TableChange(
                TableChanges.TableChangeType.CREATE, table.edit().tableId(tableId).create());
    }

    /**
     * Reads the table from JDBC metadata, restricted to {@code schemaPattern} when it is non-null.
     * Returns {@code null} when no unambiguous match exists.
     */
    private Table readTable(
            OracleConnection oracleConnection, TableId tableId, String schemaPattern) {
        final Tables tables = new Tables();
        try {
            // Oracle exposes the owner as the JDBC schema and has no meaningful catalog; passing
            // the owner as the catalog (as an unqualified read would) returns no rows at all.
            oracleConnection.readSchema(
                    tables,
                    null,
                    schemaPattern,
                    Tables.TableFilter.fromPredicate(
                            id -> matches(id, schemaPattern, tableId.table())),
                    null,
                    false);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to read schema for table %s ", tableId), e);
        }

        List<Table> candidates =
                tables.tableIds().stream()
                        .filter(id -> matches(id, schemaPattern, tableId.table()))
                        .map(tables::forTable)
                        .filter(Objects::nonNull)
                        .filter(t -> !t.columns().isEmpty())
                        .collect(Collectors.toList());

        if (candidates.size() > 1) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Table %s is ambiguous, it matches %s. Qualify it with its owner.",
                            tableId,
                            candidates.stream()
                                    .map(t -> t.id().toString())
                                    .collect(Collectors.toList())));
        }
        return candidates.isEmpty() ? null : candidates.get(0);
    }

    private static boolean matches(TableId candidate, String owner, String tableName) {
        return Objects.equals(candidate.table(), tableName)
                && (owner == null || owner.equals(candidate.schema()));
    }
}
