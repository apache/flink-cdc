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
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.ververica.cdc.connectors.sqlserver.experimental.utils.SqlServerUtils.createSqlServerDatabaseSchema;

/** A component used to get schema by table path. */
public class SqlServerSchema {
    private static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE ";
    private static final String DESC_TABLE = "DESC ";

    private final SqlServerConnectorConfig connectorConfig;
    private final SqlServerDatabaseSchema databaseSchema;
    private final Map<TableId, TableChange> schemasByTableId;

    public SqlServerSchema(SqlServerSourceConfig sourceConfig) {
        this.connectorConfig = sourceConfig.getDbzConnectorConfig();
        this.databaseSchema = createSqlServerDatabaseSchema(connectorConfig);
        this.schemasByTableId = new HashMap<>();
    }

    /**
     * Gets table schema for the given table path. It will request to MySQL server by running `SHOW
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
        final Map<TableId, TableChange> tableChangeMap = new HashMap<>();
        final String sql = "SHOW CREATE TABLE " + SqlServerUtils.quote(tableId);
        try {
            jdbc.query(
                    sql,
                    rs -> {
                        if (rs.next()) {
                            //                            final String ddl = rs.getString(2);
                            //                            final SqlServerOffsetContext offsetContext
                            // =
                            //
                            // SqlServerOffsetContext(connectorConfig);
                            //                            List<SchemaChangeEvent> schemaChangeEvents
                            // =
                            //                                    databaseSchema.parseSnapshotDdl(
                            //                                            ddl, tableId.catalog(),
                            // offsetContext, Instant.now());
                            //                            for (SchemaChangeEvent schemaChangeEvent :
                            // schemaChangeEvents) {
                            //                                for (TableChange tableChange :
                            //
                            // schemaChangeEvent.getTableChanges()) {
                            //                                    tableChangeMap.put(tableId,
                            // tableChange);
                            //                                }
                            //                            }
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to read schema for table %s by running %s", tableId, sql),
                    e);
        }
        if (!tableChangeMap.containsKey(tableId)) {
            throw new FlinkRuntimeException(
                    String.format("Can't obtain schema for table %s by running %s", tableId, sql));
        }

        return tableChangeMap.get(tableId);
    }
}
