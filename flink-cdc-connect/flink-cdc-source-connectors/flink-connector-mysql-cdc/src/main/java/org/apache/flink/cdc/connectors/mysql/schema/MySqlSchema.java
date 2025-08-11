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

package org.apache.flink.cdc.connectors.mysql.schema;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A component used to get schema by table path. */
public class MySqlSchema implements AutoCloseable {
    private static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE ";
    private static final String DESC_TABLE = "DESC ";

    private final MySqlConnectorConfig connectorConfig;
    private final MySqlDatabaseSchema databaseSchema;
    private final Map<TableId, TableChange> schemasByTableId;

    public MySqlSchema(MySqlSourceConfig sourceConfig, boolean isTableIdCaseSensitive) {
        this.connectorConfig = sourceConfig.getMySqlConnectorConfig();
        this.databaseSchema =
                DebeziumUtils.createMySqlDatabaseSchema(
                        connectorConfig,
                        isTableIdCaseSensitive,
                        sourceConfig.isParseOnLineSchemaChanges());
        this.schemasByTableId = new HashMap<>();
    }

    /**
     * Gets table schema for the given table path. It will request to MySQL server by running `SHOW
     * CREATE TABLE` if cache missed.
     */
    public TableChange getTableSchema(
            MySqlPartition partition, JdbcConnection jdbc, TableId tableId) {
        // read schema from cache first
        TableChange schema = schemasByTableId.get(tableId);
        if (schema == null) {
            schema = buildTableSchema(partition, jdbc, tableId);
            schemasByTableId.put(tableId, schema);
        }
        return schema;
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private TableChange buildTableSchema(
            MySqlPartition partition, JdbcConnection jdbc, TableId tableId) {
        final Map<TableId, TableChange> tableChangeMap = new HashMap<>();
        String showCreateTable = SHOW_CREATE_TABLE + StatementUtils.quote(tableId);
        buildSchemaByShowCreateTable(partition, jdbc, tableId, tableChangeMap);
        if (!tableChangeMap.containsKey(tableId)) {
            // fallback to desc table
            String descTable = DESC_TABLE + StatementUtils.quote(tableId);
            buildSchemaByDescTable(partition, jdbc, descTable, tableId, tableChangeMap);
            if (!tableChangeMap.containsKey(tableId)) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Can't obtain schema for table %s by running %s and %s ",
                                tableId, showCreateTable, descTable));
            }
        }
        return tableChangeMap.get(tableId);
    }

    private void buildSchemaByShowCreateTable(
            MySqlPartition partition,
            JdbcConnection jdbc,
            TableId tableId,
            Map<TableId, TableChange> tableChangeMap) {
        final String sql = SHOW_CREATE_TABLE + StatementUtils.quote(tableId);
        try {
            jdbc.query(
                    sql,
                    rs -> {
                        if (rs.next()) {
                            final String ddl = rs.getString(2);
                            parseSchemaByDdl(partition, ddl, tableId, tableChangeMap);
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to read schema for table %s by running %s", tableId, sql),
                    e);
        }
    }

    private void parseSchemaByDdl(
            MySqlPartition partition,
            String ddl,
            TableId tableId,
            Map<TableId, TableChange> tableChangeMap) {
        final MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);
        List<SchemaChangeEvent> schemaChangeEvents =
                databaseSchema.parseSnapshotDdl(
                        partition, ddl, tableId.catalog(), offsetContext, Instant.now());
        for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
            for (TableChange tableChange : schemaChangeEvent.getTableChanges()) {
                tableChangeMap.put(tableId, tableChange);
            }
        }
    }

    private void buildSchemaByDescTable(
            MySqlPartition partition,
            JdbcConnection jdbc,
            String descTable,
            TableId tableId,
            Map<TableId, TableChange> tableChangeMap) {
        List<MySqlFieldDefinition> fieldMetas = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        try {
            jdbc.query(
                    descTable,
                    rs -> {
                        while (rs.next()) {
                            MySqlFieldDefinition meta = new MySqlFieldDefinition();
                            meta.setColumnName(rs.getString("Field"));
                            meta.setColumnType(rs.getString("Type"));
                            meta.setNullable(
                                    StringUtils.equalsIgnoreCase(rs.getString("Null"), "YES"));
                            meta.setKey("PRI".equalsIgnoreCase(rs.getString("Key")));
                            meta.setUnique("UNI".equalsIgnoreCase(rs.getString("Key")));
                            meta.setDefaultValue(rs.getString("Default"));
                            meta.setExtra(rs.getString("Extra"));
                            if (meta.isKey()) {
                                primaryKeys.add(meta.getColumnName());
                            }
                            fieldMetas.add(meta);
                        }
                    });
            parseSchemaByDdl(
                    partition,
                    new MySqlTableDefinition(tableId, fieldMetas, primaryKeys).toDdl(),
                    tableId,
                    tableChangeMap);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to read schema for table %s by running %s", tableId, descTable),
                    e);
        }
    }

    @Override
    public void close() {
        databaseSchema.close();
    }
}
