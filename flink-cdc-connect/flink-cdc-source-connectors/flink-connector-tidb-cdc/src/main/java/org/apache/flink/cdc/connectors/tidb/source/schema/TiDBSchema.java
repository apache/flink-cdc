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

package org.apache.flink.cdc.connectors.tidb.source.schema;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.converter.TiDBValueConverters;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetContext;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.connector.tidb.TidbTopicSelector;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.cdc.connectors.tidb.utils.TiDBConnectionUtils.getValueConverters;

/** TiDB schema. */
public class TiDBSchema {
    private static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE ";
    private static final String DESC_TABLE = "DESC ";

    private final TiDBConnectorConfig connectorConfig;
    private final TiDBDatabaseSchema databaseSchema;
    private final SchemasByTableId schemasByTableId;

    public TiDBSchema(TiDBSourceConfig sourceConfig, boolean isTableIdCaseInSensitive) {
        this.connectorConfig = sourceConfig.getDbzConnectorConfig();
        this.databaseSchema = createTiDBDatabaseSchema(connectorConfig, isTableIdCaseInSensitive);
        this.schemasByTableId = new SchemasByTableId(isTableIdCaseInSensitive);
    }

    public TableChange getTableSchema(JdbcConnection jdbc, TableId tableId) {
        // read schema from cache first
        TableChange schema = schemasByTableId.get(tableId);
        if (schema == null) {
            schema = readTableSchema(jdbc, tableId);
            schemasByTableId.put(tableId, schema);
        }
        return schema;
    }

    public static TiDBDatabaseSchema createTiDBDatabaseSchema(
            TiDBConnectorConfig dbzTiDBConfig, boolean isTableIdCaseSensitive) {
        TopicSelector<TableId> topicSelector = TidbTopicSelector.defaultSelector(dbzTiDBConfig);
        TiDBValueConverters valueConverters = getValueConverters(dbzTiDBConfig);
        return new TiDBDatabaseSchema(
                dbzTiDBConfig, valueConverters, topicSelector, isTableIdCaseSensitive);
    }

    private TableChange readTableSchema(JdbcConnection jdbc, TableId tableId) {
        final Map<TableId, TableChange> tableChangeMap = new HashMap<>();
        String showCreateTable = SHOW_CREATE_TABLE + TiDBUtils.quote(tableId);
        final TiDBPartition partition = new TiDBPartition(connectorConfig.getLogicalName());
        buildSchemaByShowCreateTable(partition, jdbc, tableId, tableChangeMap);
        if (!tableChangeMap.containsKey(tableId)) {
            // fallback to desc table
            String descTable = DESC_TABLE + TiDBUtils.quote(tableId);
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
            TiDBPartition partition,
            JdbcConnection jdbc,
            TableId tableId,
            Map<TableId, TableChange> tableChangeMap) {
        final String sql = SHOW_CREATE_TABLE + TiDBUtils.quote(tableId);
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

    private void buildSchemaByDescTable(
            TiDBPartition partition,
            JdbcConnection jdbc,
            String descTable,
            TableId tableId,
            Map<TableId, TableChange> tableChangeMap) {
        List<TiDBFieldDefinition> fieldMetas = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        try {
            jdbc.query(
                    descTable,
                    rs -> {
                        while (rs.next()) {
                            TiDBFieldDefinition meta = new TiDBFieldDefinition();
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
                    new TiDBTableDefinition(tableId, fieldMetas, primaryKeys).toDdl(),
                    tableId,
                    tableChangeMap);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to read schema for table %s by running %s", tableId, descTable),
                    e);
        }
    }

    private void parseSchemaByDdl(
            TiDBPartition partition,
            String ddl,
            TableId tableId,
            Map<TableId, TableChange> tableChangeMap) {
        final EventOffsetContext offsetContext = EventOffsetContext.initial(connectorConfig);
        List<SchemaChangeEvent> schemaChangeEvents =
                databaseSchema.parseSnapshotDdl(
                        partition, ddl, tableId.catalog(), offsetContext, Instant.now());
        for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
            for (TableChange tableChange : schemaChangeEvent.getTableChanges()) {
                tableChangeMap.put(tableId, tableChange);
            }
        }
    }

    private static class SchemasByTableId {

        private final boolean tableIdCaseInsensitive;
        private final ConcurrentMap<TableId, TableChange> values;

        public SchemasByTableId(boolean tableIdCaseInsensitive) {
            this.tableIdCaseInsensitive = tableIdCaseInsensitive;
            this.values = new ConcurrentHashMap<>();
        }

        public void clear() {
            values.clear();
        }

        public TableChange remove(TableId tableId) {
            return values.remove(toLowerCaseIfNeeded(tableId));
        }

        public TableChange get(TableId tableId) {
            return values.get(toLowerCaseIfNeeded(tableId));
        }

        public TableChange put(TableId tableId, TableChange updated) {
            return values.put(toLowerCaseIfNeeded(tableId), updated);
        }

        private TableId toLowerCaseIfNeeded(TableId tableId) {
            return tableIdCaseInsensitive ? tableId.toLowercase() : tableId;
        }
    }
}
