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

package org.apache.flink.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlFieldDefinition;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlTableDefinition;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.utils.MySqlTypeUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.text.ParsingException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils.openJdbcConnection;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getTableId;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isSchemaChangeEvent;
import static org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils.listTables;

/** The {@link RecordEmitter} implementation for pipeline mysql connector. */
public class MySqlPipelineRecordEmitter extends MySqlRecordEmitter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlPipelineRecordEmitter.class);

    private final MySqlSourceConfig sourceConfig;
    private MySqlAntlrDdlParser mySqlAntlrDdlParser;

    // Used when startup mode is initial
    private Set<TableId> alreadySendCreateTableTables;

    // Used when startup mode is snapshot
    private boolean shouldEmitAllCreateTableEventsInSnapshotMode = true;
    private boolean isBounded = false;

    private final DebeziumDeserializationSchema<Event> debeziumDeserializationSchema;

    private final Map<TableId, CreateTableEvent> createTableEventCache;

    public MySqlPipelineRecordEmitter(
            DebeziumDeserializationSchema<Event> debeziumDeserializationSchema,
            MySqlSourceReaderMetrics sourceReaderMetrics,
            MySqlSourceConfig sourceConfig) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                sourceConfig.isIncludeSchemaChanges());
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
        this.sourceConfig = sourceConfig;
        this.alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache =
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .getCreateTableEventCache();
        this.isBounded = StartupOptions.snapshot().equals(sourceConfig.getStartupOptions());
    }

    @Override
    public void applySplit(MySqlSplit split) {
        if ((isBounded) && createTableEventCache.isEmpty() && split instanceof MySqlSnapshotSplit) {
            // TableSchemas in MySqlSnapshotSplit only contains one table.
            createTableEventCache.putAll(generateCreateTableEvent(sourceConfig));
        } else {
            for (TableChanges.TableChange tableChange : split.getTableSchemas().values()) {
                CreateTableEvent createTableEvent =
                        new CreateTableEvent(
                                toCdcTableId(tableChange.getId()),
                                buildSchemaFromTable(tableChange.getTable()));
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .applyChangeEvent(createTableEvent);
            }
        }
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<Event> output, MySqlSplitState splitState)
            throws Exception {
        if (shouldEmitAllCreateTableEventsInSnapshotMode && isBounded) {
            // In snapshot mode, we simply emit all schemas at once.
            createTableEventCache.forEach(
                    (tableId, createTableEvent) -> {
                        output.collect(createTableEvent);
                    });
            shouldEmitAllCreateTableEventsInSnapshotMode = false;
        } else if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            // In Snapshot phase of INITIAL startup mode, we lazily send CreateTableEvent to
            // downstream to avoid checkpoint timeout.
            TableId tableId = splitState.asSnapshotSplitState().toMySqlSplit().getTableId();
            if (!alreadySendCreateTableTables.contains(tableId)) {
                try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
                    sendCreateTableEvent(jdbc, tableId, output);
                    alreadySendCreateTableTables.add(tableId);
                }
            }
        } else {
            boolean isDataChangeRecord = isDataChangeRecord(element);
            if (isDataChangeRecord || isSchemaChangeEvent(element)) {
                TableId tableId = getTableId(element);
                if (!alreadySendCreateTableTables.contains(tableId)) {
                    CreateTableEvent createTableEvent = createTableEventCache.get(tableId);
                    // New created table in binlog reading phase.
                    if (createTableEvent != null) {
                        output.collect(createTableEvent);
                    }
                    alreadySendCreateTableTables.add(tableId);
                }
                // In rare case, we may miss some CreateTableEvents before DataChangeEvents.
                // Don't send CreateTableEvent for SchemaChangeEvents as it's the latest schema.
                if (isDataChangeRecord && !createTableEventCache.containsKey(tableId)) {
                    try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
                        Schema schema = getSchema(jdbc, tableId);
                        CreateTableEvent createTableEvent =
                                new CreateTableEvent(
                                        org.apache.flink.cdc.common.event.TableId.tableId(
                                                tableId.catalog(), tableId.table()),
                                        schema);
                        output.collect(createTableEvent);
                        createTableEventCache.put(tableId, createTableEvent);
                    }
                }
            }
        }
        super.processElement(element, output, splitState);
    }

    private org.apache.flink.cdc.common.event.TableId toCdcTableId(TableId dbzTableId) {
        return org.apache.flink.cdc.common.event.TableId.tableId(
                dbzTableId.catalog(), dbzTableId.table());
    }

    private void sendCreateTableEvent(
            JdbcConnection jdbc, TableId tableId, SourceOutput<Event> output) {
        Schema schema = getSchema(jdbc, tableId);
        output.collect(
                new CreateTableEvent(
                        org.apache.flink.cdc.common.event.TableId.tableId(
                                tableId.catalog(), tableId.table()),
                        schema));
    }

    private Schema getSchema(JdbcConnection jdbc, TableId tableId) {
        String ddlStatement = showCreateTable(jdbc, tableId);
        try {
            return parseDDL(ddlStatement, tableId);
        } catch (ParsingException pe) {
            LOG.warn(
                    "Failed to parse DDL: \n{}\nWill try parsing by describing table.",
                    ddlStatement,
                    pe);
        }
        ddlStatement = describeTable(jdbc, tableId);
        return parseDDL(ddlStatement, tableId);
    }

    private String showCreateTable(JdbcConnection jdbc, TableId tableId) {
        final String showCreateTableQuery =
                String.format("SHOW CREATE TABLE %s", StatementUtils.quote(tableId));
        try {
            return jdbc.queryAndMap(
                    showCreateTableQuery,
                    rs -> {
                        String ddlStatement = null;
                        while (rs.next()) {
                            ddlStatement = rs.getString(2);
                        }
                        return ddlStatement;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format("Failed to show create table for %s", tableId), e);
        }
    }

    private String describeTable(JdbcConnection jdbc, TableId tableId) {
        List<MySqlFieldDefinition> fieldMetas = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        try {
            return jdbc.queryAndMap(
                    String.format("DESC %s", StatementUtils.quote(tableId)),
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
                        return new MySqlTableDefinition(tableId, fieldMetas, primaryKeys).toDdl();
                    });
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to describe table %s", tableId), e);
        }
    }

    private Schema parseDDL(String ddlStatement, TableId tableId) {
        Table table = parseDdl(ddlStatement, tableId);
        return buildSchemaFromTable(table);
    }

    private Schema buildSchemaFromTable(Table table) {
        List<Column> columns = table.columns();
        Schema.Builder tableBuilder = Schema.newBuilder();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);

            String colName = column.name();
            DataType dataType =
                    MySqlTypeUtils.fromDbzColumn(column, sourceConfig.isTreatTinyInt1AsBoolean());
            if (!column.isOptional()) {
                dataType = dataType.notNull();
            }
            tableBuilder.physicalColumn(
                    colName,
                    dataType,
                    column.comment(),
                    column.defaultValueExpression().orElse(null));
        }
        tableBuilder.comment(table.comment());

        List<String> primaryKey = table.primaryKeyColumnNames();
        if (Objects.nonNull(primaryKey) && !primaryKey.isEmpty()) {
            tableBuilder.primaryKey(primaryKey);
        }
        return tableBuilder.build();
    }

    private synchronized Table parseDdl(String ddlStatement, TableId tableId) {
        MySqlAntlrDdlParser mySqlAntlrDdlParser = getParser();
        mySqlAntlrDdlParser.setCurrentDatabase(tableId.catalog());
        Tables tables = new Tables();
        mySqlAntlrDdlParser.parse(ddlStatement, tables);
        return tables.forTable(tableId);
    }

    private synchronized MySqlAntlrDdlParser getParser() {
        if (mySqlAntlrDdlParser == null) {
            boolean includeComments =
                    sourceConfig
                            .getDbzConfiguration()
                            .getBoolean(
                                    RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_COMMENTS
                                            .name(),
                                    false);
            mySqlAntlrDdlParser =
                    new MySqlAntlrDdlParser(
                            true, false, includeComments, null, Tables.TableFilter.includeAll());
        }
        return mySqlAntlrDdlParser;
    }

    private Map<TableId, CreateTableEvent> generateCreateTableEvent(
            MySqlSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            Map<TableId, CreateTableEvent> createTableEventCache = new HashMap<>();
            List<TableId> capturedTableIds =
                    listTables(
                            jdbc, sourceConfig.getDatabaseFilter(), sourceConfig.getTableFilter());
            for (TableId tableId : capturedTableIds) {
                Schema schema = getSchema(jdbc, tableId);
                createTableEventCache.put(
                        tableId,
                        new CreateTableEvent(
                                org.apache.flink.cdc.common.event.TableId.tableId(
                                        tableId.catalog(), tableId.table()),
                                schema));
            }
            return createTableEventCache;
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }
}
