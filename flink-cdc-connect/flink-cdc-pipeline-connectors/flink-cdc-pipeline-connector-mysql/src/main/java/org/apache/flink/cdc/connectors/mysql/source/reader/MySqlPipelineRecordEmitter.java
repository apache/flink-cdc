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
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.table.StartupMode;
import org.apache.flink.cdc.connectors.mysql.utils.MySqlSchemaUtils;
import org.apache.flink.cdc.connectors.mysql.utils.MySqlTypeUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.text.ParsingException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils.openJdbcConnection;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils.listTables;

/** The {@link RecordEmitter} implementation for pipeline mysql connector. */
public class MySqlPipelineRecordEmitter extends MySqlRecordEmitter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlPipelineRecordEmitter.class);

    private final MySqlSourceConfig sourceConfig;
    private MySqlAntlrDdlParser mySqlAntlrDdlParser;

    // Used when startup mode is initial
    private Set<TableId> alreadySendCreateTableTables;

    // Used when startup mode is not initial
    private boolean alreadySendCreateTableForBinlogSplit = false;
    private List<CreateTableEvent> createTableEventCache;

    public MySqlPipelineRecordEmitter(
            DebeziumDeserializationSchema<Event> debeziumDeserializationSchema,
            MySqlSourceReaderMetrics sourceReaderMetrics,
            MySqlSourceConfig sourceConfig) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                sourceConfig.isIncludeSchemaChanges());
        this.sourceConfig = sourceConfig;
        this.alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache = generateCreateTableEvent(sourceConfig);
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<Event> output, MySqlSplitState splitState)
            throws Exception {
        if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            // In Snapshot phase of INITIAL startup mode, we lazily send CreateTableEvent to
            // downstream to avoid checkpoint timeout.
            TableId tableId = splitState.asSnapshotSplitState().toMySqlSplit().getTableId();
            if (!alreadySendCreateTableTables.contains(tableId)) {
                try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
                    sendCreateTableEvent(jdbc, tableId, output);
                    alreadySendCreateTableTables.add(tableId);
                }
            }
        } else if (splitState.isBinlogSplitState() && !alreadySendCreateTableForBinlogSplit) {
            alreadySendCreateTableForBinlogSplit = true;
            if (sourceConfig.getStartupOptions().startupMode.equals(StartupMode.INITIAL)) {
                // In Snapshot -> Binlog transition of INITIAL startup mode, ensure all table
                // schemas have been sent to downstream. We use previously cached schema instead of
                // re-request latest schema because there might be some pending schema change events
                // in the queue, and that may accidentally emit evolved schema before corresponding
                // schema change events.
                createTableEventCache.stream()
                        .filter(
                                event ->
                                        !alreadySendCreateTableTables.contains(
                                                MySqlSchemaUtils.toDbzTableId(event.tableId())))
                        .forEach(output::collect);
            } else {
                // In Binlog only mode, we simply emit all schemas at once.
                createTableEventCache.forEach(output::collect);
            }
        }
        super.processElement(element, output, splitState);
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
                String.format("SHOW CREATE TABLE `%s`.`%s`", tableId.catalog(), tableId.table());
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
                    String.format("DESC `%s`.`%s`", tableId.catalog(), tableId.table()),
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

        List<Column> columns = table.columns();
        Schema.Builder tableBuilder = Schema.newBuilder();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);

            String colName = column.name();
            DataType dataType = MySqlTypeUtils.fromDbzColumn(column);
            if (!column.isOptional()) {
                dataType = dataType.notNull();
            }
            tableBuilder.physicalColumn(
                    colName,
                    dataType,
                    column.comment(),
                    column.defaultValueExpression().orElse(null));
        }

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
            mySqlAntlrDdlParser = new MySqlAntlrDdlParser();
        }
        return mySqlAntlrDdlParser;
    }

    private List<CreateTableEvent> generateCreateTableEvent(MySqlSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            List<CreateTableEvent> createTableEventCache = new ArrayList<>();
            List<TableId> capturedTableIds =
                    listTables(
                            jdbc, sourceConfig.getDatabaseFilter(), sourceConfig.getTableFilter());
            for (TableId tableId : capturedTableIds) {
                Schema schema = getSchema(jdbc, tableId);
                createTableEventCache.add(
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
