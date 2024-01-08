/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.connectors.mysql.schema.MySqlFieldDefinition;
import com.ververica.cdc.connectors.mysql.schema.MySqlTableDefinition;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.connectors.mysql.utils.MySqlTypeUtils;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
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

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.openJdbcConnection;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isLowWatermarkEvent;
import static com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils.listTables;

/** The {@link RecordEmitter} implementation for pipeline mysql connector. */
public class MySqlPipelineRecordEmitter extends MySqlRecordEmitter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlPipelineRecordEmitter.class);

    private final MySqlSourceConfig sourceConfig;
    private MySqlAntlrDdlParser mySqlAntlrDdlParser;

    // Used when startup mode is initial
    private Set<TableId> alreadySendCreateTableTables;

    // Used when startup mode is not initial
    private boolean alreadySendCreateTableForBinlogSplit = false;
    private final List<CreateTableEvent> createTableEventCache;

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
        this.createTableEventCache = new ArrayList<>();

        if (!sourceConfig.getStartupOptions().startupMode.equals(StartupMode.INITIAL)) {
            try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
                List<TableId> capturedTableIds = listTables(jdbc, sourceConfig.getTableFilters());
                for (TableId tableId : capturedTableIds) {
                    Schema schema = getSchema(jdbc, tableId);
                    createTableEventCache.add(
                            new CreateTableEvent(
                                    com.ververica.cdc.common.event.TableId.tableId(
                                            tableId.catalog(), tableId.table()),
                                    schema));
                }
            } catch (SQLException e) {
                throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
            }
        }
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<Event> output, MySqlSplitState splitState)
            throws Exception {
        if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            TableId tableId = splitState.asSnapshotSplitState().toMySqlSplit().getTableId();
            if (!alreadySendCreateTableTables.contains(tableId)) {
                try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
                    sendCreateTableEvent(jdbc, tableId, output);
                    alreadySendCreateTableTables.add(tableId);
                }
            }
        } else if (splitState.isBinlogSplitState()
                && !alreadySendCreateTableForBinlogSplit
                && !sourceConfig.getStartupOptions().startupMode.equals(StartupMode.INITIAL)) {
            createTableEventCache.forEach(output::collect);
            alreadySendCreateTableForBinlogSplit = true;
        }
        super.processElement(element, output, splitState);
    }

    private void sendCreateTableEvent(
            JdbcConnection jdbc, TableId tableId, SourceOutput<Event> output) {
        Schema schema = getSchema(jdbc, tableId);
        output.collect(
                new CreateTableEvent(
                        com.ververica.cdc.common.event.TableId.tableId(
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
            tableBuilder.physicalColumn(colName, dataType, column.comment());
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
}
