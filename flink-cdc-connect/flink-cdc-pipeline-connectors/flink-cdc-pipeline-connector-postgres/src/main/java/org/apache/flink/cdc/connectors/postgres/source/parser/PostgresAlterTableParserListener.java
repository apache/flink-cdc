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

package org.apache.flink.cdc.connectors.postgres.source.parser;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.postgres.utils.PostgresSchemaUtils;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import org.antlr.v4.runtime.tree.ParseTreeListener;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Parser listener that is parsing Postgres ALTER TABLE statements. */
public class PostgresAlterTableParserListener extends PostgreSQLParserBaseListener {

    private final List<ParseTreeListener> listeners;
    private final PostgresAntlrDdlParser parser;
    private final LinkedList<SchemaChangeEvent> changes;
    private final PostgresConnectorConfig dbzConfig;
    private TableEditor tableEditor;

    private ColumnDefinitionParserListener columnDefinitionListener;

    public PostgresAlterTableParserListener(
            List<ParseTreeListener> listeners,
            PostgresAntlrDdlParser parser,
            LinkedList<SchemaChangeEvent> changes,
            PostgresConnectorConfig dbzConfig) {
        this.listeners = listeners;
        this.parser = parser;
        this.changes = changes;
        this.dbzConfig = dbzConfig;
    }

    @Override
    public void enterCreatestmt(PostgreSQLParser.CreatestmtContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.qualified_name(0));
        if (!ctx.opttableelementlist().isEmpty()) {
            tableEditor = parser.databaseTables().editOrCreateTable(tableId);
            columnDefinitionListener = new ColumnDefinitionParserListener(parser, tableEditor);
            listeners.add(columnDefinitionListener);
        }
        super.exitCreatestmt(ctx);
    }

    @Override
    public void exitCreatestmt(PostgreSQLParser.CreatestmtContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Schema.Builder builder = Schema.newBuilder();
                    tableEditor.columns().forEach(column -> builder.column(toCdcColumn(column)));
                    if (tableEditor.hasPrimaryKey()) {
                        builder.primaryKey(tableEditor.primaryKeyColumnNames());
                    }
                    builder.comment(tableEditor.create().comment());
                    changes.add(
                            new CreateTableEvent(
                                    toCdcTableId(tableEditor.tableId()), builder.build()));
                    tableEditor = null;
                    listeners.remove(columnDefinitionListener);
                },
                tableEditor);
        super.exitCreatestmt(ctx);
    }

    // Currently, Postgres event triggers do not support the `truncate table` command.
    @Override
    public void exitTruncatestmt(PostgreSQLParser.TruncatestmtContext ctx) {
        TableId tableId =
                parser.parseQualifiedTableId(
                        ctx.relation_expr_list().relation_expr(0).qualified_name());
        changes.add(new TruncateTableEvent(toCdcTableId(tableId)));
        super.exitTruncatestmt(ctx);
    }

    @Override
    public void exitDropstmt(PostgreSQLParser.DropstmtContext ctx) {
        if (ctx.object_type_any_name() != null && ctx.object_type_any_name().TABLE() != null) {
            ctx.any_name_list_()
                    .any_name()
                    .forEach(
                            anyNameCtx -> {
                                TableId tableId = parser.parseQualifiedTableId(anyNameCtx);
                                changes.add(new DropTableEvent(toCdcTableId(tableId)));
                            });
        }
        super.exitDropstmt(ctx);
    }

    @Override
    public void enterAltertablestmt(PostgreSQLParser.AltertablestmtContext ctx) {
        if (ctx.TABLE() != null && !ctx.relation_expr().isEmpty()) {
            TableId tableId = parser.parseQualifiedTableId(ctx.relation_expr().qualified_name());
            tableEditor = parser.databaseTables().editOrCreateTable(tableId);
        }
        super.enterAltertablestmt(ctx);
    }

    @Override
    public void exitAltertablestmt(PostgreSQLParser.AltertablestmtContext ctx) {
        parser.runIfNotNull(() -> tableEditor = null, tableEditor);
        super.exitAltertablestmt(ctx);
    }

    @Override
    public void enterAlter_table_cmd(PostgreSQLParser.Alter_table_cmdContext ctx) {
        if (isAddColumn(ctx)) {
            columnDefinitionListener = new ColumnDefinitionParserListener(parser, tableEditor);
            listeners.add(columnDefinitionListener);
        }
        super.enterAlter_table_cmd(ctx);
    }

    @Override
    public void exitAlter_table_cmd(PostgreSQLParser.Alter_table_cmdContext ctx) {
        if (isDropColumn(ctx)) {
            String removedColName = parser.removeQuotes(ctx.colid(0));
            changes.add(
                    new DropColumnEvent(
                            toCdcTableId(tableEditor.tableId()),
                            Collections.singletonList(removedColName)));
        } else if (isAddColumn(ctx)) {
            changes.add(
                    new AddColumnEvent(
                            toCdcTableId(tableEditor.tableId()),
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            toCdcColumn(columnDefinitionListener.getColumn())))));
            listeners.remove(columnDefinitionListener);
        } else if (isAlterColumnType(ctx)) {
            String columnName = parser.removeQuotes(ctx.colid(0));
            ColumnEditor columnEditor = Column.editor().name(columnName);
            parser.resolveColumnDataType(ctx.typename(), columnEditor);
            Map<String, DataType> typeMapping = new HashMap<>();
            typeMapping.put(columnName, toCdcColumn(columnEditor.create()).getType());
            changes.add(new AlterColumnTypeEvent(toCdcTableId(tableEditor.tableId()), typeMapping));
        } else if (isChangeOptional(ctx)) {
            // Unable to retrieve raw data type, drop column event simply makes the column nullable.
            String columnName = parser.removeQuotes(ctx.colid(0));
            changes.add(
                    new DropColumnEvent(
                            toCdcTableId(tableEditor.tableId()),
                            Collections.singletonList(columnName)));
        }
        super.exitAlter_table_cmd(ctx);
    }

    @Override
    public void exitRenamestmt(PostgreSQLParser.RenamestmtContext ctx) {
        if (ctx.TABLE() != null && ctx.CONSTRAINT() == null && ctx.name().size() == 2) {
            TableId tableId = parser.parseQualifiedTableId(ctx.relation_expr().qualified_name());
            Map<String, String> renameMap = new HashMap<>();
            List<String> names =
                    ctx.name().stream()
                            .map(name -> parser.removeQuotes(name.colid()))
                            .collect(Collectors.toList());
            renameMap.put(names.get(0), names.get(1));
            changes.add(new RenameColumnEvent(toCdcTableId(tableId), renameMap));
        }
    }

    private boolean isDropColumn(PostgreSQLParser.Alter_table_cmdContext ctx) {
        return ctx.ALTER() == null
                && ctx.DROP() != null
                && !ctx.colid().isEmpty()
                && ctx.colid().size() == 1;
    }

    private boolean isAddColumn(PostgreSQLParser.Alter_table_cmdContext ctx) {
        return ctx.ADD_P() != null && ctx.columnDef() != null;
    }

    private boolean isAlterColumnType(PostgreSQLParser.Alter_table_cmdContext ctx) {
        return ctx.ALTER() != null && ctx.TYPE_P() != null && ctx.typename() != null;
    }

    private boolean isChangeOptional(PostgreSQLParser.Alter_table_cmdContext ctx) {
        return ctx.ALTER() != null
                && ctx.DROP() != null
                && ctx.NOT() != null
                && ctx.NULL_P() != null;
    }

    private org.apache.flink.cdc.common.schema.Column toCdcColumn(Column dbzColumn) {
        return PostgresSchemaUtils.toColumn(dbzColumn, dbzConfig, null);
    }

    private org.apache.flink.cdc.common.event.TableId toCdcTableId(TableId dbzTableId) {
        return org.apache.flink.cdc.common.event.TableId.tableId(
                dbzTableId.schema(), dbzTableId.table());
    }
}
