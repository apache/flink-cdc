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

package org.apache.flink.cdc.connectors.mysql.source.parser;

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

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.antlr.listener.AlterTableParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mysql.utils.MySqlTypeUtils.fromDbzColumn;

/** Copied from {@link AlterTableParserListener} in Debezium 1.9.8.Final. */
public class CustomAlterTableParserListener extends MySqlParserBaseListener {

    private static final int STARTING_INDEX = 1;

    private static final Logger LOG = LoggerFactory.getLogger(CustomAlterTableParserListener.class);

    private final MySqlAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;
    private final LinkedList<SchemaChangeEvent> changes;
    private final boolean tinyInt1isBit;
    private org.apache.flink.cdc.common.event.TableId currentTable;
    private List<ColumnEditor> columnEditors;
    private CustomColumnDefinitionParserListener columnDefinitionListener;
    private TableEditor tableEditor;
    private boolean isTableIdCaseInsensitive;
    private int parsingColumnIndex = STARTING_INDEX;

    public CustomAlterTableParserListener(
            MySqlAntlrDdlParser parser,
            List<ParseTreeListener> listeners,
            LinkedList<SchemaChangeEvent> changes,
            boolean tinyInt1isBit,
            boolean isTableIdCaseInsensitive) {
        this.parser = parser;
        this.listeners = listeners;
        this.changes = changes;
        this.tinyInt1isBit = tinyInt1isBit;
        this.isTableIdCaseInsensitive = isTableIdCaseInsensitive;
    }

    @Override
    public void exitCopyCreateTable(MySqlParser.CopyCreateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName(0).fullId());
        TableId originalTableId = parser.parseQualifiedTableId(ctx.tableName(1).fullId());
        Table original = parser.databaseTables().forTable(originalTableId);
        if (original != null) {
            parser.databaseTables()
                    .overwriteTable(
                            tableId,
                            original.columns(),
                            original.primaryKeyColumnNames(),
                            original.defaultCharsetName());
            parser.signalCreateTable(tableId, ctx);
            Schema.Builder builder = Schema.newBuilder();
            original.columns().forEach(column -> builder.column(toCdcColumn(column)));
            if (!original.primaryKeyColumnNames().isEmpty()) {
                builder.primaryKey(original.primaryKeyColumnNames());
            }
            changes.add(new CreateTableEvent(toCdcTableId(tableId), builder.build()));
        } else {
            LOG.warn(
                    "Could not find schema of {} while parsing sql of `CREATE TABLE {} LIKE {}`",
                    originalTableId,
                    tableId,
                    originalTableId);
        }
        super.exitCopyCreateTable(ctx);
    }

    @Override
    public void enterColumnCreateTable(MySqlParser.ColumnCreateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        if (parser.databaseTables().forTable(tableId) == null) {
            tableEditor = parser.databaseTables().editOrCreateTable(tableId);
        }
        super.enterColumnCreateTable(ctx);
    }

    @Override
    public void exitColumnCreateTable(MySqlParser.ColumnCreateTableContext ctx) {
        parser.runIfNotNull(
                () -> {
                    // Make sure that the table's character set has been set ...
                    if (!tableEditor.hasDefaultCharsetName()) {
                        tableEditor.setDefaultCharsetName(
                                parser.charsetForTable(tableEditor.tableId()));
                    }
                    listeners.remove(columnDefinitionListener);
                    columnDefinitionListener = null;
                    // remove column definition parser listener
                    final String defaultCharsetName = tableEditor.create().defaultCharsetName();
                    tableEditor.setColumns(
                            tableEditor.columns().stream()
                                    .map(
                                            column -> {
                                                final ColumnEditor columnEditor = column.edit();
                                                if (columnEditor.charsetNameOfTable() == null) {
                                                    columnEditor.charsetNameOfTable(
                                                            defaultCharsetName);
                                                }
                                                return columnEditor;
                                            })
                                    .map(ColumnEditor::create)
                                    .collect(Collectors.toList()));
                    parser.databaseTables().overwriteTable(tableEditor.create());
                    parser.signalCreateTable(tableEditor.tableId(), ctx);

                    Schema.Builder builder = Schema.newBuilder();
                    tableEditor.columns().forEach(column -> builder.column(toCdcColumn(column)));
                    if (tableEditor.hasPrimaryKey()) {
                        builder.primaryKey(tableEditor.primaryKeyColumnNames());
                    }
                    builder.comment(tableEditor.create().comment());
                    changes.add(
                            new CreateTableEvent(
                                    toCdcTableId(tableEditor.tableId()), builder.build()));
                },
                tableEditor);
        super.exitColumnCreateTable(ctx);
    }

    @Override
    public void enterColumnDeclaration(MySqlParser.ColumnDeclarationContext ctx) {
        parser.runIfNotNull(
                () -> {
                    String columnName = parser.parseName(ctx.uid());
                    ColumnEditor columnEditor = Column.editor().name(columnName);
                    if (columnDefinitionListener == null) {
                        columnDefinitionListener =
                                new CustomColumnDefinitionParserListener(
                                        tableEditor, columnEditor, parser, listeners);
                        listeners.add(columnDefinitionListener);
                    } else {
                        columnDefinitionListener.setColumnEditor(columnEditor);
                    }
                },
                tableEditor);
        super.enterColumnDeclaration(ctx);
    }

    @Override
    public void exitColumnDeclaration(MySqlParser.ColumnDeclarationContext ctx) {
        parser.runIfNotNull(
                () -> {
                    tableEditor.addColumn(columnDefinitionListener.getColumn());
                },
                tableEditor,
                columnDefinitionListener);
        super.exitColumnDeclaration(ctx);
    }

    @Override
    public void enterPrimaryKeyTableConstraint(MySqlParser.PrimaryKeyTableConstraintContext ctx) {
        parser.runIfNotNull(
                () -> {
                    parser.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
                },
                tableEditor);
        super.enterPrimaryKeyTableConstraint(ctx);
    }

    @Override
    public void enterUniqueKeyTableConstraint(MySqlParser.UniqueKeyTableConstraintContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (!tableEditor.hasPrimaryKey()) {
                        parser.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
                    }
                },
                tableEditor);
        super.enterUniqueKeyTableConstraint(ctx);
    }

    @Override
    public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
        this.currentTable = toCdcTableId(parser.parseQualifiedTableId(ctx.tableName().fullId()));
        super.enterAlterTable(ctx);
    }

    @Override
    public void exitAlterTable(MySqlParser.AlterTableContext ctx) {
        listeners.remove(columnDefinitionListener);
        super.exitAlterTable(ctx);
        this.currentTable = null;
    }

    @Override
    public void enterAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        String columnName = parser.parseName(ctx.uid(0));
        ColumnEditor columnEditor = Column.editor().name(columnName);
        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(
                        tableEditor, columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void exitAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    if (ctx.FIRST() != null) {
                        changes.add(
                                new AddColumnEvent(
                                        currentTable,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        toCdcColumn(column),
                                                        AddColumnEvent.ColumnPosition.FIRST,
                                                        null))));
                    } else if (ctx.AFTER() != null) {
                        String afterColumn = parser.parseName(ctx.uid(1));
                        if (isTableIdCaseInsensitive) {
                            afterColumn = afterColumn.toLowerCase(Locale.ROOT);
                        }
                        changes.add(
                                new AddColumnEvent(
                                        currentTable,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        toCdcColumn(column),
                                                        AddColumnEvent.ColumnPosition.AFTER,
                                                        afterColumn))));
                    } else {
                        changes.add(
                                new AddColumnEvent(
                                        currentTable,
                                        Collections.singletonList(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        toCdcColumn(column)))));
                    }
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void enterAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        // multiple columns are added. Initialize a list of column editors for them
        columnEditors = new ArrayList<>(ctx.uid().size());
        for (MySqlParser.UidContext uidContext : ctx.uid()) {
            String columnName = parser.parseName(uidContext);
            columnEditors.add(Column.editor().name(columnName));
        }
        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(
                        tableEditor, columnEditors.get(0), parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByAddColumns(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (columnEditors != null) {
                        // column editor list is not null when a multiple columns are parsed in one
                        // statement
                        if (columnEditors.size() > parsingColumnIndex) {
                            // assign next column editor to parse another column definition
                            columnDefinitionListener.setColumnEditor(
                                    columnEditors.get(parsingColumnIndex++));
                        }
                    }
                },
                columnEditors);
        super.exitColumnDefinition(ctx);
    }

    @Override
    public void exitAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        parser.runIfNotNull(
                () -> {
                    List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
                    columnEditors.forEach(
                            columnEditor -> {
                                Column column = columnEditor.create();
                                addedColumns.add(
                                        new AddColumnEvent.ColumnWithPosition(toCdcColumn(column)));
                            });
                    changes.add(new AddColumnEvent(currentTable, addedColumns));
                    listeners.remove(columnDefinitionListener);
                    columnEditors = null;
                    parsingColumnIndex = STARTING_INDEX;
                },
                columnEditors);
        super.exitAlterByAddColumns(ctx);
    }

    @Override
    public void enterAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        String oldColumnName = parser.parseName(ctx.oldColumn);
        ColumnEditor columnEditor = Column.editor().name(oldColumnName);
        columnEditor.unsetDefaultValueExpression();

        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(
                        tableEditor, columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByChangeColumn(ctx);
    }

    @Override
    public void exitAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    String oldColumnName =
                            isTableIdCaseInsensitive
                                    ? column.name().toLowerCase(Locale.ROOT)
                                    : column.name();
                    String newColumnName = parser.parseName(ctx.newColumn);
                    if (isTableIdCaseInsensitive && newColumnName != null) {
                        newColumnName = newColumnName.toLowerCase(Locale.ROOT);
                    }

                    Map<String, DataType> typeMapping = new HashMap<>();

                    typeMapping.put(oldColumnName, fromDbzColumn(column, tinyInt1isBit));
                    changes.add(new AlterColumnTypeEvent(currentTable, typeMapping));

                    if (newColumnName != null && !oldColumnName.equalsIgnoreCase(newColumnName)) {
                        Map<String, String> renameMap = new HashMap<>();
                        renameMap.put(oldColumnName, newColumnName);
                        changes.add(new RenameColumnEvent(currentTable, renameMap));
                    }
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByChangeColumn(ctx);
    }

    @Override
    public void enterAlterByDropColumn(MySqlParser.AlterByDropColumnContext ctx) {
        String removedColName = parser.parseName(ctx.uid());
        if (isTableIdCaseInsensitive && removedColName != null) {
            removedColName = removedColName.toLowerCase(Locale.ROOT);
        }
        changes.add(new DropColumnEvent(currentTable, Collections.singletonList(removedColName)));
        super.enterAlterByDropColumn(ctx);
    }

    @Override
    public void enterAlterByRenameColumn(MySqlParser.AlterByRenameColumnContext ctx) {
        String oldColumnName = parser.parseName(ctx.oldColumn);
        ColumnEditor columnEditor = Column.editor().name(oldColumnName);
        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(
                        tableEditor, columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByRenameColumn(ctx);
    }

    @Override
    public void enterAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        String oldColumnName = parser.parseName(ctx.uid(0));
        ColumnEditor columnEditor = Column.editor().name(oldColumnName);
        columnEditor.unsetDefaultValueExpression();

        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(
                        tableEditor, columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByModifyColumn(ctx);
    }

    @Override
    public void exitAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    Map<String, DataType> typeMapping = new HashMap<>();
                    typeMapping.put(
                            isTableIdCaseInsensitive
                                    ? column.name().toLowerCase(Locale.ROOT)
                                    : column.name(),
                            fromDbzColumn(column, tinyInt1isBit));
                    changes.add(new AlterColumnTypeEvent(currentTable, typeMapping));
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByModifyColumn(ctx);
    }

    @Override
    public void exitAlterByRenameColumn(MySqlParser.AlterByRenameColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    String oldColumnName =
                            isTableIdCaseInsensitive
                                    ? column.name().toLowerCase(Locale.ROOT)
                                    : column.name();
                    String newColumnName = parser.parseName(ctx.newColumn);
                    if (isTableIdCaseInsensitive && newColumnName != null) {
                        newColumnName = newColumnName.toLowerCase(Locale.ROOT);
                    }
                    if (newColumnName != null && !column.name().equalsIgnoreCase(newColumnName)) {
                        Map<String, String> renameMap = new HashMap<>();
                        renameMap.put(oldColumnName, newColumnName);
                        changes.add(new RenameColumnEvent(currentTable, renameMap));
                    }
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByRenameColumn(ctx);
    }

    @Override
    public void exitTruncateTable(MySqlParser.TruncateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        changes.add(new TruncateTableEvent(toCdcTableId(tableId)));
        super.exitTruncateTable(ctx);
    }

    @Override
    public void exitDropTable(MySqlParser.DropTableContext ctx) {
        ctx.tables()
                .tableName()
                .forEach(
                        evt -> {
                            TableId tableId = parser.parseQualifiedTableId(evt.fullId());
                            changes.add(new DropTableEvent(toCdcTableId(tableId)));
                        });
        super.exitDropTable(ctx);
    }

    @Override
    public void enterTableOptionComment(MySqlParser.TableOptionCommentContext ctx) {
        if (!parser.skipComments()) {
            parser.runIfNotNull(
                    () -> {
                        if (ctx.COMMENT() != null) {
                            tableEditor.setComment(
                                    parser.withoutQuotes(ctx.STRING_LITERAL().getText()));
                        }
                    },
                    tableEditor);
        }
        super.enterTableOptionComment(ctx);
    }

    private org.apache.flink.cdc.common.schema.Column toCdcColumn(Column dbzColumn) {
        return org.apache.flink.cdc.common.schema.Column.physicalColumn(
                isTableIdCaseInsensitive
                        ? dbzColumn.name().toLowerCase(Locale.ROOT)
                        : dbzColumn.name(),
                fromDbzColumn(dbzColumn, tinyInt1isBit),
                dbzColumn.comment(),
                dbzColumn.defaultValueExpression().orElse(null));
    }

    private org.apache.flink.cdc.common.event.TableId toCdcTableId(TableId dbzTableId) {
        if (isTableIdCaseInsensitive) {
            return org.apache.flink.cdc.common.event.TableId.tableId(
                    dbzTableId.catalog().toLowerCase(Locale.ROOT),
                    dbzTableId.table().toLowerCase(Locale.ROOT));
        } else {
            return org.apache.flink.cdc.common.event.TableId.tableId(
                    dbzTableId.catalog(), dbzTableId.table());
        }
    }
}
