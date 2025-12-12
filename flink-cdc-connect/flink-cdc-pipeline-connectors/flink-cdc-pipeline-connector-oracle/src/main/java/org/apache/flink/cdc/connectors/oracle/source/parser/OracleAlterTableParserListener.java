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

package org.apache.flink.cdc.connectors.oracle.source.parser;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.types.DataType;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.debezium.antlr.AntlrDdlParser.getText;
import static org.apache.flink.cdc.connectors.oracle.utils.OracleTypeUtils.fromDbzColumn;

/** Parser listener that is parsing Oracle ALTER TABLE statements. */
public class OracleAlterTableParserListener extends BaseParserListener {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    io.debezium.connector.oracle.antlr.listener.AlterTableParserListener.class);

    private static final int STARTING_INDEX = 1;
    private TableEditor tableEditor;
    private final String catalogName;
    private final String schemaName;
    private TableId previousTableId;
    private final OracleDdlParser parser;
    private final List<ParseTreeListener> listeners;
    private ColumnDefinitionParserListener columnDefinitionParserListener;
    private List<ColumnEditor> columnEditors = new ArrayList<>();
    private int parsingColumnIndex = STARTING_INDEX;
    private final LinkedList<SchemaChangeEvent> changes;

    /**
     * Package visible Constructor.
     *
     * @param catalogName Represents database name. If null, points to the current database.
     * @param schemaName Schema/user name. If null, points to the current schema.
     * @param parser Oracle Antlr parser.
     * @param listeners registered listeners.
     */
    public OracleAlterTableParserListener(
            final String catalogName,
            final String schemaName,
            final OracleDdlParser parser,
            final List<ParseTreeListener> listeners,
            LinkedList<SchemaChangeEvent> changes) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
        this.listeners = listeners;
        this.changes = changes;
    }

    @Override
    public void enterAlter_table(PlSqlParser.Alter_tableContext ctx) {
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.tableview_name()));
        tableEditor = Table.editor().tableId(tableId);
        if (tableEditor == null) {
            throw new ParsingException(
                    null,
                    "Trying to alter table "
                            + tableId
                            + ", which does not exist. Query: "
                            + getText(ctx));
        }
        super.enterAlter_table(ctx);
    }

    @Override
    public void exitAlter_table(PlSqlParser.Alter_tableContext ctx) {
        parser.runIfNotNull(
                () -> {
                    listeners.remove(columnDefinitionParserListener);
                    parser.databaseTables().overwriteTable(tableEditor.create());
                    parser.signalAlterTable(
                            tableEditor.tableId(), previousTableId, ctx.getParent());
                },
                tableEditor);
        super.exitAlter_table(ctx);
    }

    @Override
    public void enterAlter_table_properties(PlSqlParser.Alter_table_propertiesContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (ctx.RENAME() != null && ctx.TO() != null) {
                        previousTableId = tableEditor.tableId();
                        String tableName = getTableName(ctx.tableview_name());
                        final TableId newTableId =
                                new TableId(
                                        tableEditor.tableId().catalog(),
                                        tableEditor.tableId().schema(),
                                        tableName);
                        if (parser.getTableFilter().isIncluded(previousTableId)
                                && !parser.getTableFilter().isIncluded(newTableId)) {
                            LOGGER.warn(
                                    "Renaming included table {} to non-included table {}, this can lead to schema inconsistency",
                                    previousTableId,
                                    newTableId);
                        } else if (!parser.getTableFilter().isIncluded(previousTableId)
                                && parser.getTableFilter().isIncluded(newTableId)) {
                            LOGGER.warn(
                                    "Renaming non-included table {} to included table {}, this can lead to schema inconsistency",
                                    previousTableId,
                                    newTableId);
                        }
                        parser.databaseTables().overwriteTable(tableEditor.create());
                        parser.databaseTables().renameTable(tableEditor.tableId(), newTableId);
                        tableEditor = parser.databaseTables().editTable(newTableId);
                    }
                },
                tableEditor);
        super.exitAlter_table_properties(ctx);
    }

    @Override
    public void enterAdd_column_clause(PlSqlParser.Add_column_clauseContext ctx) {
        parser.runIfNotNull(
                () -> {
                    List<PlSqlParser.Column_definitionContext> columns = ctx.column_definition();
                    for (PlSqlParser.Column_definitionContext column : columns) {
                        String columnName = getColumnName(column.column_name());
                        ColumnEditor editor = Column.editor().name(columnName);
                        columnDefinitionParserListener =
                                new ColumnDefinitionParserListener(tableEditor, editor);
                        listeners.add(columnDefinitionParserListener);
                        columnEditors.add(editor);
                    }
                },
                tableEditor);
        super.enterAdd_column_clause(ctx);
    }

    @Override
    public void enterModify_column_clauses(PlSqlParser.Modify_column_clausesContext ctx) {
        parser.runIfNotNull(
                () -> {
                    List<PlSqlParser.Modify_col_propertiesContext> columns =
                            ctx.modify_col_properties();
                    columnEditors = new ArrayList<>(columns.size());
                    for (PlSqlParser.Modify_col_propertiesContext column : columns) {
                        String columnName = getColumnName(column.column_name());
                        Column existingColumn = Column.editor().name(columnName).create();
                        if (existingColumn != null) {
                            ColumnEditor columnEditor = existingColumn.edit();
                            columnDefinitionParserListener =
                                    new ColumnDefinitionParserListener(tableEditor, columnEditor);
                            listeners.add(columnDefinitionParserListener);

                            columnEditors.add(columnEditor);
                        } else {
                            throw new ParsingException(
                                    null,
                                    "trying to change column "
                                            + columnName
                                            + " in "
                                            + tableEditor.tableId().toString()
                                            + " table, which does not exist.  Query: "
                                            + getText(ctx));
                        }
                    }
                },
                tableEditor);
        super.enterModify_column_clauses(ctx);
    }

    @Override
    public void exitAdd_column_clause(PlSqlParser.Add_column_clauseContext ctx) {
        parser.runIfNotNull(
                () -> {
                    columnEditors.forEach(
                            columnEditor -> tableEditor.addColumn(columnEditor.create()));
                    listeners.remove(columnDefinitionParserListener);
                    columnDefinitionParserListener = null;
                },
                tableEditor,
                columnEditors);
        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        for (ColumnEditor editor : columnEditors) {
            addedColumns.add(new AddColumnEvent.ColumnWithPosition(toCdcColumn(editor.create())));
            changes.add(new AddColumnEvent(toCdcTableId(tableEditor.tableId()), addedColumns));
        }
        columnEditors.clear();
        super.exitAdd_column_clause(ctx);
    }

    @Override
    public void exitModify_column_clauses(PlSqlParser.Modify_column_clausesContext ctx) {
        parser.runIfNotNull(
                () -> {
                    columnEditors.forEach(
                            columnEditor -> {
                                Column column = columnEditor.create();
                                tableEditor.addColumn(column);
                                Map<String, DataType> typeMapping = new HashMap<>();
                                typeMapping.put(column.name(), fromDbzColumn(column));
                                changes.add(
                                        new AlterColumnTypeEvent(
                                                toCdcTableId(tableEditor.tableId()), typeMapping));
                            });
                    listeners.remove(columnDefinitionParserListener);
                    columnDefinitionParserListener = null;
                },
                tableEditor,
                columnEditors);
        super.exitModify_column_clauses(ctx);
    }

    @Override
    public void exitColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        columnParseForTableEditor();
        super.exitColumn_definition(ctx);
    }

    private void columnParseForTableEditor() {
        parser.runIfNotNull(
                () -> {
                    if (columnEditors != null) {
                        // column editor list is not null when a multiple columns are parsed in one
                        // statement
                        if (columnEditors.size() > parsingColumnIndex) {
                            // assign next column editor to parse another column definition
                            columnDefinitionParserListener.setColumnEditor(
                                    columnEditors.get(parsingColumnIndex++));
                        } else {
                            // all columns parsed
                            // reset global variables for next parsed statement
                            columnEditors.forEach(
                                    columnEditor -> tableEditor.addColumn(columnEditor.create()));
                            parsingColumnIndex = STARTING_INDEX;
                        }
                    }
                },
                tableEditor,
                columnEditors);
    }

    @Override
    public void exitModify_col_properties(PlSqlParser.Modify_col_propertiesContext ctx) {
        columnParseForTableEditor();
        super.exitModify_col_properties(ctx);
    }

    @Override
    public void enterDrop_column_clause(PlSqlParser.Drop_column_clauseContext ctx) {
        parser.runIfNotNull(
                () -> {
                    List<PlSqlParser.Column_nameContext> columnNameContexts = ctx.column_name();
                    columnEditors = new ArrayList<>(columnNameContexts.size());
                    for (PlSqlParser.Column_nameContext columnNameContext : columnNameContexts) {
                        String columnName = getColumnName(columnNameContext);
                        tableEditor.removeColumn(columnName);
                        changes.add(
                                new DropColumnEvent(
                                        toCdcTableId(tableEditor.tableId()),
                                        Collections.singletonList(columnName)));
                    }
                },
                tableEditor);
        super.enterDrop_column_clause(ctx);
    }

    @Override
    public void exitRename_column_clause(PlSqlParser.Rename_column_clauseContext ctx) {
        parser.runIfNotNull(
                () -> {
                    String oldColumnName = getColumnName(ctx.old_column_name());
                    String newColumnName = getColumnName(ctx.new_column_name());
                    if (newColumnName != null && !oldColumnName.equalsIgnoreCase(newColumnName)) {
                        Map<String, String> renameMap = new HashMap<>();
                        renameMap.put(oldColumnName, newColumnName);
                        changes.add(
                                new RenameColumnEvent(
                                        toCdcTableId(tableEditor.tableId()), renameMap));
                    }
                },
                tableEditor);
        super.exitRename_column_clause(ctx);
    }

    @Override
    public void enterConstraint_clauses(PlSqlParser.Constraint_clausesContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (ctx.ADD() != null) {
                        // ALTER TABLE ADD PRIMARY KEY
                        List<String> primaryKeyColumns = new ArrayList<>();
                        for (PlSqlParser.Out_of_line_constraintContext constraint :
                                ctx.out_of_line_constraint()) {
                            if (constraint.PRIMARY() != null && constraint.KEY() != null) {
                                for (PlSqlParser.Column_nameContext columnNameContext :
                                        constraint.column_name()) {
                                    primaryKeyColumns.add(getColumnName(columnNameContext));
                                }
                            }
                        }
                        if (!primaryKeyColumns.isEmpty()) {
                            tableEditor.setPrimaryKeyNames(primaryKeyColumns);
                        }
                    } else if (ctx.MODIFY() != null && ctx.PRIMARY() != null && ctx.KEY() != null) {
                        // ALTER TABLE MODIFY PRIMARY KEY columns
                        List<String> primaryKeyColumns = new ArrayList<>();
                        for (PlSqlParser.Column_nameContext columnNameContext : ctx.column_name()) {
                            primaryKeyColumns.add(getColumnName(columnNameContext));
                        }
                        if (!primaryKeyColumns.isEmpty()) {
                            tableEditor.setPrimaryKeyNames(primaryKeyColumns);
                        }
                    }
                },
                tableEditor);
        super.enterConstraint_clauses(ctx);
    }

    private org.apache.flink.cdc.common.schema.Column toCdcColumn(Column dbzColumn) {
        return org.apache.flink.cdc.common.schema.Column.physicalColumn(
                dbzColumn.name(), fromDbzColumn(dbzColumn), dbzColumn.comment());
    }

    private org.apache.flink.cdc.common.event.TableId toCdcTableId(TableId dbzTableId) {
        return org.apache.flink.cdc.common.event.TableId.tableId(
                dbzTableId.schema(), dbzTableId.table());
    }
}
