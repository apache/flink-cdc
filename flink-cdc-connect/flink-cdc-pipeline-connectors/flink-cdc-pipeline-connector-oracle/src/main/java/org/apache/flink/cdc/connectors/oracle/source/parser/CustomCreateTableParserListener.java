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

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.connectors.oracle.source.parser.listener.BaseParserListener;

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

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/** CreateTableParserListener. */
public class CustomCreateTableParserListener extends BaseParserListener {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CustomCreateTableParserListener.class);

    private final List<ParseTreeListener> listeners;

    private final LinkedList<SchemaChangeEvent> changes;

    private org.apache.flink.cdc.common.event.TableId currentTable;

    private TableEditor tableEditor;
    private String catalogName;
    private String schemaName;
    private OracleDdlParser parser;
    private CustomColumnDefinitionParserListener columnDefinitionParserListener;
    private String inlinePrimaryKey;

    CustomCreateTableParserListener(
            final String catalogName,
            final String schemaName,
            final OracleDdlParser parser,
            final List<ParseTreeListener> listeners,
            final LinkedList<SchemaChangeEvent> changes) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
        this.listeners = listeners;
        this.changes = changes;
    }

    @Override
    public void enterCreate_table(PlSqlParser.Create_tableContext ctx) {
        if (ctx.relational_table() == null) {
            throw new ParsingException(null, "Only relational tables are supported");
        }
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.tableview_name()));
        if (parser.getTableFilter().isIncluded(tableId)) {
            if (parser.databaseTables().forTable(tableId) == null) {
                tableEditor = parser.databaseTables().editOrCreateTable(tableId);
                super.enterCreate_table(ctx);
            }
        } else {
            LOGGER.debug("Ignoring CREATE TABLE statement for non-captured table {}", tableId);
        }
    }

    @Override
    public void exitCreate_table(PlSqlParser.Create_tableContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (inlinePrimaryKey != null) {
                        if (!tableEditor.primaryKeyColumnNames().isEmpty()) {
                            throw new ParsingException(
                                    null,
                                    "Can only specify in-line or out-of-line primary keys but not both");
                        }
                        tableEditor.setPrimaryKeyNames(inlinePrimaryKey);
                    }

                    Table table = getTable();
                    assert table != null;
                    parser.runIfNotNull(
                            () -> {
                                listeners.remove(columnDefinitionParserListener);
                                columnDefinitionParserListener = null;
                                parser.databaseTables().overwriteTable(table);
                                parser.signalCreateTable(tableEditor.tableId(), ctx);
                            },
                            table);
                },
                tableEditor);

        super.exitCreate_table(ctx);
    }

    @Override
    public void enterColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        parser.runIfNotNull(
                () -> {
                    String columnName = getColumnName(ctx.column_name());
                    ColumnEditor columnEditor = Column.editor().name(columnName);
                    if (columnDefinitionParserListener == null) {
                        columnDefinitionParserListener =
                                new CustomColumnDefinitionParserListener(
                                        tableEditor, columnEditor, parser, listeners);
                        columnDefinitionParserListener.enterColumn_definition(ctx);
                        listeners.add(columnDefinitionParserListener);
                    } else {
                        columnDefinitionParserListener.setColumnEditor(columnEditor);
                    }
                },
                tableEditor);
        super.enterColumn_definition(ctx);
    }

    @Override
    public void exitColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        parser.runIfNotNull(
                () -> tableEditor.addColumn(columnDefinitionParserListener.getColumn()),
                tableEditor,
                columnDefinitionParserListener);
        super.exitColumn_definition(ctx);
    }

    @Override
    public void exitInline_constraint(PlSqlParser.Inline_constraintContext ctx) {
        if (ctx.PRIMARY() != null) {
            if (ctx.getParent() instanceof PlSqlParser.Column_definitionContext) {
                PlSqlParser.Column_definitionContext columnCtx =
                        (PlSqlParser.Column_definitionContext) ctx.getParent();
                inlinePrimaryKey = getColumnName(columnCtx.column_name());
            }
        }
        super.exitInline_constraint(ctx);
    }

    @Override
    public void exitOut_of_line_constraint(PlSqlParser.Out_of_line_constraintContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (ctx.PRIMARY() != null) {
                        if (inlinePrimaryKey != null) {
                            throw new ParsingException(
                                    null, "Cannot specify inline and out of line primary keys");
                        }
                        List<String> pkColumnNames =
                                ctx.column_name().stream()
                                        .map(this::getColumnName)
                                        .collect(Collectors.toList());

                        tableEditor.setPrimaryKeyNames(pkColumnNames);
                    }
                },
                tableEditor);
        super.exitOut_of_line_constraint(ctx);
    }

    private Table getTable() {
        return tableEditor != null ? tableEditor.create() : null;
    }

    private org.apache.flink.cdc.common.event.TableId toCdcTableId(TableId dbzTableId) {
        return org.apache.flink.cdc.common.event.TableId.tableId(
                dbzTableId.catalog(), dbzTableId.table());
    }
}
