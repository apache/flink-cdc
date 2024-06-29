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

package org.apache.flink.cdc.connectors.oracle.source.parser.listener;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

import java.util.List;
import java.util.stream.Collectors;

/** This class is parsing Oracle table's column comment statements. */
public class CommentParserListener extends BaseParserListener {
    private final String catalogName;
    private final String schemaName;
    private final OracleDdlParser parser;
    private TableEditor tableEditor;

    public CommentParserListener(
            final String catalogName, final String schemaName, final OracleDdlParser parser) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
    }

    @Override
    public void enterComment_on_column(PlSqlParser.Comment_on_columnContext ctx) {
        if (!parser.skipComments()) {
            TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.column_name()));
            if (parser.getTableFilter().isIncluded(tableId)) {
                Table table = parser.databaseTables().forTable(tableId);
                if (table != null) {
                    tableEditor = parser.databaseTables().editTable(tableId);
                    parser.runIfNotNull(
                            () -> {
                                String column = getColumnName(ctx.column_name());
                                String comment =
                                        parser.withoutQuotes(ctx.quoted_string().getText());
                                List<Column> columns =
                                        table.columns().stream()
                                                .map(
                                                        m -> {
                                                            if (m.name().equalsIgnoreCase(column)) {
                                                                m =
                                                                        m.edit()
                                                                                .comment(comment)
                                                                                .create();
                                                            }
                                                            return m;
                                                        })
                                                .collect(Collectors.toList());
                                tableEditor.setColumns(columns);
                            },
                            tableEditor);
                    super.enterComment_on_column(ctx);
                }
            }
        }
    }

    @Override
    public void exitComment_on_column(PlSqlParser.Comment_on_columnContext ctx) {
        if (!parser.skipComments()) {
            parser.runIfNotNull(
                    () -> {
                        parser.databaseTables().overwriteTable(tableEditor.create());
                        parser.signalCreateTable(tableEditor.tableId(), ctx);
                    },
                    tableEditor);
        }
        super.exitComment_on_column(ctx);
    }

    @Override
    public void enterComment_on_table(PlSqlParser.Comment_on_tableContext ctx) {
        if (!parser.skipComments()) {
            TableId tableId =
                    new TableId(catalogName, schemaName, getTableName(ctx.tableview_name()));
            if (parser.getTableFilter().isIncluded(tableId)) {
                if (parser.databaseTables().forTable(tableId) != null) {
                    tableEditor = parser.databaseTables().editTable(tableId);
                    parser.runIfNotNull(
                            () -> {
                                tableEditor.setComment(
                                        parser.withoutQuotes(ctx.quoted_string().getText()));
                            },
                            tableEditor);
                    super.enterComment_on_table(ctx);
                }
            }
        }
    }

    @Override
    public void exitComment_on_table(PlSqlParser.Comment_on_tableContext ctx) {
        if (!parser.skipComments()) {
            parser.runIfNotNull(
                    () -> {
                        parser.databaseTables().overwriteTable(tableEditor.create());
                        parser.signalCreateTable(tableEditor.tableId(), ctx);
                    },
                    tableEditor);
        }
        super.exitComment_on_table(ctx);
    }
}
