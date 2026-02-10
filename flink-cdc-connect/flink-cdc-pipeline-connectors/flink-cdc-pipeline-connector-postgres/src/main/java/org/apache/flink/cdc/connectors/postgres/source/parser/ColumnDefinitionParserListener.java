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

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;

/** Parser listener that is parsing column definition part of Postgres statements. */
public class ColumnDefinitionParserListener extends PostgreSQLParserBaseListener {

    private final PostgresAntlrDdlParser parser;
    private final TableEditor tableEditor;

    private ColumnEditor columnEditor;

    public ColumnDefinitionParserListener(PostgresAntlrDdlParser parser, TableEditor tableEditor) {
        this.parser = parser;
        this.tableEditor = tableEditor;
    }

    public Column getColumn() {
        return columnEditor.create();
    }

    @Override
    public void enterColumnDef(PostgreSQLParser.ColumnDefContext ctx) {
        if (ctx.colid().identifier() != null) {
            String columnName = parser.removeQuotes(ctx.colid());
            columnEditor = Column.editor().name(columnName);
        } else {
            columnEditor = Column.editor().name(ctx.getText());
        }
        super.exitColumnDef(ctx);
    }

    @Override
    public void exitColumnDef(PostgreSQLParser.ColumnDefContext ctx) {
        parser.resolveColumnDataType(ctx.typename(), columnEditor);
        tableEditor.addColumn(columnEditor.create());
        super.exitColumnDef(ctx);
    }

    @Override
    public void exitColconstraintelem(PostgreSQLParser.ColconstraintelemContext ctx) {
        if (ctx.b_expr() != null) {
            columnEditor.defaultValueExpression(ctx.b_expr().getText());
        }
        if (ctx.PRIMARY() != null) {
            columnEditor.optional(false);
            tableEditor.setPrimaryKeyNames(columnEditor.name());
        }
        if (ctx.NOT() != null) {
            columnEditor.optional(false);
        }
        super.enterColconstraintelem(ctx);
    }
}
