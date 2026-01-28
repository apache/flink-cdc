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

import org.apache.flink.cdc.common.event.SchemaChangeEvent;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.postgresql.PgOid;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.TableId;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/** An ANTLR based parser for Postgres DDL statements. */
public class PostgresAntlrDdlParser extends AntlrDdlParser<PostgreSQLLexer, PostgreSQLParser> {

    private final PostgresConnectorConfig dbzConfig;
    private final LinkedList<SchemaChangeEvent> parsedEvents;

    public PostgresAntlrDdlParser(PostgresConnectorConfig dbzConfig) {
        super(true);
        this.dbzConfig = dbzConfig;
        this.parsedEvents = new LinkedList<>();
    }

    @Override
    protected ParseTree parseTree(PostgreSQLParser parser) {
        return parser.root();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new PostgresAntlrDdlParserListener(this, parsedEvents, dbzConfig);
    }

    @Override
    protected PostgreSQLLexer createNewLexerInstance(CharStream charStreams) {
        return new PostgreSQLLexer(charStreams);
    }

    @Override
    protected PostgreSQLParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new PostgreSQLParser(commonTokenStream);
    }

    @Override
    protected boolean isGrammarInUpperCase() {
        return false;
    }

    @Override
    protected DataTypeResolver initializeDataTypeResolver() {
        return null;
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        return null;
    }

    public List<SchemaChangeEvent> getAndClearParsedEvents() {
        List<SchemaChangeEvent> result = new ArrayList<>(parsedEvents);
        parsedEvents.clear();
        return result;
    }

    public TableId parseQualifiedTableId(PostgreSQLParser.Qualified_nameContext ctx) {
        String s1 = removeQuotes(ctx.colid());
        if (ctx.indirection() != null) {
            List<String> list =
                    ctx.indirection().indirection_el().stream()
                            .map(
                                    indirect ->
                                            removeQuotes(
                                                    indirect.attr_name().colLabel().identifier()))
                            .collect(Collectors.toList());
            switch (list.size()) {
                case 1:
                    return new TableId(null, s1, list.get(0));
                case 2:
                    return new TableId(s1, list.get(0), list.get(1));
                default:
                    throw new RuntimeException(
                            "Unsupported indirection: " + ctx.indirection().getText());
            }
        } else {
            return new TableId(null, "public", s1);
        }
    }

    public TableId parseQualifiedTableId(PostgreSQLParser.Any_nameContext ctx) {
        String s1 = removeQuotes(ctx.colid());
        if (ctx.attrs() != null) {
            List<String> list =
                    ctx.attrs().attr_name().stream()
                            .map(attr -> removeQuotes(attr.colLabel().identifier()))
                            .collect(Collectors.toList());
            switch (list.size()) {
                case 1:
                    return new TableId(null, s1, list.get(0));
                case 2:
                    return new TableId(s1, list.get(0), list.get(1));
                default:
                    throw new RuntimeException("Unsupported attrs: " + ctx.attrs().getText());
            }
        } else {
            return new TableId(null, "public", s1);
        }
    }

    public String removeQuotes(PostgreSQLParser.IdentifierContext identifierCtx) {
        if (identifierCtx.Identifier() != null) {
            return identifierCtx.Identifier().getText().toLowerCase();
        } else if (identifierCtx.QuotedIdentifier() != null) {
            String text = identifierCtx.QuotedIdentifier().getText().toLowerCase();
            return text.substring(1, text.length() - 1).replace("\"\"", "\"");
        } else {
            throw new RuntimeException("Unsupported identifier: " + identifierCtx.getText());
        }
    }

    public String removeQuotes(PostgreSQLParser.ColidContext ctx) {
        if (ctx.identifier() != null) {
            return removeQuotes(ctx.identifier());
        }
        return ctx.getText().toLowerCase();
    }

    public void runIfNotNull(Runnable function, Object... nullableObjects) {
        for (Object nullableObject : nullableObjects) {
            if (nullableObject == null) {
                return;
            }
        }
        function.run();
    }

    public void resolveColumnDataType(
            PostgreSQLParser.TypenameContext ctx, ColumnEditor columnEditor) {
        boolean isArray = !(ctx.ARRAY() == null && ctx.opt_array_bounds() != null);
        PostgreSQLParser.SimpletypenameContext simpleTypename = ctx.simpletypename();
        int length = 1;
        if (simpleTypename.numeric() != null) {
            PostgreSQLParser.NumericContext numeric = simpleTypename.numeric();
            if (numeric.INTEGER() != null || numeric.INT_P() != null) {
                if (isArray) {
                    columnEditor.nativeType(PgOid.INT4_ARRAY);
                } else {
                    columnEditor.nativeType(PgOid.INT4);
                }
            } else if (numeric.SMALLINT() != null) {
                if (isArray) {
                    columnEditor.nativeType(PgOid.INT2_ARRAY);
                } else {
                    columnEditor.nativeType(PgOid.INT2);
                }
            } else if (numeric.BIGINT() != null) {
                if (isArray) {
                    columnEditor.nativeType(PgOid.INT8_ARRAY);
                } else {
                    columnEditor.nativeType(PgOid.INT8);
                }
            } else if (numeric.FLOAT_P() != null || numeric.REAL() != null) {
                if (isArray) {
                    columnEditor.nativeType(PgOid.FLOAT4_ARRAY);
                } else {
                    columnEditor.nativeType(PgOid.FLOAT4);
                }
            } else if (numeric.DOUBLE_P() != null) {
                if (isArray) {
                    columnEditor.nativeType(PgOid.FLOAT8_ARRAY);
                } else {
                    columnEditor.nativeType(PgOid.FLOAT8);
                }
            } else if (numeric.DEC() != null
                    || numeric.DECIMAL_P() != null
                    || numeric.NUMERIC() != null) {
                length = 18;
                int scale = 0;
                if (numeric.type_modifiers_() != null) {
                    PostgreSQLParser.Expr_listContext exprList =
                            numeric.type_modifiers_().expr_list();
                    length = Integer.parseInt(exprList.a_expr(0).getText());
                    scale = Integer.parseInt(exprList.a_expr(1).getText());
                }
                columnEditor.scale(scale);
                if (isArray) {
                    columnEditor.nativeType(PgOid.NUMERIC_ARRAY);
                } else {
                    columnEditor.nativeType(PgOid.NUMERIC);
                }
            } else if (numeric.BOOLEAN_P() != null) {
                if (isArray) {
                    columnEditor.nativeType(PgOid.BOOL_ARRAY);
                } else {
                    columnEditor.nativeType(PgOid.BOOL);
                }
            }
        } else if (simpleTypename.bit() != null) {
            if (simpleTypename.bit().bitwithlength() != null) {
                length =
                        Integer.parseInt(
                                simpleTypename
                                        .bit()
                                        .bitwithlength()
                                        .expr_list()
                                        .a_expr(0)
                                        .getText());
            }
            columnEditor.nativeType(PgOid.BIT);
        } else if (simpleTypename.character() != null) {
            PostgreSQLParser.IconstContext iconst = simpleTypename.character().iconst();
            PostgreSQLParser.Character_cContext c = simpleTypename.character().character_c();
            if (iconst != null) {
                length = Integer.parseInt(iconst.getText());
            }
            if (c.VARCHAR() != null || c.varying_() != null) {
                if (isArray) {
                    columnEditor.nativeType(PgOid.VARCHAR_ARRAY);
                } else {
                    columnEditor.nativeType(PgOid.VARCHAR);
                }
            } else {
                if (isArray) {
                    columnEditor.nativeType(PgOid.CHAR_ARRAY);
                } else {
                    columnEditor.nativeType(PgOid.CHAR);
                }
            }
        } else if (simpleTypename.constdatetime() != null) {
            int scale = 6;
            PostgreSQLParser.ConstdatetimeContext constdatetime = simpleTypename.constdatetime();
            if (constdatetime.iconst() != null) {
                scale = Integer.parseInt(constdatetime.iconst().getText());
            }
            columnEditor.scale(scale);
            PostgreSQLParser.Timezone_Context timezone = simpleTypename.constdatetime().timezone_();
            boolean withoutTimeZone = timezone == null || timezone.WITHOUT() == null;
            if (constdatetime.TIMESTAMP() != null) {
                if (isArray) {
                    if (withoutTimeZone) {
                        columnEditor.nativeType(PgOid.TIMESTAMP_ARRAY);
                    } else {
                        columnEditor.nativeType(PgOid.TIMESTAMPTZ_ARRAY);
                    }
                } else {
                    if (withoutTimeZone) {
                        columnEditor.nativeType(PgOid.TIMESTAMP);
                    } else {
                        columnEditor.nativeType(PgOid.TIMESTAMPTZ);
                    }
                }
            } else {
                if (isArray) {
                    if (withoutTimeZone) {
                        columnEditor.nativeType(PgOid.TIME_ARRAY);
                    } else {
                        columnEditor.nativeType(PgOid.TIMETZ_ARRAY);
                    }
                } else {
                    if (withoutTimeZone) {
                        columnEditor.nativeType(PgOid.TIME);
                    } else {
                        columnEditor.nativeType(PgOid.TIMETZ);
                    }
                }
            }
        } else if (simpleTypename.constinterval() != null) {
            if (isArray) {
                columnEditor.nativeType(PgOid.INTERVAL_ARRAY);
            } else {
                columnEditor.nativeType(PgOid.INTERVAL);
            }
        } else if (simpleTypename.jsonType() != null) {
            if (isArray) {
                columnEditor.nativeType(PgOid.JSON_ARRAY);
            } else {
                columnEditor.nativeType(PgOid.JSON);
            }
        } else {
            columnEditor.nativeType(PgOid.TEXT);
        }
        columnEditor.length(length);
    }
}
