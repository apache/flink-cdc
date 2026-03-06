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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.TokenStream;

import java.util.List;

/**
 * Taken from
 * https://github.com/antlr/grammars-v4/blob/master/sql/postgresql/Java/PostgreSQLParserBase.java.
 */
public abstract class PostgreSQLParserBase extends Parser {

    public PostgreSQLParserBase(TokenStream input) {
        super(input);
    }

    ParserRuleContext getParsedSqlTree(String script, int line) {
        PostgreSQLParser ph = getPostgreSQLParser(script);
        ParserRuleContext result = ph.root();
        return result;
    }

    public void parseRoutineBody() {
        PostgreSQLParser.Createfunc_opt_listContext localctx =
                (PostgreSQLParser.Createfunc_opt_listContext) this.getContext();
        String lang = null;
        for (PostgreSQLParser.Createfunc_opt_itemContext coi : localctx.createfunc_opt_item()) {
            if (coi.LANGUAGE() != null) {
                if (coi.nonreservedword_or_sconst() != null) {
                    if (coi.nonreservedword_or_sconst().nonreservedword() != null) {
                        if (coi.nonreservedword_or_sconst().nonreservedword().identifier()
                                != null) {
                            if (coi.nonreservedword_or_sconst()
                                            .nonreservedword()
                                            .identifier()
                                            .Identifier()
                                    != null) {
                                lang =
                                        coi.nonreservedword_or_sconst()
                                                .nonreservedword()
                                                .identifier()
                                                .Identifier()
                                                .getText();
                                break;
                            }
                        }
                    }
                }
            }
        }
        if (null == lang) {
            return;
        }
        PostgreSQLParser.Createfunc_opt_itemContext funcAs = null;
        for (PostgreSQLParser.Createfunc_opt_itemContext a : localctx.createfunc_opt_item()) {
            if (a.func_as() != null) {
                funcAs = a;
                break;
            }
        }
        if (funcAs != null) {
            String txt = getRoutineBodyString(funcAs.func_as().sconst(0));
            switch (lang) {
                case "plpgsql":
                    // NB: Cannot be done this way.
                    // PostgreSQLParser ph = getPostgreSQLParser(txt);
                    // func_as.func_as().Definition = ph.plsqlroot();
                    break;
                case "sql":
                    // func_as.func_as().Definition = ph.root();
                    break;
            }
        }
    }

    private String trimQuotes(String s) {
        return (s == null || s.isEmpty()) ? s : s.substring(1, s.length() - 1);
    }

    public String unquote(String s) {
        int slength = s.length();
        StringBuilder r = new StringBuilder(slength);
        int i = 0;
        while (i < slength) {
            Character c = s.charAt(i);
            r.append(c);
            if (c == '\'' && i < slength - 1 && (s.charAt(i + 1) == '\'')) {
                i++;
            }
            i++;
        }
        return r.toString();
    }

    public String getRoutineBodyString(PostgreSQLParser.SconstContext rule) {
        PostgreSQLParser.AnysconstContext anysconst = rule.anysconst();
        org.antlr.v4.runtime.tree.TerminalNode stringConstant = anysconst.StringConstant();
        if (null != stringConstant) {
            return unquote(trimQuotes(stringConstant.getText()));
        }
        org.antlr.v4.runtime.tree.TerminalNode unicodeEscapeStringConstant =
                anysconst.UnicodeEscapeStringConstant();
        if (null != unicodeEscapeStringConstant) {
            return trimQuotes(unicodeEscapeStringConstant.getText());
        }
        org.antlr.v4.runtime.tree.TerminalNode escapeStringConstant =
                anysconst.EscapeStringConstant();
        if (null != escapeStringConstant) {
            return trimQuotes(escapeStringConstant.getText());
        }
        String result = "";
        List<org.antlr.v4.runtime.tree.TerminalNode> dollartext = anysconst.DollarText();
        for (org.antlr.v4.runtime.tree.TerminalNode s : dollartext) {
            result += s.getText();
        }
        return result;
    }

    public PostgreSQLParser getPostgreSQLParser(String script) {
        CharStream charStream = CharStreams.fromString(script);
        Lexer lexer = new PostgreSQLLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PostgreSQLParser parser = new PostgreSQLParser(tokens);
        lexer.removeErrorListeners();
        parser.removeErrorListeners();
        LexerDispatchingErrorListener listenerLexer =
                new LexerDispatchingErrorListener(
                        (Lexer) (((CommonTokenStream) (this.getInputStream())).getTokenSource()));
        ParserDispatchingErrorListener listenerParser = new ParserDispatchingErrorListener(this);
        lexer.addErrorListener(listenerLexer);
        parser.addErrorListener(listenerParser);
        return parser;
    }

    public boolean onlyAcceptableOps() {
        var c = ((CommonTokenStream) this.getInputStream()).LT(1);
        var text = c.getText();
        return text.equals("!") || text.equals("!!") || text.equals("!=-");
    }
}
