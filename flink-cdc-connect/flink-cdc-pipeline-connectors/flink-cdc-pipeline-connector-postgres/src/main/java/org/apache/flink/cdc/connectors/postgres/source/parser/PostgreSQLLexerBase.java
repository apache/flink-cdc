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
import org.antlr.v4.runtime.Lexer;

import java.util.Stack;

/**
 * Taken from
 * https://github.com/antlr/grammars-v4/blob/master/sql/postgresql/Java/PostgreSQLLexerBase.java.
 */
public abstract class PostgreSQLLexerBase extends Lexer {
    protected final Stack<String> tags = new Stack<>();

    protected PostgreSQLLexerBase(CharStream input) {
        super(input);
    }

    public void pushTag() {
        tags.push(getText());
    }

    public boolean isTag() {
        return getText().equals(tags.peek());
    }

    public void popTag() {
        tags.pop();
    }

    public void unterminatedBlockCommentDebugAssert() {
        // Debug.Assert(InputStream.LA(1) == -1 /*EOF*/);
    }

    public boolean checkLaMinus() {
        return getInputStream().LA(1) != '-';
    }

    public boolean checkLaStar() {
        return getInputStream().LA(1) != '*';
    }

    public boolean charIsLetter() {
        return Character.isLetter(getInputStream().LA(-1));
    }

    public void handleNumericFail() {
        getInputStream().seek(getInputStream().index() - 2);
        setType(PostgreSQLLexer.Integral);
    }

    public void handleLessLessGreaterGreater() {
        if (getText() == "<<") {
            setType(PostgreSQLLexer.LESS_LESS);
        }
        if (getText() == ">>") {
            setType(PostgreSQLLexer.GREATER_GREATER);
        }
    }

    public boolean checkIfUtf32Letter() {
        int codePoint = getInputStream().LA(-2) << 8 + getInputStream().LA(-1);
        char[] c;
        if (codePoint < 0x10000) {
            c = new char[] {(char) codePoint};
        } else {
            codePoint -= 0x10000;
            c =
                    new char[] {
                        (char) (codePoint / 0x400 + 0xd800), (char) (codePoint % 0x400 + 0xdc00)
                    };
        }
        return Character.isLetter(c[0]);
    }

    public boolean isSemiColon() {
        return ';' == (char) getInputStream().LA(1);
    }
}
