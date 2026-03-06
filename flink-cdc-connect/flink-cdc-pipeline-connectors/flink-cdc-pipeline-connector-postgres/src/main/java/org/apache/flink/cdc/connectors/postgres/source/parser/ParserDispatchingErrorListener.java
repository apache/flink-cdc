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

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ProxyErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;

import java.util.BitSet;

/**
 * Taken from
 * https://github.com/antlr/grammars-v4/blob/master/sql/postgresql/Java/ParserDispatchingErrorListener.java.
 */
public class ParserDispatchingErrorListener implements ANTLRErrorListener {
    Parser parent;

    public ParserDispatchingErrorListener(Parser parent) {
        this.parent = parent;
    }

    public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String msg,
            RecognitionException e) {
        var foo = new ProxyErrorListener(parent.getErrorListeners());
        foo.syntaxError(recognizer, offendingSymbol, line, charPositionInLine, msg, e);
    }

    public void reportAmbiguity(
            Parser recognizer,
            DFA dfa,
            int startIndex,
            int stopIndex,
            boolean exact,
            BitSet ambigAlts,
            ATNConfigSet configs) {
        ProxyErrorListener foo = new ProxyErrorListener(parent.getErrorListeners());
        foo.reportAmbiguity(recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs);
    }

    public void reportAttemptingFullContext(
            Parser recognizer,
            DFA dfa,
            int startIndex,
            int stopIndex,
            BitSet conflictingAlts,
            ATNConfigSet configs) {
        ProxyErrorListener foo = new ProxyErrorListener(parent.getErrorListeners());
        foo.reportAttemptingFullContext(
                recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs);
    }

    public void reportContextSensitivity(
            Parser recognizer,
            DFA dfa,
            int startIndex,
            int stopIndex,
            int prediction,
            ATNConfigSet configs) {
        ProxyErrorListener foo = new ProxyErrorListener(parent.getErrorListeners());
        foo.reportContextSensitivity(recognizer, dfa, startIndex, stopIndex, prediction, configs);
    }
}
