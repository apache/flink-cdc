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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.cdc.common.text.ParsingException;
import org.apache.flink.cdc.common.text.TokenStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** A utility class for creating {@link Predicate} instances. */
public class Predicates {

    public static <T> Predicate<T> includes(String regexPatterns, Function<T, String> conversion) {
        Set<Pattern> patterns = setOfRegex(regexPatterns, Pattern.CASE_INSENSITIVE);
        return includedInPatterns(patterns, conversion);
    }

    protected static <T> Predicate<T> includedInPatterns(
            Collection<Pattern> patterns, Function<T, String> conversion) {
        return (t) -> matchedByPattern(patterns, conversion).apply(t).isPresent();
    }

    /**
     * Generate a predicate function that for any supplied string returns {@code true} if <i>any</i>
     * of the regular expressions in the supplied comma-separated list matches the predicate
     * parameter.
     *
     * @param regexPatterns the comma-separated regular expression pattern (or literal) strings; may
     *     not be null
     * @return the predicate function that performs the matching
     * @throws PatternSyntaxException if the string includes an invalid regular expression
     */
    public static Predicate<String> includes(String regexPatterns) {
        return includes(regexPatterns, (str) -> str);
    }

    public static Set<Pattern> setOfRegex(String input, int regexFlags) {
        return setOf(input, RegExSplitterByComma::split, (str) -> Pattern.compile(str, regexFlags));
    }

    public static <T> Set<T> setOf(
            String input, Function<String, String[]> splitter, Function<String, T> factory) {
        if (input == null) {
            return Collections.emptySet();
        }
        Set<T> matches = new LinkedHashSet<>();
        for (String item : splitter.apply(input)) {
            T obj = factory.apply(item);
            if (obj != null) {
                matches.add(obj);
            }
        }
        return matches;
    }

    public static <T> List<T> listOf(
            String input, Function<String, String[]> splitter, Function<String, T> factory) {
        if (input == null) {
            return Collections.emptyList();
        }
        List<T> matches = new LinkedList<>();
        for (String item : splitter.apply(input)) {
            T obj = factory.apply(item);
            if (obj != null) {
                matches.add(obj);
            }
        }
        return matches;
    }

    protected static <T> Function<T, Optional<Pattern>> matchedByPattern(
            Collection<Pattern> patterns, Function<T, String> conversion) {
        return (t) -> {
            String str = conversion.apply(t);
            if (str != null) {
                for (Pattern p : patterns) {
                    if (p.matcher(str).matches()) {
                        return Optional.of(p);
                    }
                }
            }
            return Optional.empty();
        };
    }

    /**
     * A tokenization class used to split a comma-separated list of regular expressions. If a comma
     * is part of expression then it can be prepended with <code>'\'</code> so it will not act as a
     * separator.
     */
    public static class RegExSplitterByComma implements TokenStream.Tokenizer {

        public static String[] split(String identifier) {
            TokenStream stream = new TokenStream(identifier, new RegExSplitterByComma(), true);
            stream.start();

            List<String> parts = new ArrayList<>();

            while (stream.hasNext()) {
                final String part = stream.consume();
                if (part.isEmpty()) {
                    continue;
                }
                parts.add(part.trim().replace("\\,", ","));
            }

            return parts.toArray(new String[0]);
        }

        @Override
        public void tokenize(TokenStream.CharacterStream input, TokenStream.Tokens tokens)
                throws ParsingException {
            int tokenStart = 0;
            while (input.hasNext()) {
                char c = input.next();
                // Escape sequence
                if (c == '\\') {
                    if (!input.hasNext()) {
                        throw new ParsingException(
                                input.position(input.index()),
                                "Unterminated escape sequence at the end of the string");
                    }
                    input.next();
                } else if (c == ',') {
                    tokens.addToken(input.position(tokenStart), tokenStart, input.index());
                    tokenStart = input.index() + 1;
                }
            }
            tokens.addToken(input.position(tokenStart), tokenStart, input.index() + 1);
        }
    }

    /**
     * A tokenization class used to split a dot-separated list of regular expressions. If a comma is
     * part of expression then it can be prepended with <code>'\'</code> so it will not act as a
     * separator.
     */
    public static class RegExSplitterByDot implements TokenStream.Tokenizer {

        public static String[] split(String identifier) {
            TokenStream stream = new TokenStream(identifier, new RegExSplitterByDot(), true);
            stream.start();

            List<String> parts = new ArrayList<>();

            while (stream.hasNext()) {
                final String part = stream.consume();
                if (part.isEmpty()) {
                    continue;
                }
                parts.add(part.trim().replace("\\.", "."));
            }

            return parts.toArray(new String[0]);
        }

        @Override
        public void tokenize(TokenStream.CharacterStream input, TokenStream.Tokens tokens)
                throws ParsingException {
            int tokenStart = 0;
            while (input.hasNext()) {
                char c = input.next();
                // Escape sequence
                if (c == '\\') {
                    if (!input.hasNext()) {
                        throw new ParsingException(
                                input.position(input.index()),
                                "Unterminated escape sequence at the end of the string");
                    }
                    input.next();
                } else if (c == '.') {
                    tokens.addToken(input.position(tokenStart), tokenStart, input.index());
                    tokenStart = input.index() + 1;
                }
            }
            tokens.addToken(input.position(tokenStart), tokenStart, input.index() + 1);
        }
    }
}
