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

package org.apache.flink.cdc.common.text;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/** A stream of tokens that can be consumed by a parser. This class is not thread-safe. */
@NotThreadSafe
public class TokenStream {

    /**
     * An opaque marker for a position within the token stream.
     *
     * @see TokenStream#mark()
     */
    public static final class Marker implements Comparable<Marker> {
        private final int tokenIndex;
        private final Position position;

        private Marker(Position position, int index) {
            this.position = position;
            this.tokenIndex = index;
        }

        /**
         * Get the position of this marker, or null if this is at the start or end of the token
         * stream.
         *
         * @return the position.
         */
        public Position position() {
            return position;
        }

        @Override
        public int compareTo(Marker that) {
            if (this == that) {
                return 0;
            }
            return this.tokenIndex - that.tokenIndex;
        }

        @Override
        public String toString() {
            return Integer.toString(tokenIndex);
        }
    }

    public static final String ANY_VALUE = "any value";
    public static final int ANY_TYPE = Integer.MIN_VALUE;

    protected final String inputString;
    private final char[] inputContent;
    private final boolean caseSensitive;
    private final Tokenizer tokenizer;
    private List<Token> tokens;
    /**
     * This class navigates the Token objects using this iterator. However, because it very often
     * needs to access the "current token" in the "consume(...)" and "canConsume(...)" and
     * "matches(...)" methods, the class caches a "current token" and makes this iterator point to
     * the 2nd token.
     *
     * <pre>
     *     T1     T2    T3    T4    T5
     *         &circ;   &circ;  &circ;
     *         |   |  |
     *         |   |  +- The position of the tokenIterator, where tokenIterator.hasNext() will return T3
     *         |   +---- The token referenced by currentToken
     *         +-------- The logical position of the TokenStream object, where the &quot;consume()&quot; would return T2
     * </pre>
     */
    private ListIterator<Token> tokenIterator;

    private Token currentToken;
    private boolean completed;

    public TokenStream(String content, Tokenizer tokenizer, boolean caseSensitive) {
        Objects.requireNonNull(content, "content");
        Objects.requireNonNull(tokenizer, "tokenizer");
        this.inputString = content;
        this.inputContent = content.toCharArray();
        this.caseSensitive = caseSensitive;
        this.tokenizer = tokenizer;
    }

    /**
     * Begin the token stream, including (if required) the tokenization of the input content.
     *
     * @return this object for easy method chaining; never null
     * @throws ParsingException if an error occurs during tokenization of the content
     */
    public TokenStream start() throws ParsingException {
        // Create the tokens ...
        if (tokens == null) {
            TokenFactory tokenFactory =
                    caseSensitive
                            ? new CaseSensitiveTokenFactory()
                            : new CaseInsensitiveTokenFactory();
            CharacterStream characterStream = new CharacterArrayStream(inputContent);
            tokenizer.tokenize(characterStream, tokenFactory);
            this.tokens = initializeTokens(tokenFactory.getTokens());
        }

        // Create the iterator ...
        tokenIterator = this.tokens.listIterator();
        moveToNextToken();
        return this;
    }

    /**
     * Method to allow subclasses to pre-process the set of tokens and return the correct tokens to
     * use. The default behavior is to simply return the supplied tokens.
     *
     * @param tokens the tokens
     * @return list of tokens.
     */
    protected List<Token> initializeTokens(List<Token> tokens) {
        return tokens;
    }

    /**
     * Obtain a marker that records the current position so that the stream can be {@link
     * #rewind(Marker)} back to the mark even after having been advanced beyond the mark.
     *
     * @return the marker; never null
     * @throws IllegalStateException if this method was called before the stream was {@link #start()
     *     started}
     * @throws NoSuchElementException if there are no more tokens
     */
    public Marker mark() {
        if (completed) {
            return new Marker(null, tokenIterator.previousIndex());
        }
        Token currentToken = currentToken();
        Position currentPosition = currentToken != null ? currentToken.position() : null;
        return new Marker(currentPosition, tokenIterator.previousIndex());
    }

    /**
     * Reset the stream back to the position described by the supplied marker. This method does
     * nothing if the mark is invalid. For example, it is not possible to advance the token stream
     * beyond the current position.
     *
     * @param marker the marker
     * @return true if the token stream was reset, or false if the marker was invalid
     */
    public boolean rewind(Marker marker) {
        if (marker.tokenIndex >= 0 && marker.tokenIndex <= this.tokenIterator.nextIndex()) {
            completed = false;
            currentToken = null;
            tokenIterator = this.tokens.listIterator(marker.tokenIndex);
            moveToNextToken();
            return true;
        }
        return false;
    }

    /**
     * Return the value of this token and move to the next token.
     *
     * @return the value of the current token
     * @throws ParsingException if there is no such token to consume
     * @throws IllegalStateException if this method was called before the stream was {@link #start()
     *     started}
     */
    public String consume() throws ParsingException, IllegalStateException {
        if (completed) {
            throwNoMoreContent();
        }
        // Get the value from the current token ...
        String result = currentToken().value();
        moveToNextToken();
        return result;
    }

    protected void throwNoMoreContent() throws ParsingException {
        Position pos =
                tokens.isEmpty()
                        ? new Position(-1, 1, 0)
                        : tokens.get(tokens.size() - 1).position();
        throw new ParsingException(pos, "No more content");
    }

    public String peek() throws IllegalStateException {
        if (completed) {
            throwNoMoreContent();
        }
        // Get the value from the current token but do NOT advance ...
        return currentToken().value();
    }

    /**
     * Determine if the current token matches the expected value.
     *
     * <p>The {@link #ANY_VALUE ANY_VALUE} constant can be used as a wildcard.
     *
     * @param expected the expected value of the current token
     * @return true if the current token did match, or false if the current token did not match
     * @throws IllegalStateException if this method was called before the stream was {@link #start()
     *     started}
     */
    public boolean matches(String expected) throws IllegalStateException {
        return matches(ANY_TYPE, expected);
    }

    /**
     * Determine if the current token matches the expected type and a value.
     *
     * <p>The {@link #ANY_VALUE ANY_VALUE} constant can be used as a wildcard.
     *
     * @param type the expected type of the curent token
     * @param expected the expected value of the current token
     * @return true if the current token did match, or false if the current token did not match
     * @throws IllegalStateException if this method was called before the stream was {@link #start()
     *     started}
     */
    public boolean matches(int type, String expected) throws IllegalStateException {
        return !completed
                && (Objects.equals(expected, ANY_VALUE) || currentToken().matches(expected))
                && currentToken().matches(type);
    }

    /**
     * Determine if the next token matches one of the supplied values.
     *
     * @param options the options for the value of the current token
     * @return true if the current token's value did match one of the supplied options, or false
     *     otherwise
     * @throws IllegalStateException if this method was called before the stream was {@link #start()
     *     started}
     */
    public boolean matchesAnyOf(String[] options) throws IllegalStateException {
        if (completed) {
            return false;
        }
        Token current = currentToken();
        for (String option : options) {
            if (current.matches(option)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine if this stream has another token to be consumed.
     *
     * @return true if there is another token ready for consumption, or false otherwise
     * @throws IllegalStateException if this method was called before the stream was {@link #start()
     *     started}
     */
    public boolean hasNext() {
        if (tokenIterator == null) {
            throw new IllegalStateException("start() method must be called before hasNext()");
        }
        return !completed;
    }

    @Override
    public String toString() {
        ListIterator<Token> iter = tokens.listIterator(tokenIterator.previousIndex());
        StringBuilder sb = new StringBuilder();
        if (iter.hasNext()) {
            sb.append(iter.next());
            int count = 1;
            while (iter.hasNext()) {
                if (count > 20) {
                    sb.append(" ...");
                    break;
                }
                sb.append("  ");
                ++count;
                sb.append(iter.next());
            }
        }
        return sb.toString();
    }

    private void moveToNextToken(List<Token> newTokens) {
        if (newTokens != null && !newTokens.isEmpty()) {
            for (Token t : newTokens) {
                tokenIterator.add(t);
            }
            for (int i = 0; i < newTokens.size() - 1; i++) {
                tokenIterator.previous();
            }
            currentToken = newTokens.get(0);
            return;
        }
        // And move the currentToken to the next token ...
        if (!tokenIterator.hasNext()) {
            completed = true;
            currentToken = null;
        } else {
            currentToken = tokenIterator.next();
        }
    }

    private void moveToNextToken() {
        moveToNextToken(null);
    }

    /**
     * Get the current token.
     *
     * @return the current token; never null
     * @throws IllegalStateException if this method was called before the stream was {@link #start()
     *     started}
     * @throws NoSuchElementException if there are no more tokens
     */
    final Token currentToken() throws IllegalStateException, NoSuchElementException {
        if (currentToken == null) {
            if (completed) {
                throw new NoSuchElementException("No more content");
            }
            throw new IllegalStateException(
                    "start() method must be called before consuming or matching");
        }
        return currentToken;
    }

    /**
     * Interface for a Tokenizer component responsible for processing the characters in a {@link
     * CharacterStream} and constructing the appropriate {@link Token} objects.
     */
    public interface Tokenizer {
        /**
         * Process the supplied characters and construct the appropriate {@link Token} objects.
         *
         * @param input the character input stream; never null
         * @param tokens the factory for {@link Token} objects, which records the order in which the
         *     tokens are created
         * @throws ParsingException if there is an error while processing the character stream
         *     (e.g., a quote is not closed, etc.)
         */
        void tokenize(CharacterStream input, Tokens tokens) throws ParsingException;
    }

    /**
     * Interface used by a {@link Tokenizer} to iterate through the characters in the content input
     * to the {@link TokenStream}.
     */
    public interface CharacterStream {

        /**
         * Determine if there is another character available in this stream.
         *
         * @return true if there is another character (and {@link #next()} can be called), or false
         *     otherwise
         */
        boolean hasNext();

        /**
         * Obtain the next character value, and advance the stream.
         *
         * @return the next character
         * @throws NoSuchElementException if there is no {@link #hasNext() next character}
         */
        char next();

        /**
         * Get the index for the last character returned from {@link #next()}.
         *
         * @return the index of the last character returned
         */
        int index();

        /**
         * Get the position for the last character returned from {@link #next()}.
         *
         * @param startIndex the starting index
         * @return the position of the last character returned; never null
         */
        Position position(int startIndex);
    }

    /**
     * A factory for Token objects, used by a {@link Tokenizer} to create tokens in the correct
     * order.
     */
    public interface Tokens {
        /**
         * Create a single-character token at the supplied index in the character stream. The token
         * type is set to 0, meaning this is equivalent to calling <code>addToken(index,index+1)
         * </code> or <code>addToken(index,index+1,0)</code>.
         *
         * @param position the position (line and column numbers) of this new token; may not be null
         * @param index the index of the character to appear in the token; must be a valid index in
         *     the stream
         */
        default void addToken(Position position, int index) {
            addToken(position, index, index + 1, 0);
        }

        /**
         * Create a single- or multi-character token with the characters in the range given by the
         * starting and ending index in the character stream. The character at the ending index is
         * <i>not</i> included in the token (as this is standard practice when using 0-based
         * indexes). The token type is set to 0, meaning this is equivalent to calling <code>
         * addToken(startIndex,endIndex,0)</code> .
         *
         * @param position the position (line and column numbers) of this new token; may not be null
         * @param startIndex the index of the first character to appear in the token; must be a
         *     valid index in the stream
         * @param endIndex the index just past the last character to appear in the token; must be a
         *     valid index in the stream
         */
        default void addToken(Position position, int startIndex, int endIndex) {
            addToken(position, startIndex, endIndex, 0);
        }

        /**
         * Create a single- or multi-character token with the supplied type and with the characters
         * in the range given by the starting and ending index in the character stream. The
         * character at the ending index is <i>not</i> included in the token (as this is standard
         * practice when using 0-based indexes).
         *
         * @param position the position (line and column numbers) of this new token; may not be null
         * @param startIndex the index of the first character to appear in the token; must be a
         *     valid index in the stream
         * @param endIndex the index just past the last character to appear in the token; must be a
         *     valid index in the stream
         * @param type the type of the token
         */
        void addToken(Position position, int startIndex, int endIndex, int type);
    }

    /**
     * The interface defining a token, which references the characters in the actual input character
     * stream.
     *
     * @see CaseSensitiveTokenFactory
     * @see CaseInsensitiveTokenFactory
     */
    @Immutable
    public interface Token {
        /**
         * Get the value of the token, in actual case.
         *
         * @return the value
         */
        String value();

        /**
         * Determine if the token matches the supplied string.
         *
         * @param expected the expected value
         * @return true if the token's value matches the supplied value, or false otherwise
         */
        boolean matches(String expected);

        /**
         * Determine if the token matches the supplied string and is of a requested type.
         *
         * @param expectedType the expected token type
         * @param expected the expected value
         * @return true if the token's type and value matches the supplied type and value, or false
         *     otherwise
         */
        default boolean matches(int expectedType, String expected) {
            return matches(expectedType) && matches(expected);
        }

        /**
         * Determine if the token matches the supplied character.
         *
         * @param expected the expected character value
         * @return true if the token's value matches the supplied character value, or false
         *     otherwise
         */
        boolean matches(char expected);

        /**
         * Determine if the token matches the supplied type.
         *
         * @param expectedType the expected integer type
         * @return true if the token's value matches the supplied integer type, or false otherwise
         */
        boolean matches(int expectedType);

        /**
         * Get the type of the token.
         *
         * @return the token's type
         */
        int type();

        /**
         * Get the index in the raw stream for the first character in the token.
         *
         * @return the starting index of the token
         */
        int startIndex();

        /**
         * Get the index in the raw stream past the last character in the token.
         *
         * @return the ending index of the token, which is past the last character
         */
        int endIndex();

        /**
         * Get the length of the token, which is equivalent to <code>endIndex() - startIndex()
         * </code>.
         *
         * @return the length
         */
        int length();

        /**
         * Get the position of this token, which includes the line number and column number of the
         * first character in the token.
         *
         * @return the position; never null
         */
        Position position();

        /**
         * Bitmask ORed with existing type value.
         *
         * @param typeMask the mask of types
         * @return copy of Token with new type
         */
        Token withType(int typeMask);
    }

    /** An immutable {@link Token} that implements matching using case-sensitive logic. */
    @Immutable
    protected class CaseSensitiveToken implements Token {
        private final int startIndex;
        private final int endIndex;
        private final int type;
        private final Position position;

        public CaseSensitiveToken(int startIndex, int endIndex, int type, Position position) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.type = type;
            this.position = position;
        }

        @Override
        public Token withType(int typeMask) {
            int type = this.type | typeMask;
            return new CaseSensitiveToken(startIndex, endIndex, type, position);
        }

        @Override
        public final int type() {
            return type;
        }

        @Override
        public final int startIndex() {
            return startIndex;
        }

        @Override
        public final int endIndex() {
            return endIndex;
        }

        @Override
        public final int length() {
            return endIndex - startIndex;
        }

        @Override
        public final boolean matches(char expected) {
            return length() == 1 && matchString().charAt(startIndex) == expected;
        }

        @Override
        public boolean matches(String expected) {
            return matchString().substring(startIndex, endIndex).equals(expected);
        }

        @Override
        public final boolean matches(int expectedType) {
            return expectedType == ANY_TYPE
                    || (currentToken().type() & expectedType) == expectedType;
        }

        @Override
        public final String value() {
            return inputString.substring(startIndex, endIndex);
        }

        @Override
        public Position position() {
            return position;
        }

        protected String matchString() {
            return inputString;
        }

        @Override
        public String toString() {
            return value();
        }
    }

    /** An immutable {@link Token} that implements matching using case-insensitive logic. */
    @Immutable
    protected class CaseInsensitiveToken extends CaseSensitiveToken {
        public CaseInsensitiveToken(int startIndex, int endIndex, int type, Position position) {
            super(startIndex, endIndex, type, position);
        }

        @Override
        public boolean matches(String expected) {
            return matchString().substring(startIndex(), endIndex()).toUpperCase().equals(expected);
        }

        @Override
        public Token withType(int typeMask) {
            int type = this.type() | typeMask;
            return new CaseInsensitiveToken(startIndex(), endIndex(), type, position());
        }
    }

    /** An implementation of {@link Tokens} that creates {@link CaseSensitiveToken} objects. */
    protected abstract class TokenFactory implements Tokens {
        protected final List<Token> tokens = new ArrayList<Token>();

        public List<Token> getTokens() {
            return tokens;
        }
    }

    /** An implementation of {@link Tokens} that creates {@link CaseSensitiveToken} objects. */
    public class CaseSensitiveTokenFactory extends TokenFactory {
        @Override
        public void addToken(Position position, int startIndex, int endIndex, int type) {
            tokens.add(new CaseSensitiveToken(startIndex, endIndex, type, position));
        }
    }

    /** An implementation of {@link Tokens} that creates {@link CaseInsensitiveToken} objects. */
    public class CaseInsensitiveTokenFactory extends TokenFactory {
        @Override
        public void addToken(Position position, int startIndex, int endIndex, int type) {
            tokens.add(new CaseInsensitiveToken(startIndex, endIndex, type, position));
        }
    }

    /** An implementation of {@link CharacterStream} that works with a single character array. */
    public static final class CharacterArrayStream implements CharacterStream {
        private final char[] content;
        private int lastIndex = -1;
        private final int maxIndex;
        private int lineNumber = 1;
        private int columnNumber = 0;
        private boolean nextCharMayBeLineFeed;

        public CharacterArrayStream(char[] content) {
            this.content = content;
            this.maxIndex = content.length - 1;
        }

        @Override
        public boolean hasNext() {
            return lastIndex < maxIndex;
        }

        @Override
        public int index() {
            return lastIndex;
        }

        @Override
        public Position position(int startIndex) {
            return new Position(startIndex, lineNumber, columnNumber);
        }

        @Override
        public char next() {
            if (lastIndex >= maxIndex) {
                throw new NoSuchElementException();
            }
            char result = content[++lastIndex];
            ++columnNumber;
            if (result == '\r') {
                nextCharMayBeLineFeed = true;
                ++lineNumber;
                columnNumber = 0;
            } else if (result == '\n') {
                if (!nextCharMayBeLineFeed) {
                    ++lineNumber;
                }
                columnNumber = 0;
            } else if (nextCharMayBeLineFeed) {
                nextCharMayBeLineFeed = false;
            }
            return result;
        }
    }
}
