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

package org.apache.flink.cdc.runtime.functions.impl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link StringFunctions}. */
class StringFunctionsTest {

    @Test
    void testStringFunctions() {
        assertThat(StringFunctions.overlay("abcdef", "ZZ", 3)).isEqualTo("abZZef");
        assertThat(StringFunctions.overlay("abcdef", "ZZ", 3, 2)).isEqualTo("abZZef");
        assertThat(StringFunctions.overlay("abcdef", "ZZ", 0, 2)).isEqualTo("abcdef");
        assertThat(StringFunctions.position("cd", "abcdef")).isEqualTo(3);
        assertThat(StringFunctions.position("xy", "abcdef")).isZero();
        assertThat(StringFunctions.position("", "abcdef")).isEqualTo(1);
        assertThat(StringFunctions.position("cd", "abcdef", 4)).isZero();
        assertThat(StringFunctions.locate("cd", "abcdef")).isEqualTo(3);
        assertThat(StringFunctions.locate("", "abcdef", 4)).isEqualTo(1);
        assertThat(StringFunctions.instr("abcabc", "bc")).isEqualTo(2);
        assertThat(StringFunctions.ltrim("  abc  ")).isEqualTo("abc  ");
        assertThat(StringFunctions.rtrim("  abc  ")).isEqualTo("  abc");
        assertThat(StringFunctions.btrim("  abc  ")).isEqualTo("abc");
        assertThat(StringFunctions.btrim("xyabcxy", "xy")).isEqualTo("abc");
        assertThat(StringFunctions.concatWs(",", "a", null, "b")).isEqualTo("a,b");
        assertThat(StringFunctions.concatWs(",", null, null)).isEmpty();
        assertThat(StringFunctions.concatWs("~", "AA", null, "BB", "", "CC"))
                .isEqualTo("AA~BB~~CC");
        assertThat(StringFunctions.lpad("hi", 5, "?")).isEqualTo("???hi");
        assertThat(StringFunctions.rpad("hi", 5, "?")).isEqualTo("hi???");
        assertThat(StringFunctions.lpad("hello", 2, "?")).isEqualTo("he");
        assertThat(StringFunctions.lpad("hi", -1, "?")).isNull();
        assertThat(StringFunctions.lpad("hi", 5, "")).isNull();
        assertThat(StringFunctions.replace("hello", "l", "x")).isEqualTo("hexxo");
        assertThat(StringFunctions.repeat("ab", 3)).isEqualTo("ababab");
        assertThat(StringFunctions.repeat("ab", 0)).isEmpty();
        assertThat(StringFunctions.left("abcdef", 2)).isEqualTo("ab");
        assertThat(StringFunctions.right("abcdef", 2)).isEqualTo("ef");
        assertThat(StringFunctions.left("abcdef", 0)).isEmpty();
        assertThat(StringFunctions.right("abcdef", -1)).isEmpty();
        assertThat(StringFunctions.startswith("abcdef", "abc")).isTrue();
        assertThat(StringFunctions.endswith("abcdef", "def")).isTrue();
        assertThat(StringFunctions.startswith(new byte[] {1, 2, 3}, new byte[] {1, 2})).isTrue();
        assertThat(StringFunctions.startswith(new byte[] {1, 2, 3}, new byte[] {2, 3})).isFalse();
        assertThat(StringFunctions.endswith(new byte[] {1, 2, 3}, new byte[] {2, 3})).isTrue();
        assertThat(StringFunctions.endswith(new byte[] {1, 2, 3}, new byte[] {1, 2})).isFalse();
        assertThat(StringFunctions.toBase64("hello")).isEqualTo("aGVsbG8=");
        assertThat(StringFunctions.fromBase64("aGVsbG8=")).isEqualTo("hello");
    }

    @Test
    void testStringFunctionsReturnNullOnNullInput() {
        assertThat(StringFunctions.concatWs(null, "a")).isNull();
        assertThat(StringFunctions.overlay(null, "x", 1)).isNull();
        assertThat(StringFunctions.position(null, "abc")).isNull();
        assertThat(StringFunctions.locate("a", null)).isNull();
        assertThat(StringFunctions.instr("abc", null)).isNull();
        assertThat(StringFunctions.ltrim(null)).isNull();
        assertThat(StringFunctions.rtrim(null)).isNull();
        assertThat(StringFunctions.btrim(null)).isNull();
        assertThat(StringFunctions.lpad(null, 1, "?")).isNull();
        assertThat(StringFunctions.rpad("a", null, "?")).isNull();
        assertThat(StringFunctions.replace("a", null, "b")).isNull();
        assertThat(StringFunctions.repeat(null, 1)).isNull();
        assertThat(StringFunctions.left(null, 1)).isNull();
        assertThat(StringFunctions.right("a", null)).isNull();
        assertThat(StringFunctions.startswith(null, "a")).isNull();
        assertThat(StringFunctions.endswith("a", null)).isNull();
        assertThat(StringFunctions.startswith((byte[]) null, new byte[] {1})).isNull();
        assertThat(StringFunctions.endswith(new byte[] {1}, (byte[]) null)).isNull();
        assertThat(StringFunctions.toBase64(null)).isNull();
        assertThat(StringFunctions.fromBase64(null)).isNull();
    }

    @Test
    void testStringFunctionsCountUnicodeCodePoints() {
        String emoji = "\uD83D\uDE00";
        String sameHighSurrogate = "\uD83D\uDE01";
        String sameLowSurrogate = "\uD87D\uDE00";

        assertThat(StringFunctions.position("x", emoji + "x")).isEqualTo(2);
        assertThat(StringFunctions.locate("x", emoji + "x")).isEqualTo(2);
        assertThat(StringFunctions.instr(emoji + "x", "x")).isEqualTo(2);
        assertThat(StringFunctions.left(emoji + "x", 1)).isEqualTo(emoji);
        assertThat(StringFunctions.right("x" + emoji, 1)).isEqualTo(emoji);
        assertThat(StringFunctions.ltrim(sameHighSurrogate + "x", emoji))
                .isEqualTo(sameHighSurrogate + "x");
        assertThat(StringFunctions.rtrim("x" + sameLowSurrogate, emoji))
                .isEqualTo("x" + sameLowSurrogate);
        assertThat(StringFunctions.btrim(emoji + "x" + emoji, emoji)).isEqualTo("x");
    }

    @Test
    void testLegacyLikeUsesJavaRegex() {
        assertThat(StringFunctions.like("Alice", "A.*")).isTrue();
        assertThat(StringFunctions.like("xabcy", "abc")).isTrue();
        assertThat(StringFunctions.like("Alice", "A%")).isFalse();
        assertThat(StringFunctions.notLike("Alice", "A.*")).isFalse();
    }

    @Test
    void testLikeEscape() {
        assertThat(StringFunctions.like("A%", "A$%", "$")).isTrue();
        assertThat(StringFunctions.like("A_", "A$_", "$")).isTrue();
        assertThat(StringFunctions.like("Alice", "A$%", "$")).isFalse();
    }

    @Test
    void testLikeEscapeNullReturnsUnknown() {
        assertThat(StringFunctions.like("Alice", "A%", null)).isNull();
    }

    @Test
    void testRegexpReplaceKeepsLiteralReplacement() {
        assertThat(StringFunctions.regexpReplace("a", "a", "$1")).isEqualTo("$1");
        assertThat(StringFunctions.regexpReplace("a", "a", "\\")).isEqualTo("\\");
    }

    @Test
    void testSimilarTo() {
        assertThat(StringFunctions.similarTo("Alice", "(A|B)%")).isTrue();
        assertThat(StringFunctions.similarTo("Carol", "(A|B)%")).isFalse();
        assertThat(StringFunctions.notSimilarTo("Alice", "(A|B)%")).isFalse();
    }

    @Test
    void testSimilarToNullReturnsUnknown() {
        assertThat(StringFunctions.similarTo(null, "(A|B)%")).isNull();
        assertThat(StringFunctions.similarTo("Alice", null)).isNull();
        assertThat(StringFunctions.notSimilarTo(null, "(A|B)%")).isNull();
    }

    @Test
    void testRegexpFunctionsNullArguments() {
        assertThat(StringFunctions.regexpExtract(null, "a")).isNull();
        assertThat(StringFunctions.regexpExtract("a", null)).isNull();
        assertThat(StringFunctions.regexpExtractAll(null, "a")).isNull();
        assertThat(StringFunctions.regexpExtractAll("a", null)).isNull();
        assertThat(StringFunctions.regexpExtractAll("a", "a", null)).isNull();
        assertThat(StringFunctions.regexpCount(null, "a")).isNull();
        assertThat(StringFunctions.regexpCount("a", null)).isNull();
        assertThat(StringFunctions.regexpInstr(null, "a")).isNull();
        assertThat(StringFunctions.regexpInstr("a", null)).isNull();
        assertThat(StringFunctions.regexpSubstr(null, "a")).isNull();
        assertThat(StringFunctions.regexpSubstr("a", null)).isNull();
    }

    @Test
    void testRegexpFunctions() {
        assertThat(StringFunctions.regexpExtract("foothebar", "foo(.*?)(bar)", 2)).isEqualTo("bar");
        assertThat(StringFunctions.regexpExtract("foothebar", "foo(.*?)(bar)"))
                .isEqualTo("foothebar");
        assertThat(StringFunctions.regexpExtract("foothebar", "foo(.*?)(bar)", 3)).isNull();
        assertThat(StringFunctions.regexpExtract("foobar", "(foo)|(bar)", 2)).isNull();
        assertThat(StringFunctions.regexpExtract("abcd", "z", 0)).isNull();
        assertThat(StringFunctions.regexpExtract("abcd", "(", 0)).isNull();
        assertThat(StringFunctions.regexpExtract("abcd", "a", -1)).isNull();

        assertThat(StringFunctions.regexpExtractAll("100-200, 300-400", "(\\d+)-(\\d+)"))
                .containsExactly("100", "300");
        assertThat(StringFunctions.regexpExtractAll("100-200, 300-400", "(\\d+)-(\\d+)", 0))
                .containsExactly("100-200", "300-400");
        assertThat(StringFunctions.regexpExtractAll("100-200, 300-400", "(\\d+)-(\\d+)", 2))
                .containsExactly("200", "400");
        assertThat(StringFunctions.regexpExtractAll("abcdeabde", "(abcdeabde)|([a-z]*)", 2))
                .containsExactly(null, "");
        assertThat(StringFunctions.regexpExtractAll("100-200", "[a-z]", 0)).isEmpty();
        assertThat(StringFunctions.regexpExtractAll("abcdeabde", "abcdeabde")).isNull();
        assertThat(StringFunctions.regexpExtractAll("abcdeabde", "(abcdeabde)", 2)).isNull();
        assertThat(StringFunctions.regexpExtractAll("abcdeabde", "(", 0)).isNull();
        assertThat(StringFunctions.regexpExtractAll("abcdeabde", "(abcdeabde)", -1)).isNull();

        assertThat(StringFunctions.regexpCount("abc123xyz456", "\\d")).isEqualTo(6);
        assertThat(StringFunctions.regexpCount("abcd", "z")).isZero();
        assertThat(StringFunctions.regexpCount("abcd", "(")).isNull();

        assertThat(StringFunctions.regexpInstr("hello world! Hello everyone!", "Hello"))
                .isEqualTo(14);
        assertThat(StringFunctions.regexpInstr("abcd", "z")).isZero();
        assertThat(StringFunctions.regexpInstr("abcd", "(")).isNull();

        assertThat(StringFunctions.regexpSubstr("100-200, 300-400", "(\\d+)-(\\d+)"))
                .isEqualTo("100-200");
        assertThat(StringFunctions.regexpSubstr("abcd", "z")).isNull();
        assertThat(StringFunctions.regexpSubstr("abcd", "(")).isNull();
    }
}
