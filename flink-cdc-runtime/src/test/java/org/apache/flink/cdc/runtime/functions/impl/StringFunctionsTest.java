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
