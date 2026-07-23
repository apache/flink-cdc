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
    void testLegacyLikeNullReturnsUnknown() {
        assertThat(StringFunctions.like(null, "A.*")).isNull();
        assertThat(StringFunctions.like("Alice", null)).isNull();
        assertThat(StringFunctions.notLike(null, "A.*")).isNull();
        assertThat(StringFunctions.notLike("Alice", null)).isNull();
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
}
