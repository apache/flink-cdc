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

package org.apache.flink.cdc.connectors.mysql.utils;

import org.junit.jupiter.api.Test;

import static org.apache.flink.cdc.connectors.mysql.utils.MySqlSchemaUtils.removeQuotes;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MySqlSchemaUtils}. */
class MySqlSchemaUtilsTest {

    @Test
    void testRemoveQuotesWithSingleQuotes() {
        assertThat(removeQuotes("'test comment'")).isEqualTo("test comment");
        assertThat(removeQuotes("'user table'")).isEqualTo("user table");
        assertThat(removeQuotes("'0'")).isEqualTo("0");
        assertThat(removeQuotes("''")).isEqualTo("");
    }

    @Test
    void testRemoveQuotesWithDoubleQuotes() {
        assertThat(removeQuotes("\"test comment\"")).isEqualTo("test comment");
        assertThat(removeQuotes("\"user table\"")).isEqualTo("user table");
        assertThat(removeQuotes("\"0\"")).isEqualTo("0");
        assertThat(removeQuotes("\"\"")).isEqualTo("");
    }

    @Test
    void testRemoveQuotesWithoutQuotes() {
        assertThat(removeQuotes("test comment")).isEqualTo("test comment");
        assertThat(removeQuotes("0")).isEqualTo("0");
        assertThat(removeQuotes("user_table")).isEqualTo("user_table");
    }

    @Test
    void testRemoveQuotesWithMismatchedQuotes() {
        // If quotes don't match, return original string
        assertThat(removeQuotes("'test comment\"")).isEqualTo("'test comment\"");
        assertThat(removeQuotes("\"test comment'")).isEqualTo("\"test comment'");
    }

    @Test
    void testRemoveQuotesWithNullOrEmpty() {
        assertThat(removeQuotes(null)).isNull();
        assertThat(removeQuotes("")).isEqualTo("");
        assertThat(removeQuotes("'")).isEqualTo("'");
        assertThat(removeQuotes("\"")).isEqualTo("\"");
    }

    @Test
    void testRemoveQuotesWithEmbeddedQuotes() {
        // These should not be removed as they are embedded, not surrounding
        assertThat(removeQuotes("'test's comment'")).isEqualTo("test's comment");
        assertThat(removeQuotes("\"test \"quoted\" value\"")).isEqualTo("test \"quoted\" value");
    }

    @Test
    void testRemoveQuotesWithSpecialCharacters() {
        assertThat(removeQuotes("'comment with \n newline'")).isEqualTo("comment with \n newline");
        assertThat(removeQuotes("\"comment with \t tab\"")).isEqualTo("comment with \t tab");
        assertThat(removeQuotes("'中文注释'")).isEqualTo("中文注释");
        assertThat(removeQuotes("\"日本語コメント\"")).isEqualTo("日本語コメント");
    }
}
