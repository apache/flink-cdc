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

package org.apache.flink.cdc.runtime.operators.transform;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Testcases for {@link SchemaMetadataTransform}. */
class SchemaMetadataTransformTest {

    @Test
    void testTableOptionsWithCommaDelimiter() {
        SchemaMetadataTransform transform =
                new SchemaMetadataTransform(null, null, "key1=value1,key2=value2", null);
        assertThat(transform.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
    }

    @Test
    void testTableOptionsWithSemicolonDelimiter() {
        SchemaMetadataTransform transform =
                new SchemaMetadataTransform(null, null, "key1=value1;key2=value2", ";");
        assertThat(transform.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
    }

    @Test
    void testTableOptionsWithCommaInValue() {
        SchemaMetadataTransform transform =
                new SchemaMetadataTransform(
                        null,
                        null,
                        "sequence.field=gxsj,jjsj;"
                                + "file-index.range-bitmap.columns=jjsj;"
                                + "file-index.bloom-filter.columns=jjdbh",
                        ";");
        assertThat(transform.getOptions())
                .containsEntry("sequence.field", "gxsj,jjsj")
                .containsEntry("file-index.range-bitmap.columns", "jjsj")
                .containsEntry("file-index.bloom-filter.columns", "jjdbh");
    }

    @Test
    void testTableOptionsSplitByFirstEqualSign() {
        SchemaMetadataTransform transform =
                new SchemaMetadataTransform(null, null, "key1=value=1;key2=value2", ";");
        assertThat(transform.getOptions())
                .containsEntry("key1", "value=1")
                .containsEntry("key2", "value2");
    }

    @Test
    void testTableOptionsWithCustomDelimiter() {
        SchemaMetadataTransform transform =
                new SchemaMetadataTransform(null, null, "key1=value1|key2=value2", "|");
        assertThat(transform.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
    }

    @Test
    void testTableOptionsWithCustomDelimiterAndCommaInValue() {
        SchemaMetadataTransform transform =
                new SchemaMetadataTransform(
                        null,
                        null,
                        "sequence.field=gxsj,jjsj$file-index.range-bitmap.columns=jjsj",
                        "$");
        assertThat(transform.getOptions())
                .containsEntry("sequence.field", "gxsj,jjsj")
                .containsEntry("file-index.range-bitmap.columns", "jjsj");
    }

    @Test
    void testTableOptionsWithRegexSpecialCharacterDelimiter() {
        // Test with regex special characters like '.', '*', '+', '?', '[', ']', '(', ')', etc.
        // These characters have special meaning in regex and should be treated as literal
        // delimiters.

        // Test with '.' (dot) as delimiter
        SchemaMetadataTransform transformDot =
                new SchemaMetadataTransform(null, null, "key1=value1.key2=value2", ".");
        assertThat(transformDot.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");

        // Test with '*' (asterisk) as delimiter
        SchemaMetadataTransform transformAsterisk =
                new SchemaMetadataTransform(null, null, "key1=value1*key2=value2", "*");
        assertThat(transformAsterisk.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");

        // Test with '+' (plus) as delimiter
        SchemaMetadataTransform transformPlus =
                new SchemaMetadataTransform(null, null, "key1=value1+key2=value2", "+");
        assertThat(transformPlus.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");

        // Test with '?' (question mark) as delimiter
        SchemaMetadataTransform transformQuestion =
                new SchemaMetadataTransform(null, null, "key1=value1?key2=value2", "?");
        assertThat(transformQuestion.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");

        // Test with '[' (bracket) as delimiter
        SchemaMetadataTransform transformBracket =
                new SchemaMetadataTransform(null, null, "key1=value1[key2=value2", "[");
        assertThat(transformBracket.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");

        // Test with '\\' (backslash) as delimiter
        SchemaMetadataTransform transformBackslash =
                new SchemaMetadataTransform(null, null, "key1=value1\\key2=value2", "\\");
        assertThat(transformBackslash.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
    }

    @Test
    void testTableOptionsWithSpecialCharacterDelimiter() {
        // Test with newline as delimiter
        SchemaMetadataTransform transformNewline =
                new SchemaMetadataTransform(null, null, "key1=value1\nkey2=value2", "\n");
        assertThat(transformNewline.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");

        // Test with tab as delimiter
        SchemaMetadataTransform transformTab =
                new SchemaMetadataTransform(null, null, "key1=value1\tkey2=value2", "\t");
        assertThat(transformTab.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");

        // Test with carriage return as delimiter
        SchemaMetadataTransform transformCR =
                new SchemaMetadataTransform(null, null, "key1=value1\rkey2=value2", "\r");
        assertThat(transformCR.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");

        // Test with CRLF (Windows line ending) as delimiter
        SchemaMetadataTransform transformCRLF =
                new SchemaMetadataTransform(null, null, "key1=value1\r\nkey2=value2", "\r\n");
        assertThat(transformCRLF.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
    }
}
