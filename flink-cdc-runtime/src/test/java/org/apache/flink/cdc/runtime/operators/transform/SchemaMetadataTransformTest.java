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
                new SchemaMetadataTransform(null, null, "key1=value1,key2=value2");
        assertThat(transform.getOptions())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
    }

    @Test
    void testTableOptionsWithSemicolonDelimiter() {
        SchemaMetadataTransform transform =
                new SchemaMetadataTransform(null, null, "key1=value1;key2=value2");
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
                                + "file-index.bloom-filter.columns=jjdbh");
        assertThat(transform.getOptions())
                .containsEntry("sequence.field", "gxsj,jjsj")
                .containsEntry("file-index.range-bitmap.columns", "jjsj")
                .containsEntry("file-index.bloom-filter.columns", "jjdbh");
    }

    @Test
    void testTableOptionsSplitByFirstEqualSign() {
        SchemaMetadataTransform transform =
                new SchemaMetadataTransform(null, null, "key1=value=1;key2=value2");
        assertThat(transform.getOptions())
                .containsEntry("key1", "value=1")
                .containsEntry("key2", "value2");
    }
}
