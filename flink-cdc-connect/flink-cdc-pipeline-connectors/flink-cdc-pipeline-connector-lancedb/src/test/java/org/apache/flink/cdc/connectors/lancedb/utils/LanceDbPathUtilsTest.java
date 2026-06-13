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

package org.apache.flink.cdc.connectors.lancedb.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.lancedb.sink.LanceDbTestUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link LanceDbPathUtils}. */
class LanceDbPathUtilsTest {

    @Test
    void testDeriveNormalizedDatasetPath() {
        String path =
                LanceDbPathUtils.resolveDatasetPath(
                        TableId.parse("inventory.product-events"),
                        LanceDbTestUtils.defaultConfig());

        Assertions.assertThat(path).isEqualTo("/tmp/lancedb/inventory_product_events.lance");
    }

    @Test
    void testTrimTrailingSlash() {
        Assertions.assertThat(LanceDbPathUtils.trimTrailingSlash("/tmp/lancedb///"))
                .isEqualTo("/tmp/lancedb");
    }
}
